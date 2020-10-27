import datetime
from pathlib import Path
from exceptions import AccessDeniedException, NotFoundException, RootNecessaryException, NotDirectoryException, NotParentException, AlreadyExistsException, NotEmptyException, AccessDeniedAtLeastOneException, InvalidModException, AlreadyExistsDirectoryException, UserNotFoundException, GroupNotFoundException, RootDirectoryException, ItselfSubdirException
from collections_handler import get_fs, get_users, get_groups
from utils import create_file_node, create_directory_node, decode_mode, is_allowed, check_permissions, navigate_through, parse_mode, get_chunk_size, choose_replicas
from math import ceil
from itertools import chain
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

#clean collection --> db.fs.remove({})
# start mongo --> sudo systemctl start mongod

def is_allowed_recursive(path, fs, required_by, grp, operation_type):
    """Check if a user who has required an operation on a resource is authorized to do
    it; function used only for recursive operations requests to check
    if the user has the permissions needed on each sub-resource (ex. rmr, du, countr).
       
    Parameters
    ----------
    path --> pathlib.PosixPath class, path to the resource for which the function verifies if the operation requred is allowed
    fs --> pymongo.collection.Collection class, MongoDB collection
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    operation_type --> str, operation required by the user on the resource
       
    Returns
    -------
    True, False --> boolean, if the operation on the resource is allowed or not
    """
    #the directory from which to start is the root 
    curr_dir = fs.find_one({'name': '/', 'parent': None, 'type': 'd'})
    #check the permissions with ancestor role
    if not check_permissions(curr_dir, 'ancestor', required_by, grp, operation_type):
        return False
    #navigate from the second part to the penultimate one of the path
    #e.g. /user/here/the/path/file/ --> [1:-1] = [user, here, the, path]
    for directory in path.parts[1:-1]:
        curr_dir = fs.find_one({'name': directory, 'parent': curr_dir['_id'], 'type': 'd'})
        #check the permissions with ancestor role
        if not check_permissions(curr_dir, 'ancestor', required_by, grp, operation_type):
            return False
    if path.name in curr_dir['directories']: #the last part is a directory
        parent_dir = curr_dir
        #check the permissions with parent role
        curr_dir = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': path.name})
        if not check_permissions(curr_dir, 'resource', required_by, grp, operation_type):
            return False
    elif path.name == '': #the root directory
        if not check_permissions(curr_dir, 'resource', required_by, grp, operation_type):
            return False
    else: #the last part is a file
        resource = fs.find_one({'name': path.name, 'parent': curr_dir['_id'], 'type': 'f'})
        #check the permissions with resource role
        if not check_permissions(resource, 'resource', required_by, grp, operation_type):
            return False
    return True


def mkdir(client, path, required_by, grp, parent=False):
    """Allow to create a directory (make directory), also with parent mode (if the parent and the
    ancestors do not exist, the parent option allows to create them).
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    path --> pathlib.PosixPath class, path to the folder you want to create
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    parent --> boolean, if true and the parent directories don't exist, they are created, else not
    
    Returns
    -------
    (directory_id, inserted_documents, updatedone_documents) --> tuple(bson.objectid.ObjectId class, list, list), the MongoDB object id just created for representing the folder, the list of the documents to insert and the collections in which they must be inserted, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    """
    #for master namenode
    inserted_documents = []
    updatedone_documents = []
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #the directory from which to start is the root 
    curr_dir = fs.find_one({'name': '/', 'parent': None, 'type': 'd'})
    #check the permissions with ancestor role
    if not check_permissions(curr_dir, 'ancestor', required_by, grp, 'mkdir'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #navigate from the second part to the penultimate one of the path
    #e.g. /user/here/the/path/file/ --> [1:-1] = [user, here, the, path]
    for directory in path.parts[1:-1]:
        if directory in curr_dir['directories']:
            curr_dir = fs.find_one({'name': directory, 'parent': curr_dir['_id'], 'type': 'd'})
            if not check_permissions(curr_dir, 'ancestor', required_by, grp, 'mkdir'):
                logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
                raise AccessDeniedException(curr_dir['name'])
        #there is a file with the same name of a directory 
        elif directory in curr_dir['files']:
            logging.warning('Cannot create the directory: "{}" is not a directory'.format(directory))
            raise NotDirectoryException(directory)
        else:
            #a directory in the path does not exist and the flag for creating the parents is False
            if not parent:
                logging.warning('Parent directory "{}" does not exists'.format(directory))
                raise NotParentException(directory)
            #permissions denied, not enough privileges
            if not check_permissions(curr_dir, 'parent', required_by, grp, 'mkdir'):
                logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
                raise AccessDeniedException(curr_dir['name'])
            #create the missing directory, part of the path 
            fs.update_one({ '_id': curr_dir['_id'] }, {'$push': { 'directories': directory}})
            #insert into the list needed for aligning the other namenodes
            updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$push': { 'directories': directory}}, 'fs'))
            #create the directory object and insert it into MongoDB
            new_directory = create_directory_node(directory, curr_dir['_id'], required_by, required_by)
            directory_id = fs.insert_one(new_directory).inserted_id
            curr_dir = fs.find_one({'_id': directory_id})
            #insert into the list needed for aligning the other namenodes
            inserted_documents.append((curr_dir, 'fs'))
    #the directory already exists
    if path.name in curr_dir['directories']:
        logging.warning('The resource already exists')
        raise AlreadyExistsException()
    #a file with the same name of the directory already exists
    elif path.name in curr_dir['files']:
        logging.warning('Cannot create the directory: "{}" is not a directory'.format(path.name))
        raise NotDirectoryException(path.name)
    #the path is the root directory
    elif path.name == '':
        logging.warning('The resource already exists')
        raise AlreadyExistsException()
    else:
        #check the permissions
        if not check_permissions(curr_dir, 'parent', required_by, grp, 'mkdir'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #create the directory node and update the fs collection
        fs.update_one({ '_id': curr_dir['_id'] }, {'$push': { 'directories': path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$push': { 'directories': path.name}}, 'fs'))
        new_directory = create_directory_node(path.name, curr_dir['_id'], required_by, required_by)
        directory_id = fs.insert_one(new_directory).inserted_id
        curr_dir = fs.find_one({'_id': directory_id})
        #insert into the list needed for aligning the other namenodes
        inserted_documents.append((curr_dir, 'fs'))
        logging.info('Directory "{}" created'.format(path))
        return (directory_id, inserted_documents, updatedone_documents)
    
    
def touch(client, path, required_by, grp):
    """Allow to touch a file (if it does not exist create an empty file, if it exists update 
    access time), or to touch a directory (actually, no change).
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    path --> pathlib.PosixPath class, path to the file you want to touch
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    (file_id, inserted_documents, updatedone_documents) --> tuple(bson.objectid.ObjectId class, list, list), the MongoDB object id just touched for representing the file, the list of the documents to insert and the collections in which they must be inserted, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    """
    #for master namenode
    inserted_documents = []
    updatedone_documents = []
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(path, fs, required_by, grp, 'touch')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permission with parent role
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'touch'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the file already exists, just touch it
    if path.name in curr_dir['files']:
        obj = fs.find_one({'parent': curr_dir['_id'], 'type': 'f', 'name': path.name})
        if not check_permissions(obj, 'resource', required_by, grp, 'touch'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(obj['name']))
            raise AccessDeniedException(obj['name'])
        fs.update_one({ 'parent': curr_dir['_id'], 'type': 'f', 'name': path.name }, {'$set': { 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ 'parent': curr_dir['_id'], 'type': 'f', 'name': path.name }, {'$set': { 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}, 'fs'))
        logging.info('File "{}" touched'.format(path.name))
        return (None, inserted_documents, updatedone_documents)
    #the directory already exists, just touch it (do nothing)
    elif path.name in curr_dir['directories']:
        obj = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': path.name})
        if not check_permissions(obj, 'resource', required_by, grp, 'touch'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(obj['name']))
            raise AccessDeniedException(obj['name'])
        logging.info('Directory "{}" touched'.format(path.name))
        return (None, inserted_documents, updatedone_documents)
    #the path is the root directory
    elif path.name == '':
        if not check_permissions(curr_dir, 'resource', required_by, grp, 'touch'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        logging.info('Directory "{}" touched'.format(path.name))
        return (None, inserted_documents, updatedone_documents)
    #the file does not exist, create it as empty file
    else:
        #create the file node and update fs collection
        fs.update_one({ '_id': curr_dir['_id'] }, {'$push': { 'files': path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$push': { 'files': path.name}}, 'fs'))
        new_file = create_file_node(path.name, curr_dir['_id'], required_by, required_by)
        file_id = fs.insert_one(new_file).inserted_id
        curr_file = fs.find_one({'_id': file_id})
        #insert into the list needed for aligning the other namenodes
        inserted_documents.append((curr_file, 'fs'))
        logging.info('File "{}" touched'.format(path.name))
        return (file_id, inserted_documents, updatedone_documents)
    

def ls(client, path, required_by, grp):
    """Allow to list content of directory or to get info regarding a file.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    path --> pathlib.PosixPath class, path to the resource you want to list (folder or file)
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    res, [res] --> list, the list of the MongoDb objects representing the resources contained into the folder you want to list 
    """
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(path, fs, required_by, grp, 'ls')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #the last part of the path is a directory
    if path.name in curr_dir['directories']:
        curr_dir = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': path.name})
        #check permissions with parent role --> the last part of the path is the parent of the elements inside it
        if not check_permissions(curr_dir, 'parent', required_by, grp, 'ls'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #return the file and subdirectories contained into the current directory
        res = list(fs.find({'parent': curr_dir['_id']}))
        logging.info('Get ls result for {}'.format(path))
        return (res)
    #the last part of the path is a file
    elif path.name in curr_dir['files']:
        #check the permissions with parent role --> the penultimate part of the path is the parent directory of the resource
        if not check_permissions(curr_dir, 'parent', required_by, grp, 'ls'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #return the info for the file
        res = fs.find_one({'parent': curr_dir['_id'], 'type': 'f', 'name': path.name})
        logging.info('Get ls result for {}'.format(path))
        return [res]
    #the path is the root directory
    elif path.name == '':
        #check permissions with parent role --> the last part of the path is the parent of the elements inside it
        if not check_permissions(curr_dir, 'parent', required_by, grp, 'ls'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #return the file and subdirectories contained into the current directory
        res = list(fs.find({'parent': curr_dir['_id']}))
        logging.info('Get ls result for {}'.format(path))
        return (res)
    #the path does not exist
    else:
        logging.warning('The path does not exist: "{}" not found'.format(path.name))
        raise NotFoundException(path.name)


def rm(client, path, required_by, grp):
    """Allow to remove a file or a directory, only if the directory is empty.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    path --> pathlib.PosixPath class, path to the resource you want to remove (folder or file)
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    ([str(resource['_id'])], h, updatedone_documents, deletedone_documents) --> tuple(list, list, list, list), the list containing the object id you want to remove, the list of the datanodes which handle a replica of some chunk of the resource, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update, the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    """
    #for master namenode
    updatedone_documents = []
    deletedone_documents = []
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #the permissions needed for rm are equals for ancestors and parents are equals both for files and for directories
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(path, fs, required_by, grp, 'rm')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role 
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'rm'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the last part of the path is a directory
    if path.name in curr_dir['directories']:
        parent_dir = curr_dir
        curr_dir = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': path.name})
        #check the permissions with resource role
        if not check_permissions(curr_dir, 'resource', required_by, grp, 'rm_directory'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #the resource is a directory and to be deleted it must be empty
        if len(curr_dir['directories']) > 0 or len(curr_dir['files']) > 0:
            logging.warning('The directory is not empty')
            raise NotEmptyException()
        #update the fs collection
        fs.update_one({ '_id': parent_dir['_id'] }, {'$pull': { 'directories': path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': parent_dir['_id'] }, {'$pull': { 'directories': path.name}}, 'fs'))
        deleted = fs.delete_one({'_id': curr_dir['_id']})
        #insert into the list needed for aligning the other namenodes
        deletedone_documents.append(({'_id': curr_dir['_id']}, 'fs'))
        logging.info('Removed {}'.format(path))
        return (None,None, updatedone_documents, deletedone_documents)
    #the last part of the path is a file
    elif path.name in curr_dir['files']:
        resource = fs.find_one({'name': path.name, 'parent': curr_dir['_id'], 'type': 'f'})
        #check the permissions with resource role
        if not check_permissions(resource, 'resource', required_by, grp, 'rm_file'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(resource['name']))
            raise AccessDeniedException(resource['name'])
        #register where are the primary and secondary chunks to delete from the datanodes
        chunks = resource['chunks']
        for c in chunks.keys():
            tmp_c = chunks[c]
            del chunks[c]
            chunks[c.replace('[dot]', '.').replace('[colon]', ':')] = tmp_c    
        replicas = resource['replicas']
        for r in replicas.keys():
            tmp_dn = []
            for dn in replicas[r]:
                tmp_dn.append(dn.replace('[dot]', '.').replace('[colon]', ':'))
            replicas[r] = tmp_dn
        #update the fs collection
        fs.update_one({ '_id': curr_dir['_id'] }, {'$pull': { 'files': path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$pull': { 'files': path.name}}, 'fs'))
        deleted = fs.delete_one({'parent': curr_dir['_id'], 'type': 'f', 'name': path.name})
        #insert into the list needed for aligning the other namenodes
        deletedone_documents.append(({'parent': curr_dir['_id'], 'type': 'f', 'name': path.name}, 'fs'))
        hs = list(set(chain(*list(replicas.values()))))
        hm = list(chunks.keys())
        h = list(set(hm + hs))
        logging.info('Removed {}'.format(path))
        #return the prefix of the file and the datanodes which handle some replica of the file chunks, in order to delete them
        return ([str(resource['_id'])], h, updatedone_documents, deletedone_documents)
    #the path is the root directory and can not be deleted neither by the root
    elif path.name == '':
        logging.warning('Root Directory: the operation you required is not allowed')
        raise RootDirectoryException()
    else:
        logging.warning('The path does not exist: "{}" not found'.format(path.name))
        raise NotFoundException(path.name)


def rm_recursive(client, curr_dir, curr_path, obj_lst, file_lst, dir_lst):
    """Function which gets all subresources to remove.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    curr_dir --> dict, the node for the current directory, for getting all the nested resourced
    curr_path --> str, the current path
    obj_lst --> list, list containing all the resources objects which should be deleted
    file_lst --> list, list containing all the files objects which should be deleted
    dir_lst --> list, list containing all the directories objects which should be deleted
    
    Returns
    -------
    (obj_lst, file_lst, dir_lst) --> tuple(list, list, list), list containing all the resources objects which should be deleted, list containing all the files objects which should be deleted, list containing all the directories objects which should be deleted
    """
    fs = get_fs(client)
    obj_lst.append(curr_dir)
    dir_lst.append(curr_path)
    files = list(fs.find({'parent': curr_dir['_id'], 'type': 'f'}))
    #update the list of files to remove
    for f in files:
        file_lst.append(curr_path + '/' + f['name'])
    obj_lst += files
    #there isn't any subdirectory in the current directory: base case
    if len(curr_dir['directories'])==0:
        return (obj_lst, file_lst, dir_lst)
    #there is at least one subdirectory in current directory: recursive case
    else:
        directories = list(fs.find({'parent': curr_dir['_id'], 'type': 'd'}))
        #update the list of directories to remove
        for dire in directories:
            (obj_lst, file_lst, dir_lst) = rm_recursive(client, dire, curr_path + '/' + dire['name'], obj_lst, file_lst, dir_lst)
        return (obj_lst, file_lst, dir_lst)


def rmr(client, path, required_by, grp):
    """Allow to remove a file or a directory recursively.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    path --> pathlib.PosixPath class, path to the resource you want to remove recursively (folder or file)
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    (deleted_tot, h_tot, updatedone_documents, deletedone_documents) --> tuple(list, list, list, list), the list containing the objects ids you want to remove and the list of the datanodes which handle a replica of some chunk of the resources, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update, the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    """
    #for master namenode
    updatedone_documents = []
    deletedone_documents = []
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(path, fs, required_by, grp, 'rm')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role 
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'rm'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the last part of the path is a directory, so it's required to delete the directory itself and all the nested elements (subdirectories and files)
    if path.name in curr_dir['directories']:
        parent_dir = curr_dir
        curr_dir = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': path.name})
        #get all the subdirectories and files nested into the directory to remove, recursively
        (to_remove, file_lst, dir_lst) = rm_recursive(client, curr_dir, str(path), [], [], [])
        #check if, for each element, the user has the right permissions to remove it
        #if there is at least one element for which the user has not the right permissions, the operation fails 
        for elem in dir_lst:
            if not is_allowed_recursive(Path(elem), fs, required_by, grp, 'rm_directory'):
                logging.warning('Access denied at least on one resource: "{}"'.format(elem))
                raise AccessDeniedAtLeastOneException(elem)
        for elem in file_lst:
            if not is_allowed_recursive(Path(elem), fs, required_by, grp, 'rm_file'):
                logging.warning('Access denied at least on one resource: "{}"'.format(elem))
                raise AccessDeniedAtLeastOneException(elem)
        h_tot = []
        deleted_tot = []
        for elem in to_remove:
            #if the element is a file, register also the chunks to delete from the datanodes which handle either a primary or a secondary replica
            if elem['type'] == 'f':
                chunks = elem['chunks']
                for c in chunks.keys():
                    tmp_c = chunks[c]
                    del chunks[c]
                    chunks[c.replace('[dot]', '.').replace('[colon]', ':')] = tmp_c
                replicas = elem['replicas']
                for r in replicas.keys():
                    tmp_dn = []
                    for dn in replicas[r]:
                        tmp_dn.append(dn.replace('[dot]', '.').replace('[colon]', ':'))
                    replicas[r] = tmp_dn
                hs = list(set(chain(*list(replicas.values()))))
                hm = list(chunks.keys())
                h = list(set(hm + hs))
                h_tot = list(set(h+h_tot))
            #update the fs collection
            deleted = fs.delete_one({'_id': elem['_id']})
            #insert into the list needed for aligning the other namenodes
            deletedone_documents.append(({'_id': elem['_id']}, 'fs'))
            if elem['type'] == 'f':
                deleted_tot.append(str(elem['_id']))
        #update the fs collection
        fs.update_one({ '_id': parent_dir['_id'] }, {'$pull': { 'directories': path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': parent_dir['_id'] }, {'$pull': { 'directories': path.name}}, 'fs'))
        #there is not any file deleted
        if len(deleted_tot) == 0:
            logging.info('Removed {}'.format(path))
            return (None,None, updatedone_documents, deletedone_documents)
        logging.info('Removed {}'.format(path))
        return (deleted_tot, h_tot, updatedone_documents, deletedone_documents)
    #the last part of the path it's a file, so delete only the file as a simple rm command
    #the procedure is the same as rm command for a file
    elif path.name in curr_dir['files']:
        #check the permissions with parent role on the current directory
        #if not check_permissions(curr_dir, 'parent', required_by, grp, 'rm_file'):
        #    logging.warning('Access denied: the operation required is not allowed')
        #    raise AccessDeniedException()
        resource = fs.find_one({'name': path.name, 'parent': curr_dir['_id'], 'type': 'f'})
        #check the permissions with resource role on the file
        if not check_permissions(resource, 'resource', required_by, grp, 'rm_file'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(resource['name']))
            raise AccessDeniedException(resource['name'])
        chunks = resource['chunks']
        for c in chunks.keys():
            tmp_c = chunks[c]
            del chunks[c]
            chunks[c.replace('[dot]', '.').replace('[colon]', ':')] = tmp_c
        replicas = resource['replicas']
        for r in replicas.keys():
            tmp_dn = []
            for dn in replicas[r]:
                tmp_dn.append(dn.replace('[dot]', '.').replace('[colon]', ':'))
            replicas[r] = tmp_dn
        #update the fs collection
        fs.update_one({ '_id': curr_dir['_id'] }, {'$pull': { 'files': path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$pull': { 'files': path.name}}, 'fs'))
        #update the fs collection
        deleted = fs.delete_one({'parent': curr_dir['_id'], 'type': 'f', 'name': path.name})
        #insert into the list needed for aligning the other namenodes
        deletedone_documents.append(({'parent': curr_dir['_id'], 'type': 'f', 'name': path.name}, 'fs'))
        hs = list(set(chain(*list(replicas.values()))))
        hm = list(chunks.keys())
        h = list(set(hm + hs))
        logging.info('Removed {}'.format(path))
        return ([str(resource['_id'])],h, updatedone_documents, deletedone_documents)
    #the path is the root directory and can not be deleted neither by the root
    elif path.name == '':
        logging.warning('Root Directory: the operation you required is not allowed')
        raise RootDirectoryException()
    else:
        logging.warning('The path does not exist: "{}" not found'.format(path.name))
        raise NotFoundException(path.name)


def get_file(client, file_path, required_by, grp):
    """Return file object (used for cat, get, head, tail...).
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    file_path --> pathlib.PosixPath class, path to the file you want to get
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    file --> dict, the object which represents the file you want to get
    """
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(file_path, fs, required_by, grp, 'get_file')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role for the current directory
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'get_file'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the last part of the path is a file 
    if file_path.name in curr_dir['files']:
        file = fs.find_one({'parent': curr_dir['_id'], 'type': 'f', 'name': file_path.name})
        #check the permissions with resource role for the file
        if not check_permissions(file, 'resource', required_by, grp, 'get_file'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(file['name']))
            raise AccessDeniedException(file['name'])
        for c in file['chunks'].keys():
            tmp_c = file['chunks'][c]
            del file['chunks'][c]
            file['chunks'][c.replace('[dot]', '.').replace('[colon]', ':')] = tmp_c
        for r in file['replicas'].keys():
            tmp_dn = []
            for dn in file['replicas'][r]:
                tmp_dn.append(dn.replace('[dot]', '.').replace('[colon]', ':'))
            file['replicas'][r] = tmp_dn
        logging.info('Get file {}'.format(file_path))
        return file
    #the path to the file does not exist
    else:
        logging.warning('The path does not exist: "{}" not found'.format(file_path.name))
        raise NotFoundException(file_path.name)
    

def get_directory(client, orig_path, required_by, grp):
    """Return directory object (used for mv...).
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    orig_path --> pathlib.PosixPath class, path to the directory you want to get
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    directory --> dict, the object which represents the directory you want to get
    """
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(orig_path, fs, required_by, grp, 'get_directory')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role for the current directory
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'get_directory'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the last part of the path is a directory
    if orig_path.name in curr_dir['directories']:
        directory = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': orig_path.name})
        #check the permissions with resource role for the directory
        if not check_permissions(directory, 'resource', required_by, grp, 'get_directory'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(directory['name']))
            raise AccessDeniedException(directory['name'])
        logging.info('Get directory {}'.format(orig_path))
        return directory
    #the path to the directory is the pathto the root
    elif orig_path.name == '':
        logging.warning('Root Directory: the operation you required is not allowed')
        raise RootDirectoryException()
    #the path to the directory does not exist
    else:
        logging.warning('The path does not exist: "{}" not found'.format(orig_path.name))
        raise NotFoundException(orig_path.name)
    
    
def cp(client, orig_file, dest_path, required_by, grp):
    """Copy a source file into a destination path.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    orig_file --> pathlib.PosixPath class, path to the file you want to copy, the origin path
    dest_path --> pathlib.PosixPath class, path to the directory/file you want to copy the origin file, the destination path
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    (file['_id'], file_id, h, inserted_documents, updatedone_documents, deletedone_documents) --> tuple(bson.objectid.ObjectId class, bson.objectid.ObjectId class, list, list, list, list), the source object id, the destination object id, the list of the datanodes which handle a replica of any chunk for the source file, the list of the documents to insert and the collections in which they must be inserted, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update, the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    """
    #for master namenode
    inserted_documents = []
    updatedone_documents = []
    deletedone_documents = []
    #get the source file to copy into a new path
    try:
        file = get_file(client, orig_file, required_by, grp)
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    for c in file['chunks'].keys():
        tmp_c = file['chunks'][c]
        del file['chunks'][c]
        file['chunks'][c.replace('.', '[dot]').replace(':', '[colon]')] = tmp_c
    for r in file['replicas'].keys():
        tmp_dn = []
        for dn in file['replicas'][r]:
            tmp_dn.append(dn.replace('.', '[dot]').replace(':', '[colon]'))
        file['replicas'][r] = tmp_dn
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(dest_path, fs, required_by, grp, 'cp')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #the last part of the path is a directory or is the root directory, so the orig file must be copied in the destination directory with the same orig file name
    if dest_path.name in curr_dir['directories'] or dest_path.name == '':
        if dest_path.name != '':
            curr_dir = fs.find_one({'name': dest_path.name, 'parent': curr_dir['_id'], 'type': 'd'})
        #check the permissions with parent role on the final directory
        if not check_permissions(curr_dir, 'parent', required_by, grp, 'cp'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #a file with the same name already exists into this directory
        if file['name'] in curr_dir['files']:
            logging.warning('The resource already exists')
            raise AlreadyExistsException()
        #a directory with the same name already exists into this directory
        if file['name'] in curr_dir['directories']:
            logging.warning('A directory with the same name already exists')
            raise AlreadyExistsDirectoryException()
        #insert the new file into the parent directory
        fs.update_one({ '_id': curr_dir['_id'] }, {'$push': { 'files': file['name']}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$push': { 'files': file['name']}}, 'fs'))
        #create the file node and insert it into fs collection
        new_file = create_file_node(file['name'], curr_dir['_id'], required_by, required_by, size=file['size'])
        file_id = fs.insert_one(new_file).inserted_id
        curr_file = fs.find_one({'_id': file_id})
        #insert into the list needed for aligning the other namenodes
        inserted_documents.append((curr_file, 'fs'))
        dest_chunks = {}
        dest_replicas = {}
        dest_chunks_bkp = {}
        dest_replicas_bkp = {}
        orig_chunks = file['chunks']  
        orig_replicas = file['replicas']
        orig_chunks_bkp = file['chunks_bkp']  
        orig_replicas_bkp = file['replicas_bkp']
        #when a file is copied, also the chunks in the datanodes must be copied as they are, but with the prefix of the new file 
        for dn in orig_chunks:
            for c in orig_chunks[dn]:
                try:
                    dest_chunks[dn].append('{}_{}'.format(str(file_id),c.split('_')[1]))
                except:
                    dest_chunks[dn] = ['{}_{}'.format(str(file_id),c.split('_')[1])]  
                dest_chunks_bkp['{}_{}'.format(str(file_id),c.split('_')[1])] = dn
        for r in orig_replicas:
            for dn in orig_replicas[r]:
                try:
                    dest_replicas['{}_{}'.format(str(file_id),r.split('_')[1])].append(dn)
                except:
                    dest_replicas['{}_{}'.format(str(file_id),r.split('_')[1])] = [dn]
                try: 
                    dest_replicas_bkp[dn].append('{}_{}'.format(str(file_id),r.split('_')[1]))
                except:
                    dest_replicas_bkp[dn] = ['{}_{}'.format(str(file_id),r.split('_')[1])]
        #update the fs collection
        fs.update_one({ '_id': file_id }, {'$set': {'chunks': dest_chunks, 'chunks_bkp': dest_chunks_bkp, 'replicas': dest_replicas, 'replicas_bkp': dest_replicas_bkp}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': file_id }, {'$set': {'chunks': dest_chunks, 'chunks_bkp': dest_chunks_bkp, 'replicas': dest_replicas, 'replicas_bkp': dest_replicas_bkp}}, 'fs'))
        for c in orig_chunks.keys():
            tmp_c = orig_chunks[c]
            del orig_chunks[c]
            orig_chunks[c.replace('[dot]', '.').replace('[colon]', ':')] = tmp_c
        for r in orig_replicas.keys():
            tmp_dn = []
            for dn in orig_replicas[r]:
                tmp_dn.append(dn.replace('[dot]', '.').replace('[colon]', ':'))
            orig_replicas[r] = tmp_dn
        #get the list of the chunks to copy, without repetitions
        hs = list(set(chain(*list(orig_replicas.values()))))
        hm = list(orig_chunks.keys())
        h = list(set(hm + hs))
        logging.info('Copied {} content into {}'.format(orig_file, dest_path))
        #return the list of chunks to copy as they are with the new prefix into the interested datanodes
        return (file['_id'], file_id, h, inserted_documents, updatedone_documents, deletedone_documents)
    #the last part of the path is a file name
    else:
        #check the permissions on the parent directory with parent role
        if not check_permissions(curr_dir, 'parent', required_by, grp, 'cp'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #a file with the same name already exists
        if dest_path.name in curr_dir['files']:
            logging.warning('The resource already exists')
            raise AlreadyExistsException()
        #a directory with the same name alredy exists
        elif dest_path.name in curr_dir['directories']:
            logging.warning('A directory with the same name already exists')
            raise AlreadyExistsDirectoryException()
        #the file does not exist into the directory, so it must be created
        #update the fs collection
        fs.update_one({ '_id': curr_dir['_id'] }, {'$push': { 'files': dest_path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$push': { 'files': dest_path.name}}, 'fs'))
        #create the file node and update the fs collection
        new_file = create_file_node(dest_path.name, curr_dir['_id'], required_by, required_by, size=file['size'])
        file_id = fs.insert_one(new_file).inserted_id
        curr_file = fs.find_one({'_id': file_id})
        #insert into the list needed for aligning the other namenodes
        inserted_documents.append((curr_file, 'fs'))
        dest_chunks = {}
        dest_replicas = {}
        dest_chunks_bkp = {}
        dest_replicas_bkp = {}
        orig_chunks = file['chunks']
        orig_replicas = file['replicas']
        orig_chunks_bkp = file['chunks_bkp']  
        orig_replicas_bkp = file['replicas_bkp']
        #when a file is copied, also the chunks in the datanodes must be copied as they are, but with the prefix of the new file 
        for dn in orig_chunks:
            for c in orig_chunks[dn]:
                try:
                    dest_chunks[dn].append('{}_{}'.format(str(file_id),c.split('_')[1]))
                except:
                    dest_chunks[dn] = ['{}_{}'.format(str(file_id),c.split('_')[1])] 
                dest_chunks_bkp['{}_{}'.format(str(file_id),c.split('_')[1])] = dn
        for r in orig_replicas:
            for dn in orig_replicas[r]:
                try:
                    dest_replicas['{}_{}'.format(str(file_id),r.split('_')[1])].append(dn)
                except:
                    dest_replicas['{}_{}'.format(str(file_id),r.split('_')[1])] = [dn]
                try: 
                    dest_replicas_bkp[dn].append('{}_{}'.format(str(file_id),r.split('_')[1]))
                except:
                    dest_replicas_bkp[dn] = ['{}_{}'.format(str(file_id),r.split('_')[1])]
        #update the fs collection
        fs.update_one({ '_id': file_id }, {'$set': {'chunks': dest_chunks, 'chunks_bkp': dest_chunks_bkp, 'replicas': dest_replicas, 'replicas_bkp': dest_replicas_bkp}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': file_id }, {'$set': {'chunks': dest_chunks, 'chunks_bkp': dest_chunks_bkp, 'replicas': dest_replicas, 'replicas_bkp': dest_replicas_bkp}}, 'fs'))
        for c in orig_chunks.keys():
            tmp_c = orig_chunks[c]
            del orig_chunks[c] 
            orig_chunks[c.replace('[dot]', '.').replace('[colon]', ':')] = tmp_c
        for r in orig_replicas.keys():
            tmp_dn = []
            for dn in orig_replicas[r]:
                tmp_dn.append(dn.replace('[dot]', '.').replace('[colon]', ':'))
            orig_replicas[r] = tmp_dn
        #get the list of the chunks to copy, without repetitions
        hs = list(set(chain(*list(orig_replicas.values()))))
        hm = list(orig_chunks.keys())
        h = list(set(hm + hs))
        logging.info('Copied {} content into {}'.format(orig_file, dest_path))
        #return the list of chunks to copy as they are with the new prefix into the interested datanodes
        return (file['_id'], file_id, h, inserted_documents, updatedone_documents, deletedone_documents)
        
        
def mv(client, orig_path, dest_path, required_by, grp):
    """Move/Rename a resource into a destination path.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    orig_path --> pathlib.PosixPath class, path to the resource you want to move/rename, the origin path
    dest_path --> pathlib.PosixPath class, path to the resource you want to move the origin resource or new name for the origin resource, the destination path
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    updatedone_documents --> list, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    """
    #for master namenode
    updatedone_documents = []
    #get the source file/directory to rename/move into a new path
    try:
        to_move = get_file(client, orig_path, required_by, grp)
    except NotFoundException as e:
        try:
            to_move = get_directory(client, orig_path, required_by, grp)
        except AccessDeniedException as e:
            logging.warning(e.message)
            raise e
        except NotFoundException as e:
            logging.warning(e.message)
            raise e
        except RootDirectoryException as e:
            logging.warning(e.message)
            raise e
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    #the file or the directory cannot be moved because already exists
    if orig_path == dest_path:
        if to_move['type'] == 'f':
            logging.warning('The resource already exists')
            raise AlreadyExistsException()
        else:
            logging.warning('A directory with the same name already exists')
            raise AlreadyExistsDirectoryException()
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory for the orig path
    try:
        curr_dir_from = navigate_through(orig_path, fs, required_by, grp, 'mv_source')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #navigate in the file system until the parent directory for the destination path
    try:
        curr_dir_to = navigate_through(dest_path, fs, required_by, grp, 'mv_destination')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role for the orig parent
    if not check_permissions(curr_dir_from, 'parent', required_by, grp, 'mv_source'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir_from['name']))
        raise AccessDeniedException(curr_dir_from['name'])
    #check the permissions with resource role for the directory/file element to move
    if not check_permissions(to_move, 'resource', required_by, grp, 'mv_source'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(to_move['name']))
        raise AccessDeniedException(to_move['name'])
    only_folder = False
    #the destination path is a path to a directory, so the resource must be moved into this directory 
    if dest_path.name in curr_dir_to['directories']:
        curr_dir_to = fs.find_one({'parent': curr_dir_to['_id'], 'type': 'd', 'name': dest_path.name})
        only_folder = True
    #chek permissions with parent role for the current destination directory
    if not check_permissions(curr_dir_to, 'parent', required_by, grp, 'mv_destination'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir_to['name']))
        raise AccessDeniedException(curr_dir_to['name'])
    #the resource to move is a file
    if to_move['type'] == 'f':        
        if not only_folder: #the destination path has also the new name of the orig file to move
            #in the destination directory, a file with the same name already exists
            if dest_path.name in curr_dir_to['files']:
                logging.warning('The resource already exists')
                raise AlreadyExistsException()
            #in the destination directory, a directory with the same name already exists
            if dest_path.name in curr_dir_to['directories']:
                logging.warning('A directory with the same name already exists')
                raise AlreadyExistsDirectoryException()
            #the new name of the moved file is the last part of the destination path
            fs.update_one({ '_id': curr_dir_to['_id'] }, {'$push': { 'files': dest_path.name}})
            #insert into the list needed for aligning the other namenodes
            updatedone_documents.append(({ '_id': curr_dir_to['_id'] }, {'$push': { 'files': dest_path.name}}, 'fs'))
        else: #the destination path has only the name of the directory in which move the orig file
            #in the destination directory, a file with the same name already exists
            if orig_path.name in curr_dir_to['files']:
                logging.warning('The resource already exists')
                raise AlreadyExistsException()
            #in the destination directory, a directory with the same name already exists
            if orig_path.name in curr_dir_to['directories']:
                logging.warning('A directory with the same name already exists')
                raise AlreadyExistsDirectoryException()
            #the new name of the moved file is the last part of the orig path
            fs.update_one({ '_id': curr_dir_to['_id'] }, {'$push': { 'files': orig_path.name}})
            #insert into the list needed for aligning the other namenodes
            updatedone_documents.append(({ '_id': curr_dir_to['_id'] }, {'$push': { 'files': orig_path.name}}, 'fs'))
        #update the fs collection
        fs.update_one({ '_id': curr_dir_from['_id'] }, {'$pull': { 'files': orig_path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir_from['_id'] }, {'$pull': { 'files': orig_path.name}}, 'fs'))
    #the resource to move is a directory
    else:     
        if orig_path in dest_path.parents:
            logging.warning('Itself subdirectory: the operation you required is not allowed')
            raise ItselfSubdirException()
        if not only_folder: #the destination path has also the new name of the orig directory to move
            #in the destination directory, a file with the same name already exists
            if dest_path.name in curr_dir_to['files']:
                logging.warning('The resource already exists')
                raise AlreadyExistsException()
            #in the destination directory, a directory with the same name already exists
            if dest_path.name in curr_dir_to['directories']:
                logging.warning('A directory with the same name already exists')
                raise AlreadyExistsDirectoryException()
            #the new name of the moved directory is the last part of the destination path
            fs.update_one({ '_id': curr_dir_to['_id'] }, {'$push': { 'directories': dest_path.name}})
            #insert into the list needed for aligning the other namenodes
            updatedone_documents.append(({ '_id': curr_dir_to['_id'] }, {'$push': { 'directories': dest_path.name}}, 'fs'))
        else: #the destination path has only the name of the directory in which move the orig directory
            #in the destination directory, a file with the same name already exists
            if orig_path.name in curr_dir_to['files']:
                logging.warning('The resource already exists')
                raise AlreadyExistsException()
            #in the destination directory, a directory with the same name already exists
            if orig_path.name in curr_dir_to['directories']:
                logging.warning('A directory with the same name already exists')
                raise AlreadyExistsDirectoryException()
            #the new name of the moved directory is the last part of the orig path
            fs.update_one({ '_id': curr_dir_to['_id'] }, {'$push': { 'directories': orig_path.name}})
            #insert into the list needed for aligning the other namenodes
            updatedone_documents.append(({ '_id': curr_dir_to['_id'] }, {'$push': { 'directories': orig_path.name}}, 'fs'))
        #update the fs collection
        fs.update_one({ '_id': curr_dir_from['_id'] }, {'$pull': { 'directories': orig_path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir_from['_id'] }, {'$pull': { 'directories': orig_path.name}}, 'fs'))
    if not only_folder: #the destination path has also the new name of the orig resource to move
        #update the fs collection
        fs.update_one({'_id': to_move['_id']}, {'$set': {'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'parent': curr_dir_to['_id'], 'name': dest_path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({'_id': to_move['_id']}, {'$set': {'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'parent': curr_dir_to['_id'], 'name': dest_path.name}}, 'fs'))
    else: #the destination path has only the name of the directory in which move the orig resource
        #update the fs collection
        fs.update_one({'_id': to_move['_id']}, {'$set': {'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'parent': curr_dir_to['_id'], 'name': orig_path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({'_id': to_move['_id']}, {'$set': {'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'parent': curr_dir_to['_id'], 'name': orig_path.name}}, 'fs'))
    logging.info('Moved {} into {}'.format(orig_path, dest_path))
    return (updatedone_documents)


def count(client, dir_path, required_by, grp):
    """Allow to count the number of subdirectories and files into a directory.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    dir_path --> pathlib.PosixPath class, path to the directory you want to count the files and the subdirectories
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    res --> dict, key: DIR_COUNT or FILE_COUNT, value: the number of directories and files
    """
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(dir_path, fs, required_by, grp, 'count')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role for the current directory
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'count'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the last part of the path is a directory
    if dir_path.name in curr_dir['directories']:
        curr_dir = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': dir_path.name})
        #check the permissions with resource role for the last part of the path, the last directory
        if not check_permissions(curr_dir, 'resource', required_by, grp, 'count'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #return the count of the files and the count of the subdirectories
        res = {
            'DIR_COUNT': len(curr_dir['directories']),
            'FILE_COUNT': len(curr_dir['files'])
        }
        logging.info('Get count of {} directory'.format(dir_path))
        return (res)
    #the path is the root directory
    elif dir_path.name == '':
        #check the permissions with resource role for the last part of the path, the last directory
        if not check_permissions(curr_dir, 'resource', required_by, grp, 'count'):
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #return the count of the files and the count of the subdirectories
        res = {
            'DIR_COUNT': len(curr_dir['directories']),
            'FILE_COUNT': len(curr_dir['files'])
        }
        logging.info('Get count of {} directory'.format(dir_path))
        return (res)
    #the last part of the path either does not exist or is a file (the count cannot be performed)
    else:
        logging.warning('The path does not exist: "{}" not found'.format(dir_path.name))
        raise NotFoundException(dir_path.name)


def count_recursive(client, curr_dir, curr_path, file_count, dir_count, dir_lst):
    """Function which gets all subresources to count.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    curr_dir --> dict, the MongoDB object which represents the current directory
    curr_path --> str, the current path
    file_count --> int, the current number of files
    dir_count --> int, the current number of directories
    dir_lst --> list, the current list of directories
    
    Returns
    -------
    (file_count, dir_count, dir_lst) --> tuple(int, int, list), the updated count of files and directories and the updated list of directories
    """
    fs = get_fs(client)
    #update the counts
    file_count += len(curr_dir['files'])
    dir_count += len(curr_dir['directories'])
    dir_lst.append(curr_path)
    directories = list(fs.find({'parent': curr_dir['_id'], 'type': 'd'}))
    #there is at least one subdirectory in the current directory: recursive case
    for dire in directories:
        (file_count, dir_count, dir_lst) = count_recursive(client, dire, curr_path + '/' + dire['name'], file_count, dir_count, dir_lst)
    return (file_count, dir_count, dir_lst)


def countr(client, dir_path, required_by, grp):
    """Allow to count the number of subdirectories and files into a directory recursively.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    dir_path --> pathlib.PosixPath class, path to the directory you want to count the files and the subdirectories
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    res --> dict, key: DIR_COUNT or FILE_COUNT, value: the number of directories and files
    """
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(dir_path, fs, required_by, grp, 'count')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role for the current directory
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'count'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the last part of the path is a directory
    if dir_path.name in curr_dir['directories']:
        curr_dir = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': dir_path.name})
        #get all the nested elements presents inside the current directory, the last part of the path
        (file_count, dir_count, dir_lst) = count_recursive(client, curr_dir, str(dir_path), 0, 0, [])
        #check the permissions for each nested element 
        for elem in dir_lst:
            if not is_allowed_recursive(Path(elem), fs, required_by, grp, 'count'):
                logging.warning('Access denied at least on one resource: "{}"'.format(elem))
                raise AccessDeniedAtLeastOneException(elem)
        #return the count of the nested files and the count of the subdirectories
        res = {
            'DIR_COUNT': dir_count,
            'FILE_COUNT': file_count
        }
        logging.info('Get count of {} directory'.format(dir_path))
        return (res)
    #the path is the root directory
    elif dir_path.name == '':
        #get all the nested elements presents inside the current directory, the last part of the path
        (file_count, dir_count, dir_lst) = count_recursive(client, curr_dir, '', 0, 0, [])
        #check the permissions for each nested element 
        for elem in dir_lst:
            if elem == '':
                elem = '/'
            if not is_allowed_recursive(Path(elem), fs, required_by, grp, 'count'):
                logging.warning('Access denied at least on one resource: "{}"'.format(elem))
                raise AccessDeniedAtLeastOneException(elem)
        #return the count of the nested files and the count of the subdirectories
        res = {
            'DIR_COUNT': dir_count,
            'FILE_COUNT': file_count
        }
        logging.info('Get count of {} directory'.format(dir_path))
        return (res)
    #the last part of the path either does not exist or is a file (the count cannot be performed)
    else:
        logging.warning('The path does not exist: "{}" not found'.format(dir_path.name))
        raise NotFoundException(dir_path.name)


def du_recursive(client, curr_dir, curr_path, tot_size, dir_lst):
    """Function which gets total disk usage of files into a directory recursively.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    curr_dir --> dict, the MongoDB object which represents the current directory
    curr_path --> str, the current path
    tot_size --> int, the current total size of the resources
    dir_lst --> list, the current list of directories
    
    Returns
    -------
    (tot_size, dir_lst) --> tuple(int, list), the updated total size of the resources and the updated list of directories
    """    
    fs = get_fs(client)
    files = list(fs.find({'parent': curr_dir['_id'], 'type': 'f'}))
    #update the total size for disk usage
    for f in files:
        tot_size += f['size']
    dir_lst.append(curr_path)
    directories = list(fs.find({'parent': curr_dir['_id'], 'type': 'd'}))
    #there is at least one subdirectory in the current directory: recursive case 
    for dire in directories:
        (tot_size, dir_lst) = du_recursive(client, dire, curr_path + '/' + dire['name'], tot_size, dir_lst)
    return (tot_size, dir_lst)
    

def du(client, path, required_by, grp):
    """Allow to get the total disk usage of a file or a directory recursively.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    path --> pathlib.PosixPath class, path to the resource you want to discover the disk usage, either a file or a directory, recursively
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    tot_size, file['size'] --> int, total number of bytes for the resource (file or directory recursively)
    """
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(path, fs, required_by, grp, 'du')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role for the current directory
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'count'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the last part of the path is a directory, so it's needed to calculate the total size of the entire directory, with all the nested elements
    if path.name in curr_dir['directories']:
        curr_dir = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': path.name})
        #get the total size for all the nested elements inside the parent directory
        (tot_size, dir_lst) = du_recursive(client, curr_dir, str(path), 0, [])
        #check the permissions for each nested element
        for elem in dir_lst:
            if not is_allowed_recursive(Path(elem), fs, required_by, grp, 'du'):
                logging.warning('Access denied at least on one resource: "{}"'.format(elem))
                raise AccessDeniedAtLeastOneException(elem)
        logging.info('Get disk usage of {}'.format(path))
        return tot_size
    #the last part of the path is a file, so it's needed to calculate only the size of the file
    elif path.name in curr_dir['files']:
        file = fs.find_one({'parent': curr_dir['_id'], 'type': 'f', 'name': path.name})
        logging.info('Get disk usage of {}'.format(path))
        return file['size']
    #the path is the root directory
    elif path.name == '':
        #get the total size for all the nested elements inside the parent directory
        (tot_size, dir_lst) = du_recursive(client, curr_dir, '', 0, [])
        #check the permissions for each nested element
        for elem in dir_lst:
            if elem == '':
                elem = '/'
            if not is_allowed_recursive(Path(elem), fs, required_by, grp, 'du'):
                logging.warning('Access denied at least on one resource: "{}"'.format(elem))
                raise AccessDeniedAtLeastOneException(elem)
        logging.info('Get disk usage of {}'.format(path))
        return tot_size
    #the path does not exist
    else:
        logging.warning('The path does not exist: "{}" not found'.format(path.name))
        raise NotFoundException(path.name)


def chown(client, path, new_own, required_by, grp):
    """Allow to change owner of a resource.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    path --> pathlib.PosixPath class, path to the resource you want to change the owner
    new_own --> str, the new owner of the resource
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    updatedone_documents --> list, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    """
    #for master namenode
    updatedone_documents = []
    #get fs (filesystem) and users MongoDB collections
    fs = get_fs(client)
    users = get_users(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(path, fs, required_by, grp, 'chown')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role for the parent directory 
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'chown'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the last part of the path is a directory
    if path.name in curr_dir['directories']:
        directory = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': path.name})
        #only the root and the owner of the directory can change the owner  
        if required_by != directory['own'] and required_by != 'root':
            logging.warning('Access denied: the operation required is not allowed on {}'.format(directory['name']))
            raise AccessDeniedException(directory['name'])
        #the new owner is not a user
        if users.count_documents({'name':new_own}) == 0:
            logging.warning('The user does not exist: {}'.format(new_own))
            raise UserNotFoundException(new_own)
        #update the fs collection
        fs.update_one({ '_id': directory['_id'] }, {'$set': {'own': new_own}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': directory['_id'] }, {'$set': {'own': new_own}}, 'fs'))
        logging.info('The owner of {} has changed: now is {}'.format(path, new_own))
        return updatedone_documents
    #the last part of the path is a file
    elif path.name in curr_dir['files']:
        file = fs.find_one({'parent': curr_dir['_id'], 'type': 'f', 'name': path.name})
        #only the root and the owner of the file can change the owner 
        if required_by != file['own'] and required_by != 'root':
            logging.warning('Access denied: the operation required is not allowed on {}'.format(file['name']))
            raise AccessDeniedException(file['name'])
        #the new owner is not a user
        if users.count_documents({'name':new_own}) == 0:
            logging.warning('The user does not exist: {}'.format(new_own))
            raise UserNotFoundException(new_own)
        #update the fs collection
        fs.update_one({ '_id': file['_id'] }, {'$set': {'own': new_own, 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': file['_id'] }, {'$set': {'own': new_own, 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}, 'fs'))
        logging.info('The owner of {} has changed: now is {}'.format(path, new_own))
        return updatedone_documents
    #the path is the root directory
    elif path.name == '':
        #only the root and the owner of the file can change the owner 
        if required_by != curr_dir['own'] and required_by != 'root':
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #the new owner is not a user
        if users.count_documents({'name':new_own}) == 0:
            logging.warning('The user does not exist: {}'.format(new_own))
            raise UserNotFoundException(new_own)
        #update the fs collection
        fs.update_one({ '_id': curr_dir['_id'] }, {'$set': {'own': new_own, 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$set': {'own': new_own, 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}, 'fs'))
        logging.info('The owner of {} has changed: now is {}'.format(path, new_own))
        return updatedone_documents
    #the path does not exist
    else:
        logging.warning('The path does not exist: "{}" not found'.format(path.name))
        raise NotFoundException(path.name)
    

def chgrp(client, path, new_grp, required_by, grp):
    """Allow to change group of a resource.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    path --> pathlib.PosixPath class, path to the resource you want to change the owner
    new_grp --> str, the new group of the resource
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    updatedone_documents --> list, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    """
    #for master namenode
    updatedone_documents = []
    #get fs (filesystem) and groups MongoDB collections
    fs = get_fs(client)
    groups = get_groups(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(path, fs, required_by, grp, 'chgrp')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role for the parent directory 
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'chgrp'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the last part of the path is a directory
    if path.name in curr_dir['directories']:
        directory = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': path.name})
        #only the root and the owner of the directory can change the group  
        if required_by != directory['own'] and required_by != 'root':
            logging.warning('Access denied: the operation required is not allowed on {}'.format(directory['name']))
            raise AccessDeniedException(directory['name'])
        #the new group does not exist
        if groups.count_documents({'name':new_grp}) == 0:
            logging.warning('The group does not exist: {}'.format(new_grp))
            raise GroupNotFoundException(new_grp)
        #update the fs collection
        fs.update_one({ '_id': directory['_id'] }, {'$set': {'grp': new_grp}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': directory['_id'] }, {'$set': {'grp': new_grp}}, 'fs'))
        logging.info('The group of {} has changed: now is {}'.format(path, new_grp))
        return updatedone_documents
    #the last part of the path is a file
    elif path.name in curr_dir['files']:
        file = fs.find_one({'parent': curr_dir['_id'], 'type': 'f', 'name': path.name})
        #only the root and the owner of the file can change the group  
        if required_by != file['own'] and required_by != 'root':
            logging.warning('Access denied: the operation required is not allowed on {}'.format(file['name']))
            raise AccessDeniedException(file['name'])
        #the new group does not exist
        if groups.count_documents({'name':new_grp}) == 0:
            logging.warning('The group does not exist: {}'.format(new_grp))
            raise GroupNotFoundException(new_grp)
        #update the fs collection
        fs.update_one({ '_id': file['_id'] }, {'$set': {'grp': new_grp, 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': file['_id'] }, {'$set': {'grp': new_grp, 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}, 'fs'))
        logging.info('The group of {} has changed: now is {}'.format(path, new_grp))
        return updatedone_documents
    #the path is the root directory
    elif path.name == '':
        #only the root and the owner of the file can change the group  
        if required_by != curr_dir['own'] and required_by != 'root':
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #the new group does not exist
        if groups.count_documents({'name':new_grp}) == 0:
            logging.warning('The group does not exist: {}'.format(new_grp))
            raise GroupNotFoundException(new_grp)
        #update the fs collection
        fs.update_one({ '_id': curr_dir['_id'] }, {'$set': {'grp': new_grp, 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$set': {'grp': new_grp, 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}, 'fs'))
        logging.info('The group of {} has changed: now is {}'.format(path, new_grp))
        return updatedone_documents
    #the path does not exist
    else:
        logging.warning('The path does not exist: "{}" not found'.format(path.name))
        raise NotFoundException(path.name)
    
    
def chmod(client, path, new_mode, required_by, grp):
    """Allow to change mode (permissions) of a resource.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    path --> pathlib.PosixPath class, path to the resource you want to change the owner
    new_mode --> str, the new permissions for the file, in the form of [0-7][0-7][0-7]
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    updatedone_documents --> list, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    """
    #for master namenode
    updatedone_documents = []
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(path, fs, required_by, grp, 'chmod')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role for the parent directory 
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'chmod'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #parse the new mod passed and verify that it's correct
    new_own,new_grp,new_others = parse_mode(new_mode)
    if new_own is None or new_grp is None or new_others is None:
        logging.warning('invalid mode {}'.format(new_mode))
        raise InvalidModException(new_mode)
    #the last part of the path is a directory
    if path.name in curr_dir['directories']:
        directory = fs.find_one({'parent': curr_dir['_id'], 'type': 'd', 'name': path.name})
        #only the root and the owner of the directory can change the mod
        if required_by != directory['own'] and required_by != 'root':
            logging.warning('Access denied: the operation required is not allowed on {}'.format(directory['name']))
            raise AccessDeniedException(directory['name'])
        #update the fs collection
        fs.update_one({ '_id': directory['_id'] }, {'$set': {'mod.own': new_own, 'mod.grp': new_grp, 'mod.others': new_others}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': directory['_id'] }, {'$set': {'mod.own': new_own, 'mod.grp': new_grp, 'mod.others': new_others}}, 'fs'))
        logging.info('The permissions of {} have changed: now are {}'.format(path, new_mode))
        return updatedone_documents
    #the last part of the path is a file
    elif path.name in curr_dir['files']:
        file = fs.find_one({'parent': curr_dir['_id'], 'type': 'f', 'name': path.name})
        #only the root and the owner of the file can change the mod
        if required_by != file['own'] and required_by != 'root':
            logging.warning('Access denied: the operation required is not allowed on {}'.format(file['name']))
            raise AccessDeniedException(file['name'])
        #update the fs collection
        fs.update_one({ '_id': file['_id'] }, {'$set': {'mod.own': new_own, 'mod.grp': new_grp, 'mod.others': new_others, 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': file['_id'] }, {'$set': {'mod.own': new_own, 'mod.grp': new_grp, 'mod.others': new_others, 'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}, 'fs'))
        logging.info('The permissions of {} have changed: now are {}'.format(path, new_mode))
        return updatedone_documents
    #the path is the root directory
    elif path.name == '':
        #only the root and the owner of the file can change the group 
        if required_by != curr_dir['own'] and required_by != 'root':
            logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
            raise AccessDeniedException(curr_dir['name'])
        #update the fs collection
        fs.update_one({ '_id': curr_dir['_id'] }, {'$set': {'mod.own': new_own, 'mod.grp': new_grp, 'mod.others': new_others}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$set': {'mod.own': new_own, 'mod.grp': new_grp, 'mod.others': new_others}}, 'fs'))
        logging.info('The permissions of {} have changed: now are {}'.format(path, new_mode))
        return updatedone_documents
    #the path does not exist
    else:
        logging.warning('The path does not exist: "{}" not found'.format(path.name))
        raise NotFoundException(path.name)

    
def put_file(client, file_path, file_size, required_by, grp, nodes):
    """Allow to put a file into the dfs from the current file system.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    file_path --> pathlib.PosixPath class, path to the file you want to put in the dfs
    file_size --> int, the entire size of the file
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    nodes --> list, the list of datanodes which are up at the moment of the file creation
    
    Returns
    -------
    (file_id, chunks, replicas, inserted_documents, updatedone_documents) --> tuple(bson.objectid.ObjectId class, dict, dict, list, list), the object id just created for representing the file, key: datanode, value: list of chunks for which the key datanode is master, key: replica id, value: list of datanodes which handle a replica of the key chunk, the list of the documents to insert and the collections in which they must be inserted, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    """        
    #for master namenode
    inserted_documents = []
    updatedone_documents = []
    #get fs (filesystem) MongoDB collection
    fs = get_fs(client)
    #navigate in the file system until the parent directory
    try:
        curr_dir = navigate_through(file_path, fs, required_by, grp, 'put_file')
    except AccessDeniedException as e:
        logging.warning(e.message)
        raise e
    except NotFoundException as e:
        logging.warning(e.message)
        raise e
    #check the permissions with parent role for the parent directory
    if not check_permissions(curr_dir, 'parent', required_by, grp, 'put_file'):
        logging.warning('Access denied: the operation required is not allowed on {}'.format(curr_dir['name']))
        raise AccessDeniedException(curr_dir['name'])
    #the file already exists
    if file_path.name in curr_dir['files']:
        logging.warning('The resource already exists')
        raise AlreadyExistsException()
    #a directory with the same name of the new file already exists
    elif file_path.name in curr_dir['directories'] or file_path.name == '':
        logging.warning('A directory with the same name already exists')
        raise AlreadyExistsDirectoryException()
    #create the file
    else:
        #update the fs collection
        fs.update_one({ '_id': curr_dir['_id'] }, {'$push': { 'files': file_path.name}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': curr_dir['_id'] }, {'$push': { 'files': file_path.name}}, 'fs'))
        #create the file node and insert it into MongoDB
        new_file = create_file_node(file_path.name, curr_dir['_id'], required_by, required_by, file_size)
        file_id = fs.insert_one(new_file).inserted_id
        curr_file = fs.find_one({'_id': file_id})
        #insert into the list needed for aligning the other namenodes
        inserted_documents.append((curr_file, 'fs'))
        max_chunk_size = get_chunk_size()
        #e.g. file size = 75 bytes, chunk size = 10 bytes --> number of chunks = 8 **(ceil(75/10))**
        c_number = ceil(file_size/max_chunk_size)
        chunks = {}
        replicas = {}
        chunks_bkp = {}
        replicas_bkp = {}
        #decide how many chunks and which are the datanodes which handle the primary and seconday replicas for the current file just created
        for c in range(c_number): 
            #decide the namenode which handles the primary replica for the current chunk 
            try:
                chunks[nodes[c%len(nodes)]].append('{}_{}'.format(str(file_id), str(c)))
            except:
                chunks[nodes[c%len(nodes)]] = ['{}_{}'.format(str(file_id), str(c))]
            chunks_bkp['{}_{}'.format(str(file_id), str(c))] = nodes[c%len(nodes)].replace('.', '[dot]').replace(':', '[colon]')
            tmpn = nodes[:]
            tmpn.remove(nodes[c%len(nodes)])
            #decide the namenodes which handle the secondary replicas for the current chunk
            dn_replica = choose_replicas(tmpn)
            replicas['{}_{}'.format(str(file_id), str(c))] = dn_replica
            for dn in dn_replica:
                try: 
                    replicas_bkp[dn.replace('.', '[dot]').replace(':', '[colon]')].append('{}_{}'.format(str(file_id), str(c)))
                except: 
                    replicas_bkp[dn.replace('.', '[dot]').replace(':', '[colon]')] = ['{}_{}'.format(str(file_id), str(c))]
        mongo_chunks = {} 
        mongo_replicas = {}
        for c in chunks.keys():
            mongo_chunks[c.replace('.', '[dot]').replace(':', '[colon]')] = chunks[c]
        for r in replicas.keys():
            tmp_dn = []
            for dn in replicas[r]:
                tmp_dn.append(dn.replace('.', '[dot]').replace(':', '[colon]'))
            mongo_replicas[r] = tmp_dn
        #update the fs collection
        fs.update_one({ '_id': file_id }, {'$set': {'chunks': mongo_chunks, 'replicas': mongo_replicas, 'chunks_bkp': chunks_bkp, 'replicas_bkp': replicas_bkp}})
        #insert into the list needed for aligning the other namenodes
        updatedone_documents.append(({ '_id': file_id }, {'$set': {'chunks': mongo_chunks, 'replicas': mongo_replicas, 'chunks_bkp': chunks_bkp, 'replicas_bkp': replicas_bkp}}, 'fs'))
        logging.info('File {} put'.format(file_path))
        #return the list of the chunks to create and the list of the namenodes which must handle the replicas
        return (file_id, chunks, replicas, inserted_documents, updatedone_documents)
