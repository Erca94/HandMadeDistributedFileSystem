import datetime
import json 
import multiprocessing
import random
from bson.objectid import ObjectId
from exceptions import AccessDeniedException, NotFoundException


conf = json.load(open('conf.json','r'))


def create_file_node(name, parent, own, grp, size=0):
    """Return a file node as a dict.
    
    Parameters
    ----------
    name --> str, the name of the file
    parent --> bson.objectid.ObjectId class, the MongoDB object id of the directory which contains the file
    own --> str, owner of the file
    grp --> str, group of the file, the main user's group
    size --> int, size of the file in bytes
    
    Returns
    -------
    file --> dict, file object 
    """
    #default permissions 
    #owner --> rw
    #group --> r 
    #others --> r
    #create the file node for MongoDB
    file = {
            'name': name,
            'parent': parent, 
            'type': 'f',
            'chunks': {},
            'chunks_bkp': {},
            'replicas': {},
            'replicas_bkp': {},
            'size': size, 
            'creation': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'own': own,
            'grp': grp,
            'mod': {
                    'own': 6,
                    'grp': 4,
                    'others': 4
                    }
            }
    return file


#tested
def create_directory_node(name, parent, own, grp):
    """Return a directory node as a dict.
    
    Parameters
    ----------
    name --> str, the name of the directory
    parent --> bson.objectid.ObjectId class, the MongoDB object id of the directory which contains the directory
    own --> str, owner of the directory
    grp --> str, group of the directory, the main user's group
    
    Returns
    -------
    directory --> dict, directory object 
    """
    #default permissions
    #owner --> rwx
    #group --> rx
    #others --> rx
    #create the directory node for MongoDB
    directory = {
                'name': name, 
                'parent': parent, 
                'type': 'd',
                'files': [], 
                'directories': [],
                'creation': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'own': own,
                'grp': grp,
                'mod': {
                        'own': 7,
                        'grp': 5,
                        'others': 5
                        }
                }
    return directory


def create_group_node(name, users):
    """Return a group node as a dict.
    
    Parameters
    ----------
    name --> str, name of the group
    users --> list, list of the users which belong to the group
    
    Returns
    -------
    grp --> dict, group object 
    """
    #create the group node for MongoDB
    grp = {
                'name': name, 
                'creation': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'users': users,
                }
    return grp


def create_user_node(name, password, groups):
    """Return a user node as a dict.
    
    Parameters
    ----------
    name --> str, user's name
    password --> str, password for the new user
    groups --> list, list of the groups to which belong the user
    
    Returns
    -------
    user --> dict, user object 
    """
    #create the user node for MongoDB
    user = {
                'name': name, 
                'password': password,
                'creation': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'groups': groups,
                }
    return user


#tested
def decode_mode(mode):
    """Return a permission set decoded (ex: 755 --> rwx,rx,rx).
    
    Parameters
    ----------
    dict --> key: own, grp or others, value: permissions level
    
    Returns
    -------
    (own_permissions, grp_permissions, others_permissions) --> tuple(str, str, str), the permissions decoded, in the form of "w", "r", "x"
    """
    #the allowed modes permissions, in linux style
    modes = {
        0: '',
        1: 'x',
        2: 'w',
        3: 'wx',
        4: 'r',
        5: 'rx',
        6: 'rw',
        7: 'rwx'
    }
    own_permissions = modes[mode['own']]
    grp_permissions = modes[mode['grp']]
    others_permissions = modes[mode['others']]
    return (own_permissions, grp_permissions, others_permissions)


#tested
def is_allowed(operation_type, mode, role):
    """Check if there is the permissions owned are sufficient to do the operation required.
    ex: (ls, parent, rwx) --> True
    
    Parameters
    ----------
    operation_type --> str, operation required
    mode --> str, users's permissions in that resource 
    role --> str, type of the resource in that kind of operation
    
    Returns
    -------
    True, False --> boolean, True if allowed, False otherwise
    """
    #the available operations and the required permissions on the resources
    #ancestors are all the directories, except the last one, the parent one
    #parent is the directory which contains the final resource
    #resource is the file or the interested directory
    op = {
        'mkdir': {
            'ancestor': 'x',
            'parent': 'wx',
            'resource': None
        },
        'touch': {
            'ancestor': 'x',
            'parent': 'wx',
            'resource': 'w'
        },
        'put_file': {
            'ancestor': 'x',
            'parent': 'wx',
            'resource': None
        },
        'ls': {
            'ancestor': 'x',
            'parent': 'rx',
            'resource': None
        },
        'rm': {
            'ancestor': 'x',
            'parent': 'wx',
            'resource': None
        },
        'rm_directory': {
            'ancestor': 'x',
            'parent': 'wx',
            'resource': 'wx'
        },
        'rm_file': {
            'ancestor': 'x',
            'parent': 'wx',
            'resource': 'w'
        },
        'get_file': {
            'ancestor': 'x',
            'parent': 'rx',
            'resource': 'r'
        },
        'get_directory': {
            'ancestor': 'x',
            'parent': 'rx',
            'resource': 'rx'
        },
        'cp': {
            'ancestor': 'x',
            'parent': 'wx',
            'resource': 'w'
        },
        'mv_source': {
            'ancestor': 'x',
            'parent': 'wx',
            'resource': 'w'
        },
        'mv_destination': {
            'ancestor': 'x',
            'parent': 'wx',
            'resource': None
        },
        'count': {
            'ancestor': 'x',
            'parent': 'x',
            'resource': 'rx'
        },
        'du': {
            'ancestor': 'x',
            'parent': 'rx',
            'resource': 'rx'
        },
        'chown': {
            'ancestor': 'x',
            'parent': 'rx',
            'resource': None
        },
        'chmod': {
            'ancestor': 'x',
            'parent': 'rx',
            'resource': None
        },
        'chgrp': {
            'ancestor': 'x',
            'parent': 'rx',
            'resource': None
        }
    }
    #get the needed permissions for the current operation and resouce role
    mode_needed = op[operation_type][role]
    if mode_needed:
        for m in mode_needed:
            if m not in mode:
                return False #there the lack of some permission, so the operation is not allowed
        return True #the operation is allowed
    return True #the operation is allowed, because no permission is needed


#obj_role: ancestor, parent, resource
def check_permissions(obj, obj_role, own, grp, operation_type):
    """Check if a user who has required an operation has the permission to do 
    it on the object; an object could be an ancestor, a parent or a resource.
    
    Parameters
    ----------
    obj --> dict, object on which it's needed to check permissions
    obj_role --> str, role of the object for that operation
    own --> str, user who required the operation
    grp --> list, groups to which the user belogns
    operation_type --> str, operation required
    
    Returns
    -------
    True, False --> boolean, True if the permissions are ok, False otherwise
    """
    #decode the mode from the form 0,1,2,3,4,5,6,7 to the form r,rw,rx...
    own_permissions, grp_permissions, others_permissions = decode_mode(obj['mod'])
    obj_own = obj['own']
    obj_grp = obj['grp']
    if own == 'root':
        return True #the root can do anything
    #if the owner of the resouce is not the user who required the operation and the user has not the right permission verify for the group
    if not (own == obj_own and is_allowed(operation_type, own_permissions, obj_role)):
        #if the group of the resouce is not the group of who required the operation and the group has not the right permission verify for the others
        if not (obj_grp in grp and is_allowed(operation_type, grp_permissions, obj_role)):
            #if the others have not the right permissions 
            if not is_allowed(operation_type, others_permissions, obj_role):
                return False #permission denied
    return True


def navigate_through(path, fs, own, grp, operation_type):
    """Return the object related to the parent directory in a path, the penultimate part
    of a path (ex: /path/to/resource/file.txt --> resource.
    
    Parameters
    ----------
    path --> pathlib.PosixPath class, path to which it's required to navigate through
    fs --> pymongo.collection.Collection class, MongoDb collection which handles fs metadata
    own --> str, user who required the operation
    grp --> list, groups to which the user belogns
    operation_type --> str, operation required
    
    Returns
    -------
    curr_dir --> dict, last directory, MongoDB object
    """
    #the start directory is the root one
    curr_dir = fs.find_one({'name': '/', 'parent': None, 'type': 'd'})
    #verify the permissions for the root
    if not check_permissions(curr_dir, 'ancestor', own, grp, operation_type):
        raise AccessDeniedException(curr_dir['name'])
    #navigate from the first directory to the parent one
    #path = /user/here/the/path/file.txt --> path[1:-1] = [user, here, the, path]
    for directory in path.parts[1:-1]:
        if directory in curr_dir['directories']:
            #get current directory
            curr_dir = fs.find_one({'name': directory, 'parent': curr_dir['_id'], 'type': 'd'})
            #verify the permissions for the current directory
            if not check_permissions(curr_dir, 'ancestor', own, grp, operation_type):
                raise AccessDeniedException(curr_dir['name'])
        else:
            raise NotFoundException(directory) #the user has not the right permission to navigate through
    return curr_dir


def parse_mode(new_mode):
    """Function to parse a mode give in input to chmod function.
    
    Parameters
    ----------
    new_mode --> str, the new resource permissions, in the form of [0-7][0-7][0-7]
    
    Returns
    -------
    (own,grp,others) --> tuple(int,int,int), the permissions parsed
    """
    #the mode should have a length of 3, e.g 755, 777, 644, etc.
    if not len(new_mode) == 3:
        return (None,None,None)
    try:
        #a permission must be a number between 0 and 7, in linux style
        own,grp,others = int(new_mode[0]), int(new_mode[1]), int(new_mode[2])
        if own<0 or own>7 or grp<0 or grp>7 or others<0 or others>7:
            return (None,None,None)
        return (own,grp,others)
    except: 
        return (None,None,None)
    
    
def choose_replicas(av_dn):
    """Function for choosing the datanodes that will be the ones which own the replica chunks for a file; the datanodes will be choosen randomly.
    
    Parameters
    ----------
    av_dn --> list, list of the available datanodes
    
    Returns
    -------
    dn --> list, datanodes choosen for maintaining the replicas
    """
    dn = []
    #the replica set is the number of replicas each chunk must have distributed in the datanodes
    #the decrement is done because the first replica is the master replica
    #if replica set is 3, then a chunk must have 1 master replica and 2 secondary replicas
    replicas = get_replica_set()-1
    #chose the datanodes which handle the replicas randomly
    for i in range(replicas):
        elem = random.choice(av_dn)
        dn.append(elem)
        #remove the choosen datanode from the list of the available ones 
        av_dn.remove(elem)
    return dn


def choose_recovery_replica(chunks_to_replicate):
    """Function for choosing the datanodes that will be the new replica nodes for a the chunks owned by a failed datanode (for disaster recovery strategy).
    
    Parameters
    ----------
    chunks_to_replicate --> list, list of the chunks to replicate, in the form of dictionaries with keys chunk, not_good, master
    
    Returns
    -------
    chunks_to_replicate --> list, list of the chunks to replicate with the replica datanode choosed, in the form of dictionaries with keys chunk, master, new_replica
    """
    nodes = get_datanodes_list()
    for c in chunks_to_replicate:
        #the datanodes which cannot handle the replicas of a chunk after the recovery process are the ones which either have failed, or already handle a replica of the chunk or are the master for the chunk
        not_good = c['not_good'] + [c['master']]
        #the new datanode is choose randomly from the list of the available ones
        new_replica = random.choice(list(set(nodes)-set(not_good)))
        c['new_replica'] = new_replica
        del c['not_good']
    return chunks_to_replicate


def decode_mongodoc(lst, type_lst):
    """Function for decoding MongoDB documents and conditions for updating/deleting; when the documents/conditions are passed as parameters throught xml rpc, they must not contain ObjectId objects because they cannot be encoded into xml.
    
    Parameters
    ----------
    lst --> list, either the list of documents inserted, or the list of conditions for updating, or the list of conditions for deleting
    type_lst --> str, the type of list to decode, (inserted_documents, updatedone_documents, deletedone_documents)
    
    Returns
    -------
    lst --> list, the input list decoded to be passed throught xml rpc
    """
    #cast the MongoDB ObjectIds to strings, because they cannot be marshalled using xml rpc 
    #this must be done when the slave namenodes must be aligned to the master one
    if type_lst == 'inserted_documents':
        for (doc, col) in lst:
            if col == 'fs':
                doc['_id'] = str(doc['_id']) #each id of the MongoDB documents must be casted
                if doc['parent']:
                    doc['parent'] = str(doc['parent'])
                else: 
                    doc['parent'] = False #the root directory has None as a parent
            elif col == 'users':
                doc['_id'] = str(doc['_id']) #each id of the MongoDB documents must be casted
            elif col == 'groups':
                doc['_id'] = str(doc['_id']) #each id of the MongoDB documents must be casted
            elif col == 'trash':
                doc['_id'] = str(doc['_id']) #each id of the MongoDB documents must be casted
            else:
                pass
        return lst
    elif type_lst == 'updatedone_documents':
        for (doc_id, query, col) in lst:
            try:
                doc_id['_id'] = str(doc_id['_id']) #each id of the MongoDB documents must be casted
            except:
                pass
            try:
                doc_id['parent'] = str(doc_id['parent']) #each id of the MongoDB documents must be casted
            except:
                pass
            try:
                query['$set']['parent'] = str(query['$set']['parent']) #each id of the MongoDB documents must be casted
            except:
                pass
        return lst
    elif type_lst == 'deletedone_documents':
        for (doc_id, col) in lst:
            try:
                doc_id['_id'] = str(doc_id['_id']) #each id of the MongoDB documents must be casted
            except:
                pass
            try:
                doc_id['parent'] = str(doc_id['parent'])
            except:
                pass
        return lst
    else:
        return None
    
    
def encode_mongodoc(lst, type_lst):
    """Function for encoding MongoDB documents and conditions for updating/deleting; when the documents/conditions have been received as parameters throught xml rpc, they must contain ObjectId objects instead of string decoded boject, because they are needed for inserting/updating/deleting MongoDB documents.
    
    Parameters
    ----------
    lst --> list, either the list of documents to insert, or the list of conditions for updating, or the list of conditions for deleting
    type_lst --> str, the type of list to encode, (inserted_documents, updatedone_documents, deletedone_documents)
    
    Returns
    -------
    lst --> list, the input list encoded to be used for inserting/updating/deleting
    """
    #cast back the MongoDB strings ids to ObjectIds, because they have been passed through a xml rpc and now they have to be inserted/updated/deleted into MongoDB
    if type_lst == 'inserted_documents':
        for (doc, col) in lst:
            if col == 'fs':
                doc['_id'] = ObjectId(doc['_id']) #each id of the MongoDB documents must be casted back
                if not doc['parent']:
                    doc['parent'] = None #the root directory has None as a parent
                else:
                    doc['parent'] = ObjectId(doc['parent'])
            elif col == 'users':
                doc['_id'] = ObjectId(doc['_id']) #each id of the MongoDB documents must be casted back
            elif col == 'groups':
                doc['_id'] = ObjectId(doc['_id']) #each id of the MongoDB documents must be casted back
            elif col == 'trash':
                doc['_id'] = ObjectId(doc['_id'])#each id of the MongoDB documents must be casted back
            else:
                pass
        return lst
    elif type_lst == 'updatedone_documents':
        for (doc_id, query, col) in lst:
            try:
                doc_id['_id'] = ObjectId(doc_id['_id']) #each id of the MongoDB documents must be casted back
            except:
                pass
            try:
                doc_id['parent'] = ObjectId(doc_id['parent']) #each id of the MongoDB documents must be casted
            except:
                pass
            try:
                query['$set']['parent'] = ObjectId(query['$set']['parent']) #each id of the MongoDB documents must be casted
            except:
                pass
        return lst
    elif type_lst == 'deletedone_documents':
        for (doc_id, col) in lst:
            try:
                doc_id['_id'] = ObjectId(doc_id['_id']) #each id of the MongoDB documents must be casted back
            except:
                pass
            try:
                doc_id['parent'] = ObjectId(doc_id['parent'])
            except:
                pass
        return lst
    else:
        return None
 
    
def get_chunk_size():
    """Function for getting the chunks size from the configuration file.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    conf['max_chunk_size'] --> int, the max size for every chunk, in bytes
    """
    return conf['max_chunk_size']


def get_max_concurrency():
    """Function for getting the max concurrency setting from the configuration file.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    threads_n --> int, the maximum concurrency threshold allowed
    """
    try:
        #verify the max pool thread capacity is an integer
        threads_n = int(conf['max_thread_concurrency'])
    except:
        #take as max pool thread capacity the number of cpu
        threads_n = multiprocessing.cpu_count()
    return threads_n


def get_replica_set():
    """Function for getting the number of replicas from the configuration file.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    replicas --> int, the replica factor for every chunk
    """
    #the replica set must be a positive integer
    try: 
        replicas = int(conf['replica_set'])
        if replicas <= 0:
            replicas = 3
    except:
        replicas = 3
    return replicas


def get_datanodes_list():
    """Function for getting the datanodes identities from the configuration file.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    conf['datanodes'] --> list, the list of the datanodes
    """
    return conf['datanodes']


def get_datanode_setting(datanode):
    """Function for getting the datanode in input as parameter setting from the configuration file.
    
    Parameters
    ----------
    datanode --> str, the datanode for which you want to get the setting info
    
    Returns
    -------
    conf['datanodes_setting'][datanode] --> dict, the setting for the datanode in input
    """
    return conf['datanodes_setting'][datanode]


def get_datanodes():
    """Function for getting the datanodes settings from the configuration file.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    conf['datanodes_setting'] --> dict, the setting info for all the datanodes
    """
    return conf['datanodes_setting']


def get_namenode_setting(namenode):
    """Function for getting the namenode in input as parameter setting from the configuration file.
    
    Parameters
    ----------
    namenode --> str, the namenode for which you want to get the setting info
    
    Returns
    -------
    conf['namenodes_setting'][namenode] --> dict, the namenode setting info
    """
    return conf['namenodes_setting'][namenode]


def get_namenodes():
    """Function for getting the namenodes settings from the configuration file.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    conf['namenodes_setting'] --> dict, the setting info for all the datanodes
    """
    return conf['namenodes_setting']

