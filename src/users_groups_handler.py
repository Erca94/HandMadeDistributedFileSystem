from pathlib import Path
from collections_handler import get_fs, get_users, get_groups
from utils import create_group_node, create_user_node
from exceptions import AccessDeniedException, GroupAlreadyExistsException, UserAlreadyExistsException, UserNotFoundException, GroupNotFoundException, MainUserGroupException
from fs_handler import mkdir, chown, chgrp
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

def groupadd(client, required_by, group_name):
    """Allow to create a group, only if you have root privileges.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    required_by --> str, user who required the operation
    group_name --> str, the name of the group for which the operation is required
    
    Returns
    -------
    (grp_id, inserted_documents) --> tuple(bson.objectid.ObjectId class, list), the MongoDB object id just created for representing the group, the list of the documents to insert and the collections in which they must be inserted
    """
    #for master namenode
    inserted_documents = []
    #only the root can perform this operation
    if required_by != 'root':
        logging.warning('Operation not allowed: you MUST be root')
        raise RootNecessaryException()
    groups = get_groups(client)
    #verify if the new group is already present
    if not groups.find_one({'name': group_name}):
        #create the group node and insert it into MongoDB 
        grp = create_group_node(group_name, [])
        grp_id = groups.insert_one(grp).inserted_id
        curr_grp = groups.find_one({'_id': grp_id})
        #insert into the list needed for aligning the other namenodes
        inserted_documents.append((curr_grp, 'groups'))
        logging.info('Group {} added'.format(group_name))
        return (grp_id, inserted_documents)
    logging.warning('The group already exists')
    raise GroupAlreadyExistsException() #the group is already present


def useradd(client, required_by, username, password):
    """Allow to create a group, only if you have root privileges; 
    when a user is created, it will be created also a group with the
    same user's name and it will be added the user to this group.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    required_by --> str, user who required the operation
    username --> str, the name of the user for which the operation is required
    password --> str, the password for the user
    
    Returns
    -------
    (user_id, grp_id, dir_id, inserted_documents, updatedone_documents) --> tuple(bson.objectid.ObjectId class, bson.objectid.ObjectId class, bson.objectid.ObjectId class, list, list), the MongoDB result after having added the user, the list of the documents to insert and the collections in which they must be inserted, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    """
    #for master namenode
    inserted_documents = []
    updatedone_documents = []
    #only the root can perform this operation
    if required_by != 'root':
        logging.warning('Operation not allowed: you MUST be root')
        raise RootNecessaryException()
    fs = get_fs(client)
    users = get_users(client)
    groups = get_groups(client)
    #verify both if the new user is already present and if a group with the same name of the new group is already present
    if not users.find_one({'name': username}):
        if not groups.find_one({'name': username}):
            #create the user and the main group for the user nodes and insert them into MongoDB  
            user = create_user_node(username, password, [username])
            grp = create_group_node(username, [username])
            user_id = users.insert_one(user).inserted_id
            curr_usr = users.find_one({'_id': user_id})
            #insert into the list needed for aligning the other namenodes
            inserted_documents.append((curr_usr, 'users'))
            grp_id = groups.insert_one(grp).inserted_id
            curr_grp = groups.find_one({'_id': grp_id})
            #insert into the list needed for aligning the other namenodes
            inserted_documents.append((curr_grp, 'groups'))
            #create the home directory for the new user
            (directory_id, inserted_documents_mkdir, updatedone_documents_mkdir) = mkdir(client, Path('/{}'.format(username)), required_by, ['root'], parent=True)
            #insert into the list needed for aligning the other namenodes
            inserted_documents.extend(inserted_documents_mkdir)
            updatedone_documents.extend(updatedone_documents_mkdir)
            updatedone_documents_chown = chown(client, Path('/{}'.format(username)), username, required_by, ['root'])
            updatedone_documents.extend(updatedone_documents_chown)
            updatedone_documents_chgrp = chgrp(client, Path('/{}'.format(username)), username, required_by, ['root'])
            updatedone_documents.extend(updatedone_documents_chgrp)
            logging.info('User {} added'.format(username))
            return (user_id, grp_id, directory_id, inserted_documents, updatedone_documents)
        logging.warning('The group already exists')
        raise GroupAlreadyExistsException() #a group with the same name of the new user is already present
    logging.warning('The user already exists')
    raise UserAlreadyExistsException() #the user is already present
    

def groupdel(client, required_by, group_name):
    """Allow to delete a group, only if you have root privileges.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    required_by --> str, user who required the operation
    group_name --> str, the name of the group for which the operation is required
    
    Returns
    -------
    (deleted, res_updt, updatedone_documents, updatedmany_documents, deletedone_documents) --> tuple(pymongo.results.DeleteResult class, pymongo.results.UpdateResult class, list, list, list), the MongoDB result after having deleted the group, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update, the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    """
    #for master namenode
    updatedone_documents = []
    updatedmany_documents = []
    deletedone_documents = []
    #only the root can perform this operation
    if required_by != 'root':
        logging.warning('Operation not allowed: you MUST be root')
        raise RootNecessaryException()
    groups = get_groups(client)
    users = get_users(client)
    fs = get_fs(client)
    #check if the group already exists
    if not groups.find_one({'name': group_name}):
        logging.warning('The group does not exist: {}'.format(group_name))
        raise GroupNotFoundException(group_name) #the group does not exist
    #check if the group is the main one of a user
    if users.find_one({'name': group_name}):
        logging.warning('The group {} is the main group of a user'.format(group_name))
        raise MainUserGroupException(group_name) #the group cannot be deleted because it's the main one of a user
    grp = groups.find_one({'name': group_name})
    for u in grp['users']:
        #delete the group from the list of the groups for each user
        users.update_one({'name': u}, {'$pull': {'groups': group_name}})
        #insert into the list for aligning the other namenodes
        updatedone_documents.append(({'name': u}, {'$pull': {'groups': group_name}}, 'users'))
    #delete the group
    deleted = groups.delete_one({'_id': grp['_id']})
    #insert into the list for aligning the other namenodes
    deletedone_documents.append(({'_id': grp['_id']}, 'groups'))
    #update the documents whose group was the one which has been deleted
    res_updt = fs.update_many({'grp': group_name}, {'$set': {'grp': required_by}})
    #insert into the list for aligning the other namenodes
    updatedmany_documents.append(({'grp': group_name}, {'$set': {'grp': required_by}}, 'fs'))
    logging.info('Group {} deleted'.format(group_name))
    return (deleted, res_updt, updatedone_documents, updatedmany_documents, deletedone_documents)


def userdel(client, required_by, username):
    """Allow to delete a user, only if you have root privileges.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    required_by --> str, user who required the operation
    username --> str, the name of the user for which the operation is required
    
    Returns
    -------
    (deleted, f_deleted, d_updt, updatedone_documents, updatedmany_documents, deletedone_documents, deletemany_documents) --> tuple(pymongo.results.DeleteResult class, pymongo.results.DeleteResult class, pymongo.results.UpdateResult, list, list, list, list), the MongoDB result after having deleted the user, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update, the list of the conditions for deleting MongoDB documents and the collection in which perform the delete, the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    """
    #for master namenode
    updatedone_documents = []
    updatedmany_documents = []
    deletedone_documents = []
    deletemany_documents = []
    #only the root can perform this operation
    if required_by != 'root':
        logging.warning('Operation not allowed: you MUST be root')
        raise RootNecessaryException()
    groups = get_groups(client)
    users = get_users(client)
    fs = get_fs(client)
    #check if the user already exists
    if not users.find_one({'name': username}):
        logging.warning('The user does not exist: {}'.format(username))
        raise UserNotFoundException(username) #the user does not exist
    usr = users.find_one({'name': username})
    #remove the user from the groups to which it belongs
    for g in usr['groups']:
        groups.update_one({'name': g}, {'$pull': {'users': username}})
        updatedone_documents.append(({'name': g}, {'$pull': {'users': username}}, 'groups'))
    #delete the user
    deleted = users.delete_one({'_id': usr['_id']})
    #insert into the list for aligning the other namenodes
    deletedone_documents.append(({'_id': usr['_id']}, 'users'))
    #delete the file whose owner was the user to delete
    f_deleted = fs.delete_many({'type': 'f', 'own': username})
    #insert into the list for aligning the other namenodes
    deletemany_documents.append(({'type': 'f', 'own': username}, 'fs'))
    #set "root" as the new owner of the directories whose owner was the user to delete 
    d_updt = fs.update_many({'type': 'd', 'own': username}, {'$set': {'own': required_by}})
    #insert into the list for aligning the other namenodes
    updatedmany_documents.append(({'type': 'd', 'own': username}, {'$set': {'own': required_by}}, 'fs'))
    logging.info('User {} deleted'.format(username))
    return (deleted, f_deleted, d_updt, updatedone_documents, updatedmany_documents, deletedone_documents, deletemany_documents)


def passwd(client, required_by, username, new_password):
    """Allow to change the password of a user.
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    required_by --> str, user who required the operation
    username --> str, the name of the user for which the operation is required
    new_password --> str, the new password for the user
    
    Returns
    -------
    updatedone_documents --> list, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update 
    """
    #for master namenode
    updatedone_documents = []
    #only the root and the user itself can perform this operation
    #e.g. only me can change my password (or the sysadmin)
    if required_by != 'root' and required_by != username:
        logging.warning('Access denied: the operation required is not allowed on {}'.format(username))
        raise AccessDeniedException(username)
    users = get_users(client)
    #check if the user exists
    if not users.find_one({'name': username}):
        logging.warning('The user does not exist: {}'.format(username))
        raise UserNotFoundException(username) #the user does not exist
    #update the new password
    users.update_one({'name': username}, {'$set': {'password': new_password}})
    #insert into the list for aligning the other namenodes
    updatedone_documents.append(({'name': username}, {'$set': {'password': new_password}}, 'users'))
    logging.info('Password updated for user {}'.format(username))
    return updatedone_documents
    

def usermod(client, required_by, username, grps, operation):
    """Allow to add/delete a user to multiple groups. Operations allowed = + (add) or - (delete).
    
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    required_by --> str, user who required the operation
    username --> str, the name of the user for which the operation is required
    grps --> list, the groups list for which the operation is required
    operation --> str, add or delete (+ or -)
    
    Returns
    -------
    updatedone_documents --> list, the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    """
    #for master namenode
    updatedone_documents = []
    #only the root can perform this operation
    if required_by != 'root':
        logging.warning('Operation not allowed: you MUST be root')
        raise RootNecessaryException
    users = get_users(client)
    groups = get_groups(client)
    usr = users.find_one({'name': username})
    if not usr:
        logging.warning('The user does not exist: {}'.format(username))
        raise UserNotFoundException(username) #the user does not exist
    for g in grps:
        
        #check if the groups already exist
        if not groups.find_one({'name': g}):
            logging.warning('The group does not exist: {}'.format(g))
            raise GroupNotFoundException(g) #at least one of the group does not exist
    #it's required to add the user to new groups 
    if operation == '+':
        for g in grps:
            #the user already belongs to the current group, so do nothing
            if g in usr['groups']:
                continue
            #add the group to the user's groups
            users.update_one({'_id': usr['_id']}, {'$push': { 'groups': g}})
            #insert into the list for aligning the other namenodes
            updatedone_documents.append(({'_id': usr['_id']}, {'$push': { 'groups': g}}, 'users'))
            #add the user to the group users
            groups.update_one({'name': g}, {'$push': {'users': username}})
            #insert into the list for aligning the other namenodes
            updatedone_documents.append(({'name': g}, {'$push': {'users': username}}, 'groups'))
        #print('User {} added to the following groups: {}'.format(username, grps))
        logging.info('User {} added to the following groups: {}'.format(username, grps))
    #it's required to delete the user from groups to which it belongs
    else: 
        for g in grps:
            #the user doesn't belong to the current group, so do nothing
            if g not in usr['groups']:
                continue
            #remove the group from the user's groups
            users.update_one({'_id': usr['_id'] }, {'$pull': { 'groups': g}})
            #insert into the list for aligning the other namenodes
            updatedone_documents.append(({'_id': usr['_id'] }, {'$pull': { 'groups': g}}, 'users'))
            #remove the user from the group users
            groups.update_one({'name': g}, {'$pull': {'users': username}})
            #insert into the list for aligning the other namenodes
            updatedone_documents.append(({'name': g}, {'$pull': {'users': username}}, 'groups'))
        #print('User {} deleted from the following groups: {}'.format(username, grps))
        logging.info('User {} deleted from the following groups: {}'.format(username, grps))
    return updatedone_documents

