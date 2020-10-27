from collections_handler import get_fs, get_users, get_groups, get_trash
from utils import create_user_node, create_group_node, create_directory_node, get_datanodes_list
from requests import delete
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

#tested
def mkfs(client, required_by):
    """Allow to initialize a file system but the user has required the operation must
    to be root, because everything in the file system will be deleted and will be created 
    a new empty one; there will be created a root directory (/) and a directory for a default user.
       
    Parameters
    ----------
    client --> pymoMongoClient class, MongoDB client
    required_by --> str, user who required the operation (must be root)
       
    Returns
    -------
    (root_usr_id, user_usr_id, root_grp_id, user_grp_id, root_id, user_id, inserted_documents) --> tuple(bson.objectid.ObjectId class, bson.objectid.ObjectId class, bson.objectid.ObjectId class, bson.objectid.ObjectId class, bson.objectid.ObjectId class, bson.objectid.ObjectId class, list), MongoDB object id for the user root, MongoDB object id for the user user, MongoDB object id for the group root, MongoDB object id for the group user, MongoDB object id for the directory / (root), MongoDB object id for the directory /user, the list of the documents to insert and the collections in which they must be inserted
    """
    #for master namenode
    inserted_documents = []
    #this operation can be performed only by the root
    if required_by != 'root':
        logging.warning('Operation not allowed: who requires this operation MUST be root')
        raise RootNecessaryException() #if the user it's not root raise the exception
    #get the MongoDB metadata collections 
    fs = get_fs(client)
    users = get_users(client)
    groups = get_groups(client)
    trash = get_trash(client)
    #clean all the MongoDB metadata collections 
    res1 = fs.delete_many({})
    res2 = users.delete_many({})
    res3 = groups.delete_many({})
    res4 = trash.delete_many({})
    logging.info('Metadata DB cleaned')
    #create the root user
    root_usr = create_user_node('root', 'root1.', ['root'])
    #create the default user "user", a user which has not the root privileges
    user_usr = create_user_node('user', 'user1.', ['user'])
    #create the respective "root" and "user" groups
    root_grp = create_group_node('root', ['root'])
    user_grp = create_group_node('user', ['user'])
    #insert into MongoDB the new users/groups just created
    #and take note of this for the other slave namenodes for aligning them 
    root_usr_id = users.insert_one(root_usr).inserted_id
    root_usr = users.find_one({'_id': root_usr_id})
    inserted_documents.append((root_usr, 'users'))
    user_usr_id = users.insert_one(user_usr).inserted_id
    user_usr = users.find_one({'_id': user_usr_id})
    inserted_documents.append((user_usr, 'users'))
    root_grp_id = groups.insert_one(root_grp).inserted_id
    root_grp = groups.find_one({'_id': root_grp_id})
    inserted_documents.append((root_grp, 'groups'))
    user_grp_id = groups.insert_one(user_grp).inserted_id
    user_grp = groups.find_one({'_id': user_grp_id})
    inserted_documents.append((user_grp, 'groups'))
    #create the root directory, which has not any parent (of course, let's say!)
    root = create_directory_node('/', None, 'root', 'root')
    root['directories'].append('user')
    root_id = fs.insert_one(root).inserted_id
    root = fs.find_one({'_id': root_id})
    inserted_documents.append((root, 'fs'))
    #create the home directory for the default user
    user_dir = create_directory_node('user', root_id, 'user', 'user')
    user_id = fs.insert_one(user_dir).inserted_id
    user = fs.find_one({'_id': user_id})
    inserted_documents.append((user, 'fs'))
    #clean the storage directory of each datanode
    datanodes = get_datanodes_list()
    for dn in datanodes: 
        delete('http://{}/mkfs'.format(dn))
    logging.info('DFS initializated')
    return (root_usr_id, user_usr_id, root_grp_id, user_grp_id, root_id, user_id, inserted_documents)

