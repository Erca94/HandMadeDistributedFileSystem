#example --> python3 namenode.py namenode1

import sys
from xmlrpc.server import SimpleXMLRPCServer
from pymongo import MongoClient
import threading
import time
import asyncio
import websockets
import functools
from pathlib import Path
import xmlrpc.client
import logging

import fs_handler as fsh
import initializer as ini
import users_groups_handler as ugh
from collections_handler import get_fs, get_trash, get_users, get_groups
from utils import get_namenode_setting, get_datanodes_list, get_datanodes, choose_recovery_replica, get_namenodes, get_replica_set, decode_mongodoc, encode_mongodoc
from chunks_handler import start_recovery, start_flush
from exceptions import InvalidSyntaxException, CommandNotFoundException, UserNotFoundException, AccessDeniedException, NotFoundException, RootNecessaryException, NotDirectoryException, NotParentException, AlreadyExistsException, NotEmptyException, AccessDeniedAtLeastOneException, InvalidModException, GroupAlreadyExistsException, UserAlreadyExistsException, GroupNotFoundException, MainUserGroupException

namenode = get_namenode_setting(sys.argv[1])
#MongoDb client, to interact with the metadata database
client = MongoClient(namenode['host_metadata'], namenode['port_metadata'])
collections = {
    'fs': get_fs(client),
    'users': get_users(client),
    'groups': get_groups(client),
    'trash': get_trash(client)
}
#get the list of datanodes setting
datanodes = get_datanodes()
start = {}
countdown_threads = {}
for dn in datanodes:
    #the time without receiving heartbeats before a datanode is considered down is 10 seconds 
    start[datanodes[dn]['host']+':'+str(datanodes[dn]['port'])] = 10
    countdown_threads[datanodes[dn]['host']+':'+str(datanodes[dn]['port'])] = None
#at the beginning, no namenode is considered as master
you_the_master = False

#get the list of the namenodes setting and delete the current namenode from it 
namenodes = get_namenodes()
del namenodes[sys.argv[1]]
namenodes = [nn for nn in namenodes.values()]

    
def mkdir(path, required_by, grp, parent):
    """Allow to execute mkdir command.
    
    Parameters
    ----------
    path --> str, path to the folder for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    parent --> str, T or F, T stands for True and is used if it's required also the creation of the parent directories if they don't exist, F stands for False and it's the contrary
    
    Returns
    -------
    str(directory_id) --> str, the the MongoDB object id just created for representing the folder
    """
    #execute mkdir command for metadata
    (directory_id, inserted_documents, updatedone_documents) = fsh.mkdir(client, Path(path), required_by, grp, parent)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    inserted_documents = decode_mongodoc(inserted_documents, 'inserted_documents')
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.mkdir_s(inserted_documents, updatedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return str(directory_id)
    

def touch(path, required_by, grp):
    """Allow to execute touch command.
    
    Parameters
    ----------
    path --> str, path to the file for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    str(file_id) --> str, the MongoDB object id for representing the touched file
    """
    #execute touch command for metadata
    (file_id, inserted_documents, updatedone_documents) = fsh.touch(client, Path(path), required_by, grp)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    inserted_documents = decode_mongodoc(inserted_documents, 'inserted_documents')
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.touch_s(inserted_documents, updatedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return str(file_id)
    

def ls(path, required_by, grp):
    """Allow to execute ls command.
    
    Parameters
    ----------
    path --> str, path to the resource for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    res --> list, the list of the MongoDB objects inside the resource path in input
    """
    #execute ls command for metadata
    res = fsh.ls(client, Path(path), required_by, grp)
    for elem in res:
        elem['_id'] = str(elem['_id'])
        elem['parent'] = str(elem['parent'])
    return res


def rm(path, required_by, grp):
    """Allow to execute rm command.
    
    Parameters
    ----------
    path --> str, path to the resource for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    (deleted, hosts) --> tuple(list, list), the list containing the object id you want to remove and the list of the datanodes which handle a replica of some chunk of the resource
    """
    #execute rm command for metadata
    (deleted, hosts, updatedone_documents, deletedone_documents) = fsh.rm(client, Path(path), required_by, grp)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    deletedone_documents = decode_mongodoc(deletedone_documents, 'deletedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.rm_s(updatedone_documents, deletedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return (deleted, hosts)


def rmr(path, required_by, grp):
    """Allow to execute rmr command.
    
    Parameters
    ----------
    path --> str, path to the resource for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    (deleted, hosts) --> tuple(list, list), the list containing the objects ids you want to remove and the list of the datanodes which handle a replica of some chunk of the resources 
    """
    #execute rmr command for metadata
    (deleted,hosts, updatedone_documents, deletedone_documents) = fsh.rmr(client, Path(path), required_by, grp)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    deletedone_documents = decode_mongodoc(deletedone_documents, 'deletedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.rmr_s(updatedone_documents, deletedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return (deleted,hosts) 


def get_file(path, required_by, grp):
    """Allow to execute get_file command (used for get_file, cat, tail, head).
    
    Parameters
    ----------
    path --> str, path to the resource for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    file --> dict, the object which represents the file you want to get
    """
    #execute get_file command for metadata
    file = fsh.get_file(client, Path(path), required_by, grp)
    file['_id'] = str(file['_id'])
    file['parent'] = str(file['parent'])
    return file 


def cp(orig, dest, required_by, grp):
    """Allow to execute cp command.
    
    Parameters
    ----------
    orig --> str, origin path to the resource for which the operation is required
    dest --> str, destination path to the resource for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    (old_id, new_id, hosts) --> tuple(str, str, list), the source object id, the destination object id, the list of the datanodes which handle a replica of any chunk for the source file
    """
    #execute cp command for metadata
    (old_id, new_id, hosts, inserted_documents, updatedone_documents, deletedone_documents) = fsh.cp(client, Path(orig), Path(dest), required_by, grp)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    inserted_documents = decode_mongodoc(inserted_documents, 'inserted_documents')
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    deletedone_documents = decode_mongodoc(deletedone_documents, 'deletedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.cp_s(inserted_documents, updatedone_documents, deletedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    old_id = str(old_id)
    new_id = str(new_id)
    return (old_id, new_id, hosts)


def mv(orig, dest, required_by, grp):
    """Allow to execute mv command.
    
    Parameters
    ----------
    orig --> str, origin path to the resource for which the operation is required
    dest --> str, destination path to the resource for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    None
    """
    #execute mv command for metadata
    updatedone_documents = fsh.mv(client, Path(orig), Path(dest), required_by, grp)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.mv_s(updatedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return


def count(path, required_by, grp):
    """Allow to execute count command.
    
    Parameters
    ----------
    path --> str, path to the resource for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    res --> dict, key: DIR_COUNT or FILE_COUNT, value: the number of directories and files
    """
    #execute count command for metadata
    res = fsh.count(client, Path(path), required_by, grp)
    return res


def countr(path, required_by, grp):
    """Allow to execute countr command.
    
    Parameters
    ----------
    path --> str, path to the resource for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    res --> dict, key: DIR_COUNT or FILE_COUNT, value: the number of directories and files
    """
    #execute countr command for metadata
    res = fsh.countr(client, Path(path), required_by, grp)
    return res


def du(path, required_by, grp):
    """Allow to execute du command.
    
    Parameters
    ----------
    path --> str, path to the resource for which the operation is required
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    size --> int, the total size of the disk usage for the input resource
    """
    #execute du command for metadata
    size = fsh.du(client, Path(path), required_by, grp)
    return size


def chown(path, new_own, required_by, grp):
    """Allow to execute chown command.
    
    Parameters
    ----------
    path --> str, path to the resource for which the operation is required
    new_own --> str, the new owner of the resource
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    None
    """
    #execute chown command for metadata
    updatedone_documents = fsh.chown(client, Path(path), new_own, required_by, grp)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.chown_s(updatedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return 


def chgrp(path, new_grp, required_by, grp):
    """Allow to execute chgrp command.
    
    Parameters
    ----------
    path --> str, path to the resource for which the operation is required
    new_grp --> str, the new group of the resource
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    None
    """
    #execute chgrp command for metadata
    updatedone_documents = fsh.chgrp(client, Path(path), new_grp, required_by, grp)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.chgrp_s(updatedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return 


def chmod(path, new_mod, required_by, grp):
    """Allow to execute chmod command.
    
    Parameters
    ----------
    path --> str, path to the resource for which the operation is required
    new_mod --> str, the new permissions of the resource
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    None
    """
    #execute chmod command for metadata
    updatedone_documents = fsh.chmod(client, Path(path), new_mod, required_by, grp)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.chmod_s(updatedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return


def put_file(file_path, size, required_by, grp):
    """Allow to execute put_file command.
    
    Parameters
    ----------
    file_path --> str, path to the resource for which the operation is required
    size --> int, the file size it's required the put operation
    required_by --> str, user who required the operation
    grp --> list, groups to which the user belogns
    
    Returns
    -------
    (fid,chunks_to_write, replicas) --> tuple(str, dict, dict), the MongoDB object id just created, key: datanode, value: list of chunks for which the key datanode is master, key: replica id, value: list of datanodes which handle a replica of the key chunk
    """
    global start
    up_nodes = list(filter(lambda x: start[x]>0, start.keys()))
    #execute put_file command for metadata
    (fid,chunks_to_write, replicas, inserted_documents, updatedone_documents) = fsh.put_file(client, Path(file_path), size, required_by, grp, up_nodes)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    inserted_documents = decode_mongodoc(inserted_documents, 'inserted_documents')
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    fid = str(fid)
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.put_file_s(inserted_documents, updatedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return (fid, chunks_to_write, replicas)


def mkfs(required_by):
    """Allow to execute mkfs command.
    
    Parameters
    ----------
    required_by --> str, user who required the operation
    
    Returns
    -------
    (root_usr_id, user_usr_id, root_grp_id, user_grp_id, root_id, user_id) --> tuple(str, str, str, str, str, str), MongoDB object id for the user root, MongoDB object id for the user user, MongoDB object id for the group root, MongoDB object id for the group user, MongoDB object id for the directory / (root), MongoDB object id for the directory /user
    """
    #execute mkfs command for metadata
    (root_usr_id, user_usr_id, root_grp_id, user_grp_id, root_id, user_id, inserted_documents) = ini.mkfs(client, required_by)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    inserted_documents = decode_mongodoc(inserted_documents, 'inserted_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.mkfs_s(inserted_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    root_usr_id = str(root_usr_id)
    user_usr_id = str(user_usr_id) 
    root_grp_id = str(root_grp_id)
    user_grp_id = str(user_grp_id)
    root_id = str(root_id)
    user_id = str(user_id)
    return (root_usr_id, user_usr_id, root_grp_id, user_grp_id, root_id, user_id)


def groupadd(required_by, group):
    """Allow to execute groupadd command.
    
    Parameters
    ----------
    required_by --> str, user who required the operation
    group --> str, the name of the group for which the operation is required
    
    Returns
    -------
    grp_id --> bson.objectid.ObjectId class, the MongoDB object id just created for representing the group
    """
    #execute groupadd command for metadata
    (grp_id, inserted_documents) = ugh.groupadd(client, required_by, group)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    inserted_documents = decode_mongodoc(inserted_documents, 'inserted_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.groupadd_s(inserted_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return grp_id 


def groupdel(required_by, group):
    """Allow to execute groupdel command.
    
    Parameters
    ----------
    required_by --> str, user who required the operation
    group --> str, the name of the group for which the operation is required
    
    Returns
    -------
    res --> tuple(pymongo.results.DeleteResult class, pymongo.results.UpdateResult class), the MongoDB result after having deleted the group
    """
    #execute groupdel command for metadata
    (deleted, res_updt, updatedone_documents, updatedmany_documents, deletedone_documents) = ugh.groupdel(client, required_by, group)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    deletedone_documents = decode_mongodoc(deletedone_documents, 'deletedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.groupdel_s(updatedone_documents, updatedmany_documents, deletedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return (deleted, res_updt)
    
    
def useradd(required_by, username, password):
    """Allow to execute useradd command.
    
    Parameters
    ----------
    required_by --> str, user who required the operation
    username --> str, the name of the user for which the operation is required
    password --> str, the password for the user
    
    Returns
    -------
    res --> tuple(bson.objectid.ObjectId class, bson.objectid.ObjectId class, bson.objectid.ObjectId class), the MongoDB result after having added the user
    """
    #execute useradd command for metadata
    (user_id, grp_id, dir_id, inserted_documents, updatedone_documents) = ugh.useradd(client, required_by, username, password)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    inserted_documents = decode_mongodoc(inserted_documents, 'inserted_documents')
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.useradd_s(inserted_documents, updatedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return (user_id, grp_id, dir_id)


def userdel(required_by, username):
    """Allow to execute userdel command.
    
    Parameters
    ----------
    required_by --> str, user who required the operation
    username --> str, the name of the user for which the operation is required
    
    Returns
    -------
    res --> tuple(pymongo.results.DeleteResult class, pymongo.results.DeleteResult class, pymongo.results.UpdateResult), the MongoDB result after having deleted the user
    """
    #execute userdel command for metadata
    (deleted, f_deleted, d_updt, updatedone_documents, updatedmany_documents, deletedone_documents, deletemany_documents) = ugh.userdel(client, required_by, username)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    deletedone_documents = decode_mongodoc(deletedone_documents, 'deletedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.userdel_s(updatedone_documents, updatedmany_documents, deletedone_documents, deletemany_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return (deleted, f_deleted, d_updt)


def passwd(required_by, username, new_password):
    """Allow to execute passwd command.
    
    Parameters
    ----------
    required_by --> str, user who required the operation
    username --> str, the name of the user for which the operation is required
    new_password --> str, the new password for the user
    
    Returns
    -------
    None 
    """
    #execute passwd command for metadata
    updatedone_documents = ugh.passwd(client, required_by, username, new_password)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.passwd_s(updatedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return 


def usermod(required_by, username, groups, operation):
    """Allow to execute usermod command.
    
    Parameters
    ----------
    required_by --> str, user who required the operation
    username --> str, the name of the user for which the operation is required
    groups --> list, the groups list for which the operation is required
    operation --> str, add or delete (+ or -)
    
    Returns
    -------
    None
    """
    #execute usermod command for metadata
    updatedone_documents = ugh.usermod(client, required_by, username, groups, operation)
    #decode for aligning the other slave datanodes metadata database
    #cast the MongoDB ObjectIds to strings
    updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the slave namenodes metadata database with a rpc call
    for nn in namenodes:
        loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
        with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
            try:
                proxy.usermod_s(updatedone_documents) #xml rpc call
            except Exception as e:
                #the namenode is not reachable
                logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
    return 


def mkdir_s(inserted_documents, updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after mkdir command
    
    Parameters
    ----------
    inserted_documents --> list(list), the list of the documents to insert and the collections in which they must be inserted
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    inserted_documents = encode_mongodoc(inserted_documents, 'inserted_documents')
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata inserting the new documents
    for (doc, col) in inserted_documents:
        collections[col].insert_one(doc)
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - mkdir')

    
def touch_s(inserted_documents, updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after touch command
    
    Parameters
    ----------
    inserted_documents --> list(list), the list of the documents to insert and the collections in which they must be inserted
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    inserted_documents = encode_mongodoc(inserted_documents, 'inserted_documents')
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata inserting the new documents
    for (doc, col) in inserted_documents:
        collections[col].insert_one(doc)
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - touch')
    
    
def rm_s(updatedone_documents, deletedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after rm command
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    deletedone_documents --> list(list), the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    deletedone_documents = encode_mongodoc(deletedone_documents, 'deletedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    #align the matadata deleting the documents
    for (condition, col) in deletedone_documents:
        collections[col].delete_one(condition)
    logging.info('Align slave namenode to the master - rm')
    
    
def rmr_s(updatedone_documents, deletedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after rmr command
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    deletedone_documents --> list(list), the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    deletedone_documents = encode_mongodoc(deletedone_documents, 'deletedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    #align the matadata deleting the documents
    for (condition, col) in deletedone_documents:
        collections[col].delete_one(condition)
    logging.info('Align slave namenode to the master - rmr')
    
    
def cp_s(inserted_documents, updatedone_documents, deletedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after cp command
    
    Parameters
    ----------
    inserted_documents --> list(list), the list of the documents to insert and the collections in which they must be inserted
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    deletedone_documents --> list(list), the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    inserted_documents = encode_mongodoc(inserted_documents, 'inserted_documents')
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    deletedone_documents = encode_mongodoc(deletedone_documents, 'deletedone_documents')
    #align the matadata inserting the new documents
    for (doc, col) in inserted_documents:
        collections[col].insert_one(doc)
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    #align the matadata deleting the documents
    for (condition, col) in deletedone_documents:
        collections[col].delete_one(condition)
    logging.info('Align slave namenode to the master - cp')
    
    
def mv_s(updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after mv command
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - mv')
    
    
def put_file_s(inserted_documents, updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after put_file command
    
    Parameters
    ----------
    inserted_documents --> list(list), the list of the documents to insert and the collections in which they must be inserted
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    inserted_documents = encode_mongodoc(inserted_documents, 'inserted_documents')
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata inserting the new documents
    for (doc, col) in inserted_documents:
        collections[col].insert_one(doc)
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - put_file')
    
    
def chown_s(updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after chown command
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - chown')
    
    
def chgrp_s(updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after chgrp command
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - chgrp')
    
    
def chmod_s(updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after chgrp command
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - chmod')    
    
    
def groupadd_s(inserted_documents):
    """Function for updating filesystem metadata for the slave namenodes after groupadd command
    
    Parameters
    ----------
    inserted_documents --> list(list), the list of the documents to insert and the collections in which they must be inserted
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    inserted_documents = encode_mongodoc(inserted_documents, 'inserted_documents')
    #align the matadata inserting the new documents
    for (doc, col) in inserted_documents:
        collections[col].insert_one(doc)
    logging.info('Align slave namenode to the master - groupadd') 
    
    
def useradd_s(inserted_documents, updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after useradd command
    
    Parameters
    ----------
    inserted_documents --> list(list), the list of the documents to insert and the collections in which they must be inserted
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    inserted_documents = encode_mongodoc(inserted_documents, 'inserted_documents')
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata inserting the new documents
    for (doc, col) in inserted_documents:
        collections[col].insert_one(doc)
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - useradd')
    
    
def groupdel_s(updatedone_documents, updatedmany_documents, deletedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after groupdel command
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    updatedmany_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    deletedone_documents --> list(list), the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    deletedone_documents = encode_mongodoc(deletedone_documents, 'deletedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    #align the matadata updating the documents
    for (condition, update, col) in updatedmany_documents:
        collections[col].update_many(condition, update)
    #align the matadata deleting the documents
    for (condition, col) in deletedone_documents:
        collections[col].delete_one(condition)
    logging.info('Align slave namenode to the master - groupdel')
    
    
def userdel_s(updatedone_documents, updatedmany_documents, deletedone_documents, deletemany_documents):
    """Function for updating filesystem metadata for the slave namenodes after userdel command
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    updatedmany_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    deletedone_documents --> list(list), the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    deletemany_documents --> list(list), the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    deletedone_documents = encode_mongodoc(deletedone_documents, 'deletedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    #align the matadata updating the documents
    for (condition, update, col) in updatedmany_documents:
        collections[col].update_many(condition, update)
    #align the matadata deleting the documents
    for (condition, col) in deletedone_documents:
        collections[col].delete_one(condition)
    #align the matadata deleting the documents
    for (condition, col) in deletemany_documents:
        collections[col].delete_many(condition)
    logging.info('Align slave namenode to the master - userdel')
    
    
def passwd_s(updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after passwd command
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - passwd')
    
    
def usermod_s(updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after usermod command
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - usermod')
        

def mkfs_s(inserted_documents):
    """Function for updating filesystem metadata for the slave namenodes after mkfs command
    
    Parameters
    ----------
    inserted_documents --> list(list), the list of the documents to insert and the collections in which they must be inserted
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    inserted_documents = encode_mongodoc(inserted_documents, 'inserted_documents')
    res1 = collections['fs'].delete_many({})
    res2 = collections['users'].delete_many({})
    res3 = collections['groups'].delete_many({})
    res4 = collections['trash'].delete_many({})
    #align the matadata inserting the new documents
    for (doc, col) in inserted_documents:
        collections[col].insert_one(doc)
    logging.info('Align slave namenode to the master - mkfs')

        
def record_trash_s(inserted_documents):
    """Function for updating filesystem metadata for the slave namenodes after recording trash, the chunks to delete after the namenode is up again after a failure
    
    Parameters
    ----------
    inserted_documents --> list(list), the list of the documents to insert and the collections in which they must be inserted
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    inserted_documents = encode_mongodoc(inserted_documents, 'inserted_documents')
    #align the matadata inserting the new documents
    for (doc, col) in inserted_documents:
        collections[col].insert_one(doc)
    logging.info('Align slave namenode to the master - recording trash')
    
    
def flush_trash_s(deletemany_documents):
    """Function for updating filesystem metadata for the slave namenodes after flushing trash, the chunks to delete after the namenode is up again after a failure
    
    Parameters
    ----------
    deletemany_documents --> list(list), the list of the conditions for deleting MongoDB documents and the collection in which perform the delete
    
    Returns
    -------
    None
    """
    #align the matadata deleting the documents
    for (condition, col) in deletemany_documents:
        collections[col].delete_many(condition)
    logging.info('Align slave namenode to the master - flushing trash')
    
    
def recover_from_disaster_s(updatedone_documents):
    """Function for updating filesystem metadata for the slave namenodes after a recovery from disaster.
    
    Parameters
    ----------
    updatedone_documents --> list(list), the list of the conditions for updating MongoDB documents, the values which have to be updated and the collection in which perform the update
    
    Returns
    -------
    None
    """
    #encode the MongoDB document to be used, cast back the ids from strings to ObjectId 
    updatedone_documents = encode_mongodoc(updatedone_documents, 'updatedone_documents')
    #align the matadata updating the documents
    for (condition, update, col) in updatedone_documents:
        collections[col].update_one(condition, update)
    logging.info('Align slave namenode to the master - recovering from disaster')


def get_user(username):
    """Function for getting a user information (username, groups to which it belogs, etc).
    
    Parameters
    ----------
    username --> str, the name of the user for which the operation is required
    
    Returns
    -------
    usr --> dict, the user's info
    """
    users = get_users(client)
    #get the user obejct from MongoDB
    usr = users.find_one({'name': username})
    if not usr:
        logging.warning('The username "{}" does not exist'.format(username))
        return None #the username does not exist
    usr['_id'] = str(usr['_id'])
    logging.info('Username "{}" found'.format(username))
    return usr


def get_status():
    """Function for getting the status of the namenode.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    'OK' --> str, the namenode is ok
    """
    #if it's possible to invoke this rpc, it means the status of this namenode is OK 
    return 'OK'


async def listen_for_heartbeats(websocket, path, lock):
    """Function for waiting for heartbeat from datanodes.
    
    Parameters
    ----------
    websocket --> websockets.server.WebSocketServerProtocol class, websocket channel for listening for heartbeats
    path --> str
    lock --> _thread.lock class, the lock for locking a shared resource in multithreading
    
    Returns
    -------
    None
    """
    #the namenode listens for heartbeat from a particular datanode 
    datanode = await websocket.recv()
    logging.info('{} is alive!'.format(datanode))
    global start, you_the_master
    lock.acquire() 
    #if this namenode receives some heartbeats from datanodes, then it's the master one
    you_the_master = True
    #reset the count down for that datanode 
    #the datanode has 10 seconds to send a heartbeat before being considered as down 
    start[datanode] = 10
    lock.release()
    #send an answer to the datanode
    await websocket.send("OK! got it!")
        
        
class HeartbeatThread(threading.Thread):
    """Thread Class for running a web socket server which listens for hearbeats from the datanodes in order to understand if they are in a good status."""
    
    def __init__(self, loop, server):
        threading.Thread.__init__(self)
        self.loop = loop
        self.server = server
        
    def get_loop(self):
        """Method for getting the 'loop' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        
        Returns
        -------
        self.loop --> asyncio.unix_events._UnixSelectorEventLoop class
        """
        return self.loop

    def set_loop(self, loop):
        """Method for setting the 'loop' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        loop --> asyncio.unix_events._UnixSelectorEventLoop class
        
        Returns
        -------
        None
        """
        self.loop = loop
        
    def get_server(self):
        """Method for getting the 'server' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        
        Returns
        -------
        self.server --> websockets.server.Serve class
        """
        return self.server

    def set_server(self, server):
        """Method for setting the 'server' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        server --> websockets.server.Serve class
        
        Returns
        -------
        None
        """
        self.server = server
        
    def run(self):
        """Target method for the class; the server starts.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        
        Returns
        -------
        None
        """
        #start the thread which is responsible of handling the heartbeats from the datanodes
        self.get_loop().run_until_complete(self.get_server())
        self.get_loop().run_forever()
        

class CountdownThread(threading.Thread):
    """Thread Class for running a countdown; if the namenode doesn't receive an heartbeat from a datanode for more than a given time, it will be considered in a down status and the namenode will start a recovery process."""
    
    def __init__(self, lock, dn, client):
        threading.Thread.__init__(self)
        self.lock = lock###
        self.dn = dn###
        self.client = client###
        self.recovered = False
        
    def get_lock(self):
        """Method for getting the 'lock' object attribute.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        
        Returns
        -------
        self.lock --> _thread.lock class
        """
        return self.lock

    def set_lock(self, lock):
        """Method for setting the 'lock' object attribute.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        lock --> _thread.lock class
        
        Returns
        -------
        None
        """
        self.lock = lock
        
    def get_dn(self):
        """Method for getting the 'dn' object attribute.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        
        Returns
        -------
        self.dn --> str, the datanode from which this namenode wait for heartbeats
        """
        return self.dn

    def set_dn(self, dn):
        """Method for setting the 'dn' object attribute.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        dn --> str, the datanode from which this namenode wait for heartbeats
        
        Returns
        -------
        None
        """
        self.dn = dn
        
    def get_client(self):
        """Method for getting the 'client' object attribute.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        
        Returns
        -------
        self.client --> pymongo.mongo_client.MongoClient, the MongoDB client instance
        """
        return self.client

    def set_client(self, client):
        """Method for setting the 'client' object attribute.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        client --> pymongo.mongo_client.MongoClient, the MongoDB client instance
        
        Returns
        -------
        None
        """
        self.client = client
        
    def get_recovered(self):
        """Method for getting the 'recovered' object attribute.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        
        Returns
        -------
        self.recovered --> boolean
        """
        return self.recovered

    def set_recovered(self, recovered):
        """Method for setting the 'recovered' object attribute.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        recovered --> boolean
        
        Returns
        -------
        None
        """
        self.recovered = recovered
        
    def record_trash(self, trash):
        """Allow to sign into mongodb which are the chunks the failed node should delete after recovery (a failed node will not be considered a master/replica node for a chunk anymore bcause other datanodes took its place).
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        trash --> list, the list of dictionaries, with key: failed datanode, value: chunk to delete
        
        Returns
        -------
        ids --> pymongo.results.InsertManyResult class, trash objects to delete just inserted into MongoDB
        """
        inserted_documents = []
        #get the trash collection for recording the chunks which must be deleted when a datanode is recovered after a failure because it must not handle the replicas of a chunk anymore
        trash_col = get_trash(self.get_client())
        ids = None
        #insert the chunks to delete from the failed datanode into MongoDB 
        if len(trash) > 0:
            ids = trash_col.insert_many(trash).inserted_ids
        logging.info('Trash recorded')
        if ids:
            for i in ids:
                doc = trash_col.find_one({'_id': i})
                #insert into the list needed for aligning the other namenodes
                inserted_documents.append((doc, 'trash'))
        #decode for aligning the other slave datanodes metadata database
        #cast the MongoDB ObjectIds to strings
        inserted_documents = decode_mongodoc(inserted_documents, 'inserted_documents')
        #align the slave namenodes metadata database with a rpc call
        for nn in namenodes:
            loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
            with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
                try:
                    proxy.record_trash_s(inserted_documents) #xml rpc call
                except Exception as e:
                    #the namenode is not reachable
                    logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
        return ids
        
    def recover_from_disaster(self):
        """After a node has failed, allow to choose new master/replica nodes for the chunks the failed node was a master/replica.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        
        Returns
        -------
        None
        """
        updatedone_documents = []
        #get the fs (filesystem) MongoDB collection
        fs = get_fs(self.get_client())
        #get all the files for which the failed datanode handles either a primary replica or a secondary replica for the chunks of them
        #these chunks must be replicated on other datanodes
        query = {"$or": [{"chunks.{}".format(self.get_dn().replace('.', '[dot]').replace(':', '[colon]')) : {"$exists" : "true"}}, {"replicas_bkp.{}".format(self.get_dn().replace('.', '[dot]').replace(':', '[colon]')) : {"$exists" : "true"}}]}
        files = fs.find(query)
        c_to_replicate_tot = []
        for f in files:
            c_to_replicate = []
            #the chunks for which the failed datanode handles a primary replica  
            c_to_replace = list(f['chunks'][self.get_dn().replace('.', '[dot]').replace(':', '[colon]')])
            for c in c_to_replace:
                #the first datanode which handles a secondary replica of the chunk becomes the master datanode for that chunk 
                new_master = f['replicas'][c][0]
                #the new master will be removed from the list of the secondary replicas
                remaining_replicas = f['replicas'][c][1:]
                #set the new master for that chunk 
                f['chunks'][new_master].append(c)
                #remove the old failed master datanode
                f['chunks'][self.get_dn().replace('.', '[dot]').replace(':', '[colon]')].remove(c)
                f['chunks_bkp'][c] = new_master
                f['replicas'][c] = remaining_replicas
                f['replicas_bkp'][new_master].remove(c)
                #insert the current chunk in the list of the ones to replicate one time
                c_to_replicate.append({'chunk': c, 'not_good': list(map(lambda x: x.replace('[dot]', '.').replace('[colon]', ':'), f['replicas'][c]))+[self.get_dn()], 'master': new_master.replace('[dot]', '.').replace('[colon]', ':')})
            #the chunks for which the failed datanode handles a secondary replica  
            r_to_replace = list(f['replicas_bkp'][self.get_dn().replace('.', '[dot]').replace(':', '[colon]')])
            for r in r_to_replace:
                #remove the failed datanode from the list of the nodes which handle a seconday replica for that chunk
                f['replicas'][r].remove(self.get_dn().replace('.', '[dot]').replace(':', '[colon]'))
                f['replicas_bkp'][self.get_dn().replace('.', '[dot]').replace(':', '[colon]')].remove(r)
                #insert the current chunk in the list of the ones to replicate one time
                c_to_replicate.append({'chunk': r, 'not_good': list(map(lambda x: x.replace('[dot]', '.').replace('[colon]', ':'), f['replicas'][c]))+[self.get_dn()], 'master': f['chunks_bkp'][r].replace('[dot]', '.').replace('[colon]', ':')})
            #for each chunk to replicate choose a new datanode which handles a secondary replica
            c_to_replicate = choose_recovery_replica(c_to_replicate)
            for c in c_to_replicate:
                #update the MongoDB document which represents the current file with the new values of primary and secondary datanodes 
                f['replicas'][c['chunk']].append(c['new_replica'].replace('.', '[dot]').replace(':', '[colon]'))
                f['replicas_bkp'][c['new_replica'].replace('.', '[dot]').replace(':', '[colon]')].append(c['chunk'])
            del f['chunks'][self.get_dn().replace('.', '[dot]').replace(':', '[colon]')]
            del f['replicas_bkp'][self.get_dn().replace('.', '[dot]').replace(':', '[colon]')]
            #update the MongoDB file document with the new values
            fs.update_one({ '_id': f['_id'] }, {'$set': {'chunks': f['chunks'], 'chunks_bkp': f['chunks_bkp'], 'replicas': f['replicas'], 'replicas_bkp': f['replicas_bkp']}})
            #insert into the list needed for aligning the other namenodes
            updatedone_documents.append(({ '_id': f['_id'] }, {'$set': {'chunks': f['chunks'], 'chunks_bkp': f['chunks_bkp'], 'replicas': f['replicas'], 'replicas_bkp': f['replicas_bkp']}}, 'fs'))
            c_to_replicate_tot.extend(c_to_replicate)
        start_recovery(c_to_replicate_tot)
        #fill the trash collection with the chunks to delete from teh failed datanode
        #when the failed datanode will be up again, the primary and secondary replicas handled by it mu be deleted because it's not the handler anymore, some other datanode took its place
        trash = list(map(lambda x: {'datanode': self.get_dn(), 'chunk': x['chunk']}, c_to_replicate_tot))
        ids = self.record_trash(trash)
        #mark the node as recovered 
        self.set_recovered(True)
        logging.info('Disaster recovered')
        #decode for aligning the other slave datanodes metadata database
        #cast the MongoDB ObjectIds to strings
        updatedone_documents = decode_mongodoc(updatedone_documents, 'updatedone_documents')
        #align the slave namenodes metadata database with a rpc call
        for nn in namenodes:
            loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
            with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
                try:
                    proxy.recover_from_disaster_s(updatedone_documents) #xml rpc call
                except Exception as e:
                    #the namenode is not reachable
                    logging.error("Something went wrong during slave namenodes alignment: {}".format(e)) 
        return
    
    def flush_trash(self):
        """After a failed node has recovered after a failure, allow to delete chunks for which the recovered node was a master/replica node because it's not a master/replica anymore it's been replaced by other nodes.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        
        Returns
        -------
        None
        """
        deletemany_documents = []
        #get the MongoDB trash collection
        #inside this collection there are the chunks which must be deleted from a datanode after its failure
        trash_col = get_trash(self.get_client())
        chunks_to_del = trash_col.find({'datanode': self.get_dn()})
        chunks_to_del = list(map(lambda x: x['chunk'], chunks_to_del))
        #delete the chunks from the recovered datanode
        start_flush(chunks_to_del, self.get_dn())
        logging.info('Trash Flushed')
        #delete the chunks from the trash collection
        ids = trash_col.delete_many({'datanode': self.get_dn()})
        #insert into the list needed for aligning the other namenodes
        deletemany_documents.append(({'datanode': self.get_dn()}, 'trash'))
        #align the slave namenodes metadata database with a rpc call
        for nn in namenodes:
            loc_namenode = 'http://{}:{}/'.format(nn['host'], nn['port'])
            with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
                try:
                    proxy.flush_trash_s(deletemany_documents) #xml rpc call
                except Exception as e:
                    #the namenode is not reachable
                    logging.error("Something went wrong during slave namenodes alignment: {}".format(e))
        return
        
    def run(self):
        """Target method for the class; it waits N seconds before considering a node as dead; if a node has failed, it will be replaced with other nodes.
        
        Parameters
        ----------
        self --> CountdownThread class, self reference to the object instance
        
        Returns
        -------
        None
        """
        global start, you_the_master 
        while True:
            #if this namenode is not the master, then it must not hanlde the heartbeats from datanodes
            #only the master namenode must handle the heartbeats
            if not you_the_master:
                continue #in this case, this namenode is not the current master
            if start[self.get_dn()]>0: #the datanode still has some time to send heartbeats before been considered as down 
                #if the datanode has failed, the chunks handled by it had been recovered and now it's running again, then start flush process
                if self.get_recovered():
                    self.flush_trash()
                #set recovered as False, the situation is returned normal
                self.set_recovered(False)
                logging.info('{}: seconds before considered down {}'.format(self.get_dn(), start[self.get_dn()]))
                #wait 1 second
                time.sleep(1)
                self.get_lock().acquire() 
                #decrement the countdown before considering the datanode as failed
                start[self.get_dn()] -= 1
                self.get_lock().release()
            else: #the datanode has expired the time before been considered down
                logging.error('{} is down!'.format(self.get_dn()))
                #the datanode is down --> recover from disaster
                if not self.get_recovered():
                    #check that the up datanodes are at least the number of replica set desired
                    if len(list(filter(lambda x: x>0, start.values()))) >= get_replica_set():
                        self.recover_from_disaster()
                    #there aren't enough datanodes available, e.g. 2 datanodes up and 3 as replica factor
                    else: 
                        logging.critical('Not enough datanodes available to guarantee the replica set')
                time.sleep(10)      
    
    
class ServerThread(threading.Thread):
    """Thread Class for running a RPC server which listens for commands by the clients."""
    
    def __init__(self):
        threading.Thread.__init__(self)
        self.server = SimpleXMLRPCServer((namenode['host'], namenode['port']), allow_none=True)
        #register all the rpc functions that can be invoked remotely by a client
        self.server.register_function(mkdir, 'mkdir')
        self.server.register_function(touch, 'touch')
        self.server.register_function(ls, 'ls')
        self.server.register_function(rm, 'rm')
        self.server.register_function(rmr, 'rmr')
        self.server.register_function(get_file, 'get_file')
        self.server.register_function(cp, 'cp')
        self.server.register_function(mv, 'mv')
        self.server.register_function(count, 'count')
        self.server.register_function(countr, 'countr')
        self.server.register_function(du, 'du')
        self.server.register_function(chown, 'chown')
        self.server.register_function(chgrp, 'chgrp')
        self.server.register_function(chmod, 'chmod')
        self.server.register_function(put_file, 'put_file')
        self.server.register_function(mkfs, 'mkfs')
        self.server.register_function(groupadd, 'groupadd')
        self.server.register_function(useradd, 'useradd')
        self.server.register_function(groupdel, 'groupdel')
        self.server.register_function(userdel, 'userdel')
        self.server.register_function(passwd, 'passwd')
        self.server.register_function(usermod, 'usermod')
        self.server.register_function(get_user, 'get_user') 
        self.server.register_function(mkdir_s, 'mkdir_s')
        self.server.register_function(touch_s, 'touch_s')
        self.server.register_function(rm_s, 'rm_s')
        self.server.register_function(rmr_s, 'rmr_s')
        self.server.register_function(cp_s, 'cp_s')
        self.server.register_function(mv_s, 'mv_s')
        self.server.register_function(put_file_s, 'put_file_s')
        self.server.register_function(chown_s, 'chown_s')
        self.server.register_function(chgrp_s, 'chgrp_s')
        self.server.register_function(chmod_s, 'chmod_s')
        self.server.register_function(groupadd_s, 'groupadd_s')
        self.server.register_function(useradd_s, 'useradd_s')
        self.server.register_function(groupdel_s, 'groupdel_s')
        self.server.register_function(userdel_s, 'userdel_s')
        self.server.register_function(passwd_s, 'passwd_s')
        self.server.register_function(usermod_s, 'usermod_s')
        self.server.register_function(mkfs_s, 'mkfs_s')  
        self.server.register_function(record_trash_s, 'record_trash_s')
        self.server.register_function(flush_trash_s, 'flush_trash_s')
        self.server.register_function(recover_from_disaster_s, 'recover_from_disaster_s')
        self.server.register_function(get_status, 'get_status')
        
    def get_server(self):
        """Method for getting the 'server' object attribute.
        
        Parameters
        ----------
        self --> ServerThread class, self reference to the object instance
        
        Returns
        -------
        self.server --> xmlrpc.server.SimpleXMLRPCServer, server instance for the xmlrpc
        """
        return self.server

    def set_server(self, server):
        """Method for setting the 'server' object attribute.
        
        Parameters
        ----------
        self --> ServerThread class, self reference to the object instance
        server --> xmlrpc.server.SimpleXMLRPCServer, server instance for the xmlrpc
        
        Returns
        -------
        None
        """
        self.server = server

    def run(self):
        """Target method for the class; the server starts.
        
        Parameters
        ----------
        self --> ServerThread class, self reference to the object instance
        
        Returns
        -------
        None
        """
        logging.info('Listening on port {}...'.format(namenode['port']))
        #start the server
        self.get_server().serve_forever()
    
    
def main():
    """Main function, the entry point."""
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    #check if the number of replica set is at least equal to the number of datanodes 
    if get_replica_set() > len(get_datanodes_list()):
        logging.critical('Impossible to start! Not enough datanodes to handle the replica set')
        return
    logging.info('Namenode started')
    #create the server thread for handling rpc invokations
    server_thread = ServerThread()
    server_thread.start()
    lock = threading.Lock()
    new_loop = asyncio.new_event_loop()
    bound_handler = functools.partial(listen_for_heartbeats, lock=lock)
    start_server = websockets.serve(bound_handler, namenode['host'], namenode['port_heartbeat'], loop=new_loop)
    #create the thread which listens for heartbeats from the datanodes
    heartbeat_thread = HeartbeatThread(new_loop, start_server)
    heartbeat_thread.start()
    global countdown_threads
    #for each datanode, create a thread responsible to check and evaluate its status
    #and decides if it's good to start the recovery process
    for dn in countdown_threads:
        countdown_threads[dn] = CountdownThread(lock, dn, client)
        countdown_threads[dn].start()
    logging.info('Datanodes countdowns started')
    server_thread.join()
    heartbeat_thread.join()
    for dn in countdown_threads:
        countdown_threads[dn].join()
    
    
if __name__ == '__main__':
    main()
