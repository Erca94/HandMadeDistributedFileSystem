from getpass import getpass
import re
import os
from requests import put, get
from pathlib import Path
from itertools import product
import xmlrpc.client
import logging

import fs_handler as fsh
import initializer as ini
import users_groups_handler as ugh
from collections_handler import get_users
from exceptions import InvalidSyntaxException, CommandNotFoundException, UserNotFoundException, AccessDeniedException, NotFoundException, RootNecessaryException, NotDirectoryException, NotParentException, AlreadyExistsException, NotEmptyException, AccessDeniedAtLeastOneException, InvalidModException, GroupAlreadyExistsException, UserAlreadyExistsException, GroupNotFoundException, MainUserGroupException
import chunks_handler as ch
from utils import get_chunk_size, get_datanodes, get_namenodes

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
#get datanodes and namenodes settings
datanodes = [dn for dn in get_datanodes().values()]
namenodes = [nn for nn in get_namenodes().values()]

def mkdir(cmd, grp, loc_namenode):
    """Allow to execute mkdir command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path, parent = cmd.split()
    if parent == 'T':
        parent = True
    else:
        parent = False
    #call the mkdir command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            directory_id = proxy.mkdir(path, required_by, grp, parent) #no print
        except xmlrpc.client.Fault as err:
            #the user is not allowed to create the directory
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the user want to create a directory with a name which has already been used for a file 
            if 'NotDirectoryException' in err.faultString:
                logging.warning(err.faultString)
            #the user want to create a directory in path which does not exist and the opetion parent has not been set
            if 'NotParentException' in err.faultString:
                logging.warning(err.faultString)
            #a directory with the same name already exists
            if 'AlreadyExistsException' in err.faultString:
                logging.warning(err.faultString)
    return


def touch(cmd, grp, loc_namenode):
    """Allow to execute touch command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path = cmd.split()
    #call the touch command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            file_id = proxy.touch(path, required_by, grp) #print
        except xmlrpc.client.Fault as err:
            #the user is not allowed to touch the file/directory
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the path in which the user want to touch the file/directory does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
    return


def ls(cmd, grp, loc_namenode):
    """Allow to execute ls command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path = cmd.split()
    #call the ls command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            res = proxy.ls(path, required_by, grp) #gives back result
            #for any file or directory in the result, print the type, the creation timestamp, the owner, the group and the mode 
            for r in res:
                print(r['type'], r['creation'], r['own'], r['grp'], '{}{}{}'.format(r['mod']['own'],r['mod']['grp'],r['mod']['others']), r['name'])
        except xmlrpc.client.Fault as err:
            #the user is not allowed to list the file/directory
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the resource does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
    return


def rm(cmd, grp, loc_namenode):
    """Allow to execute rm command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path = cmd.split()
    #call the rm command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            (deleted, hosts) = proxy.rm(path, required_by, grp) #no print
        except xmlrpc.client.Fault as err:
            #the user is not allowed to remove
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the path does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            #the directory the user want to delete is not empty
            if 'NotEmptyException' in err.faultString:
                logging.warning(err.faultString)
            #the directory the user want to delete is the root
            if 'RootDirectoryException' in err.faultString:
                logging.warning(err.faultString)
            return
    #if some file has been deleted, delete the chunks from the datanodes 
    if deleted:
        ch.delete_chunks(deleted, hosts)
    return


def rmr(cmd, grp, loc_namenode):
    """Allow to execute rmr command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path = cmd.split()
    #call the rm command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            (deleted,hosts) = proxy.rmr(path, required_by, grp) #no print
        except xmlrpc.client.Fault as err:
            #the user is not allowed to remove the directory inserted
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the path does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            #the user is not allowed to remove some nested resource
            if 'AccessDeniedAtLeastOneException' in err.faultString:
                logging.warning(err.faultString)
            #the directory the user want to delete is the root
            if 'RootDirectoryException' in err.faultString:
                logging.warning(err.faultString)
            return
    #if some file has been deleted, delete the chunks from the datanodes 
    if deleted:
        ch.delete_chunks(deleted, hosts)
    return


def get_file(cmd, grp, loc_namenode):
    """Allow to execute get_file command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path, local_path = cmd.split()
    #call the get_file command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            file = proxy.get_file(path, required_by, grp) #gives back result
        except xmlrpc.client.Fault as err:
            #the user is not allowed to get the file
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the file does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            return
    chunks = []
    #create the list of the chunks and the datanodes which handle the replicas of every chunk
    for dn in file['chunks']:
        for c in file['chunks'][dn]:
            datanodes = [dn] + file['replicas'][c]
            #insert into the list the chunk, the list of the datanodes which handle the chunk and the sequence number
            chunks.append((datanodes,c,int(c.split('_')[1])))
    #sort in base on the sequence number
    chunks.sort(key = lambda x: x[2])
    #get the content of every chunk which composes the entire file
    try:
        tot = ch.get_chunks(chunks)
    except GetFileException as e:
        logging.warning(e.message)
        return
    #write the file content in output on the local filesystem
    try:
        lf = open(local_path, 'wb')
        for k in sorted(tot.keys()):
            lf.write(tot[k])
        lf.close()
    except Exception as e:
        logging.warning(e)
    return


def get_chunks(cmd, grp, loc_namenode):
    """Allow to get the chunks of a file and to print on the console.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path = cmd.split()
    #call the get_file command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            file = proxy.get_file(path, required_by, grp) #gives back result
        except xmlrpc.client.Fault as err:
            #the user is not allowed to get the file
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the file does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            return
    #print the namenodes which handle the chunks which compose the file
    print('file: {}'.format(file['name']))
    for chunk in sorted(file['chunks_bkp']):
        master_replica = file['chunks_bkp'][chunk].replace('[dot]', '.').replace('[colon]', ':')
        slave_replica = file['replicas'][chunk]
        slave_replica = list(map(lambda x: x.replace('[dot]', '.').replace('[colon]', ':'), slave_replica))
        print('{} --> master replica: {}, secondary replicas: {}'.format(chunk, master_replica, ', '.join(slave_replica)))
    return


def cat(cmd, grp, loc_namenode):
    """Allow to print a file content on the console.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path = cmd.split()
    #call the get_file command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            file = proxy.get_file(path, required_by, grp) #gives back result
        except xmlrpc.client.Fault as err:
            #the user is not allowed to get the file
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the file does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            return
    chunks = []
    #create the list of the chunks and the datanodes which handle the replicas of every chunk
    for dn in file['chunks']:
        for c in file['chunks'][dn]:
            datanodes = [dn] + file['replicas'][c]
            #insert into the list the chunk, the list of the datanodes which handle the chunk and the sequence number
            chunks.append((datanodes,c,int(c.split('_')[1])))
    #sort in base on the sequence number
    chunks.sort(key = lambda x: x[2])
    #get the content of every chunk which composes the entire file
    try:
        tot = ch.get_chunks(chunks)
    except GetFileException as e:
        logging.warning(e.message)
        return
    #print the content of the chunks in ouput
    for k in sorted(tot.keys()):
        print(tot[k].decode('ISO-8859-1'), end='')
    print('')
    return


def head(cmd, grp, loc_namenode):
    """Allow to print the first N bytes of a file content on the console.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, n_bytes, path = cmd.split()
    #the number of start bytes the user want to read
    n_bytes = int(n_bytes)
    #call the get_file command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            file = proxy.get_file(path, required_by, grp) #gives back result
        except xmlrpc.client.Fault as err:
            #the user is not allowed to get the file
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the file does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            return
    chunks = []
    #create the list of the chunks and the datanodes which handle the replicas of every chunk
    for dn in file['chunks']:
        for c in file['chunks'][dn]:
            datanodes = [dn] + file['replicas'][c]
            #insert into the list the chunk, the list of the datanodes which handle the chunk and the sequence number
            chunks.append((datanodes,c,int(c.split('_')[1])))
    #sort in base on the sequence number
    chunks.sort(key = lambda x: x[2])
    #the number of chunks which contain the bytes the user want to read
    n_chunks = int(n_bytes/get_chunk_size())
    #the number of remaning bytes which don't fit into the last chunk
    remain_bytes = n_bytes%get_chunk_size()
    #get the content of chunks selected
    try:
        tot = ch.get_chunks(chunks[:n_chunks+1])
    except GetFileException as e:
        logging.warning(e.message)
        return
    tmp = sorted(tot.keys())
    #print the content of the chunks in ouput
    for k in tmp[:-1]:
        print(tot[k].decode('ISO-8859-1'), end='')
    #for the last chunk, print only the needed first bytes
    print(tot[tmp[-1]][:remain_bytes].decode('ISO-8859-1'), end='')
    print('')
    return


def tail(cmd, grp, loc_namenode):
    """Allow to print the last N bytes of a file content on the console.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, n_bytes, path = cmd.split()
    #the number of end bytes the user want to read
    n_bytes = int(n_bytes)
    #call the get_file command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            file = proxy.get_file(path, required_by, grp) #gives back result
        except xmlrpc.client.Fault as err:
            #the user is not allowed to get the file
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the file does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            return
    chunks = []
    #create the list of the chunks and the datanodes which handle the replicas of every chunk
    for dn in file['chunks']:
        for c in file['chunks'][dn]:
            datanodes = [dn] + file['replicas'][c]
            #insert into the list the chunk, the list of the datanodes which handle the chunk and the sequence number
            chunks.append((datanodes,c,int(c.split('_')[1])))
    #sort in base on the sequence number
    chunks.sort(key = lambda x: x[2])
    #the number of chunks which contain the bytes the user want to read
    n_chunks = int(n_bytes/get_chunk_size())
    #the number of remaning bytes which don't fit into the last chunk
    remain_bytes = n_bytes%get_chunk_size()
    #get the content of chunks selected
    try:
        tot = ch.get_chunks(chunks[-(n_chunks+1):])
    except GetFileException as e:
        logging.warning(e.message)
        return 
    tmp = sorted(tot.keys())
    #print the content of the chunks in ouput
    first = True
    for k in tmp:
        #for the first chunk to print, you must not take the entire content, but only the last remain_bytes
        if first:
            print(tot[k][-remain_bytes:].decode('ISO-8859-1'), end='')
            first = False
        #for the other final chunks, you must print the entire content
        else:
            print(tot[k].decode('ISO-8859-1'), end='')
    print('')
    return


def cp(cmd, grp, loc_namenode):
    """Allow to execute cp command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, orig, dest = cmd.split()
    #call the cp command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            (old_id, new_id, hosts) = proxy.cp(orig, dest, required_by, grp)
        except xmlrpc.client.Fault as err:
            #the user is not allowed to read the source file or to write into the destination
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #either the source or the destination does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            #a directory with the same name already exists
            if 'AlreadyExistsDirectoryException' in err.faultString:
                logging.warning(err.faultString)
            #a file with the same name already exists
            if 'AlreadyExistsException' in err.faultString:
                logging.warning(err.faultString)
            return
    #start deleting the chunks associated to a file which has been overwritten 
    #if id_to_del and dn_to_del_from:
    #    ch.delete_chunks([id_to_del], dn_to_del_from)
    #start copying the source chunks contents into the new chunks 
    ch.copy_chunks(old_id, new_id, hosts)
    return


def mv(cmd, grp, loc_namenode):
    """Allow to execute mv command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, orig, dest = cmd.split()
    #call the mv command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            proxy.mv(orig, dest, required_by, grp) #no print, no result
        except xmlrpc.client.Fault as err:
            #the user is not allowed to move the resource
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the path does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            #a file with the same name already exists
            if 'AlreadyExistsException' in err.faultString:
                logging.warning(err.faultString)
            #a directory with the same name already exists
            if 'AlreadyExistsDirectoryException' in err.faultString:
                logging.warning(err.faultString)
            #tried to rename the root directory
            if 'RootDirectoryException' in err.faultString:
                logging.warning(err.faultString)
            #tried to move a directory into a subdirectory of itself
            if 'ItselfSubdirException' in err.faultString:
                logging.warning(err.faultString)
    return


def count(cmd, grp, loc_namenode):
    """Allow to execute count command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path = cmd.split()
    #call the count command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            res = proxy.count(path, required_by, grp) #gives back result
            print('directories count: {}'.format(res['DIR_COUNT']))
            print('files count: {}'.format(res['FILE_COUNT']))
        except xmlrpc.client.Fault as err:
            #the user is not allowed to count the files and subdirectories in the directory
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the directory does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
    return


def countr(cmd, grp, loc_namenode):
    """Allow to execute countr command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path = cmd.split()
    #call the countr command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            res = proxy.countr(path, required_by, grp) #gives back result
            print('directories count: {}'.format(res['DIR_COUNT']))
            print('files count: {}'.format(res['FILE_COUNT']))
        except xmlrpc.client.Fault as err:
            #the user is not allowed to count the files and subdirectories in the directory
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the directory does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            #the user is not allowed to count the files and subdirectories in a nested subdirectory
            if 'AccessDeniedAtLeastOneException' in err.faultString:
                logging.warning(err.faultString) 
    return


def du(cmd, grp, loc_namenode):
    """Allow to execute du command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path = cmd.split()
    #call the du command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            size = proxy.du(path, required_by, grp) #gives back result
            print('total size: {} B'.format(size))
        except xmlrpc.client.Fault as err:
            #the user is not allowed to get the disk usage for the directory/file
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the directory/file does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            #the user is not allowed to get the disk usage on some nested files and subdirectories
            if 'AccessDeniedAtLeastOneException' in err.faultString:
                logging.warning(err.faultString)
    return


def chown(cmd, grp, loc_namenode):
    """Allow to execute chown command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path, new_own = cmd.split()
    #call the chown command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            proxy.chown(path, new_own, required_by, grp) #no print, no result
        except xmlrpc.client.Fault as err:
            #the user is not allowed to change the owner of the resource
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the new own is not a user
            if 'UserNotFoundException' in err.faultString:
                logging.warning(err.faultString)
                return
            #the path does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
    return


def chgrp(cmd, grp, loc_namenode):
    """Allow to execute chgrp command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path, new_grp = cmd.split()
    #call the chgrp command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            proxy.chgrp(path, new_grp, required_by, grp) #no print, no result
        except xmlrpc.client.Fault as err:
            #the user is not allowed to change the group of the resource
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the new own is not a user
            if 'GroupNotFoundException' in err.faultString:
                logging.warning(err.faultString)
                return
            #the path does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
    return


def chmod(cmd, grp, loc_namenode):
    """Allow to execute chmod command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, path, new_mod = cmd.split()
    #call the chmod command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            proxy.chmod(path, new_mod, required_by, grp) #no print, no result
        except xmlrpc.client.Fault as err:
            #the user is not allowed to change the premissions of the resource
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the path does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            #the mode permissions given is not valid
            if 'InvalidModException' in err.faultString:
                logging.warning(err.faultString)
    return


def put_file(cmd, grp, loc_namenode):
    """Allow to execute put_file command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, local_file_path, file_path = cmd.split()
    #get the binary content of the local file 
    try:
        size = os.path.getsize(local_file_path)
        with open(local_file_path, 'rb') as f:
            content = f.read()
    except Exception as e:
        logging.warning(e)
        return
    #call the put_file command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            (fid,chunks_to_write, replicas) = proxy.put_file(file_path, size, required_by, grp)
        except xmlrpc.client.Fault as err:
            #the user is not allowed to put the local file into the inserted path
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the directory in which put the file does not exist
            if 'NotFoundException' in err.faultString:
                logging.warning(err.faultString)
            #a file with the same name already exists
            if 'AlreadyExistsException' in err.faultString:
                logging.warning(err.faultString)
            #a directory with the same name of the file already exists
            if 'AlreadyExistsDirectoryException' in err.faultString:
                logging.warning(err.faultString)
            return
    #write the content of the local file into the datanodes
    ch.write_chunks(chunks_to_write, content, replicas)
    return


def mkfs(cmd, grp, loc_namenode):
    """Allow to execute mkfs command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by = cmd.split()
    #call the mkfs command with a rpc (initialization of the filesystem)
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            res = proxy.mkfs(required_by) #no print
        except xmlrpc.client.Fault as err:
            #only the root can perform this kind of operation
            if 'RootNecessaryException' in err.faultString:
                logging.warning(err.faultString)
    return


def groupadd(cmd, grp, loc_namenode):
    """Allow to execute groupadd command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, group = cmd.split()
    #call the groupadd command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            grp_id = proxy.groupadd(required_by, group) #no print
        except xmlrpc.client.Fault as err:
            #the user is not allowed to add the group
            if 'RootNecessaryException' in err.faultString:
                logging.warning(err.faultString)
            #the user want to add a group which already exists
            if 'GroupAlreadyExistsException' in err.faultString:
                logging.warning(err.faultString)
    return


def useradd(cmd, grp, loc_namenode):
    """Allow to execute useradd command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, username, password = cmd.split()
    #call the useradd command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            res = proxy.useradd(required_by, username, password) #no print
        except xmlrpc.client.Fault as err:
            #the user is not allowed to add the user
            if 'RootNecessaryException' in err.faultString:
                logging.warning(err.faultString)
            #the user want to add a user whose name already exists for a group
            if 'GroupAlreadyExistsException' in err.faultString:
                logging.warning(err.faultString)
            #the user want to add a user which already exists
            if 'UserAlreadyExistsException' in err.faultString:
                logging.warning(err.faultString)
    return


def groupdel(cmd, grp, loc_namenode):
    """Allow to execute groupdel command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, group = cmd.split()
    #call the groupdel command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            res = proxy.groupdel(required_by, group) #no print
        except xmlrpc.client.Fault as err:
            #the user is not allowed to delete the group
            if 'RootNecessaryException' in err.faultString:
                logging.warning(err.faultString)
            #the group the user want to delete does not exist
            if 'GroupNotFoundException' in err.faultString:
                logging.warning(err.faultString)
            #the group the user want to delete is the main group of a user (must delete the user first)
            if 'MainUserGroupException' in err.faultString:
                logging.warning(err.faultString)
    return


def userdel(cmd, grp, loc_namenode):
    """Allow to execute userdel command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, username = cmd.split()
    #call the userdel command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            res = proxy.userdel(required_by, username) #no print
        except xmlrpc.client.Fault as err:
            #the user is not allowed to delete the user
            if 'RootNecessaryException' in err.faultString:
                logging.warning(err.faultString)
            #the user the user want to delete does not exist
            if 'UserNotFoundException' in err.faultString:
                logging.warning(err.faultString)
    return


def passwd(cmd, grp, loc_namenode):
    """Allow to execute passwd command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    f, required_by, username, new_password = cmd.split()
    #call the passwd command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            proxy.passwd(required_by, username, new_password) #print, no res
        except xmlrpc.client.Fault as err:
            #the user is not allowed to change the password of a user
            if 'AccessDeniedException' in err.faultString:
                logging.warning(err.faultString)
            #the user does not exist
            if 'UserNotFoundException' in err.faultString:
                logging.warning(err.faultString)
    return


def usermod(cmd, grp, loc_namenode):
    """Allow to execute usermod command.
    
    Parameters
    ----------
    cmd --> str, the command
    grp --> list, the list of groups to which the user belongs
    loc_namenode --> str, the master namenode in the moment in which the command has been invoked
    
    Returns
    -------
    None
    """
    params = cmd.split()
    f, required_by, username, groups, operation = params[0], params[1], params[2], params[3:-1], params[-1]
    #call the usermod command with a rpc
    with xmlrpc.client.ServerProxy(loc_namenode) as proxy:
        try:
            #create a list of groups without repetitions
            groups = list(set(groups))
            proxy.usermod(required_by, username, groups, operation) #print, no res
        except xmlrpc.client.Fault as err:
            #the user is not allowed to the change the groups list a user belongs 
            if 'RootNecessaryException' in err.faultString:
                logging.warning(err.faultString)
            #the user does not exist
            if 'UserNotFoundException' in err.faultString:
                logging.warning(err.faultString)
            #at least a group does not exist
            if 'GroupNotFoundException' in err.faultString:
                logging.warning(err.faultString)
    return


def status():
    """Allow to execute status command.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    None
    """
    print('-----------------')
    print('namenodes status:')
    print('-----------------')
    for nn in namenodes:
        #call the status command with a rpc for each namenode
        with xmlrpc.client.ServerProxy('http://{}:{}/'.format(nn['host'], nn['port'])) as proxy:
            #if the rpc has been invoked without problems then the status of the namenode is ok, otherwise is ko
            try:
                stat = proxy.get_status() 
                print('{}:{} --> {}, priority --> {}'.format(nn['host'], nn['port'], stat, nn['priority']))
            except:
                print('{}:{} --> KO, priority --> {}'.format(nn['host'], nn['port'], nn['priority']))
    print('')
    print('-----------------')
    print('datanodes status:')
    print('-----------------')
    for dn in datanodes:
        #call the status command with a rpc for each datanode
        with xmlrpc.client.ServerProxy('http://{}:{}/'.format(dn['host'], dn['port_gencom'])) as proxy:
            #if the rpc has been invoked without problems then the status of the datanode is ok, otherwise is ko
            try:
                stat = proxy.get_status() 
                print('{}:{} --> {}'.format(dn['host'], dn['port'], stat))
            except:
                print('{}:{} --> KO'.format(dn['host'], dn['port']))
    return

#these are the available commands a user can call with the distributed filesystem
available_cmds = {
    'mkdir': {
        'func': mkdir, 
        'pattern': '^mkdir [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+ [FT]$', 
        'example': 'mkdir <USERNAME> <PATH> <PARENT>'},
    'touch': {
        'func': touch, 
        'pattern': '^touch [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'touch <USERNAME> <PATH>'},
    'ls': {
        'func': ls, 
        'pattern': '^ls [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'ls <USERNAME> <PATH>'},
    'rm': {
        'func': rm, 
        'pattern': '^rm [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'rm <USERNAME> <PATH>'},
    'rmr': {
        'func': rmr, 
        'pattern': '^rmr [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'rmr <USERNAME> <PATH>'},
    'get_file': {
        'func': get_file, 
        'pattern': '^get_file [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'get_file <USERNAME> <PATH> <LOCAL_FILE_PATH>'},
    'get_chunks': {
        'func': get_chunks, 
        'pattern': '^get_chunks [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'get_chunks <USERNAME> <PATH>'},
    'cat': {
        'func': cat, 
        'pattern': '^cat [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'cat <USERNAME> <PATH>'},
    'head': {
        'func': head, 
        'pattern': '^head [A-Za-z0-9_]+ [1-9][0-9]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'head <USERNAME> <NUMBER_OF_BYTES> <PATH>'},
    'tail': {
        'func': tail, 
        'pattern': '^tail [A-Za-z0-9_]+ [1-9][0-9]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'tail <USERNAME> <NUMBER_OF_BYTES> <PATH>'},
    'cp': {
        'func': cp, 
        'pattern': '^cp [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'cp <USERNAME> <ORIG_PATH> <DEST_PATH>'},
    'mv': {
        'func': mv, 
        'pattern': '^mv [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'mv <USERNAME> <ORIG_PATH> <DEST_PATH>'},
    'count': {
        'func': count, 
        'pattern': '^count [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'count <USERNAME> <PATH>'},
    'countr': {
        'func': countr, 
        'pattern': '^countr [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'countr <USERNAME> <PATH>'},
    'du': {
        'func': du, 
        'pattern': '^du [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'du <USERNAME> <PATH>'},
    'chown': {
        'func': chown, 
        'pattern': '^chown [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+ [A-Za-z0-9_]+$', 
        'example': 'chown <USERNAME> <PATH> <NEW_OWN>'},
    'chgrp': {
        'func': chgrp, 
        'pattern': '^chgrp [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+ [A-Za-z0-9_]+$', 
        'example': 'chgrp <USERNAME> <PATH> <NEW_GRP>'},
    'chmod': {
        'func': chmod, 
        'pattern': '^chmod [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+ [0-9]+$', 
        'example': 'chmod <USERNAME> <PATH> <NEW_MOD>'},
    'put_file': {
        'func': put_file, 
        'pattern': '^put_file [A-Za-z0-9_]+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+ (/([A-Za-z0-9_\-\.]+/)*([A-Za-z0-9_\-\.]+)*/*)+$', 
        'example': 'put_file <USERNAME> <LOCAL_FILE_PATH> <PATH>'},
    'mkfs': {
        'func': mkfs, 
        'pattern': '^mkfs [A-Za-z0-9_]+$', 
        'example': 'mkfs <USERNAME>'},
    'groupadd': {
        'func': groupadd, 
        'pattern': '^groupadd [A-Za-z0-9_]+ [A-Za-z0-9_]+$', 
        'example': 'groupadd <USERNAME> <GROUP>'},
    'useradd': {
        'func': useradd, 
        'pattern': '^useradd [A-Za-z0-9_]+ [A-Za-z0-9_]+ [^ ]+$', 
        'example': 'useradd <USERNAME> <USER> <PASSWORD>'},
    'groupdel': {
        'func': groupdel, 
        'pattern': '^groupdel [A-Za-z0-9_]+ [A-Za-z0-9_]+$', 
        'example': 'groupdel <USERNAME> <GROUP>'},
    'userdel': {
        'func': userdel, 
        'pattern': '^userdel [A-Za-z0-9_]+ [A-Za-z0-9_]+$', 
        'example': 'userdel <USERNAME> <USER>'},
    'passwd': {
        'func': passwd, 
        'pattern': '^passwd [A-Za-z0-9_]+ [A-Za-z0-9_]+ [^ ]+$', 
        'example': 'passwd <USERNAME> <USER> <NEW_PASSWORD>'},
    'usermod': {
        'func': usermod, 
        'pattern': '^usermod [A-Za-z0-9_]+ [A-Za-z0-9_]+ ([A-Za-z0-9_]+ )+[\+\-]$', 
        'example': 'usermod <USERNAME> <USER> <GROUPS>{1,N} <OPERATION>'},
    'status': {
        'func': status,
        'pattern': '^status [A-Za-z0-9_]+$',
        'example': 'status <USERNAME>'}
}


def validate_cmd(cmd):
    """Function for validating the syntax of a command in input.
    
    Parameters
    ----------
    cmd --> str, the command
    
    Returns
    -------
    available_cmds[cmd.split()[0]]['func'] --> function, the function required by the command
    """
    try:
        #get the string pattern for the command invoked
        pattern = available_cmds[cmd.split()[0]]['pattern']
        pattern = re.compile(pattern)
        #if the command matchs the pattern then ok, otherwise the syntax is invalid 
        if pattern.match(cmd):
            return available_cmds[cmd.split()[0]]['func']
        else:
            raise InvalidSyntaxException(available_cmds[cmd.split()[0]]['example'])
    except:
        #the command does not exist
        raise CommandNotFoundException()
        

def get_master_namenode():
    """Function for retrieving the master namenode at the moment the command is invoked; the function will ask each datanode which is the current namenode and use quorum to understand which is the real master.
    
    Parameters
    ----------
    
    Returns
    -------
    max(quorum,key=quorum.count) --> str, the current master namenode
    """
    quorum = []
    for dn in datanodes:
        uri = "http://{}:{}/".format(dn['host'], dn['port_gencom'])
        try:
            #get the current master namenode for the datanode with a rpc
            with xmlrpc.client.ServerProxy(uri, allow_none=True) as proxy:
                master = proxy.get_master_namenode()
                quorum.append(master)
        except ConnectionRefusedError:
            #the datanode is down currently
            logging.error("datanode {}:{} down!!!".format(dn['host'], dn['port_gencom']))
            pass
    #the master namenode is the one which is master for the highest number of datanodes 
    return max(quorum,key=quorum.count) 


def exec_cmd(cmd):
    """Allow the execution of a command in input.
    
    Parameters
    ----------
    cmd --> str, the command
    
    Returns
    -------
    None
    """
    try:
        func = validate_cmd(cmd) #check the syntax of the command inserted by the user
    except (InvalidSyntaxException, CommandNotFoundException) as e:
        logging.warning(e.message)
        return
    username = cmd.split()[1]
    loc_namenode = 'http://{}/'.format(get_master_namenode()) #get the current master namenode
    try:
        #get the user object from MongoDB with a rpc
        with xmlrpc.client.ServerProxy(loc_namenode, allow_none=True) as proxy:
            usr = proxy.get_user(username)
            if not usr:
                raise UserNotFoundException(username) #the user does not exist
    except ConnectionRefusedError:
        logging.error('namenode down') #the master namenode is down
        return
    password = getpass('Insert your password, please:')
    #verify if the password inserted by the user is the same of the user MongoDB object
    if usr['password'] != password:
        raise AccessDeniedException()
    if func.__name__ == 'status':
        if username == 'root': 
            func()
            return
        else:
            raise RootNecessaryException() #only the root can perform the status function
    #execute the function
    func(cmd, usr['groups'], loc_namenode)
    return
    
    