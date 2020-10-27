from requests import put, get, delete, post
from utils import get_chunk_size, get_max_concurrency
import json
import queue 
import threading 
from chunks_utils import WriterThread, ReaderThread


def write_chunks(chunks_to_write, content, replicas):
    """Function for writing chunks into the datanodes.
    
    Parameters
    ----------
    chunks_to_write --> dict, key: datanode in which to write, value: list of chunks to write
    content --> bytes, file content
    replicas --> dict, key: chunks, value: list of node which have the replice for the chunk
    
    Returns
    -------
    None
    """
    chunks = []
    #create a list of datanode, chunk name and sequence number
    for host in chunks_to_write:
        for chunk in chunks_to_write[host]:
            chunks.append([host,chunk,int(chunk.split('_')[1])])
    #sort the list using the sequence number
    chunks.sort(key=lambda x: x[2])
    start=0
    end=get_chunk_size()
    queue_lock = threading.Lock() 
    chunks_queue = queue.Queue() 
    queue_lock.acquire() 
    #insert all the elements present inside the sorted list of chunks to write into a queue, 
    #taking also start and end bytes (each chunk contains a part of the entire content of a file)
    for [host,chunk,number] in chunks[:-1]:
        rep = replicas[chunk]
        chunks_queue.put([host,chunk,number,start,end,rep])
        start = end
        end += get_chunk_size()
    [host,chunk,number] = chunks[-1]
    rep = replicas[chunk]
    #the last chunk will be smaller, or at least equal to the maximum, in terms of bytes size 
    chunks_queue.put([host,chunk,number,start,end,rep])
    queue_lock.release()   
    threads = []
    thread_id = 1
    #start the writing process
    #initialize a pool of threads which will write concurrently 
    #the pool can contain at most get_max_concurrency() threads 
    for i in range(get_max_concurrency()):
        thread = WriterThread(thread_id, chunks_queue, queue_lock, content) 
        thread.start() 
        threads.append(thread) 
        thread_id += 1
    for t in threads: 
        t.join() 
    return


def delete_chunks(deleted, hosts):
    """Function for deleting chunks stored in the datanodes.
    
    Parameters
    ----------
    deleted --> list, files prefixes to delete
    hosts --> list, datanode which have some replica of the chunks for the files in input
    
    Returns
    -------
    None
    """
    #for each datanode delete all the chunks that have the same prefix
    #the chunks with the same prefix belong to the same file
    for dn in hosts:
        delete('http://{}/chunks'.format(dn), data = {'chunks_prefix': json.dumps(deleted)})
    return


def copy_chunks(old_id, new_id, hosts):
    """Function for copying the content of the chunks of a file (operation required for cp).
    
    Parameters
    ----------
    old_id --> str, source file prefix
    new_id --> str, destination file prefix
    hosts --> list, the datanodes which maintain some replicas of the chunks for source file
    
    Returns
    -------
    None
    """
    #for each datanode copy the content of the chunks with the same prefix into chunks with the new file prefix
    #the chunks with the same prefix belong to the same file
    for dn in hosts:
        post('http://{}/chunks'.format(dn), data = {'old_prefix': old_id, 'new_prefix': new_id})
    return


def get_chunks(chunks):
    """Function for getting the chunks content of a file (operation required for get_file, head, tail, cat).
    
    Parameters
    ----------
    chunks --> list, tuples which contains (list of datanodes which handle a replica of a chunk of file, chunk name, sequence number of the chunk)
    
    Returns
    -------
    tot --> dict, key: sequence number, value: content of the i chunk
    """
    tot = {}
    queue_lock = threading.Lock() 
    chunks_queue = queue.Queue() 
    queue_lock.acquire() 
    #create a queue in which every element contains the datanodes responsible for a certain chunk, the chunk name and the sequence number
    for (dn, c, sn) in chunks:
        chunks_queue.put([dn, c, sn])
    queue_lock.release()   
    threads = []
    thread_id = 1
    #start the reading process
    #initialize a pool of threads which will read concurrently 
    #the pool can contain at most get_max_concurrency() threads 
    for i in range(get_max_concurrency()):
        thread = ReaderThread(thread_id, chunks_queue, queue_lock, tot) 
        thread.start() 
        threads.append(thread) 
        thread_id += 1
    for t in threads: 
        t.join() 
    return tot


def start_recovery(chunks_to_replicate):
    """Function for executing the recovery after a datanode failure (the new master will copy the content of a chunk for which is master in a new choosen replica).
    
    Parameters
    ----------
    chunks_to_replicate --> list, the list of the chunks for which it's needed the recovery process, list of dictionaries with keys chunk, master, new_replica
    
    Returns
    -------
    None
    """
    replicas_tasks = {}
    #create a dictionary in which there are, for each master datanode, the names of the chunks to replicate and the new slave datanode choosen for the replication
    for c in chunks_to_replicate:
        try:
            replicas_tasks[c['master']].append({'chunk': c['chunk'], 'new_replica': c['new_replica']})
        except: 
            replicas_tasks[c['master']] = [{'chunk': c['chunk'], 'new_replica': c['new_replica']}]
    #for each master datanode, start to write the new replica
    for dn in replicas_tasks: 
        put('http://{}/recovery'.format(dn), data = {'to_recover': json.dumps(replicas_tasks[dn])})
    return


def start_flush(chunks_to_flush, dn):
    """Function for deleting the chunks for which a failed datanode is not a master/replica node anymore after the node has recovered from failure.
    
    Parameters
    ----------
    chunks_to_flush --> list, the chunks to flush
    dn --> str, the datanode for which the flush is needed, the recovered one 
    
    Returns
    -------
    None
    """
    #delete from a datanode all the chunks for which it's not neither a master nor a slave anymore
    delete('http://{}/recovery'.format(dn), data = {'chunks': json.dumps(chunks_to_flush)})
    return
        
