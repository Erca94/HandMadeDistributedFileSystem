import threading 
import json
from requests import put, get, delete, post
from requests.exceptions import RequestException
from exceptions import GetFileException
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

class WriterThread(threading.Thread): 
    """Thread Class for writing concurrently the chunks inside the datanodes. The thread will read the next chunk to write from a queue and start the writing process throught REST web services."""
    
    def __init__(self, thread_id, queue, lock, content): 
        threading.Thread.__init__(self) 
        self.thread_id = thread_id 
        self.queue = queue 
        self.lock = lock
        self.content = content 
        
    def get_thread_id(self):
        """Method for getting the 'thread_id' object attribute.
        
        Parameters
        ----------
        self --> WriterThread class, self reference to the object instance
        
        Returns
        -------
        self.thread_id --> int, the id of the thread
        """
        return self.thread_id
      
    def set_thread_id(self, thread_id):
        """Method for setting the 'thread_id' object attribute.
        
        Parameters
        ----------
        self --> WriterThread class, self reference to the object instance
        thread_id --> int, the id of the thread
        
        Returns
        -------
        None
        """
        self.thread_id = thread_id
        
    def get_queue(self):
        """Method for getting the 'queue' object attribute.
        
        Parameters
        ----------
        self --> WriterThread class, self reference to the object instance
        
        Returns
        -------
        self.queue --> queue.Queue class, the queue of the chunks to write
        """
        return self.queue
      
    def set_queue(self, queue):
        """Method for setting the 'queue' object attribute.
        
        Parameters
        ----------
        self --> WriterThread class, self reference to the object instance
        queue --> queue.Queue class, the queue of the chunks to write
        
        Returns
        -------
        None
        """
        self.queue = queue
        
    def get_lock(self):
        """Method for getting the 'lock' object attribute.
        
        Parameters
        ----------
        self --> WriterThread class, self reference to the object instance
        
        Returns
        -------
        self.lock --> _thread.lock class
        """
        return self.lock
      
    def set_lock(self, lock):
        """Method for setting the 'lock' object attribute.
        
        Parameters
        ----------
        self --> WriterThread class, self reference to the object instance
        lock --> _thread.lock class
        
        Returns
        -------
        None
        """
        self.lock = lock
        
    def get_content(self):
        """Method for getting the 'content' object attribute.
        
        Parameters
        ----------
        self --> WriterThread class, self reference to the object instance
        
        Returns
        -------
        self.content --> bytes, the file content
        """
        return self.content
      
    def set_content(self, content):
        """Method for setting the 'content' object attribute.
        
        Parameters
        ----------
        self --> WriterThread class, self reference to the object instance
        content --> bytes, the file content
        
        Returns
        -------
        None
        """
        self.content = content
        
    def run(self): 
        """Target method for the class; the thread will get from a queue the next chunk to write and the nodes which must have a copy of the chunk (pimary node and replica nodes) and start the writing process throught REST web services.
        
        Parameters
        ----------
        self --> WriterThread class, self reference to the object instance
        
        Returns
        -------
        None
        """ 
        #while the queue of chunks to write is not empty, take the next one and start the writing process
        while not self.get_queue().empty():
            #acquire the lock on the queue in order not to create concurrency errors
            self.get_lock().acquire()
            [host,chunk,number,start,end,rep] = self.get_queue().get()
            #release the lock 
            self.get_lock().release() 
            try:
                #call the REST service for writing the current chunk
                put('http://{}/chunks'.format(host), data={'chunk_replicas': json.dumps(rep), 'chunk_name': chunk, 'chunk_payload': self.get_content()[start:end]})
            except RequestException as e:
                logging.error(e)
            
            
class ReaderThread(threading.Thread): 
    """Thread Class for reading concurrently the chunks content from the datanodes. The thread will read the next chunk to read from a queue and start the reading process throught REST web services."""
    
    def __init__(self, thread_id, queue, lock, tot): 
        threading.Thread.__init__(self) 
        self.thread_id = thread_id 
        self.queue = queue
        self.lock = lock 
        self.tot = tot
        
    def get_thread_id(self):
        """Method for getting the 'thread_id' object attribute.
        
        Parameters
        ----------
        self --> ReaderThread class, self reference to the object instance
        
        Returns
        -------
        self.thread_id --> int, the id of the thread
        """
        return self.thread_id
      
    def set_thread_id(self, thread_id):
        """Method for setting the 'thread_id' object attribute.
        
        Parameters
        ----------
        self --> ReaderThread class, self reference to the object instance
        thread_id --> int, the id of the thread
        
        Returns
        -------
        None
        """
        self.thread_id = thread_id
        
    def get_queue(self):
        """Method for getting the 'queue' object attribute.
        
        Parameters
        ----------
        self --> ReaderThread class, self reference to the object instance
        
        Returns
        -------
        self.queue --> queue.Queue class, the queue of the chunks to read
        """
        return self.queue
      
    def set_queue(self, queue):
        """Method for setting the 'queue' object attribute.
        
        Parameters
        ----------
        self --> ReaderThread class, self reference to the object instance
        queue --> queue.Queue class, the queue of the chunks to read
        
        Returns
        -------
        None
        """
        self.queue = queue
        
    def get_lock(self):
        """Method for getting the 'lock' object attribute.
        
        Parameters
        ----------
        self --> ReaderThread class, self reference to the object instance
        
        Returns
        -------
        self.lock --> _thread.lock class
        """
        return self.lock
      
    def set_lock(self, lock):
        """Method for setting the 'lock' object attribute.
        
        Parameters
        ----------
        self --> ReaderThread class, self reference to the object instance
        lock --> _thread.lock class
        
        Returns
        -------
        None
        """
        self.lock = lock
        
    def get_tot(self):
        """Method for getting the 'tot' object attribute.
        
        Parameters
        ----------
        self --> ReaderThread class, self reference to the object instance
        
        Returns
        -------
        self.tot --> dict, the total content of the read file
        """
        return self.tot
      
    def set_tot(self, tot):
        """Method for setting the 'tot' object attribute.
        
        Parameters
        ----------
        self --> ReaderThread class, self reference to the object instance
        tot --> dict, the total content of the read file
        
        Returns
        -------
        None
        """
        self.tot = tot
        
    def run(self): 
        """Target method for the class; the thread will get from a queue the next chunk to read and the list of nodes which have a copy of the chunk (primary and replica nodes) and start the writing process throught REST web services.
        
        Parameters
        ----------
        self --> ReaderThread class, self reference to the object instance
        
        Returns
        -------
        None
        """ 
        #while the queue of chunks to read is not empty, take the next one and start the reading process
        while not self.get_queue().empty():
            #acquire the lock on the queue in order not to create concurrency errors
            self.get_lock().acquire()
            [datanodes, c, sn] = self.get_queue().get()
            #release the lock 
            self.get_lock().release() 
            got = False
            #a chunk can be read not only from the master datanode, but also from the slaves one
            for dn in datanodes:
                try:
                    content = get('http://{}/chunks'.format(dn), params={'chunk_name': c}).content
                    self.get_tot()[sn] = content[1:-2]
                    #the chunk content has been got, so stop the reading process for that chunk because it's completed
                    got = True
                    break
                except RequestException as e:
                    #raise SystemExit(e)
                    logging.error(e)
            #if the content of the current chunk has not been got, than raise an exception
            #the file is corrupted
            if not got:
                raise GetFileException()
                
                