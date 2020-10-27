###example --> python3 datanode.py datanode1

import sys
from flask import Flask, request
from flask_restful import Resource, Api
from utils import get_datanode_setting, get_replica_set, get_datanodes_list
import os
import glob
import json
from shutil import copyfile
from pubsub import pub
from requests.exceptions import RequestException
from requests import put, get, delete, post
import asyncio
import websockets
import functools
import logging
import datetime
from datanode_utils import HeartbeatThread, ServerThread, GeneralCommunicationsThread, write_replica, take_best_active_nn

s = get_datanode_setting(sys.argv[1])
(heartbeat_to, host_master, port_master) = take_best_active_nn()


class ChunksHandler(Resource):
    """REST web service class for handling the operations for the chunks (write chunk content, get chunk content, delete chunk, copy chunk content into another chunk)."""
    
    def get(self):
        """get request --> used for getting chunks content, for reading operations.
        
        Parameters
        ----------
        self --> ChunksHandler class, self reference to the object instance
        
        Returns
        -------
        chunk_content --> str, the content of the chunk
        """
        chunk_name = request.args['chunk_name']
        #get the chunk content as an array of bytes
        with open(s['storage']+chunk_name, 'rb') as fb:
            chunk_content = bytearray(fb.read()).decode('ISO-8859-1')
        fb.close()
        logging.info('Get chunk {}'.format(chunk_name))
        return chunk_content

    def put(self):
        """put request --> used for writing chunks, for writing operations.
        
        Parameters
        ----------
        self --> ChunksHandler class, self reference to the object instance
        
        Returns
        -------
        None
        """
        chunk_replicas = request.form['chunk_replicas']
        chunk_name = request.form['chunk_name']
        chunk_payload = bytearray(request.form['chunk_payload'], encoding = 'ISO-8859-1')
        #write the binary content into the chunk 
        with open(s['storage']+chunk_name, 'wb') as fb:
            fb.write(chunk_payload)
        fb.close()
        logging.info('Put chunk {}'.format(chunk_name))
        #publish a message in the channel "replicas" with the chunk to replicate, the content/payload and the list of datanodes which must handle the replicas for that chunk
        pub.sendMessage('replicas', chunk_name=request.form['chunk_name'], chunk_payload=request.form['chunk_payload'], chunk_replicas=request.form['chunk_replicas'])
        return
        
    def delete(self):
        """delete request --> used for deleting chunks, for removing operations (except flush operation after recovery from failure).
        
        Parameters
        ----------
        self --> ChunksHandler class, self reference to the object instance
        
        Returns
        -------
        None
        """
        chunks = json.loads(request.form['chunks_prefix'])
        chunks_lst = []
        #delete all the chunks which have a prefix present into "chunks"
        #the chunks with the same prefix belong to the same file
        for pref in chunks:
            chunks_lst.extend(glob.glob(s['storage']+pref+'*'))
        for cp in chunks_lst:
            try:
                logging.info('Delete chunk {}'.format(cp))
                os.remove(cp) #remove the chunks which start with the current prefix
            except Exception as e:
                logging.error(str(e))
        return
    
    def post(self):
        """post request --> used for copying chunks, for copying operations.
        
        Parameters
        ----------
        self --> ChunksHandler class, self reference to the object instance
        
        Returns
        -------
        None
        """
        old_prefix = request.form['old_prefix']
        new_prefix = request.form['new_prefix']
        #for each chunk whom name starts with the old prefix, copy the content into the new chunk
        for c in glob.glob(s['storage']+old_prefix+'*'):
            try:
                src = os.path.basename(os.path.normpath(c)) #content of the chunk
                #copy the content into the new chunk
                copyfile(c, os.path.join(s['storage'], new_prefix + '_' + src.split('_')[1]))
                logging.info('Copy chunk {} into chunk {}'.format(c, os.path.join(s['storage'], new_prefix + '_' + src.split('_')[1])))
            except Exception as e:
                logging.error(str(e))
        return
        
        
class MkfsHandler(Resource):
    """REST web service class for handling the initialization of the dfs; it cleans completely the data folder."""
    
    def delete(self):
        """delete request --> used for cleaning the data folder.
        
        Parameters
        ----------
        self --> MkfsHandler class, self reference to the object instance
        
        Returns
        -------
        None
        """
        #flush the content of the storage directory of the datanode
        #empty the directory
        for r, d, f in os.walk(s['storage']):
            for file in f:
                os.remove(os.path.join(r, file))
        return
    
    
class DisasterRecoveryHandler(Resource):
    """REST web service class for handling the recovery after a datanode failure, in particular flushing the failed datanode after recovery and generating new replicas."""
    
    def put(self):
        """put request --> used generating new replicas starting from the primary datanode for a chunk.
        
        Parameters
        ----------
        self --> DisasterRecoveryHandler class, self reference to the object instance
        
        Returns
        -------
        None
        """
        to_recover = json.loads(request.form['to_recover'])
        for c in to_recover:
            try:
                #read the content of the current chunk
                with open(s['storage']+c['chunk'], 'rb') as f:
                    content = f.read()
                    logging.info('Get chunk {}'.format(c['chunk']))
            except Exception as e:
                logging.error(str(e))
                return
            #publish a message in the channel "replicas" with the chunk to replicate, the content/payload and the datanode which must handle the replicas for that chunk
            pub.sendMessage('replicas', chunk_name=c['chunk'], chunk_payload=content, chunk_replicas=json.dumps([c['new_replica']]))
        return
    
    def delete(self):
        """delete request --> used for flushing the failed datanode after recovery, deleting the chunks for which it's not a primary or a replica node anymore.
        
        Parameters
        ----------
        self --> DisasterRecoveryHandler class, self reference to the object instance
        
        Returns
        -------
        None
        """
        chunks = json.loads(request.form['chunks'])
        for c in chunks:
            try:
                #remove the current chunk from the datanode which previously handled a replica of that one
                os.remove(s['storage']+c) 
                logging.info('Flush chunk {} after recovery'.format(c))
            except Exception as e:
                logging.error(str(e))
        return


def main():
    """Main function, the entry point."""
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    #check if the number of replica set is at least equal to the number of datanodes 
    if get_replica_set() > len(get_datanodes_list()):
        logging.critical('Impossible to start! Not enough datanodes to handle the replica set')
        return
    logging.info('Datanode started')
    #create the server which exposes the REST services
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(ChunksHandler, '/chunks')
    api.add_resource(MkfsHandler, '/mkfs')
    api.add_resource(DisasterRecoveryHandler, '/recovery')
    #start the thread which runs the server for the REST services
    server_thread = ServerThread(app, s['host'], s['port'])
    server_thread.start()
    #create a publish/subscribe channel for handling the replicas writing process
    #when an event is present into the channel, the "write_replica" function will start  
    pub.subscribe(write_replica, 'replicas')
    new_loop = asyncio.new_event_loop()
    #start the thread which handles the heartbeat process
    heartbeat_thread = HeartbeatThread(new_loop, heartbeat_to, host_master, port_master, s['host']+':'+str(s['port']))
    heartbeat_thread.start() 
    #start the thread for the general communications
    gencom_thread = GeneralCommunicationsThread(s['host'], s['port_gencom'], heartbeat_thread)
    gencom_thread.start()
    server_thread.join()
    heartbeat_thread.join()
    gencom_thread.join()
    
if __name__ == '__main__':
    main()
    