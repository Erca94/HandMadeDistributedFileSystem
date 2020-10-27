import threading
import time
import datetime
import websockets
from requests.exceptions import RequestException
from requests import put, get, delete, post
import json
from xmlrpc.server import SimpleXMLRPCServer
import logging
from utils import get_namenodes

#get the namenodes settings and mark them as active
namenodes = get_namenodes()
for n in namenodes:
    namenodes[n]['active'] = True
    
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')


def mark_as_inactive(host, port):
    """Function for marking a namenode as inactive after viewed it's unreacheble for a certain time for sending heartbeats.
    
    Parameters
    ----------
    host --> str, heartbeat destination namenode ip address
    port --> int, heartbeat destination namenode port
    
    Returns
    -------
    None
    """
    global namenodes
    for k in namenodes:
        if namenodes[k]['host'] == host and namenodes[k]['port_heartbeat'] == port:
            #mark the namenode ad inactive because it's not reachable anymore
            namenodes[k]['active'] = False
            logging.warning('Namenode {} marked as inactive'.format(namenodes[k]['host'] + ':' + str(namenodes[k]['port'])))
            break
    return


def take_best_active_nn():
    """Function for marking a namenode as inactive after viewed it's unreacheble for a certain time for sending heartbeats.
    
    Parameters
    ----------
    
    Returns
    -------
    (str(best['host']+':'+str(best['port_heartbeat'])), best['host'], best['port']) --> tuple(str, str, int), the active namenode with the highest priority to consider as the master namenode 
    """
    global namenodes
    actives = []
    #take the list of the active namenodes
    for dn in namenodes:
        if namenodes[dn]['active']:
            actives.append(namenodes[dn])
    #if there isn't any active namenode, return none
    if len(actives) == 0:
        return (None, None, None)
    #sort the list of the active namenodes in base of the priority and take the one with the highest priority
    best = sorted(actives, key = lambda i: i['priority'])[0]
    logging.info('New master namenode {}'.format(str(best['host']+':'+str(best['port_heartbeat']))))
    return (str(best['host']+':'+str(best['port_heartbeat'])), best['host'], best['port'])


def write_replica(chunk_name, chunk_payload, chunk_replicas):
    """Function for generating a replica for a chunk; this function starts when a message it's found in the dedicated channel (publisher/subscriber).
    
    Parameters
    ----------
    chunk_name --> str, the of the chunk for which it's necessary to write a replica
    chunk_payload --> str, the content of the chunk 
    chunk_replicas --> str, the string representation of the datanodes list choosen for being replica nodes for the chunk in input
    
    Returns
    -------
    None
    """
    chunk_replicas = json.loads(chunk_replicas)
    try:
        #take the first datanode to which write the new replica
        host = chunk_replicas.pop(0)
    except: #there isn't any datanode to write the new replica, then exit
        return
    #start the write process for the new datanode
    try:
        put('http://{}/chunks'.format(host), data={'chunk_replicas': json.dumps(chunk_replicas), 'chunk_name': chunk_name, 'chunk_payload': chunk_payload})
        logging.info('Write chunk {} replica to {}'.format(chunk_name, 'http://{}/chunks'.format(host)))
    except RequestException as e:
        raise SystemExit(e)
        logging.error(str(e))


async def send_heartbeat(heartbeat_to, datanode):
    """Function for sending a heartbeat to the namenode in order to report all works well; the heartbeat is sent using a web socket.
    
    Parameters
    ----------
    heartbeat_to --> str, the identity of the namenode to which the datanode sends a heartbeat
    datanode --> str, the identity of the datanode which sends a heartbeat
    
    Returns
    -------
    None
    """
    uri = 'ws://{}'.format(heartbeat_to)
    async with websockets.connect(uri) as websocket:
        #send the heartbeat to the master namenode
        await websocket.send(datanode)
        #wait for the answer from the master namenode
        answer = await websocket.recv()
        logging.info(answer)
        

class HeartbeatThread(threading.Thread):
    """Thread Class for sending at regular time intervals a heartbeat to the namenode in order to report all works well; the heartbeat is sent every 2 seconds."""
    
    def __init__(self, loop, heartbeat_to, host_master, port_master, datanode):
        threading.Thread.__init__(self)
        self.loop = loop
        self.heartbeat_to = heartbeat_to
        self.host_master = host_master
        self.port_master = port_master
        self.datanode = datanode
        self.down_count = 0

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

    def get_heartbeat_to(self):
        """Method for getting the 'heartbeat_to' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        
        Returns
        -------
        self.heartbeat_to --> str, the master namenode host to which send the heartbeat
        """
        return self.heartbeat_to

    def set_heartbeat_to(self, heartbeat_to):
        """Method for setting the 'heartbeat_to' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        heartbeat_to --> str, the master namenode host to which send the heartbeat
        
        Returns
        -------
        None
        """
        self.heartbeat_to = heartbeat_to
        
    def get_host_master(self):
        """Method for getting the 'host_master' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        
        Returns
        -------
        self.host_master --> str, the ip address of the master namenode 
        """
        return self.host_master

    def set_host_master(self, host_master):
        """Method for setting the 'host_master' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        host_master --> str, the ip address of the master namenode 
        
        Returns
        -------
        None
        """
        self.host_master = host_master

    def get_port_master(self):
        """Method for getting the 'port_master' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        
        Returns
        -------
        self.port_master --> int, the port of the master namenode 
        """
        return self.port_master

    def set_port_master(self, port_master):
        """Method for setting the 'port_master' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        port_master --> int, the port of the master namenode 
        
        Returns
        -------
        None
        """
        self.port_master = port_master   

    def get_datanode(self):
        """Method for getting the 'datanode' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        
        Returns
        -------
        self.datanode --> str, this datanode host
        """
        return self.datanode

    def set_datanode(self, datanode):
        """Method for setting the 'datanode' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        datanode --> str, this datanode host
        
        Returns
        -------
        None
        """
        self.datanode = datanode  

    def get_down_count(self):
        """Method for getting the 'down_count' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        
        Returns
        -------
        self.down_count --> int, the count of how many times the master namenode is considered down
        """
        return self.down_count

    def set_down_count(self, down_count):
        """Method for setting the 'down_count' object attribute.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        down_count --> int, the count of how many times the master namenode is considered down
        
        Returns
        -------
        None
        """
        self.down_count = down_count
                
    def run(self):
        """Target method for the class; the thread will send an heartbeat every 2 second forever.
        
        Parameters
        ----------
        self --> HeartbeatThread class, self reference to the object instance
        
        Returns
        -------
        None
        """
        while True:
            try:
                if self.get_heartbeat_to():
                    logging.info('Send heartbeat to {}'.format(self.get_heartbeat_to()))
                    #send a heartbeat
                    self.get_loop().run_until_complete(send_heartbeat(self.get_heartbeat_to(), self.get_datanode()))
                    #wait for 2 second before the next heartbeat
                    time.sleep(2)
                else: 
                    logging.critical('No namenode active!!!!') #currently there is no active namenode 
                    time.sleep(5) #wait for 5 seconds
                    continue
            except: #the current master namenode is not reachable
                if self.get_heartbeat_to():
                    logging.warning('Namenode {} is down!'.format(self.get_heartbeat_to()))
                #wait 5 "not reachable" before considering the current master namenode down and changing it 
                if self.get_down_count() < 5:
                    self.set_down_count(self.get_down_count()+1)
                else:
                    tmp = self.get_heartbeat_to().split(':')
                    host, port = tmp[0], int(tmp[1])
                    #mark the current master namenode as inactive
                    mark_as_inactive(host, port)
                    #take the next master namenode
                    (new_hearbeat_to, new_host_master, new_port_master) = take_best_active_nn()
                    if not new_hearbeat_to: #currently there is no active namenode 
                        logging.critical('No namenode active!!!!')
                        #print('no namenode active!!!!')
                        self.set_heartbeat_to(new_hearbeat_to)
                        self.set_host_master(new_host_master)
                        self.set_port_master(new_port_master)
                        #wait 5 seconds
                        time.sleep(5)
                        continue
                    logging.warning('Changing main namenode: {}'.format(new_hearbeat_to))
                    #set the new master namenode to which send the heartbeats 
                    self.set_heartbeat_to(new_hearbeat_to)
                    self.set_host_master(new_host_master)
                    self.set_port_master(new_port_master)
                    self.set_down_count(0)
                #if the current master namenode is not reachable wait 5 seconds
                time.sleep(5)
        

class ServerThread(threading.Thread):
    """Thread Class for running the server for the REST web services."""
    
    def __init__(self, app, host, port):
        threading.Thread.__init__(self)
        self.app = app
        self.host = host
        self.port = port
        
    def get_app(self):
        """Method for getting the 'app' object attribute.
        
        Parameters
        ----------
        self --> ServerThread class, self reference to the object instance
        
        Returns
        -------
        self.app --> flask.app.Flask class, the Flask app reference
        """
        return self.app
      
    def set_app(self, app):
        """Method for setting the 'app' object attribute.
        
        Parameters
        ----------
        self --> ServerThread class, self reference to the object instance
        app --> flask.app.Flask class, the Flask app reference
        
        Returns
        -------
        None
        """
        self.app = app     
        
    def get_host(self):
        """Method for getting the 'host' object attribute.
        
        Parameters
        ----------
        self --> ServerThread class, self reference to the object instance
        
        Returns
        -------
        self.host --> str, datanode host ip address
        """
        return self.host
      
    def set_host(self, host):
        """Method for setting the 'host' object attribute.
        
        Parameters
        ----------
        self --> ServerThread class, self reference to the object instance
        host --> str, datanode host ip address
        
        Returns
        -------
        None
        """
        self.host = host     
        
    def get_port(self):
        """Method for getting the 'port' object attribute.
        
        Parameters
        ----------
        self --> ServerThread class, self reference to the object instance
        
        Returns
        -------
        self.port --> int, datanode host port
        """
        return self.port
      
    def set_port(self, port):
        """Method for setting the 'port' object attribute.
        
        Parameters
        ----------
        self --> ServerThread class, self reference to the object instance
        port --> int, datanode host port
        
        Returns
        -------
        None
        """
        self.port = port

    def run(self):
        """Target method for the class.
        
        Parameters
        ----------
        self --> ServerThread class, self reference to the object instance
        
        Returns
        -------
        None
        """
        #run the server which handles the REST services on this current thread
        logging.info('Starting app')
        self.get_app().run(debug=False, host=self.get_host(), port=self.get_port())
        

class GeneralCommunicationsThread(threading.Thread):
    """Thread Class for running a RPC server for general communications for the datanode (e.g. telling which is the master namenode at a certain time)."""
    
    def __init__(self, host, port, heartbeat_thread):
        threading.Thread.__init__(self)
        self.heartbeat_thread = heartbeat_thread
        self.server = SimpleXMLRPCServer((host, port), allow_none=True)
        self.server.register_function(self.get_master_namenode, 'get_master_namenode') 
        self.server.register_function(self.get_status, 'get_status')
        
    def get_heartbeat_thread(self):
        """Method for getting the 'heartbeat_thread' object attribute.
        
        Parameters
        ----------
        self --> GeneralCommunicationsThread class, self reference to the object instance
        
        Returns
        -------
        self.heartbeat_thread --> HeartbeatThread class, the thread responsible of sending heartbeats to the master namenode
        """
        return self.heartbeat_thread
      
    def set_heartbeat_thread(self, heartbeat_thread):
        """Method for setting the 'heartbeat_thread' object attribute.
        
        Parameters
        ----------
        self --> GeneralCommunicationsThread class, self reference to the object instance
        heartbeat_thread --> HeartbeatThread class, the thread responsible of sending heartbeats to the master namenode
        
        Returns
        -------
        None
        """
        self.heartbeat_thread = heartbeat_thread      
        
    def get_server(self):
        """Method for getting the 'server' object attribute.
        
        Parameters
        ----------
        self --> GeneralCommunicationsThread class, self reference to the object instance
        
        Returns
        -------
        self.server --> xmlrpc.server.SimpleXMLRPCServer class, server for general communications
        """
        return self.server
      
    def set_server(self, server):
        """Method for setting the 'server' object attribute.
        
        Parameters
        ----------
        self --> GeneralCommunicationsThread class, self reference to the object instance
        server --> xmlrpc.server.SimpleXMLRPCServer class, server for general communications
        
        Returns
        -------
        None
        """
        self.server = server
        
    def get_master_namenode(self):
        """Method for setting the 'server' object attribute.
        
        Parameters
        ----------
        self --> GeneralCommunicationsThread class, self reference to the object instance
        
        Returns
        -------
        self.get_heartbeat_thread().get_host_master() + ':' + str(self.get_heartbeat_thread().get_port_master()) --> str, the master namenode
        """
        logging.info('Current master namenode required')
        #return the current master namenode
        return (self.get_heartbeat_thread().get_host_master() + ':' + str(self.get_heartbeat_thread().get_port_master()))
    
    def get_status(self):
        """Function for getting the status of the datanode.
    
        Parameters
        ----------
        self --> GeneralCommunicationsThread class, self reference to the object instance
        
        Returns
        -------
        'OK' --> str, the datanode is ok
        """
        #if it's possible to invoke this rpc, it means the status of this datanode is OK 
        return 'OK'

    def run(self):
        """Target method for the class; the server starts.
        
        Parameters
        ----------
        self --> GeneralCommunicationsThread class, self reference to the object instance
        
        Returns
        -------
        None
        """
        #run the server for xml rpc on this thread
        self.get_server().serve_forever()
        