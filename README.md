# HandMadeDistributedFileSystem

## H(M)DFS - HandMade Distributed File System

The HandMade Distributed File System - H(M)DFS - is a simplified implementation of a distributed file system realized by Marco Pavia that runs in a cluster of nodes and is designed to be fault-tolerant. 
The system has been implemented in Python 3.7 and any machine that supports Python 3 can run the software.

## Architecture description

There are two types of nodes: 

- the Namenodes: they manage the file system namespace and metadata, regulate access to files and directories by clients;
- the Datanodes: they manage the storage, the real content of the files.

The system namespace is maintained by the Namenodes into MongoDB instances, inside of them there are four different collections responsible of maitaining the following information:

- fs: this collection handles data regarding the structure of the file system, so the directories tree info and the files info; inside this collection there are two types of documents, the files documents and the directories documents;
- users: this collection handles data regarding the users who have access to the H(M)DFS; inside this collection there is only a type of document, the users documents;
- groups: this  collection handles data regarding the groups to which the different users partecipate to (the concept besides a group is quite similar to what is a group in Linux); inside this collection there is only a type of document, the groups documents;
- trash: this collection handles some data used when a Datanode has recovered from a failure, we will discuss it later; inside this collection there is only a type of document, documents that register, for each failed Datanode, which are the chunks that must be deleted after recovery from disaster.

Internally a file is splitted into several "chunks", which are stored inside the Datanodes; you can think of a chunk as a contiguous subset of the entire set of bytes which compose a file. Imagine to have a very huge file of M bytes; this file, when it will be loaded into the H(M)DFS, will be splitted into several small chunks, each of these of size N bytes; the total number of chunks for that file will be M/N and the first K-1 chunks will have a size of N bytes, while the last K chunk will have a size of M - [(K-1) * N] bytes. Moreover, each chunk is replicated across different Datanodes, in order to make the system fault-tolerant, and each replica of a certain chunk must be maintained by a different Datanode (in other words, a Datanode cannot maintain two replicas of the same chunk). The Datanode stores H(M)DFS data in files in its local file system and has no knowledge about H(M)DFS files; it stores each chunk of H(M)DFS data in a separate file in its local file system. The DataNode creates all files in the same directory, that can be configured.
Summarily, the Namenodes execute file system namespace operations like opening, closing, and renaming files and directories and determine the mapping of chunks to Datanodes, which are responsible for serving read and write requests from the file system client and also perform chunk creation, deletion and replication. H(M)DFS supports a traditional hierarchical file organization, with a namespace Linux-like (excluded hard links and soft links). A user of the system can create directories and store files inside these directories; it's possible to create and to remove files, to move a file from one directory to another, or to rename a file. The Namenodes maintain the file system namespace. Any change to the file system namespace or its properties is recorded by the Namenodes. The number of replicas of each chunk of a file that should be maintained can be specified as a configuration parameter, as well as the max size of each chunk. The master Namenode makes all decisions regarding replication of chunks and periodically receives a heartbeat from each of the Datanodes in the cluster; receiving a heartbeat from a Datanode implies that the DataNode is functioning properly.

The communication protocols used are:

- WebSocket: to ensure that the Datanodes send heartbeats to the master Namenode;
- XML-RPC: to ensure a client invokes the operations on the master Namenode and to ensure the master Namenode can align the namespace maintained by the other slave Namenodes;
- HTTP REST Web Service: to ensure the client can execute chunks creation, deletion and replication on the Namenodes.

We will discuss more in detail the communication between the different elements of the system later.
Each Datanode sends a heartbeat message to the master Namenode periodically. When the Namenode doesn't receive any heartbeat from a Datanode after a certain time interval, the Namenode marks it as down (or dead) and starts the recovery process, that is start creating new replicas for the primary or secondary chunks which were handled by the failed Datanode. When the Datanode will be recovered from the disaster, then the Namenode starts the flush process, that is start deleting from the recovered Datanode the chunks that previously were handled by it and now are handled by other Datanodes. The Datanodes send heartbeats to what they recognize as the master Namenode; if the Master Namenode goes down, then the Datanodes will choose another Namenode that becomes the new master and start to send heartbeats to this new master Namenode. The new  master is choosen using a priority list of Datanodes; the prioritization can be configured. 

## H(M)DFS - Reading process communication schema

![Screenshot](images/read_process.PNG)

The schema above represents a typical communication schema of what happens when a client wants to get/read a file from the H(M)DFS; during the process can be identified different phases:

- pahse 1: the client invokes a get/read command, and calls a remote procedure using XML-RPC on the master Namenode;
- pahse 2: the Namenode gets the information required for the file from the MongoDB instance on which it maintains the metadata regarding the file system namespace, executing a query; by the way, there's a series of checks regarding the permissions of the user who required the file; 
- phase 3: MongoDB provides the Namenode with the document which represents the file, inside of this there are all the information about it, e.g. the file name, the creation and update time, the parent directory and, the most important thing, which are the Datanodes that handle the primary and secondary chunks on which there is the real content of the file;
- phase 4: the Namenode provides the client with the info regarding the file needed for getting the content from the Datanodes;
- phases 5.1, ..., 5.M: the client starts some HTTP get requests using the REST web services exposed by the Namenodes for getting the chunks content; during these phases, for each chunk, the client executes a get request on the Datanode which handles the current chunk passing the file id that represents the file uniquely and the chunk sequence number; for each chunk, the first request is done on the primary Datanode handler for it and, if the primary Datanode is down, then it's started another request on the secondary Datanoted, and so on;
- phases 6.1, ..., 6.M: the Datanode gets the chunk content for the chunk required from its local file system and provides the client with the chunk content;
- phase 7 (optional): if the invocation is a file get, then the file will be rebuild using the chunks contents got sorted by the chunks sequences numbers and the file will be saved on the client local file system; instead, if the invocation is just a file read, then the file will not be saved on the client local file system, but just showed. 

## H(M)DFS - Writing process communication schema

![Screenshot](images/writing_process.PNG)

The schema above represents a typical communication schema of what happens when a client wants to write a file to the H(M)DFS; during the process can be identified different phases:

- pahse 1: the client invokes a write command, and calls a remote procedure using XML-RPC on the master Namenode;
- pahse 2: the Namenode create the MongoDB document and chooses which are the Datanodes that handle the primary and secondary replicas of each chunk in which the file will be divided; the Namenode execute the insert of the document into MongoDB
- phase 3: MongoDB insert the document representing the file into the fs collection and provides the Namenode with unique id of the file document just inserted; 
- phases 4.1, ... 4.N: the master Namenode aligns the other Namenodes with the file info just created and invokes a XML-RPC to do it for each Namenode that is up;
- phases 5.1, ..., 5.N: the Namenodes give a feedback to the master Namenode about the alignment of their MongoDb instances;
- phases 6: the Datanode provides the client with the list of the chunks to write and the respective primary and secondary Datanodes that will handle the replicas of each chunk; 
- phase 7: the client gets the file content from the local file system;
- phases 8.1, ..., 8.M: the client starts some HTTP put requests using the REST web services exposed by the Namenodes for writing the chunks content; during these phases, for each chunk, the client executes a put request on the primary Datanode designed to handle the current chunk passing the file id that represents the file uniquely, the chunk sequence number, the chunks content and the list of the secondary Datanodes for the current chunks;
- pahses 9.1, ..., 9.M-1: for each chunk, after the primary Datanode has completed to write the chunk on the local file system, it publishes a message on a publish/subscribe system in order to start the replica writing process on the other secondary Datanodes; so the primary Datanode executes a HTTP put request on the first secondary Datanode, then the first secondary Datanode executes a HTTP put request on the second secondary Datanode and so on; 
- phases 10.1, ..., 10.M: for each chunk, the primary Datanode gives an HTTP put response for signilaing the writing process has ended. 

