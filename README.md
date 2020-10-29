# HandMadeDistributedFileSystem

### H(M)DFS - HandMade Distributed File System

The HandMade Distributed File System - H(M)DFS - is an implementation of a distributed file system realized by Marco Pavia that runs in a cluster of nodes and is designed to be fault-tolerant. 
The system has been implemented in Python 3.7 and any machine that supports Python 3 can run the software.
There are two types of nodes: 

- the Namenodes: they manage the file system namespace and metadata, regulate access to files and directories by clients;
- the Datanodes: they manage the storage, the real content of the files.

The system namespace is maintained by the Namenodes into MongoDB instances, inside of them there are four different collections responsible of maitaining the following information:

- fs: this collection handles data regarding the structure of the file system, so the directories tree info and the files info;
- users: this collection handles data regarding the users who have access to the H(M)DFS;
- groups: this  collection handles data regarding the groups to which the different users partecipate to (the concept besides a group is quite similar to what is a group in Linux);
- trash: this collection handles some data used when a Datanode has recovered from a failure, we will discuss it later.

Internally a file is splitted into several "chunks", which are stored inside the Datanodes; you can think of a chunk as a contiguous subset of the entire set of bytes which compose a file. Imagine to have a very huge file of M bytes; this file, when it will be loaded into the H(M)DFS, will be splitted into several small chunks, each of these of size N bytes; the total number of chunks for that file will be M/N and the first K-1 chunks will have a size of N bytes, while the last K chunk will have a size of M - [(K-1) * N] bytes. Moreover, each chunk is replicated across different Datanodes, in order to make the system fault-tolerant, and each replica of a certain chunk must be maintained by a different Datanode (in other words, a Datanode cannot maintain two replicas of the same chunk).
Summarily, the Namenodes execute file system namespace operations like opening, closing, and renaming files and directories and determine the mapping of chunks to Datanodes, which are responsible for serving read and write requests from the file system client and also perform chunk creation, deletion and replication. H(M)DFS supports a traditional hierarchical file organization, with a namespace Linux-like (excluded hard links and soft links). A user of the system can create directories and store files inside these directories; it's possible to create and to remove files, to move a file from one directory to another, or to rename a file. The Namenodes maintain the file system namespace. Any change to the file system namespace or its properties is recorded by the Namenodes. The number of replicas of each chunk of a file that should be maintained can be specified as a configuration parameter.
