# HandMadeDistributedFileSystem

### H(M)DFS - HandMade Distributed File System

The HandMade Distributed File System - H(M)DFS - is an implementation of a distributed file system realized by Marco Pavia that runs in a cluster of nodes and is designed to be fault-tolerant. There are two types of nodes: 
- the Namenodes: they manage the file system namespace and metadata and regulate access to files by clients;
- the Datanodes: they manage the storage.
The system namespace is maintained by the Namenodes into MongoDB instances, inside of them there will be four different collections responsible of maitaining the following information:
- fs: this collection handles data regarding the structure of the file system, so the directories tree info and the files info;
- users: this collection handles data regarding the users who will have access to the H(M)DFS;
- groups: this  collection handles data regarding the groups to which the different users partecipate to (the concept besides a group is quite similar to what is a group in Linux);
- trash: this collection handles some data used when a Datanode has recovered from a failure, we will discuss it later.
Internally a file is splitted into several "chunks", which are sotred inside the Datanodes; you can think of a chunk as a portion of bytes. Imagine to have a very huge file of M bytes; this file, when it will be loaded into the H(M)DFS, it will be splitted into several small chunks, each of these of size N bytes; the total number of chunks for that file will be M/N and the first K-1 chunks will have a size of N bytes, while the last K chunk will have a size of M - [(K-1) * N] bytes. Moreover, each chunk is replicated across different Datanodes, and each replica of a particular chunk must be maintained by a different Datanode (in other words, a Datanode cannot maintain two replicas of the same chunk).
