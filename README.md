# HandMadeDistributedFileSystem

### H(M)DFS - HandMade Distributed File System

The HandMade Distributed File System - H(M)DFS - is an implementation of a distributed file system realized by Marco Pavia that runs in a cluster of nodes and is designed to be fault-tolerant. There are two types of nodes: 
  - the Namenodes: they manage the file system namespace and metadata and regulate access to files by clients;
  - the Datanodes: they manage the storage.
Internally a file is splitted into several "chunks"; you can think of a chunk as a portion of bytes. Imagine to have a very huge file of M bytes; this file, when it will be loaded into the H(M)DFS, it will be splitted into several small chunks, each of these of size N bytes; the total number of chunks for that file will be M/N and the first K-1 chunks will have a size of N bytes, while the last K chunk will have a size of M - [(K-1) * N] bytes.
