# Sealfs Design Document

## User Interface And System Call Process

### Object Storage, File Storage, Block Storage Interface
At present, there are three kinds of interfaces provided for storage services: object storage, file storage and block storage.
In daily use, most of them are file storage, and most of the applications operate on the basis of files, and the requests are handed over to the file system. Block storage is located under the file system and directly connected with the storage device. Its access target is a section of the storage device. The file storage request is converted to block storage after processing.
Both of them provide complete interface standards in posix. In the Linux system, these two types of storage are also included in the kernel, which handles system calls of programs.
Object storage is a third-party storage interface that provides reading and writing of a single file. Typical standards include aws-s3. This type of interface is independent of the Linux system, and access requires a separate client.

- Local file (block) storage call process
One call process of local file storage includes:
1. User status request
2. VFS
3. Kernel file system
4. Block device drive

If the request is a block device request, it is directly delivered to the kernel's block device driver (two steps are omitted in the middle, which does not affect). It can be seen that no matter what kind of request, there is only one kernel user mode switch.

![alt fs](https://pic1.zhimg.com/v2-c0ee0e4d7fc8b78c149c7fd8cb38f93a_1440w.jpg)


### Network File (Block) Storage Call Process

#### Fuse
There are many different ways to implement network file storage, and fuse is the easiest to implement and use.


![alt fuse](https://imgconvert.csdnimg.cn/aHR0cHM6Ly9tbWJpei5xcGljLmNuL21tYml6X3BuZy9kNGhvWUpseE9qTnNvaWNRQkUwM01aRDBrWjNmY3VpYWVRZzJmV1RlNFlWV3RUYko5aWN1cG1iZ1IwZGd1RUlrTTloTzZzaWJQdU80VTlFNzlpYWczWWljdlE4US82NDA?x-oss-process=image/format,png)

In fuse, a call process for file storage through the network includes:
1. User status request
2. VFS (switching)
3. FUSE kernel module
4. User mode client (switching)
5. User mode server
6. vfs (switching)
7. Kernel file system
8. Block device drive

The process involves three user kernel state switches in total, and six after the call returns, which greatly increases the cost.

#### Kernel File System
In order to reduce the number of calls, another implementation scheme is to directly implement the file system in the kernel state, and network requests are implemented in the kernel state.
1. User status request
2. VFS (switching)
3. Kernel client
4. User mode server
5. vfs (switching)
6. Kernel file system
7. Block device drive

This scheme can reduce the number of handovers to 2, but its disadvantages are also obvious:
1. The debugging of kernel programming is complex
2. You need to install additional kernel modules for the client

#### System Call Hijacking
There is another option. In the figure in the previous section, we can see that user requests are not directly handed over to the Linux kernel, but are submitted through glibc (or other libc libraries). This means that the address of system calls can be replaced at the libc layer to achieve system call hijacking.
1. User status request
2. System call hijacking client
3. User mode server
4. vfs (switching)
5. Kernel file system
6. Block device drive
In this case, the number of user kernel mode switches is only once, and the network interaction is completely implemented in the user mode, both the switching cost and the programming cost can be minimized.

Unfortunately, not all applications make system calls through libc, such as the binary program of golang, which does not use any dynamic links.

### Object Storage Call Process
Since object storage is independent of system calls, but is defined by a third party, it is independent of any user kernel state switching, and there is no restriction on this level.
This is partly why there are so many open source object storage products with high performance.

#### Conclusion

As a way of achieving the lowest cost, fuse is the best choice to implement sealfs. However, you should pay attention to some problems existing in fuse, which mainly need to solve:
- Double cache

- Inode conversion

- Redundancy mechanism under other fuse

It is not a good choice to develop the kernel file system. There may be risks and limitations in installing the kernel patch on the node where the user applies.

The system call hijacking is not difficult to implement and has better performance. It can be used as an optional support item for special demand scenarios.

## Network Architecture

![image](https://user-images.githubusercontent.com/14962503/189853670-d10c29e8-34d7-468e-baa6-36c8fa65a3c9.png)

#### Client
Use libfuse to build the client, which is used to mount a directory and process all requests under the directory.
A file request will be mapped to a server by the client using an online algorithm and transmitted through the socket link.

#### Sever
A server is used to organize some file contents and receive all file requests from clients at any location for the target server. There can be multiple servers on a node, and the file contents managed by all server organizations do not overlap.

#### Data Flow Connection

A data flow connection is used to maintain an active server connection. There is only one (or a limited number of configurable) data flow connection between a client and a server, which is used to process all file requests of the client at the service node.
The possible implementation method is to directly use RPC framework, or based on socket or other network protocols, or various RDMA. At present, it is implemented directly using socket, which is somewhat complex. The specific process is described and implemented in the request process.

#### Request Process

1. The client receives the request and creates a processing thread. The work of creating processing threads is implemented by libfuse, and the functions implemented by sealfs can be considered as independent threads.
2. The server where the calculation file is located, and the content is in metadata management, which is not detailed in this section.
3. Send a file request to the server and hold the thread. The process of sending requests should consider the parallel processing of multiple requests. Setting up a socket for each request is the simplest implementation, but the connection creation delay is too high, and the number of network connections may be too large. Maintaining multiple long connections ensures the delay of connection creation. However, in the case of large concurrency, it is still unable to solve the problem of excessive network connections. At the same time, the code implementation is slightly complicated. Therefore, a long connection is used to share multiple file requests. When a thread sends a request, it needs to include the request ID and data length. At the same time, it needs to implement an additional thread safe queue to store the lock of the thread after sending the request.
4. The server processes the file request and returns the request result to the client. The requested id is always maintained during processing.
5. The client receives data, activates the request thread, and processes the return value. One (or a limited number of) independent threads are used to receive the request result, which contains the request ID, so it is necessary to query the thread lock corresponding to the request ID in the queue, write the result and release the thread lock, activate the original request thread and return the result to the application.

The above process can be expanded horizontally. A client can have multiple long connections to solve the CPU bottleneck caused by a thread processing socket requests.

#### Memory Copy

The method that multiple requests share the same thread is adopted. When the socket sends a request, it needs to send a length variable in advance to avoid packet sticking due to the variable length of the data. There are two different solutions:
One is to use multiple sockets to realize the connection pool. One socket is used to send one request each time. This scheme does not have the problem of packet continuity and can be sent multiple times.
The other is to use the same socket, but to ensure data continuity, string splicing is required. When memory copying is involved, the overhead will increase. To avoid this problem, threads need to be locked each time data is sent. This is the implementation scheme of the first phase.

## Metadata Management

- No distributed metadata
>In the big data environment, the volume of metadata is also very large, and the access performance of metadata is the key to the performance of the entire distributed file system. Common metadata management can be divided into centralized and distributed metadata management architectures. The centralized metadata management architecture uses a single metadata server, which is simple to implement, but has a single point of failure and other problems. The distributed metadata management architecture disperses metadata on multiple nodes, thus solving the performance bottleneck of the metadata server and improving the scalability of the metadata management architecture, but the implementation is more complex and introduces the problem of metadata consistency. In addition, there is a distributed architecture without metadata server, which organizes data through online algorithms, and does not require a dedicated metadata server. However, it is difficult to guarantee the data consistency of this architecture. The implementation is more complex. The file directory traversal operation is inefficient and lacks the global monitoring and management function of the file system.

In order to efficiently adopt the metadata free distributed architecture, a simple implementation is to use the file name to hash, obtain the corresponding server ID, and put all the data and metadata related to the file on the server. Its scalability is also good. On the one hand, it can realize node expansion and contraction based on consistent hash, and on the other hand, it can also realize file segmentation based on offset to balance storage space. Refer to gekkofs for this method.

There are two problems. One is the traversal of the file directory. Because the metadata is scattered, it requires multiple requests from each server to obtain the metadata directory information. Second, the capacity of directory storage space is difficult to monitor. The possible solution is to delay updating the metadata of all parent directories, but there may be many complex problems in the actual situation, such as the request to delete a directory still needs to traverse all data. These problems cannot be solved by GlusterFS and other storage solutions. It will be very interesting to solve these problems and make sealfs unique.

- Local metadata management

A leveldb local database is responsible for storing metadata of each node, and saving the file name that can be hashed to the node: file metadata in the form of kv. When a request arrives, it first queries the database for metadata, and then accesses the file content at the corresponding location.

- Cluster metadata management

For the entire cluster, in addition to file metadata, server information needs to be stored, with several nodes. Because the update and read frequency is not high (it will change only when the capacity is expanded or shrunk), you can use the lift maintenance directly.

## Some Other Extensions

- Data reliability and high availability
If a file is hashed to a node, multiple hashes can be distributed to multiple nodes to achieve replica.

-Data expansion
Capacity expansion and reduction are implemented based on consistency hash. The details will not be discussed for the time being. It needs to be clear that after adding or deleting nodes, the cluster will be rebalanced. This is what consistency hash itself needs to do without additional design. Rebalance will cause the cluster performance to decline and may take a long time, but it can provide services continuously. The work to be done during rebalance is as follows:

| Start capacity expansion | Migrate data | Complete capacity expansion |
| ---- | ---- | ---- |
|Updating cluster metadata | The client makes a second request to confirm the consistency of the data after migration and the data before migration, and writes the data to the new node; Simultaneous migration task for data migration and synchronization | Confirm cluster metadata|

-Tenant Management
For the disks that different clients apply to mount, perform capacity limitation isolation

-Memory cache
The memory cache of files is implemented on the server side to avoid storage bottlenecks. At the same time, there must be no cluster level power failure. Optional implementation.


## Implementation Plan

- Function realization 12.30:
  - Core 9.30:
    - [ ] Socket network communication
    - [ ] fuse file system interface
    - [ ] Multi node cluster metadata management
  - Performance optimization:
    - [ ] System call hijacking
    - [ ] Coordinated schedule
    - [ ] RDMA
    - [ ] Memory cache 
  - Testing
    - [ ] IO500
    - [ ] Comparison test

- Common file system, estimated to be 2023.6.30:
  - Other functions
    - [ ] Document segmentation
    - [ ] Expansion capacity
    - [ ] Tenant capacity management
    - [ ] Data redundancy
    
- IO500 competition, expected on May 2023
