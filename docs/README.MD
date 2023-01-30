# Sealfs Design Document

## System Architecture
The architecture of sealfs is non centralized, and there is no single metadata node. 

Sealfs consists of three components
-Server:Responsible for storing files and metadata.Metadata is undoubtedly the hot file of the distributed file system, so we store data and metadata in the way of separate disks. Users can choose better hardware to store metadata.
-client:It implements the file system in user mode, intercepts file requests, and stores and addresses them through the hash algorithm.
-manager:Responsible for Coordinate cluster.

The System Architecture picture as follows:
![](images/architecture.jpg)

### User Mode in Overall Chain 
We hope to create an overall chain user mode distributed file storage system from client file request hijacking to network to storage with specific hardware, so as to obtain the ultimate performance experience.

## Client
On the client side, we support two types of file systems: the more common fuse file system and the user mode file system which we hope to improve the performance in this way.

### Fuse

#### Kernel File System
In order to reduce the number of calls, an implementation scheme is to directly implement the file system in the kernel state, and network requests are implemented in the kernel state.
1. User status request
2. VFS
3. Kernel file system
4. network

This scheme can reduce the number of handovers to 2, but its disadvantages are also obvious:
1. The debugging of kernel programming is complex
2. You need to install additional kernel modules for the client
3. File system crash affects other processes

#### fuse
There are different ways to implement network file storage, and fuse is an easy way to implement and use.

![alt fuse](https://imgconvert.csdnimg.cn/aHR0cHM6Ly9tbWJpei5xcGljLmNuL21tYml6X3BuZy9kNGhvWUpseE9qTnNvaWNRQkUwM01aRDBrWjNmY3VpYWVRZzJmV1RlNFlWV3RUYko5aWN1cG1iZ1IwZGd1RUlrTTloTzZzaWJQdU80VTlFNzlpYWczWWljdlE4US82NDA?x-oss-process=image/format,png)

In fuse, a call process for file storage through the network includes:
1. User status request
2. VFS (switching)
3. fuse driver
4. fuse library
5. network

It should be noted that fuse also reduces the performance of the file system while avoiding the above problems.

### System Call Hijacking
This is the second scheme we implemented, namely, to implement a user mode file system. In the figure in the previous section, we can see that user requests are not directly handed over to the Linux kernel, but are submitted through glibc (or other libc libraries). This means that the address of system calls can be replaced at the libc layer to achieve system call hijacking.
1. User status request
2. System call hijacking client
3. network

## Network 
For the network part, two network transmission modes are also provided, RPC and RDMA.

### RPC
![image](https://user-images.githubusercontent.com/14962503/189853670-d10c29e8-34d7-468e-baa6-36c8fa65a3c9.png)

#### Location algorithm
A file request will be mapped to a server by the client using a hash algorithm and transmitted through the socket link.

#### Request Process

1. The client receives the request and creates a processing thread. The work of creating processing threads is implemented by libfuse, and the functions implemented by sealfs can be considered as independent threads.
2. The server where the calculation file is located, and the content is in metadata management, which is not detailed in this section.
3. Send a file request to the server and hold the thread. The process of sending requests should consider the parallel processing of multiple requests. Setting up a socket for each request is the simplest implementation, but the connection creation delay is too high, and the number of network connections may be too large. Maintaining multiple long connections ensures the delay of connection creation. However, in the case of large concurrency, it is still unable to solve the problem of excessive network connections. At the same time, the code implementation is slightly complicated. Therefore, a long connection is used to share multiple file requests. When a thread sends a request, it needs to include the request ID and data length. At the same time, it needs to implement an additional thread safe queue to store the lock of the thread after sending the request.
4. The server processes the file request and returns the request result to the client. The requested id is always maintained during processing.
5. The client receives data, activates the request thread, and processes the return value. One (or a limited number of) independent threads are used to receive the request result, which contains the request ID, so it is necessary to query the thread lock corresponding to the request ID in the queue, write the result and release the thread lock, activate the original request thread and return the result to the application.

#### Memory Copy

The method that multiple requests share the same thread is adopted. When the socket sends a request, it needs to send a length variable in advance to avoid packet sticking due to the variable length of the data. There are two different solutions:
One is to use multiple sockets to realize the connection pool. One socket is used to send one request each time. This scheme does not have the problem of packet continuity and can be sent multiple times.
The other is to use the same socket, but to ensure data continuity, string splicing is required. When memory copying is involved, the overhead will increase. To avoid this problem, threads need to be locked each time data is sent. This is the implementation scheme of the first phase.

### RDMA

## Manager
Manager used for managing server cluster.

### heart Manger
When the server node is online or offline, it will report the heartbeat information to the management node. The client will subscribe to the heartbeat information for location calculation. At the same time, the server will subscribe to the heartbeat information for data migration and other aspects.

## Server
The server mainly stores two types of data. One is the metadata information of the file and the content of the file itself.

As far as distributed file storage is concerned, metadata is undoubtedly hot data. Therefore, we use the method of separate hanging disks to mount metadata and file data in different disks. One economic way is to mount metadata data in SSD disks, while ordinary file data is stored in hdds. Of course, it can be matched at will.

### Metadata Management

>In the big data environment, the volume of metadata is also very large, and the access performance of metadata is the key to the performance of the entire distributed file system. Common metadata management can be divided into centralized and distributed metadata management architectures. The centralized metadata management architecture uses a single metadata server, which is simple to implement, but has a single point of failure and other problems. The distributed metadata management architecture disperses metadata on multiple nodes, thus solving the performance bottleneck of the metadata server and improving the scalability of the metadata management architecture, but the implementation is more complex and introduces the problem of metadata consistency. In addition, there is a distributed architecture without metadata server, which organizes data through online algorithms, and does not require a dedicated metadata server. However, it is difficult to guarantee the data consistency of this architecture. The implementation is more complex. The file directory traversal operation is inefficient and lacks the global monitoring and management function of the file system.

At present, sealfs chooses the metadata node architecture, which avoids the single point of failure of the metadata node, but metadata traversal becomes a difficult problem.

#### Metadata persistent memory storage

In order to improve the performance of metadata, we plan to combine the hardware supporting persistent memory to design metadata storage.

### Data storage

#### Bypass local file system
To improve performance, sealfs directly stores files across file systems. Of course, this will bring more complexity.

#### Adapt to different hardware
Different SSDs have different characteristics. We will adapt different hardware, design different data structures, and hope to achieve better results for each type of hardware used by users.

## Some Other Extensions
These expansion points are not implemented in the first version of the plan
- Data reliability and high availability
  - multi-replica
    
    The multi replica is temporarily planned to use the raft protocol, and the consistent hash algorithm is used to calculate the replica locations and distribute them to multiple nodes to achieve replica.
  - erasure coding
- Data expansion

Capacity expansion and reduction are implemented based on consistency hash. The details will not be discussed for the time being. It needs to be clear that after adding or deleting nodes, the cluster will be rebalanced. This is what consistency hash itself needs to do without additional design. Rebalance will cause the cluster performance to decline and may take a long time, but it can provide services continuously. The work to be done during rebalance is as follows:

| Start capacity expansion | Migrate data | Complete capacity expansion |
| ---- | ---- | ---- |
|Updating cluster metadata | The client makes a second request to confirm the consistency of the data after migration and the data before migration, and writes the data to the new node; Simultaneous migration task for data migration and synchronization | Confirm cluster metadata|

- Tenant Management

  For the disks that different clients apply to mount, perform capacity limitation isolation

