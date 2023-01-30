# SEALFS
[English](https://github.com/labring/sealfs/blob/main/README.MD)|[简体中文](https://github.com/labring/sealfs/blob/main/README-ZH.MD)

The storage system of [sealos](https://github.com/labring/sealos),aim to be a high-performance,high reliability and auto-scaling distributed file system which suit to cloud native.

## System Architecture
The architecture of sealfs is non centralized, and there is no single metadata node. We hope to improve the read and write performance as much as possible and solve the problem of storing large amounts of small files.

Sealfs consists of three components
-Server:Responsible for storing files and metadata。Metadata is undoubtedly the hot file of the distributed file system, so we store data and metadata in the way of separate disks. Users can choose better hardware to store metadata.
-client:It implements the file system in user mode, intercepts file requests, and stores and addresses them through the hash algorithm.
-manager:Responsible for Coordinate cluster.

The System Architecture picture as follows:
![](docs/images/architecture.jpg)

### User Mode in Overall Chain
We hope to create an overall chain user mode distributed file storage system from client file request hijacking to network to storage with specific hardware, so as to obtain the ultimate performance experience.

More designs can be referred to:
### Design Document
[design document](https://github.com/labring/sealfs/blob/main/docs/README.MD)

## RoadMap
At present, we are committed to improving the performance of the whole link. For other aspects of construction, such as high reliability and high availability, the priority will be lower.
- first version Function:
  - Client:
    - [ ] fuse file system interface
    - [ ] System call hijacking(file system of user mode)
    - [ ] location algorithm
    - [ ] batch process 

  - Sever:
    - [ ] bypass  file system
    - [ ] file Storage
    - [ ] disk manager
    - [ ] catalogue manager
    - [ ] Metadata persistent memory storage
    - [ ] file index
    - [ ] file lock
    - [ ] Persistent data structure
    
  - Manger:
    - [ ] heart manager
    
  - Network:
    - [ ] RDMA
    - [ ] socket network 
    
  - Test
    - [ ] IO500
    - [ ] function test

## Compile

```bash
cargo build
```

## Quick Start

### Mount

```bash
mkdir ~/fs
./target/debug/server &
./target/debug/client ~/fs
```

### File Operations

show directory
```bash
ll ~/fs
```
create file
TODO

read file
TODO

write file
TODO

delete file
TODO

## LICENSE
[Apache License 2.0](https://github.com/labring/sealfs/blob/main/LICENSE)