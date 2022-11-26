# SEALFS
[English](https://github.com/labring/sealfs/blob/main/README.MD)|[简体中文](https://github.com/labring/sealfs/blob/main/README-ZH.MD)

The storage system of [sealos](https://github.com/labring/sealos),aim to be a high-performance,high reliability and auto-scaling distributed file system which suit to cloud native.

## System Architecture
The architecture of sealfs is non centralized, and there is no single metadata node. We hope to improve the read and write performance as much as possible and solve the problem of storing large amounts of small files.

Sealfs consists of three components, server, client, and manager. Server is responsible for storing files and metadata, client is responsible for sending requests, and manager is responsible for maintaining the cluster heartbeat and other tasks.
![](docs/images/architecture.jpg)
More designs can be referred to
### Design Document
[design document](https://github.com/labring/sealfs/blob/main/docs/README.MD)

## RoadMap
- first version Function:
  - Core:
    - [ ] Socket network communication
    - [ ] fuse file system interface
    - [ ] cluster heartbeat management
    - [ ] file Storage
    - [ ] Location algorithm
    - [ ] Distributed transaction
    - [ ] multi-replication
    - [ ] Consensus Protocol
    
  - Performance optimization:
    - [ ] System call hijacking(file system of user mode)
    - [ ] RDMA
    - [ ] Combination of software and hardware
    
  - Testing
    - [ ] IO500
    - [ ] Comparison test

- IO500 competition

## Install

## Quick Start

## LICENSE
[Apache License 2.0](https://github.com/labring/sealfs/blob/main/LICENSE)