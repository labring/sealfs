# SEALFS
[English](https://github.com/labring/sealfs/blob/main/README.MD)|[简体中文](https://github.com/labring/sealfs/blob/main/README-ZH.MD)

high-performance,high reliability and auto-scaling distributed file system.


## Why Build A New Distributed File System For Cloud Native

#### Use Local Storage

- Single point of failure
  The biggest problem of cloud native applications using local persistent storage is a single point of failure. Because the data is located at a specific node, the node downtime causes data unavailable, and the bound applications cannot be restarted, and it is difficult to migrate to other nodes. In addition, single node disk failure may even cause data loss.
  At present, for some applications requiring high data reliability, the distributed architecture is implemented in the application layer, essentially giving the complexity to the application. For complex applications, the cost is too high.

- Storage planning and capacity expansion
  Applying local storage means that capacity planning is required for each node, and additional migration configuration is required for the original distributed architecture in case of node failure, which brings huge workload and error probability.
  On the other hand, when the original capacity is insufficient, node expansion is also a problem that local storage cannot solve.

- Inefficient performance
  Due to disk device IO, there is a bottleneck in the performance of local storage, especially when multiple applications share disks.

#### Use Existing Distributed Storage

- Performance
  Although theoretically, the data performance of distributed storage can be unlimited with the increase of nodes, it is actually affected in several aspects. First, the bottleneck brought by cluster consistency, and the problems of distributed solutions using raft and other protocols; The second is the memory copy cost caused by the extension of the data call process, and the problems caused by many file systems using fuse; The third is the bottleneck of metadata request, typically GFS. The cost of directory traversal under large-scale clusters is very high.

- Configuration
  The configuration is complex, especially for node expansion, and many restrictions are very demanding. Typically, GFS capacity expansion is extremely complex. Cephfs has poor stability.

- Price
  High performance commercial distributed storage is expensive, and most open source products cannot be compared.

## Application Scenes

Extreme performance scheme × Reliable data experience

- High performance network data access
  - Scheme without distributed metadata
  - intercept
  - Complete posix request (efficient implementation of link, rename and delete)

- Highly reliable data management
  - Data high reliability
  - Data service high availability
  - Storage Node Scalable

- Convenient auto scaling

## Design Document
[design document](https://github.com/labring/sealfs/blob/main/docs/README.MD)

## LICENSE
[Apache License 2.0](https://github.com/labring/sealfs/blob/main/LICENSE)

## Implementation Plan

- first version Function:
  - Core:
    - [ ] Socket network communication
    - [ ] fuse file system interface
    - [ ] cluster heartbeat management
    - [ ] file Storage
    - [ ] Location algorithm
  - Performance optimization:
    - [ ] System call hijacking(file system of user mode)
    - [ ] RDMA
    - [ ] Combination of software and hardware(DPDK SPDK)
  - Testing
    - [ ] IO500
    - [ ] Comparison test

- Common file system:
  - Other functions
    - [ ] Distributed transaction
    - [ ] Auto-scaling capacity
    - [ ] Tenant management
    - [ ] Data redundancy

- IO500 competition

## Install

## Quick Start
