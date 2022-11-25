# SEALFS

[sealos](https://github.com/labring/sealos)的存储底座，我们希望构建一个适用于云原生的高性能，高可靠，弹性伸缩的分布式文件存储

## 系统架构
sealfs的架构为无中心架构，且无独立的元数据节点，我们希望尽最大可能提升读写性能和解决存储海量小文件问题

sealfs包含三个组件，server，client，manager，其中server负责文件以及元数据存储，client负责发送请求，manager负责维护集群心跳以及其他工作。
![](docs/images/architecture.jpg)
更多设计可以参考：
### 设计文档
[设计文档](https://github.com/labring/sealfs/blob/main/docs/README-ZH.MD)

## 实现计划

- 第一版功能实现：
  - 核心：
    - [ ] socket网络通信
    - [ ] fuse文件系统接口
    - [ ] 心跳管理
    - [ ] 文件存储
    - [ ] 选址算法
    - [ ] 分布式事务
    - [ ] 多副本
    - [ ] 共识协议

  - 性能优化：
    - [ ] 系统调用劫持（用户态文件系统）
    - [ ] RDMA
    - [ ] 软硬件结合优化

  - 测试
    - [ ] IO500
    - [ ] 对比测试

- IO500比赛

## 安装

## 快速开始

## 开源协议
[Apache License 2.0](https://github.com/labring/sealfs/blob/main/LICENSE)