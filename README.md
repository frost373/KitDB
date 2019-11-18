# KitDB


## Overview

KitDB是一个内嵌式持久型的 高速NoSQL存储 lib，以jar 包方式嵌入到应用中。   
KitDB 提供了类似Redis 的数据结构。如KV、List、Map、ZSET等。也提供了TTL（生存时间）、备份、ACID事物，多节点强一致性等功能。   
KitDB完全基于磁盘存储，并提供最高百万级别的查询性能和数十万的写入性能。   

## Features

- 完全基于磁盘，不受内存限制
- KV、List、Map、Zet、ZSET等丰富的数据结构
- 百万级别的查询性能和数十万级的写入性能
- 原子性写入，读写无冲突
- TTL（生存时间）
- 备份与恢复
- ACID事物
- 多节点一致性支持（官方插件使用Raft协议支持强一致性，也可自行使用其他协议或方式）


## Requirements
编译要求：JDK 8+和Maven 3.2.5+

## Documents


## Explain
store模块为KitDB本体，raft模块为官方Raft协议插件


## Notice
目前版本为最终预览版，主要开发工作已完成，正在完善文档和最后测试



