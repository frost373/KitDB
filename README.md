# KitDB
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
![build](https://img.shields.io/badge/build-passing-brightgreen)
[![Maven Central](http://thinkin.top/pic/maven.svg)](https://search.maven.org/search?q=top.thinkin.kitdb)   
![jdk](https://img.shields.io/badge/JDK-1.8%2B-yellow)
![OS](https://img.shields.io/badge/OS-Linux%7CWindows%7CMac-orange)


更多内容参见：http://kitdb.top/

## Overview

KitDB是一个内嵌式持久型的 高速NoSQL存储 lib，以jar 包方式嵌入到应用中。   
KitDB 提供了类似Redis 的数据结构。如KV、List、Map、ZSET等。也提供了TTL（生存时间）、备份、ACID事物，多节点强一致性等功能。   
KitDB完全基于磁盘存储，并提供最高百万级别的查询性能和十万的写入性能。   

## GIT IT
```xml
 <dependency>
        <groupId>top.thinkin.kitdb</groupId>
        <artifactId>store</artifactId>
        <version>VERSION</version>
  </dependency>
```
VERSION是Maven Central的最新版本号。您可以点击此图像找到当前版本：
[![Maven Central](http://thinkin.top/pic/maven.svg)](https://search.maven.org/search?q=top.thinkin.kitdb)

## Features

- 完全基于磁盘，不受内存限制
- KV、List、Map、Zet、ZSET等丰富的数据结构
- 最高百万级别的查询性能和十万级的写入性能
- 原子性写入，读写无冲突
- TTL（生存时间）
- 备份与恢复
- ACID事物
- 多节点一致性支持（官方插件使用Raft协议支持强一致性，也可自行使用其他协议或方式）

#### KitDB和Redis的性能对比


<img src="https://raw.githubusercontent.com/wiki/frost373/KitDB/pic/readme/test_with_redis1.png" width="70%">

> 注意：KitDB的测试为本地操作，和Redis对比无意义，只为说明KitDB的性能级别


## Requirements
编译要求：JDK 8+和Maven 3.2.5+

> 单元测试默认数据存储路径为/data/kitdb 如需调整，可用-Dkitdb_path 指定，例如：
```
test -Dkitdb_path=D:\\temp\\db -f pom.xml
```


## Documents
http://kitdb.top/



## Explain
store模块为KitDB本体，raft模块为官方Raft协议插件


## 操作系统兼容问题
对常用进行开发和运行环境的**操作系统**进行测试，操作系统使用**官方镜像重新安装，排除干扰**。开发环境IDE使用IntelliJ IDEA Community。
   
操作系统 |系统位数|环境| Java虚拟机 | Java虚拟机版本 | 结果
---|---|---|---|---|---
Windows 10|64 | 开发| OpenJDK 64-Bit Server VM | 13.0.2+8| 通过
Windows 7 |64| 开发 | OpenJDK 64-Bit Server VM| 11.0.5+10-b520.388|  通过
Windows Server 2008 R2  |64| 运行| OpenJDK 64-Bit Server VM | 13.0.2+8|  通过
Ubuntu 18.04 |64| 运行 | OpenJDK 64-Bit Server VM| 11.0.6+10-post-Ubuntu-1ubuntu118.04.1 |  通过
Ubuntu 16.04 |64| 运行 | OpenJDK 64-Bit Server VM| 9-internal+0-2016-04-14-195246.buildd.src | 通过
CentOS 8.0   |64| 运行 | OpenJDK 64-Bit Server VM| 11.0.5+10-LTS|  通过


有少量朋友反应在开发环境无法使用KitDB，这可能是由于所用Windows缺少某些运行包引起的。   
可以尝试安装如下环境 [去微软官网下载13.9 MB](https://www.microsoft.com/zh-CN/download/details.aspx?id=48145)
