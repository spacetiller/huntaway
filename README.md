# Huntaway (Warining: Stop maintenance. Only for tutorial.)
A nutcracker edition for redis cluster, revised on Twemproxy v2.4.0. Extensions include RW seperation, Hot/Cold data, and failover, etc.
这是2013年做的一个Nutcracker改版，为了实现redis集群，当时3.0还没出来，初步实现了Redis的集群功能，

Nutcracker 又称 Twemproxy ，是一个memcache、redis协议的轻量级代理，一个用于sharding 的中间件。有了Twemproxy，客户端不直接访问Redis服务器，而是通过Twemproxy 代理中间件间接访问。 Twemproxy 为 twitter 开源产品, 目前版本可以查看 [这里](https://github.com/twitter/twemproxy)

Redis 是一个开源（BSD许可）的，内存中的数据结构存储系统，它可以用作数据库、缓存和消息中间件。 它支持多种类型的数据结构，如 字符串（strings）， 散列（hashes）， 列表（lists）， 集合（sets）， 有序集合（sorted sets） 与范围查询， bitmaps， hyperloglogs 和 地理空间（geospatial） 索引半径查询。 Redis 内置了 复制（replication），LUA脚本（Lua scripting）， LRU驱动事件（LRU eviction），事务（transactions） 和不同级别的 磁盘持久化（persistence）， 并通过 Redis哨兵（Sentinel）和自动 分区（Cluster）提供高可用性（high availability）。

2013-12-03： NutCracker修改初步思路；

在服务器池中引入组的概念，在nc_server中增加group(>=0)，0表示没有组，为独立服务器，相同组号的服务器作为一个单位来竞选NC的操作；

假设m为服务器池中所有服务器的数量，则一个组中包含服务器数量n为1~m个；

组中的服务器保存相同的数据。

写操作：使NC能够辨认服务器组，并向组中的主服务器写数据；（如何保证组中服务器数据同步？）

读操作的NC主要针对slave redis，可以在默认值下（作为独立服务器）进行读操作；

修改配置文件，使多个服务器编为一组；

宕机：在组服务器的基础上，如果组中多个服务器中的最多n-1个服务器宕机，NC会将第一个（或缓存的）辅助服务器作为主服务器。

过程：
修改打印之后不起作用，发现nutcracker命令使用了/usr/local/sbin中的旧文件，而不是/usr/local/bin下面的；


>	NC对client端需要返回状态，从Redis Server返回，通过NC到client，这也就是NC无法实现select的原因，详细看代码。

>	在配置文件中，服务器添加组识别域，变成诸如：10.0.2.70:8604:1|1:1:m，最后的三个符号分别是：组号、组权重和组读写标志。
结果：无法解析，nutcracker报错停止。修改，OK。

>	服务器设置在conf_handler函数中，这个函数会对得到的key:value对进行扫描，然后与常量命令组conf_commands中的关键字进行对照，符合则执行对应的设置函数，比如对服务器就是conf_add_server ()，这个函数解析出IP、Port等参数，放在struct sockinfo{ int family; socklen_t addrlen; union { struct sockaddr_in in…}addr }结构体中；

>	Nc_conf.c：找到配置的关键：
conf_add_server():delim[]，修改delim[] = “ :::”，然后在对delim的循环中，添加对最后一个“:”的处理，并变为可选。

>	Nc_server.h：在struct server中增加属性：uint32_t gid；

>	Nc_server.c：在配置单个服务器的函数conf_server_each_transform()中，从conf_server中将group赋值给server->gid。

>	分发过程：NC初始化之后，先在接收事件挂载proxy_recv，真正接到消息请求后，在proxy_recv中挂载真正的消息接收、发生函数（conn_get函数中）；  请求的分发在消息接收函数中，有个server_pool_server函数，在一个server pool中选择一个server来分发，核心的路由机制很可能会在这里修改。

## Huntaway结构图
[Huntaway结构图](https://github.com/spacetiller629/huntaway/blob/master/images/Huntaway1-%E7%BB%93%E6%9E%84.png)

## 组模型示意图
[组模型示意图](https://github.com/spacetiller629/huntaway/blob/master/images/Huntaway2-%E7%BB%84%E6%A8%A1%E5%9E%8B.png)

## 另附：Nutcracker实体关系图
[Nutcracker实体关系图](https://github.com/spacetiller629/huntaway/blob/master/images/Nutcracker%E5%AE%9E%E4%BD%93%E5%85%B3%E7%B3%BB%E5%9B%BE.png)

