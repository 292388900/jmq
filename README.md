JMQ
=================
JMQ<sup>TM</sup> is a distributed messaging and streaming data platform with low latency, high performance, reliability, trillion-level capacity and flexible scalability.


### JMQ Features ###
* Technical Standards
   - Custom protocol specifications
   - Compatibility with Kafka protocol
   
* HA(High Availability)
   - Master/Slave mode deployment. Automatic failover
   - Messages are asynchronously archived to cloud storage
   - Fault tolerance with unified exception message retry service for   

* Low Latency
   - Synchronous disk flush, TPS can reach over 20,000 while response time is just 0.004 second, when message size is not greater than 1KB
   - Asynchronous disk flush, TPS can reach over 53,000 while response time is just 0.004 second, when message size is not greater than 1KB
   
* Industrial-grade Sustainability
   - Trillion-level messaging capacity guaranteed
   
* Lightweight-client Model
   - Only communicating with brokers
   - Consumers supporting Pull model
   - Ordered Messaging
   - Transactional Messaging
   - Multiple IDC deployment, nearby message production & consumption
   - Parallel consumption
   
* Flexible Replication Policy
   - Default synchronous replication or degraded asynchronous replication
   - Fixed or dynamic leader election
   - Slave consumption


### User Guide ###
   -  [Quick Start](QuickStart.md)
   -  [Simple Example](QuickStart.md)
   -  FAQ

### Deployment & Operation ###
   - [Deployment](Deployment.md) 
   - Operation
   
### Best Practices ###
   - Core Concepts

### Contributors ###
   
   - hexiaofeng
   - dingjun
   - lindeqiang
   - zhangkepeng
   - luoruiheng
   - tianya
   - weiqisong
   - hujunliang
