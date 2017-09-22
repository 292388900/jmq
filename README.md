JMQ
=================
JMQ is distributed messaging and streaming data platform with low latency, high performance and reliability, trillion-level 

capacity and flexible scalability.


### JMQ Feature ###
  1. Technical standard
   - Custom protocol specification
   - Compatibility with kafka protocol
  2. HA
   - Master/Slave mode deployment. Support failover
   - Messages are asynchronously archived to cloud storage
   - Unified exception message retry service for fault tolerance
  3. Low Latency
   - Sync flush disk, TPS for 1K data is 21000.Response time is 0.004 second
   - Async flush disk, TPS for 1K data is 53894.Response time is 0.004 second
  4. Industry Sustainable
   - Trillion-level message capacity guaranteed 
  5. Light-client Model
   - Only communicate with broker
   - Consumer support pull model
   - Order-message
   - Transaction-message
   - Multi IDC deployment, send and consume nearby
   - Parallel consumption
  6. Flexible Replication Policy
   - Default sync replication or degrade async replication
   - Fixed or dynamic election strategy
   - Support for consumption from slave



### USER GUIDE ###
   -  [Quick Start](QuickStart.md)
   -  Simple Example
   -  FQA 

### DEPLOYMENT & OPERATIONS ###
   - [Deployment](Deployment.md) 
   - Operations 
   
### BEST PRACTICE ###
   - Core Concept

### Contribution ###
  At present, our community is still under construction.
   
  The initial contributors below
   
   - hexiaofeng
   - dingjun
   - lindeqiang
   - zhangkepeng
   - luoruiheng
   - tianya
   - weiqisong
   - hujunliang
