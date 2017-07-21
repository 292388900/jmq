JMQ
=================
JMQ is distributed messaging and streaming data platform with low latency, high performance and reliability, trillion-level 

capacity and flexible scalability.

JD researched the JMQ independently, with all of its proprietary intellectual property and manufacturing technology.

### JMQ feature ###
The following softwares are assumed installed:
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
  5. light-client model
   - Only communicate with broker
   - Consumer support pull model
   - Transaction-message
   - Order-message
   - Multi IDC deployment, send and consume nearby
   - Parallel consumption
  6. Flexible replication policy
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

