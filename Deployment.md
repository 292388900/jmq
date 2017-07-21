Deployment
=================
This section introduces production-ready deployment solution. 

Generally speaking, we are deploying a resilient JMQ cluster having no single point of failure.

### Prerequisite ###
Before starting this section, make sure you have read Quick Start section, 

and are familiars with the core concepts and components of JMQ.

Because the simplest deployment way is Quick Start.

But this way, In general speaking, this plan is for learning and testing purposes only. 

The production environment will not be used it in most of the cases.

The most serious problem is a single Point of Failure. 

### Deployment ###
In order to solve the problem, we provide master-slave deployment with a topic multi shard(broker) way

In most of the cases, we will use this policy. 

If so, we need to adjust their configuration information for broker and topic.

#### Broker demonstration: ####
     [{"alias":"jmq100_m","dataCenter":9,"id":2,"ip":"192.168.1.2","permission":"FULL","port":50088,"retryType":"DB","syncMode":"SYNCHRONOUS"},
     {"alias":"jmq100_s","dataCenter":9,"id":2,"ip":"192.168.1.3","permission":"NONE","port":50088,"retryType":"DB","syncMode":"SYNCHRONOUS"},
     {"alias":"jmq102_m","dataCenter":9,"id":0,"ip":"192.168.1.5","permission":"FULL","port":50088,"retryType":"DB","syncMode":"SYNCHRONOUS"},
     {"alias":"jmq102_s","dataCenter":9,"id":3,"ip":"192.168.1.6","permission":"NONE","port":50088,"retryType":"DB","syncMode":"SYNCHRONOUS"}]

#### Topic demonstration: ####
     [{"archive":false,"consumers":{"app4Consumer":{}},"groups":["jmq100","jmq102"],"importance":1,"producers":{"app4Product":{}},"queues":5,"topic":"topic_simple","type":"TOPIC"}]

Tips: When you update data, you need to compress it so that \s notation does not affect the correctness of the data

Then you may be making appropriate changes and use the telnet command to update them.

If you don't know this way, please take look back to the quick start.

So far, we solved the single Point of Failure and the throughput of the message is increased.

But this program requires at least 4 machines or dockers.If you don't have so many resources in the current.

You can do it in a degraded manner which only need 2 machines or dockers.

#### Broker demonstration: ####
     [{"alias":"jmq102_m","dataCenter":9,"id":0,"ip":"192.168.1.5","permission":"FULL","port":50088,"retryType":"DB","syncMode":"SYNCHRONOUS"},
     {"alias":"jmq102_s","dataCenter":9,"id":1,"ip":"10.42.0.141","permission":"NONE","port":50088,"retryType":"DB","syncMode":"SYNCHRONOUS"}]

#### Topic demonstration: ####

     [{"archive":false,"consumers":{"app4Consumer":{}},"groups":["jmq102"],"importance":1,"producers":{"app4Product":{}},"queues":5,"topic":"topic_simple","type":"TOPIC"}]

In addition, we also provide a way used raft protocol to dynamically elect the master in broker group.

But at present it's still testing. Interested students can see the source code directly. 

Later, we will introduce the relevant official version. Please look forward to.
   
   