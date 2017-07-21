JMQ
=================
This quick start guide is a detailed instruction of setting up JMQ messaging system 

on your local machine to send and receive messages.

### Prerequisite ###
The following softwares are assumed installed:
  1. 64bit OS, Linux/Unix/Mac is recommended or win7/8/10
  2. 64bit JDK 1.7+
  3. Maven 3.1.x
  4. Git
  5. Zookeeper 3.0+

### Clone AND Build ###

    > git clone -b develop https://github.com/ipdcode/jmq
    > cd jmq
    > mvn -Prelease-all -DskipTests clean install -U

Follow instructions in http://kafka.apache.org/documentation.html#quickstart

### Start Zookeeper ###
Download or use your owned zookeeper to provide service

### Start BROKER ###
The default registry service of broker is zookeeper,the address is 127.0.0.1:2181

If you want to edit it, please see the pom.xml of jmq-broker-runtime in which the most of config infos is

The service of broker is jmq-broker-runtime module

    > cd jmq-broker-runtime/jmq-server/jmq-broker-runtime/target/jmq-server/jmq-server/bin
    > chmod 755 *
    > nohup sh bin/startmq.sh &
    > tail -f ~/export/Logs/jmq.server/{address}/jmq-server.log

### Init Cluster Infos ###
At present we only provide the config approach for broker and topic

The path of broker template file is jmq-service/jmq-broker/test/resources/broker_init.json

The path of topic template file is jmq-service/jmq-broker/test/resources/topic_init.json

You may make appropriate changes or use the default config to initialize the cluster infos

Initialize cluster infos use the telnet protocol

Suppose the address of broker is 192.168.1.5

    >  telnet 127.0.0.1 10088
    >  auth -u jmq -p jmq
    >  topic set [{"archive":false,"consumers":{"app4Consumer":{}},"groups":["jmq102"],"importance":1,"producers":{"app4Product":{}},"queues":5,"topic":"topic_simple","type":"TOPIC"}]
    >  topic get 
    

    > broker set [{"alias":"jmq102_m","dataCenter":9,"id":0,"ip":"192.168.1.5","permission":"FULL","port":50088,"retryType":"DB","syncMode":"SYNCHRONOUS"}]
    > broker get
    > ctrl + ]
    > q

### Send & Receive Messages ###

Send simple example(refer to jmq-example/ApiProducerTest.java)

```java
        // connection Configuration
        TransportConfig config = new TransportConfig();
        config.setApp(app);
        config.setAddress(address);
        config.setUser("jmq");
        config.setPassword("jmq");
        config.setSendTimeout(10000);
        // set epoll pattern，windows set false and linux set true
        config.setEpoll(false);
        manager = new ClusterTransportManager(config);
        manager.start();
        producer = new MessageProducer(manager);
        producer.start();
        
        Message message = new Message(topic, "" + i, "business ID" + i);
        producer.send(message);
```

Receive simple example (refer to jmq-example/ApiConsumerTest.java)

```java
       // connection Configuration
       TransportConfig config = new TransportConfig();
       config.setApp(app);
       config.setAddress(address);
       config.setUser("jmq");
       config.setPassword("jmq");
       config.setSendTimeout(5000);
       // set epoll pattern，windows set false and linux set true
       config.setEpoll(false);
       ConsumerConfig consumerConfig = new ConsumerConfig();
       manager = new ClusterTransportManager(config);
       messageConsumer = new MessageConsumer(consumerConfig, manager, null);
       // launch consumer service
       messageConsumer.start();
       
       // subscribe topic 
       messageConsumer.subscribe(topic, messageListener);
       CountDownLatch latch = new CountDownLatch(1);
       latch.await();
```
    
So far if you completed the previous work, congratulations! you're getting started.
Then you will see some advanced features such as order-message, transaction-message, parallel consumption,replication policy.

