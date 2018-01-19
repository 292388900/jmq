Quick Start
=================
This quick start guide is a set of detailed instructions of setting up JMQ messaging system 
on your local machine to send and receive messages.

### Prerequisites ###
The following softwares are assumed installed:

  1. 64-bit OS
   * Linux/Unix/Mac OS (recommended)
   * Windows 7/8/10
  2. 64-bit JDK 1.7+
  3. Maven 3.1.x
  4. Git
  5. Zookeeper 3.0+

### Clone & Build ###

    > git clone https://github.com/tigcode/jmq
    > cd jmq
    > mvn -Prelease-all -DskipTests clean install -U

### Start Zookeeper ###
Start your Zookeeper server. 

### Start Broker ###
The default broker registration is provided by Zookeeper at the address 127.0.0.1:2181. That address can be changed in the pom.xml of the module jmq-server/jmq-broker-runtime, which contains most configuration items that can be customized.

The jmq-broker-runtime module is the part of broker service.

    > cd jmq-broker-runtime/jmq-server/jmq-broker-runtime/target/jmq-server/jmq-server/bin
    > chmod 755 *
    > nohup sh bin/startmq.sh &
    > tail -f ~/export/Logs/jmq.server/{address}/jmq-server.log

### Set up a Cluster ###
So far we just introduces the config approach for brokers and topics.

The broker template file can be found at *jmq-service/jmq-broker/test/resources/broker_init.json*.

The topic template file can be found at *jmq-service/jmq-broker/test/resources/topic_init.json*.

You may make appropriate changes or just use the defaults to initialize the cluster infomation.

Initialize cluster infos use the telnet protocol.

Suppose the address of broker is 192.168.1.5

    >  telnet 192.168.1.5 10088
    >  auth -u jmq -p jmq
    >  topic set [{"archive":false,"consumers":{"app4Consumer":{}},"groups":["jmq102"],"importance":1,"producers":{"app4Product":{}},"queues":5,"topic":"topic_simple","type":"TOPIC"}]
    >  topic get 
    

    > broker set [{"alias":"jmq102_m","dataCenter":9,"id":0,"ip":"192.168.1.5","permission":"FULL","port":50088,"retryType":"DB","syncMode":"SYNCHRONOUS"}]
    > broker get
    > ctrl + ]
    > q

### Send & Receive Messages ###

A simple example for message sending(refer to jmq-example/ApiProducerTest.java)

```java
        // connection settings
        TransportConfig config = new TransportConfig();
        config.setApp(app);
        config.setAddress(address);
        config.setUser("jmq");
        config.setPassword("jmq");
        config.setSendTimeout(10000);
        // epoll mode is used on Linux, not used on Windows
        config.setEpoll(false);
        manager = new ClusterTransportManager(config);
        manager.start();
        producer = new MessageProducer(manager);
        producer.start();
        
        Message message = new Message(topic, "" + i, "business ID" + i);
        producer.send(message);
```

A simple example for message receiving(refer to jmq-example/ApiConsumerTest.java)

```java
       // connection settings
       TransportConfig config = new TransportConfig();
       config.setApp(app);
       config.setAddress(address);
       config.setUser("jmq");
       config.setPassword("jmq");
       config.setSendTimeout(5000);
       // epoll mode is used on Linux, not used on Windows
       config.setEpoll(false);
       ConsumerConfig consumerConfig = new ConsumerConfig();
       manager = new ClusterTransportManager(config);
       messageConsumer = new MessageConsumer(consumerConfig, manager, null);
       // launch consumer service
       messageConsumer.start();
       
       // subscribe the topic 
       messageConsumer.subscribe(topic, messageListener);
       CountDownLatch latch = new CountDownLatch(1);
       latch.await();
```
    
So far the set up is done. Congratulations!

Next, you will see advanced features such as ordered messaging, transactional messaging, parallel consumption,replication policies.

