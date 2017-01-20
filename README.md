# 消息中间件:jmq


jmq系统的目标就是建立一消息平台对外提供高可用，可扩展的消息传递服务,并能消息进行有效的治理.




# 目录结构

### jmq-client

这里存放的是客户端工具

- jmq-client-core ：客户端核心代码,包括发送者和消费者等相关功能代码
- jmq-client-spring ：客户端spring配置支持代码

### jmq-common

这里存放的是系统的公用组件


- jmq-cache ：缓存相关代码
- jmq-model ：公共数据模型类
- jmq-network ：网络传输相关代码
- registry ：注册中心客户端相关代码
- tookit ：工具代码

### jmq-service

这里存放的是服务器端代码
- jmq-broker ：服务器核心代码。
- jmq-context ：上下文管理代码。
- jmq-replication ：主从同步代码。
- jmq-store ：存储代码。

### 其他
    项目正在建设中，后续陆续开放相关代码

