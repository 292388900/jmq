/**
 *
 */
package com.ipd.jmq.client.config;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * mq名空间处理器
 *
 * @author lindeqiang
 */
public class MqNamespaceHandler extends NamespaceHandlerSupport {

    @Override
    public void init() {
        registerBeanDefinitionParser("transport", new TransportBeanDefinitionParser());
        registerBeanDefinitionParser("consumer", new ConsumerBeanDefinitionParser());
        registerBeanDefinitionParser("producer", new ProducerBeanDefinitionParser());
    }
}
