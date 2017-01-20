package com.ipd.jmq.client.config;


import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * 传输通道管理器类bean定义.
 *
 * @author lindeqiang
 * @since 14-6-5 上午9:31
 */
public class TransportBeanDefinitionParser extends AbstractMqBeanDefinitionParser {
    @Override
    public String getDefaultId() {
        return "jmq.transport";
    }

    @Override
    public Class<?> getBeanClass(Element element) {
        return ClusterTransportManager.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        this.addConstructorArgVal(builder, createConfigBeanDefinition(element));
        builder.setInitMethodName("start");
        builder.setDestroyMethodName("stop");
        builder.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);


    }

    protected BeanDefinition createConfigBeanDefinition(Element element) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(TransportConfig.class);

        this.addValue(builder, element, "address", true);
        this.addValue(builder, element, "user", true);
        this.addValue(builder, element, "password", true);
        this.addValue(builder, element, "app", true);
        this.addValue(builder, element, "tempPath", false);
        this.addValue(builder, element, "connectionTimeout", false);
        this.addValue(builder, element, "sendTimeout", false);
        this.addValue(builder, element, "workerThreads", false);
        this.addValue(builder, element, "callbackExecutorThreads", false);
        this.addValue(builder, element, "selectorThreads", false);
        this.addValue(builder, element, "maxIdleTime", false);
        this.addValue(builder, element, "reuseAddress", false);
        this.addValue(builder, element, "soLinger", false);
        this.addValue(builder, element, "tcpNoDelay", false);
        this.addValue(builder, element, "keepAlive", false);
        this.addValue(builder, element, "soTimeout", false);
        this.addValue(builder, element, "socketBufferSize", false);
        this.addValue(builder, element, "epoll", false);
        this.addValue(builder, element, "maxOneway", false);
        this.addValue(builder, element, "maxAsync", false);

        return builder.getBeanDefinition();

    }
}
