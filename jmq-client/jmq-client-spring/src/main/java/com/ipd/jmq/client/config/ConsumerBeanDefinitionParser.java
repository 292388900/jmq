package com.ipd.jmq.client.config;

import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.client.consumer.MessageConsumer;
import com.ipd.jmq.client.consumer.TopicSetting;
import com.ipd.jmq.toolkit.retry.RetryPolicy;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedSet;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import java.util.List;
import java.util.Set;

/**
 * 消费者 bean定义.
 *
 * @author lindeqiang
 * @since 14-6-5 上午9:31
 */
public class ConsumerBeanDefinitionParser extends AbstractMqBeanDefinitionParser {
    @Override
    public String getDefaultId() {
        return "consumer";
    }

    @Override
    public Class<?> getBeanClass(Element element) {
        return MessageConsumer.class;
    }


    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {


        Set<BeanDefinition> settings = parseTopicSetting(element);
        this.addConstructorArgVal(builder, createConfigBeanDefinition(element));

        if (StringUtils.hasLength(element.getAttribute("transport"))) {
            this.addConstructorArgRef(builder, element, "transport");
        } else {
            this.addConstructorArgRef(builder, "jmq.transport");
        }
        this.addConstructorArgVal(builder, settings);

        builder.setInitMethodName("start");
        builder.setDestroyMethodName("stop");
        builder.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
    }


    protected BeanDefinition createConfigBeanDefinition(Element element) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(ConsumerConfig.class);

        this.addValue(builder, element, "prefetchSize", false);
        this.addValue(builder, element, "longPull", false);
        this.addValue(builder, element, "pullInterval", false);
        this.addValue(builder, element, "pullEmptySleep", false);
        this.addValue(builder, element, "pullTimeout", false);
        this.addValue(builder, "retryPolicy", createRetryPolicyBeanDefinition(element));
        this.addValue(builder, element, "autoStart", false);
        this.addValue(builder, element, "persistConsumerOffsetInterval", false);
        this.addValue(builder, element, "clientName", false);
        this.addValue(builder, element, "maxConcurrent", false);
        this.addValue(builder, element, "minConcurrent", false);

        return builder.getBeanDefinition();

    }

    protected BeanDefinition createRetryPolicyBeanDefinition(Element element) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(RetryPolicy.class);
        if (StringUtils.hasLength(element.getAttribute("maxRetrys"))) {
            this.addValue(builder, element, "maxRetrys", false);
        } else {
            this.addValue(builder, "maxRetrys", 2);
        }

        if (StringUtils.hasLength(element.getAttribute("maxRetryDelay"))) {
            this.addValue(builder, element, "maxRetryDelay", false);
        } else {
            this.addValue(builder, "maxRetryDelay", 1000);
        }

        if (StringUtils.hasLength(element.getAttribute("retryDelay"))) {
            this.addValue(builder, element, "retryDelay", false);
        } else {
            this.addValue(builder, "retryDelay", 1000);
        }

        if (StringUtils.hasLength(element.getAttribute("useExponentialBackOff"))) {
            this.addValue(builder, element, "useExponentialBackOff", false);
        } else {
            this.addValue(builder, "useExponentialBackOff", false);
        }

        if (StringUtils.hasLength(element.getAttribute("backOffMultiplier"))) {
            this.addValue(builder, element, "backOffMultiplier", false);
        } else {
            this.addValue(builder, "backOffMultiplier", 2);
        }

        if (StringUtils.hasLength(element.getAttribute("expireTime"))) {
            this.addValue(builder, element, "expireTime", false);
        } else {
            this.addValue(builder, "expireTime", 0);
        }

        return builder.getBeanDefinition();
    }


    private Set<BeanDefinition> parseTopicSetting(Element element) {
        List<Element> childElements = DomUtils.getChildElementsByTagName(element, "listener");
        Set<BeanDefinition> consumers = new ManagedSet<BeanDefinition>(childElements.size());
        for (Element childElement : childElements) {
            consumers.add(createListenerBeanDefinition(childElement));
        }
        return consumers;
    }

    public BeanDefinition createListenerBeanDefinition(Element element) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(TopicSetting.class);
        this.addConstructorArgVal(builder, element, "topic", true);
        this.addConstructorArgVal(builder, element, "selector", false);
        this.addConstructorArgRef(builder, element, "listener", true);
        this.addConstructorArgVal(builder, createConfigBeanDefinition(element));
        return builder.getBeanDefinition();
    }

}
