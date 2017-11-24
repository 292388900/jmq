package com.ipd.jmq.client.config;

import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.common.model.ProducerConfig;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * 生产者 bean定义.
 *
 * @author lindeqiang
 * @since 14-6-5 上午9:31
 */
public class ProducerBeanDefinitionParser extends AbstractMqBeanDefinitionParser {
    @Override
    public String getDefaultId() {
        return "producer";
    }

    @Override
    public Class<?> getBeanClass(Element element) {
        return MessageProducer.class;
    }


    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        if (StringUtils.hasLength(element.getAttribute("transport"))) {
            this.addConstructorArgRef(builder, element, "transport");
        } else {
            this.addConstructorArgRef(builder, "jmq.transport");
        }
        this.addValue(builder, "config", createConfigBeanDefinition(element));
        this.addValue(builder, element, "feedbackManager", false);

        builder.setInitMethodName("start");
        builder.setDestroyMethodName("stop");
        builder.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
    }


    protected BeanDefinition createConfigBeanDefinition(Element element) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(ProducerConfig.class);
        this.addValue(builder, element, "retryTimes", false);
        this.addValue(builder, element, "acknowledge", false);
        return builder.getBeanDefinition();

    }
}
