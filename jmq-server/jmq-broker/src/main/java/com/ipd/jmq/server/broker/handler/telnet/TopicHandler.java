package com.ipd.jmq.server.broker.handler.telnet;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetHandler;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetHeader;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetRequest;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetResponse;
import com.ipd.jmq.common.telnet.Commands;
import com.ipd.jmq.common.telnet.TelnetCode;
import com.ipd.jmq.common.telnet.TelnetResult;
import com.ipd.jmq.registry.PathData;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.toolkit.io.Compressors;
import com.ipd.jmq.toolkit.io.Zip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Topic命令处理器
 *
 * @author songzhimao
 * @date 2017/7/11
 */
public class TopicHandler implements TelnetHandler<Transport> {
    public static Logger logger = LoggerFactory.getLogger(TopicHandler.class);
    public static final String SET = "SET";
    public static final String GET = "GET";

    // 配置
    private BrokerConfig config;

    public void setConfig(BrokerConfig config) {
        this.config = config;
    }

    @Override
    public String command() {
        return Commands.TOPIC;
    }

    @Override
    public String help() {
        return null;
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        Registry registry = config.getRegistry();
        TelnetResult telnetResult = new TelnetResult();
        TelnetRequest payload = (TelnetRequest) command.getPayload();
        String args[] = payload.getArgs();
        try {
            if (args.length > 1 && args[0].equalsIgnoreCase(SET)) {
                String content = args[1];
                List<TopicConfig> topicConfigList = JSON.parseArray(content, TopicConfig.class);
                if (topicConfigList != null && !topicConfigList.isEmpty()) {
                    byte[] bytes = content.getBytes(Charset.forName("UTF-8"));
                    if (config.isCompressed()) {
                        bytes = Compressors.compress(bytes, Zip.INSTANCE);
                    }
                    PathData pathData = new PathData(config.getTopicPath(), bytes);
                    registry.update(pathData);
                    telnetResult.setMessage("ok");
                    telnetResult.setStatus(TelnetCode.NO_ERROR);
                } else {
                    telnetResult.setStatus(TelnetCode.PARAM_ERROR);
                    telnetResult.setMessage(TelnetCode.ERROR_TEXT.get(TelnetCode.PARAM_ERROR));
                }
            } else if (args.length == 1 && args[0].equalsIgnoreCase(GET)) {
                PathData pathData = registry.getData(config.getTopicPath());
                byte[] data = pathData.getData();
                if (pathData == null || data.length == 0) {
                    telnetResult.setMessage("{}");
                    telnetResult.setStatus(TelnetCode.NO_ERROR);
                } else {
                    String content;
                    if (config.isCompressed()) {
                        content = new String(Compressors.decompress(data, Zip.INSTANCE));
                    } else {
                        content = new String(data, Charset.forName("UTF-8"));
                    }
                    telnetResult.setMessage(content);
                    telnetResult.setStatus(TelnetCode.NO_ERROR);
                    if (logger.isInfoEnabled()) {
                        logger.info("get topic config.");
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("get topic config." + content);
                    }
                }
            } else {
                // 没有找到对应的参数直接报异常
                telnetResult.setStatus(TelnetCode.PARAM_ERROR);
                telnetResult.setMessage(TelnetCode.ERROR_TEXT.get(TelnetCode.PARAM_ERROR));
            }
        } catch (IOException ioe) {
            logger.error("compress is error", ioe);
            telnetResult.setStatus(TelnetCode.COMPRESS_ERROR);
            telnetResult.setMessage(TelnetCode.ERROR_TEXT.get(TelnetCode.COMPRESS_ERROR));
        } catch (RegistryException re){
            logger.error("registry is error", re);
            telnetResult.setStatus(TelnetCode.COMPRESS_ERROR);
            telnetResult.setMessage(TelnetCode.ERROR_TEXT.get(TelnetCode.COMPRESS_ERROR));
        }
        catch (Exception e) {
            logger.error("unknown error caused", e);
            telnetResult.setStatus(TelnetCode.COMPRESS_ERROR);
            telnetResult.setMessage(TelnetCode.ERROR_TEXT.get(TelnetCode.UNKNOWN));
        }

        return new Command(TelnetHeader.Builder.response(),
                new TelnetResponse(JSON.toJSONString(telnetResult), true, true));
    }
}
