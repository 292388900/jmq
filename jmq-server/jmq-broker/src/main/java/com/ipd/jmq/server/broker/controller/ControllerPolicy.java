package com.ipd.jmq.server.broker.controller;

import com.ipd.jmq.common.cluster.Permission;
import org.apache.commons.lang3.StringUtils;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhangkepeng on 16-5-12.
 */
public class ControllerPolicy {

    private static final String SPECIAL_SPLIT_STRING = "@";

    // 当前分组
    private String currentGroup;
    // 控制详情,key为控制类别,value为控制内容
    private Map<ControllerType, ControllerParse> controllerDetail = new EnumMap<ControllerType, ControllerParse>(ControllerType.class);
    // 分组
    private String group;
    // 主题
    private String topic;
    // 应用
    private String app;

    public ControllerPolicy(ControllerType controllerType, String controllerInfo) {
        this(controllerType, controllerInfo, null);
    }

    public ControllerPolicy(ControllerType controllerType, String controllerInfo, String currentGroup) {
        this.currentGroup = currentGroup;
        ControllerParse controllerContent = parseControllerInfo(controllerType, controllerInfo);
        if (controllerContent != null) {
            this.controllerDetail.put(controllerType, controllerContent);
        }
    }

    /**
     * 根据控制信息获取元信息
     * 控制信息包含控制内容或分组,主题与应用的组合
     * 例如:content@jmq1@@topic@app
     * @param controllerType
     * @param controllerInfo
     * @return
     */
    private ControllerParse parseControllerInfo(ControllerType controllerType, String controllerInfo) {
        if (controllerType == null || controllerInfo == null) {
            return null;
        }
        // 获取内容
        String content = StringUtils.substringBefore(controllerInfo, SPECIAL_SPLIT_STRING);
        // 解析group,topic和app
        String appLevel = StringUtils.substringAfterLast(controllerInfo, SPECIAL_SPLIT_STRING);
        if (StringUtils.isNoneBlank(appLevel)) {
            this.app = appLevel;
        }
        String regex = Pattern.quote(SPECIAL_SPLIT_STRING) + "(.*?)" + Pattern.quote(SPECIAL_SPLIT_STRING);
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(controllerInfo);
        int index = 1;
        while (matcher.find()) {
            if (StringUtils.isNoneBlank(matcher.group(1)) && 1 == index) {
                this.group = matcher.group(1);
            }
            if (StringUtils.isNoneBlank(matcher.group(1)) && 2 == index) {
                this.topic = matcher.group(1);
            }
            index++;
        }
        ControllerParse controllerParse = new ControllerParseImpl();
        Map<String, String> controllerContent = null;
        if (controllerType == ControllerType.TOPIC_BROKER_PERMISSION) {
            controllerContent = parseTopicBrokerPermission(content);
            if (controllerContent != null && !controllerContent.isEmpty()) {
                controllerParse.setParseContent(controllerContent);
            }
        } else if (controllerType == ControllerType.APP_CLIENT_IP_LIMIT) {
            controllerContent = parseAppClientIPLimit(content);
            if (controllerContent != null && !controllerContent.isEmpty()) {
                controllerParse.setParseContent(controllerContent);
            }
        }
        return controllerParse;
    }

    /**
     * 解析topic控制broker读写权限的控制内容
     * @param content
     * @return
     */
    private Map<String, String> parseTopicBrokerPermission(String content) {
        if (null == content) {
            return null;
        }
        boolean isIncludeGroup = false;
        Map<String, String> controllerContent = new HashMap<String, String>();
        if (StringUtils.contains(content, currentGroup)) {
            isIncludeGroup = true;
        }
        String[] contentArray = StringUtils.split(content, ";");
        for (int count = 0; count < contentArray.length; count++) {
            String[] groupPermissionArray = StringUtils.split(contentArray[count],":");
            if (2 == groupPermissionArray.length) {
                // 如果为Full不用添加,并删除旧数据不做判断
                if (!StringUtils.contains(groupPermissionArray[1], "FULL")) {
                    if (isIncludeGroup) {
                        controllerContent.put(groupPermissionArray[0], groupPermissionArray[1]);
                    }
                } else {
                    controllerContent.remove(groupPermissionArray[0]);
                }
            }
        }
        return controllerContent;
    }

    /**
     * 解析应用控制客户端某个ip生产或消费的控制内容
     * @param content
     * @return
     */
    private Map<String,String> parseAppClientIPLimit(String content) {
        if (null == content) {
            return null;
        }
        Map<String, String> controllerContent = new HashMap<String, String>();
        String[] contentArray = StringUtils.split(content, ";");
        for (int count = 0; count < contentArray.length; count++) {
            String[] ipPermissionArray = StringUtils.split(contentArray[count],":");
            if (2 == ipPermissionArray.length) {
                // 如果为Full不用添加,并删除旧数据不做判断
                if (!StringUtils.contains(ipPermissionArray[1], "FULL")) {
                    controllerContent.put(ipPermissionArray[0], ipPermissionArray[1]);
                } else {
                    controllerContent.remove(ipPermissionArray[0]);
                }
            }
        }
        return controllerContent;
    }

    /**
     * 判断主题该分组是否被添加了控制写
     * type:true 判断是否限制写权限
     * type:fasle 判断是否限制读权限
     * @param group
     * @return
     */
    public boolean topicBrokerPermission(String group, boolean type) {
        if (group != null && controllerDetail != null &&
                !controllerDetail.isEmpty()) {
            ControllerParse controllerParse = controllerDetail.get(ControllerType.TOPIC_BROKER_PERMISSION);
            if (controllerParse == null) {
                return false;
            }
            Map<String, String> contents = (Map<String,String>) controllerParse.getParseContent();
            if (contents == null || contents.isEmpty()) {
                return false;
            }
            String permission = contents.get(group);
            if (permission != null) {
                if (type) {
                    if (permission.equals(Permission.READ.toString()) ||
                            permission.equals(Permission.NONE.toString())) {
                        return true;
                    }
                } else {
                    if (permission.equals(Permission.WRITE.toString()) ||
                            permission.equals(Permission.NONE.toString())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * 判断连接地址是否具有读或写权限
     * type:true 需要判断写
     * type:false 需要判断读
     * @param address 连接的ip
     * @return
     */
    public boolean appClientIPLimit(String address, boolean type) {
        if (address != null && controllerDetail != null &&
                !controllerDetail.isEmpty()) {
            ControllerParse controllerParse = null;
            Map<String, String> contents = null;

            controllerParse = controllerDetail.get(ControllerType.APP_CLIENT_IP_LIMIT);
            if (controllerParse == null) {
                return false;
            }

            String permission = null;
            contents = (Map<String, String>) controllerParse.getParseContent();

            // 获取IP地址
            String ip = StringUtils.substringBefore(address, ":");
            if (contents != null) {
                permission = contents.get(ip);
            }
            if (permission != null) {
                if (type) {
                    if (!permission.equals(Permission.WRITE.toString())) {
                        return true;
                    }
                } else {
                    if (!permission.equals(Permission.READ.toString())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public Map<ControllerType, ControllerParse> getControllerDetail() {
        return controllerDetail;
    }

    public void setControllerDetail(Map<ControllerType, ControllerParse> controllerDetail) {
        this.controllerDetail = controllerDetail;
    }

    public String getCurrentGroup() {
        return currentGroup;
    }

    public void setCurrentGroup(String currentGroup) {
        this.currentGroup = currentGroup;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }
}