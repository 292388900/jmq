package com.ipd.jmq.common.model;

import java.util.HashSet;
import java.util.Set;

/**
 * 指标
 */
public class Metric extends BaseModel {

    // 代码
    private String code;
    // 名称
    private String name;
    // 指标类型
    private MetricType type;
    // 指标来源
    private String source;
    // 聚集函数
    private Aggregator aggregator;
    // 分组字段
    private int groupField;
    // 汇总级别
    private SummaryLevel summaryLevel;


    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public MetricType getType() {
        return type;
    }

    public void setType(MetricType type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Aggregator getAggregator() {
        return aggregator;
    }

    public void setAggregator(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    public int getGroupField() {
        return groupField;
    }

    public void setGroupField(int groupField) {
        this.groupField = groupField;
    }

    public SummaryLevel getSummaryLevel() {
        return summaryLevel;
    }

    public void setSummaryLevel(SummaryLevel summaryLevel) {
        this.summaryLevel = summaryLevel;
    }


    @Override
    public String toString() {
        return "Metric{" +
                "code='" + code + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", source='" + source + '\'' +
                ", aggregator=" + aggregator +
                ", groupField=" + groupField +
                ", summaryLevel=" + summaryLevel +
                '}';
    }

    /**
     * 添加分组字段
     *
     * @param field 字段标示
     */
    public void addField(int field) {
        groupField |= field;
    }

    /**
     * 包含某个字段
     *
     * @param field 字段
     * @return
     */
    public boolean containField(int field) {
        return (groupField & field) > 0;
    }

    /**
     *
     */
    public enum GroupField{
        // 应用字段
        APP_FIELD(0x1,"app"), // 0000 0001
        // 主题字段
        TOPIC_FIELD(0x2,"topic") , // 0000 0010
        // Broker字段
        BROKER_FIELD(0x4,"broker"), // 0000 0100
        // 分组字段
        GROUP_FIELD(0x8,"group"),  // 0000 1000
        // 主机字段
        HOST_FIELD(0x10,"host"), // 0001 0000
         //  时间字段
        TIME_FIELD(0x20,"time"), // 0010 0000
        // 域字段
        DOMAIN_FIELD(0x40,"field"); //0100 0000

        private int operatorNum;
        private String name;

        public String getName() {
            return name;
        }

        public int getOperatorNum() {
            return operatorNum;
        }

        private GroupField(int operatorNum, String name) {
            this.operatorNum = operatorNum;
            this.name = name;
        }
        public static Set<String> getGroupInfo(int num){
            Set<String> fields = new HashSet<String>();
            for(GroupField groupField:values()){
                if((num & groupField.operatorNum) > 0){
                    fields.add(groupField.name);
                }
            }
            return fields;
        }
    }
    /**
     * 指标类型
     */
    public enum MetricType {
        /**
         * 原子
         */
        ATOMIC,
        /**
         * 聚集
         */
        AGGREGATOR
    }

    /**
     * 聚集函数
     */
    public enum Aggregator {
        /**
         * 求和
         */
        SUM,
        /**
         * 平均值
         */
        AVG,
        /**
         * 最大值
         */
        MAX,
        /**
         * 最小值
         */
        MIN
    }

    public enum SummaryLevel {
        /**
         * 不汇总
         */
        NONE,
        /**
         * 汇总到小时
         */
        HOUR,
        /**
         * 汇总到天
         */
        DAY
    }

}
