package com.ipd.jmq.common.cache;



import com.ipd.jmq.toolkit.lang.LifeCycle;
import  com.ipd.jmq.toolkit.plugin.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 缓存集群管理器，用于分布式缓存
 * Created by hexiaofeng on 16-1-27.
 */
public interface CacheCluster extends ServicePlugin, LifeCycle {

    /**
     * 获取缓存值
     *
     * @param key 键
     * @return 值
     */
    String get(String key);

    /**
     * 获取原有值并设置新值
     *
     * @param key   键
     * @param value 值
     * @return 原有的值
     */
    String getSet(String key, String value);

    /**
     * 设置缓存值
     *
     * @param key   键
     * @param value 值
     */
    void set(String key, String value);

    /**
     * 设置缓存值，并设置超时时间
     *
     * @param key     键
     * @param value   值
     * @param timeout 超时时间
     * @param unit    单位
     */
    void setEx(String key, String value, long timeout, TimeUnit unit);

    /**
     * 当键不存在，设置值
     *
     * @param key   键
     * @param value 值
     * @return 是否成功
     */
    boolean setNX(String key, String value);

    /**
     * 当键不存在，设置值，并设置超时时间
     *
     * @param key     键
     * @param value   值
     * @param timeout 超时时间
     * @param unit    单位
     * @return 是否成功
     */
    boolean setNXEx(String key, String value, long timeout, TimeUnit unit);

    /**
     * 设置过期时间
     *
     * @param key     键
     * @param timeout 超时时间
     * @param unit    耽误
     * @return 成功
     */
    boolean expire(String key, long timeout, TimeUnit unit);

    /**
     * 删除指定键对应的缓存值
     *
     * @param key 键
     * @return 成功
     */
    boolean del(String key);

    /**
     * 递增
     *
     * @param key 键
     * @return 递增后的值
     */
    long incr(String key);

    /**
     * 增加
     *
     * @param key   键
     * @param delta 增量
     * @return 递增后的值
     */
    long incrBy(String key, long delta);

    /**
     * 递减
     *
     * @param key 键
     * @return 递减后的值
     */
    long decr(String key);

    /**
     * 减少
     *
     * @param key   键
     * @param delta 增量
     * @return 减少后的值
     */
    long decrBy(String key, long delta);

    /**
     * 命令用于将一个或多个值插入到列表的尾部(最右边)
     *
     * @param key    键
     * @param values 值数组
     * @return 列表长度
     */
    long rPush(String key, String... values);

    /**
     * 命令用于将一个或多个值插入到列表的头部(最左边)
     *
     * @param key    键
     * @param values 值数组
     * @return 列表长度
     */
    long lPush(String key, String... values);

    /**
     * 用于返回列表的长度。 如果列表 key 不存在，则 key 被解释为一个空列表，返回 0 。 如果 key 不是列表类型，返回一个错误。
     *
     * @param key 键
     * @return 列表长度
     */
    long lLen(String key);

    /**
     * 用于移除并返回列表的第一个元素
     *
     * @param key 键
     * @return 第一个元素
     */
    String lPop(String key);

    /**
     * 用于移除并返回列表的最后一个元素
     *
     * @param key 键
     * @return 最后一个元素
     */
    String rPop(String key);

    /**
     * 返回列表中指定区间内的元素，区间以偏移量 START 和 END 指定。 其中 0 表示列表的第一个元素， 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2表示列表的倒数第二个元素，以此类推
     *
     * @param key   键
     * @param begin 起始位置
     * @param end   结束位置
     * @return 区间元素列表
     */
    List<String> lRange(String key, final long begin, final long end);

}
