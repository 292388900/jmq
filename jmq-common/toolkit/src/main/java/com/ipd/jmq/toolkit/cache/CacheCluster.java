package com.ipd.jmq.toolkit.cache;



import com.ipd.jmq.toolkit.lang.LifeCycle;
import com.ipd.jmq.toolkit.plugin.ServicePlugin;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 缓存集群管理器，用于分布式缓存
 * Created by hexiaofeng on 16-1-27.
 */
public interface CacheCluster extends ServicePlugin, LifeCycle {


    /**
     * 增加键值对
     *
     * @param key   键
     * @param value 值
     */
    public void put(String key, String value);


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
    /**
     * 追加到现有键对应列表中
     *
     * @param key      键
     * @param value    值
     * @param maxCount 列表最大长度，如超过最大长度，将移除最早的元素
     */
    public void rpush(String key, String value, int maxCount);

    /**
     * 追加到现有键对应列表中
     *
     * @param key      键
     * @param value    值
     * @param maxCount 列表最大长度，如超过最大长度，将移除最早的元素
     */
    public void rpush(byte[] key, byte[] value, int maxCount);

    /**
     * 取出对应键中相应范围内的值
     *
     * @param key  键
     * @param from 开始索引
     * @param to   结束索引
     * @return 值的列表
     */
    public List<String> range(String key, int from, int to);

    /**
     * 取出对应键中相应范围内的值
     *
     * @param key  键
     * @param from 开始索引
     * @param to   结束索引
     * @return 值的列表
     */
    public List<byte[]> range(byte[] key, int from, int to);


    /**
     * 获取对应键的值
     *
     * @param key 键
     * @return 值
     */
    public String get(String key);

    /**
     * 获取对应键的值
     *
     * @param key 键
     * @return 值
     */
    public byte[] get(byte[] key);



    /**
     * 原子性的将对应键的值减去指定值
     *
     * @param key   键
     * @param count 值
     * @return 自增完成的值
     */
    public Long decrBy(String key, long count);

    /**
     * 删除对应的键值对
     *
     * @param key 键
     */
    public void delete(String key);



    /**
     * 设置有效期键值
     *
     * @param key    键
     * @param seconds 有效期
     * @param value  值
     * @return
     */
    public void setex(byte[] key, int seconds, byte[] value);

    /**
     * 设置有效期键值
     *
     * @param key     键
     * @param seconds 有效期
     * @param value   值
     * @return
     */
    public void setex(String key, int seconds, String value);

    /**
     * 添加sorted set
     *
     * @param key    键
     * @param score  分数
     * @param member 值
     */
    public void zadd(String key, double score, String member);

    /**
     * 通过位置返回sorted set指定区间内的成员
     *
     * @param key   键
     * @param start 其实位置
     * @param end   结束位置
     * @return 返回所有符合条件的成员
     */
    public Set<String> zrange(String key, long start, long end);

    /**
     * 通过分数返回sorted set指定区间内的成员
     *
     * @param key 键
     * @param min 最小评分
     * @param max 最大评分
     * @return 返回所有符合条件的成员
     */
    public Set<String> zrangeByScore(String key, double min, double max);

    /**
     * 通过分数返回sorted set指定区间内的成员
     *
     * @param key    键
     * @param min    最小评分
     * @param max    最大评分
     * @param offset 偏移位置
     * @param count  返回数量
     * @return 返回所有符合条件的成员
     */
    public Set<String> zrangeByScore(String key, double min, double max, long offset, long count);

    /**
     * 统计score在min和max之间的成员数
     *
     * @param key 键
     * @param min 最小score
     * @param max 最大score
     * @return 符合条件的成员数
     */
    public Long zcount(String key, double min, double max);

    /**
     * 统计成员数量
     * @param key
     * @return
     */
    public Long zcard(String key);

    /**
     * 移除sorted set中的一个或多个成员
     *
     * @param key    键
     * @param member 值
     * @return 被成功移除的成员的数量，不包括被忽略的成员
     */
    public Long zrem(String key, String... member);

    /**
     * 获取指定成员的评分
     * @param key 键
     * @param member 值
     * @return 评分，不存在则返回null
     */
    public Double zscore(String key, String member);

    /**
     * 如果不存在则设置
     *
     * @param key
     * @param value
     * @return 是否成功
     */
    public Boolean setnx(String key, String value);

    /**
     * 设置过期时间
     *
     * @param key     键
     * @param seconds 毫秒
     * @return 是否成功
     */
    public Boolean expire(String key, int seconds);

    /**
     * 获取key的剩余生存时间
     *
     * @param key 键
     * @return 当 key 不存在时，返回 -2
     * 当 key 存在但没有设置剩余生存时间时，返回 -1
     * 否则，以秒为单位，返回 key 的剩余生存时间
     */
    public Long ttl(String key);

    /**
     * 移出并获取列表的第一个元素
     *
     * @param key 键
     * @return 第一个元素
     */
    public String lpop(String key);

    /**
     * 在列表中添加一个或多个值
     *
     * @param key   键
     * @param value 值
     * @return 最新列表长度
     */
    public Long rpush(String key, String... value);

    /**
     * 获取列表长度
     *
     * @param key 键
     * @return 长度
     */
    public Long llen(String key);

    /**
     * 从key对应list中删除count个和value相同的元素
     *
     * @param key
     * @param count
     *      count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count 。
     *      count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值。
     *      count = 0 : 移除表中所有与 value 相等的值。
     * @param value
     * @return 被移除的个数
     */
    public Long lrem(String key, final long count, String value);

    /**
     * 移除并返回集合中的一个随机元素
     *
     * @param key
     * @return
     */
    public String spop(String key);

    /**
     * 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略。
     * 假如 key 不存在，则创建一个只包含 member 元素作成员的集合。
     *
     * @param key
     * @param values
     * @return 被添加到集合中的新元素的数量，不包括被忽略的元素
     */
    public Long sadd(String key, String... values);

    /**
     * 返回集合 key 的基数(集合中元素的数量)。
     *
     * @param key
     * @return 当 key 不存在时，返回 0
     */
    public Long scard(String key);

    /**
     * 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略。
     * 当 key 不是集合类型，返回一个错误。
     *
     * @param key
     * @param values
     * @return 被成功移除的元素的数量，不包括被忽略的元素。
     */
    public Long srem(String key, String... values);

    /**
     * 获取委托的缓存
     * @return
     */
    public CacheCluster getDelegate();

}
