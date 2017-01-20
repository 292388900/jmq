package com.ipd.jmq.common.exception;

import java.util.HashMap;
import java.util.Map;

/**
 * JMQ异常码枚举类
 * <p/>
 * <p>
 * 该类定义了JMQ框架抛出的所有异常码和消息提示文本，用于统一约定和管理所有异常信息
 * </p>
 * <p>
 * JMQ按功能大致分为：公用、服务端、客户端、存储、复制以及流程六大模块。为了方便
 * 管理异常码,所有的异常码命名和分段也按照模块来划分。
 * </p>
 * <p/>
 * <p>以下是具体的码段及前缀分配规则：</p>
 * <table>
 * <tr>
 * <th>模块名</th>
 * <th>预留码段</th>
 * <th>命名前缀</th>
 * <th>举例</th>
 * </tr>
 * <tr>
 * <td>成功</td>
 * <td>0</td>
 * <td>SUCCESS</td>
 * <td>SUCCESS(0, '成功')</td>
 * </tr>
 * <tr>
 * <td>公用</td>
 * <td>1 ~ 50</td>
 * <td>CN_</td>
 * <td>CN_COMMON_ERROR(23,"公用出错")</td>
 * </tr>
 * <tr>
 * <td>服务端</td>
 * <td>51 ~ 70</td>
 * <td>SR_</td>
 * <td>SR_SERVER_ERROR(51, "服务端出错")</td>
 * </tr>
 * <tr>
 * <td>客户端</td>
 * <td>71 ~ 90</td>
 * <td>CT_</td>
 * <td>CL_CLIENT_ERROR(71, "客户端出错")</td>
 * </tr>
 * <tr>
 * <td>存储</td>
 * <td>91 ~ 110</td>
 * <td>SE_</td>
 * <td>SE_STORAGE_ERROR(91, "存储出错")</td>
 * </tr>
 * <tr>
 * <td>复制</td>
 * <td>111 ~ 130</td>
 * <td>CY_</td>
 * <td>CY_COPY_ERROR(111, "复制出错")</td>
 * </tr>
 * <p/>
 * <tr>
 * <td>流程</td>
 * <td>131 ~ 150</td>
 * <td>FW_</td>
 * <td>FW_FLOW_ERROR(131, "流程出错")</td>
 * </tr>
 * </table>
 * <p/>
 * <strong>注：实现者应严格按照该规则定义自己的异常码</strong>
 *
 * @author Jame.HU
 * @version V1.0
 * @date 14-4-19 上午10:43
 */
public enum JMQCode {
    // 0：表示成功
    SUCCESS(0, "成功"),

    // 1 ~ 50 公共异常码段, 以CN_开头
    CN_NO_PERMISSION(1, "无权限"), //TODO 无权限太笼统，须详细划分一下
    CN_AUTHENTICATION_ERROR(2, "认证失败"),
    CN_SERVICE_NOT_AVAILABLE(3, "服务不可用"),
    CN_UNKNOWN_ERROR(4, "未知异常"),
    CN_DB_ERROR(5, "数据库异常"),
    CN_PARAM_ERROR(6, "参数错误"),
    CN_NEGATIVE_VOTE(7, "反对票"),
    CN_CHECKSUM_ERROR(8, "校验和出错"),

    CN_CONNECTION_ERROR(20, "连接出错,%s"),
    CN_CONNECTION_TIMEOUT(21, "连接超时,%s"),
    CN_REQUEST_TIMEOUT(22, "请求超时,%s"),
    CN_REQUEST_ERROR(23, "请求发送异常"),
    CN_REQUEST_EXCESSIVE(24, "异步请求过多"),
    CN_THREAD_INTERRUPTED(25, "线程被中断"),
    CN_THREAD_EXECUTOR_BUSY(26, "线程执行器繁忙"),
    CN_COMMAND_UNSUPPORTED(27, "请求命令不被支持,%s"),
    CN_DECODE_ERROR(28, "解码出错"),
    CN_PLUGIN_NOT_IMPLEMENT(29, "插件没有实现"),

    CN_TRANSACTION_PREPARE_ERROR(30, "事务准备失败"),
    CN_TRANSACTION_EXECUTE_ERROR(31, "本地事务执行失败"),
    CN_TRANSACTION_COMMIT_ERROR(32, "事务提交失败"),
    CN_TRANSACTION_ROLLBACK_ERROR(33, "事务回滚失败"),
    CN_TRANSACTION_NOT_EXISTS(34, "事务不存在"),
    CN_TRANSACTION_UNSUPPORTED(35, "分布式事务不支持"),

    // 71 ~ 90 客户端异常码段, 以CT_开头
    CT_NO_CLUSTER(71, "主题 %s 没有可用集群信息"),
    CT_SEQUENTIAL_BROKER_AMBIGUOUS(72, "多余顺序消息BROKER信息"),
    CT_NO_CONSUMER_RECORD(73, "本地偏移量管理消费没有消息信息记录"),
    CT_LIMIT_REQUEST(74,"请求限流"),
    CT_LOW_VERSION(75,"客户端版本低"),
    CT_MESSAGE_BODY_NULL(76, "消息体为空"),

    // 91 ~ 110 存储异常码段, 以SE_开头
    SE_IO_ERROR(91, "IO异常"),
    SE_OFFSET_OVERFLOW(92, "偏移量越界"),
    SE_MESSAGE_SIZE_EXCEEDED(93, "消息体大小超过最大限制"),
    SE_DISK_FULL(94, "磁盘满了"),
    SE_CREATE_FILE_ERROR(95, "创建文件失败"),
    SE_FLUSH_TIMEOUT(96, "刷盘超时"),
    SE_INVALID_JOURNAL(97, "无效日志数据，文件:%d 位置:%d"),
    SE_INVALID_OFFSET(98, "无效位置，位置:%d"),
    SE_REPLICATION_ERROR(99, "复制不成功"),
    SE_ENQUEUE_SLOW(100, "创建队列，入队慢"),
    SE_DISK_FLUSH_SLOW(101, "磁盘刷新慢"),
    SE_APPEND_MESSAGE_SLOW(102, "追加消息处理慢"),
    SE_QUEUE_NOT_EXISTS(103, "消费队列不存在，主题:%s"),
    SE_FATAL_ERROR(104, "致命异常"),

    // 111 ~ 130 复制异常码段, 以CY_开头
    CY_REPLICATE_ENQUEUE_TIMEOUT(111, "复制入队超时"),
    CY_REPLICATE_TIMEOUT(112, "复制超时"),
    CY_REPLICATE_ERROR(113, "复制异常"),
    CY_GET_OFFSET_ERROR(114, "从主取复制位置错误"),
    CY_FLUSH_OFFSET_ERROR(115, "刷新偏移量异常"),
    CY_STATUS_ERROR(116, "同步状态不对"),
    CY_NOT_DEGRADE(117, "复制不能降级"),

    // 131 ~ 150 服务端流程异常, 以FW_开头
    FW_CONNECTION_EXISTS(131, "连接已经存在"),
    FW_CONNECTION_NOT_EXISTS(132, "连接不存在"),
    FW_PRODUCER_NOT_EXISTS(134, "生产者不存在"),
    FW_CONSUMER_NOT_EXISTS(136, "消费者不存在"),
    FW_TRANSACTION_EXISTS(137, "事务已经存在"),
    FW_TRANSACTION_NOT_EXISTS(138, "事务不存在"),
    FW_COMMIT_ERROR(139, "提交事务失败"),
    FW_CONSUMER_ACK_FAIL(140, "消费者ack失败"),
    FW_PUT_MESSAGE_ERROR(141, "添加消息失败"),
    FW_GET_MESSAGE_ERROR(142, "获取消息失败"),
    FW_FLUSH_SEQUENTIAL_STATE_ERROR(143, "刷新顺序消息服务异常"),
    FW_PUT_MESSAGE_TOPIC_NOT_WRITE(144, "该分组被主题设置为禁止发送"),
    FW_GET_MESSAGE_TOPIC_NOT_READ(145, "该分组被主题设置为禁止消费"),
    FW_PUT_MESSAGE_APP_CLIENT_IP_NOT_WRITE(146, "该连接被应用者禁止发送"),
    FW_GET_MESSAGE_APP_CLIENT_IP_NOT_READ(147, "该连接被应用者禁止消费"),
    FW_TRANSACTION_LIMIT(148, "该主题未提交的事务数量达到限制数"),

    // 151~160 agent错误，以JA开头
    JA_COMMAND_ERROR(151, "命令执行失败"),
    JA_COMMAND_EXISTS(152, "该命令正在执行"),

    // 161~170 agent错误，以TN开头
    TN_COMMAND_NOT_EXISTS(161, "命令不存在"),
    TN_COMMAND_ERROR(162, "命令%s执行失败"),
    TN_COMMAND_FORMAT_ERROR(163, "命令%s格式错误"),
    TN_COMMAND_PWD_ERROR(164, "账号密码错误");


    private static Map<Integer, JMQCode> codes = new HashMap<Integer, JMQCode>();
    private int code;
    private String message;

    JMQCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * 获取错误代码
     *
     * @param code
     * @return
     */
    public static JMQCode valueOf(int code) {
        if (codes.isEmpty()) {
            synchronized (codes) {
                if (codes.isEmpty()) {
                    for (JMQCode jmqCode : JMQCode.values()) {
                        codes.put(jmqCode.code, jmqCode);
                    }
                }
            }
        }
        return codes.get(code);
    }

    public int getCode() {
        return code;
    }

    public String getMessage(Object... args) {
        if (args.length < 1) {
            return message;
        }
        return String.format(message, args);
    }
}
