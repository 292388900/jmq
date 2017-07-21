package com.ipd.jmq.common.network.v3.netty.telnet;

import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.protocol.telnet.*;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.v3.netty.Keys;
import com.ipd.jmq.toolkit.lang.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.Attribute;

import java.util.List;

/**
 * Telnet协议，解析字符串<br>
 * Windows:每按一个字符，就会把正行数据输入，退格键不自动删除<br>
 * Linux:只有按了回车符号，才输入正行数据，自动处理退格键<br>
 * TELNET的操作协商使用NVT命令，即最高位为1的字节流，每条NVT命令以字节IAC（0xFF）开始。原理如下：
 * 只要客户机或服务器要发送命令序列而不是数据流，它就在数据流中插入一个特殊的保留字符，
 * 该保留字符叫做“解释为命令”（IAC  ，Interpret As Command) 字符。
 * 当接收方在一个入数据流中发现IAC字符时，它就把后继的字节处理为一个命令序列。
 * 下面列出了所有的Telnet NVT命令，其中很少用到
 * <table>
 * <th>名称</th>
 * <th>编码</th>
 * <th>说明</th>
 * <tr><td>EOF</td><td>236</td><td>文件结束符</td></tr>
 * <tr><td>SUSP</td><td>237</td><td>挂起当前进程</td></tr>
 * <tr><td>ABORT</td><td>238</td><td>中止进程</td></tr>
 * <tr><td>EOR</td><td>239</td><td>记录结束符</td></tr>
 * <tr><td>SE</td><td>240</td><td>子选项结束</td></tr>
 * <tr><td>NOP</td><td>241</td><td>空操作</td></tr>
 * <tr><td>DM</td><td>242</td><td>数据标记</td></tr>
 * <tr><td>BRK</td><td>243</td><td>终止符（break）</td></tr>
 * <tr><td>IP</td><td>244</td><td>终止进程</td></tr>
 * <tr><td>AO</td><td>245</td><td>终止输出</td></tr>
 * <tr><td>AYT</td><td>246</td><td>请求应答</td></tr>
 * <tr><td>EC</td><td>247</td><td>终止符</td></tr>
 * <tr><td>EL</td><td>248</td><td>擦除一行</td></tr>
 * <tr><td>GA</td><td>249</td><td>继续</td></tr>
 * <tr><td>SB</td><td>250</td><td>子选项开始</td></tr>
 * <tr><td>WILL</td><td>251</td><td>选项协商</td></tr>
 * <tr><td>WONT</td><td>252</td><td>选项协商</td></tr>
 * <tr><td>DO</td><td>253</td><td>选项协商</td></tr>
 * <tr><td>DONT</td><td>254</td><td>选项协商</td></tr>
 * <tr><td>IAC</td><td>255</td><td>字符0XFF</td></tr>
 * </table>
 */
public class TelnetServerDecoder extends LineBasedFrameDecoder {

    public static final int MAX_LENGTH = 1024 * 1024;
    // 执行命令历史
    protected BackspaceHandler backspaceHandler = new BackspaceHandler();
    protected UpHandler upHandler = new UpHandler();
    protected DownHandler downHandler = new DownHandler();
    protected ControlHandler controllHandler = new ControlHandler();
    protected EnterHandler enterHandler;
    // 提醒符号
    protected String prompt = ">";
    // 最大历史条数
    protected int maxHistorySize = 100;
    // 命令执行器
    protected CommandHandlerFactory factory;

    public TelnetServerDecoder(CommandHandlerFactory factory) {
        this(factory, ">", 100, null, MAX_LENGTH, true, true);
    }

    public TelnetServerDecoder(CommandHandlerFactory factory, String prompt, int maxHistorySize, EnterHandler handler) {
        this(factory, prompt, maxHistorySize, handler, MAX_LENGTH, false, true);
    }

    public TelnetServerDecoder(CommandHandlerFactory factory, String prompt, int maxHistorySize, EnterHandler handler,
                               int maxLength, boolean stripDelimiter, boolean failFast) {
        super(maxLength, stripDelimiter, failFast);
        if (factory == null) {
            throw new IllegalArgumentException("factory can not be null.");
        } else if (prompt == null || prompt.isEmpty()) {
            throw new IllegalArgumentException("prompt can not be empty.");
        }
        this.factory = factory;
        this.maxHistorySize = maxHistorySize;
        this.prompt = prompt;
        if (handler == null){
            this.enterHandler = new EnterHandler(factory);
        }else {
            this.enterHandler = handler;
        }
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        ByteBuf lineBuf = (ByteBuf) super.decode(ctx, buffer);

        if (lineBuf == null || lineBuf.readableBytes() == 0) {
            return null;
        } else {
            // 上下文
            Channel channel = ctx.channel();
            Attribute<TelnetInput> inputAttr = channel.attr(Keys.INPUT);
            Attribute<Transport> transportAttr = channel.attr(Keys.TRANSPORT);
            Transport transport = transportAttr.get();
            TelnetInput input = inputAttr.get();
            if (input == null) {
                input = new TelnetInput(prompt, maxHistorySize);
                if (!inputAttr.compareAndSet(null, input)) {
                    input = inputAttr.get();
                }
            }

            // 读取缓冲区数据
            byte message[] = new byte[lineBuf.readableBytes()];
            lineBuf.readBytes(message);
            // 请求
            Command request;
            if (TelnetInput.isBackspace(message)) {
                // 处理Windows退格键
                request = new Command(TelnetHeader.Builder.request(), new TelnetRequest(message));
                transport.acknowledge(request, backspaceHandler.process(transport, request), null);
            } else if (TelnetInput.isUp(message)) {
                //上键翻阅历史命令
                request = new Command(TelnetHeader.Builder.request(), new TelnetRequest(message));
                transport.acknowledge(request, upHandler.process(transport, request), null);
            } else if (TelnetInput.isDown(message)) {
                //下键翻阅历史命令
                request = new Command(TelnetHeader.Builder.request(), new TelnetRequest(message));
                transport.acknowledge(request, downHandler.process(transport, request), null);
            } else if (TelnetInput.isExit(message)) {
                //退出操作
                transport.stop();
            } else if (TelnetInput.isEnter(message)) {
                // 回车
                request = new Command(TelnetHeader.Builder.request(),
                        new TelnetRequest(message, new String(message, Charsets.UTF_8)));
                transport.acknowledge(request, enterHandler.process(transport, request), null);
            } else if (message[0] == TelnetEscape.NVT_IAC) {
                // 控制命令，暂时忽略
                request = new Command(TelnetHeader.Builder.request(), new TelnetRequest(message));
                transport.acknowledge(request, controllHandler.process(transport, request), null);
            } else {
                // 追加到当前缓冲器
                input.append(new String(message, Charsets.UTF_8));
            }
        }
        return null;
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(Keys.INPUT).remove();
        super.channelInactive(ctx);
    }
}
