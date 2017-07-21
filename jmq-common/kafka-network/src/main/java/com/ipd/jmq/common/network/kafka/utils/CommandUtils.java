package com.ipd.jmq.common.network.kafka.utils;

import com.ipd.jmq.common.network.kafka.exception.KafkaException;
import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * Created by zhangkepeng on 16-7-27.
 */
public class CommandUtils {
    private static final String ProtocolEncoding = "UTF-8";

    /**
     * 读取字符串
     *
     * @param in 输入缓冲区
     * @return 字符串
     * @throws Exception
     */
    public static String readShortString(final ByteBuf in) throws TransportException.CodecException {
        int size = in.readShort();
        if (size < 0) {
            return null;
        }
        byte bytes[] = new byte[size];
        in.readBytes(bytes);
        String string = null;
        try {
            string = new String(bytes, ProtocolEncoding);
        } catch (IOException e) {
            throw new TransportException.CodecException("read short string failed");
        }
        return string;
    }

    public static int readIntInRange(ByteBuf buffer, String name, int min, int max) {
        int value = buffer.readInt();
        if (value < min || value > max) {
            throw new KafkaException(name + " has value " + value + " which is invalid");
        } else {
            return value;
        }
    }

    /**
     * 写字符串
     *
     * @param string 字符串
     * @param out   输出缓冲区
     * @throws Exception
     */
    public static void writeShortString(final ByteBuf out, final String string) throws TransportException.CodecException {
        if (out == null) {
            return;
        }
        if (string == null) {
            out.writeShort((short) -1);
        } else {
            try {
                byte[] encodedString = string.getBytes(ProtocolEncoding);
                if (encodedString.length > Short.MAX_VALUE) {
                    throw new KafkaException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
                }
                out.writeShort(encodedString.length);
                out.writeBytes(encodedString);
            } catch (IOException e) {
                throw new TransportException.CodecException("write short string failed");
            }
        }
    }

//    public static String readShortString(ByteBuffer buffer) throws Exception{
//        int size = buffer.getShort();
//        if (size < 0) {
//            return null;
//        }
//        byte bytes[] = new byte[size];
//        buffer.get(bytes);
//        return new String(bytes, ProtocolEncoding);
//    }
//
//    public static void writeShortString(ByteBuffer buffer, String string) throws Exception{
//        if (string == null) {
//            buffer.putShort((short) -1);
//        } else {
//            byte[] encodedString = string.getBytes(ProtocolEncoding);
//            if (encodedString.length > Short.MAX_VALUE) {
//                throw new Exception("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
//            } else {
//                buffer.putShort((short)encodedString.length);
//                buffer.put(encodedString);
//            }
//        }
//    }
//
//    public static int shortStringLength(String string) {
//        if (string == null) {
//            return 2;
//        } else {
//            try {
//                byte[] encodedString = string.getBytes(ProtocolEncoding);
//                if (encodedString.length > Short.MAX_VALUE) {
//                    throw new KafkaException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
//                } else {
//                    return 2 + encodedString.length;
//                }
//            } catch (UnsupportedEncodingException e) {
//                throw new KafkaException(e);
//            }
//
//        }
//    }
}
