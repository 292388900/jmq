package com.ipd.jmq.common.network.kafka.model;

import com.ipd.jmq.common.network.kafka.exception.UnknownCodecException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-30.
 */
public enum CompressionCodec {

    NoCompressionCodec(0, "none"),
    GZIPCompressionCodec(1, "gzip"),
    SnappyCompressionCodec(2, "snappy"),
    LZ4CompressionCodec(3, "lz4");

    private CompressionCodec(int code, String name) {
        this.code = code;
        this.name = name;
    }

    private static Map<Integer, CompressionCodec> codecs = new HashMap<Integer, CompressionCodec>();

    private int code;
    private String name;

    public int getCode() {
        return code;
    }

    public static CompressionCodec valueOf(int codec) {
        if (codecs.isEmpty()) {
            synchronized (codecs) {
                if (codecs.isEmpty()) {
                    for (CompressionCodec compressionCodec : CompressionCodec.values()) {
                        codecs.put(compressionCodec.code, compressionCodec);
                    }
                }
            }
        }
        CompressionCodec compressionCodec = codecs.get(codec);
        if (compressionCodec != null) {
            return compressionCodec;
        }
        throw new UnknownCodecException(String.format("%d is an unknown compression codec", codec));
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
