package com.ipd.jmq.common.network.kafka.message;

import com.ipd.jmq.common.network.kafka.exception.UnknownCodecException;
import com.ipd.jmq.common.network.kafka.model.CompressionCodec;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by zhangkepeng on 16-8-30.
 */
public final class CompressionCodecFactory {

    public static OutputStream apply(CompressionCodec codec, OutputStream stream) throws IOException{
        switch (codec) {
            case GZIPCompressionCodec:
                return new GZIPOutputStream(stream);
            case SnappyCompressionCodec:
                return new SnappyOutputStream(stream);
            case LZ4CompressionCodec:
                return new KafkaLZ4BlockOutputStream(stream);
            default:
                throw new UnknownCodecException("Unknown Codec: " + codec);
        }
    }

    public static InputStream apply(CompressionCodec codec, InputStream stream) throws IOException{
        switch (codec) {
            case GZIPCompressionCodec:
                return new GZIPInputStream(stream);
            case SnappyCompressionCodec:
                return new SnappyInputStream(stream);
            case LZ4CompressionCodec:
                return new KafkaLZ4BlockInputStream(stream);
            default:
                throw new UnknownCodecException("Unkonwn Codec: " + codec);
        }
    }
}
