package com.ipd.jmq.server.store.journal;

import java.io.IOException;

/**
 * 日志文件异常
 * Created by hexiaofeng on 16-7-14.
 */
public class JournalException extends IOException {

    public static final String ILLEGAL_STATE = "IllegalState";
    public static final String CREATE_FILE = "CreateFile";
    public static final String POSITION = "Position";
    public static final String MAPPED_FILE = "MappedFile";
    public static final String INVALID_POSITION = "InvalidPosition";
    public static final String READ_FILE = "ReadFile";
    public static final String FATAL = "Fatal";
    public static final String CAPACITY = "Capacity";
    public static final String FILE_PERMISSION = "FilePermission";
    public static final String TIMEOUT = "timeout";

    public JournalException() {
    }

    public JournalException(String message) {
        super(message);
    }

    public JournalException(String message, Throwable cause) {
        super(message, cause);
    }

    public JournalException(Throwable cause) {
        super(cause);
    }

    /**
     * 属于致命异常
     */
    public static class FatalException extends JournalException {
        public FatalException() {
            super(FATAL);
        }

        public FatalException(String message) {
            super(message);
        }

        public FatalException(String message, Throwable e) {
            super(message, e);
        }

        public static FatalException build() {
            return new FatalException();
        }

        public static FatalException build(String message) {
            return new FatalException(message);
        }

        public static FatalException build(String message, Throwable e) {
            return new FatalException(message, e);
        }

    }

    /**
     * 服务没有启动
     */
    public static class IllegalStateException extends JournalException {
        public IllegalStateException() {
            super(ILLEGAL_STATE);
        }

        public IllegalStateException(String message) {
            super(message);
        }

        public static IllegalStateException build() {
            return new IllegalStateException();
        }

        public static IllegalStateException build(String message) {
            return new IllegalStateException(message);
        }

    }

    /**
     * 超时异常
     */
    public static class TimeoutStateException extends JournalException {
        public TimeoutStateException() {
            super(TIMEOUT);
        }

        public TimeoutStateException(String message) {
            super(message);
        }

        public static TimeoutStateException build() {
            return new TimeoutStateException();
        }

        public static TimeoutStateException build(String message) {
            return new TimeoutStateException(message);
        }

    }

    /**
     * 创建文件异常，属于致命异常，这个时候如果有数据进入只读状态直到把数据消费完成
     */
    public static class CreateFileException extends FatalException {
        public CreateFileException() {
            super(CREATE_FILE);
        }

        public CreateFileException(String message) {
            super(message);
        }

        public static CreateFileException build() {
            return new CreateFileException();
        }

        public static CreateFileException build(String message) {
            return new CreateFileException(message);
        }
    }

    /**
     * 文件权限异常，属于致命异常
     */
    public static class FilePermissionException extends FatalException {
        public FilePermissionException() {
            super(FILE_PERMISSION);
        }

        public FilePermissionException(String message) {
            super(message);
        }

        public static FilePermissionException build() {
            return new FilePermissionException();
        }

        public static FilePermissionException build(String message) {
            return new FilePermissionException(message);
        }
    }

    /**
     * 创建内存映射文件失败
     */
    public static class MappedFileException extends FatalException {
        public MappedFileException() {
            super(MAPPED_FILE);
        }

        public MappedFileException(String message) {
            super(message);
        }

        public MappedFileException(String message, Throwable e) {
            super(message, e);
        }

        public static MappedFileException build() {
            return new MappedFileException();
        }

        public static MappedFileException build(String message) {
            return new MappedFileException(message);
        }

        public static MappedFileException build(String message, Throwable e) {
            return new MappedFileException(message, e);
        }

    }

    /**
     * 读取文件异常，属于致命异常，禁止读写
     */
    public static class ReadFileException extends FatalException {
        public ReadFileException() {
            super(READ_FILE);
        }

        public ReadFileException(String message) {
            super(message);
        }

        public ReadFileException(String message, Throwable e) {
            super(message, e);
        }

        public static ReadFileException build() {
            return new ReadFileException();
        }

        public static ReadFileException build(String message) {
            return new ReadFileException(message);
        }

        public static ReadFileException build(String message, Throwable e) {
            return new ReadFileException(message, e);
        }

    }

    /**
     * 写文件异常，属于致命异常，禁止读写
     */
    public static class WriteFileException extends FatalException {
        public WriteFileException() {
            super(READ_FILE);
        }

        public WriteFileException(String message) {
            super(message);
        }

        public WriteFileException(String message, Throwable e) {
            super(message, e);
        }

        public static WriteFileException build() {
            return new WriteFileException();
        }

        public static WriteFileException build(String message) {
            return new WriteFileException(message);
        }

        public static WriteFileException build(String message, Throwable e) {
            return new WriteFileException(message, e);
        }

    }

    /**
     * 设置位置异常，致命错误，禁止读写
     */
    public static class PositionException extends FatalException {
        public PositionException() {
            super(POSITION);
        }

        public PositionException(String message) {
            super(message);
        }

        public PositionException(String message, Throwable e) {
            super(message, e);
        }

        public static PositionException build() {
            return new PositionException();
        }

        public static PositionException build(String message) {
            return new PositionException(message);
        }

        public static PositionException build(String message, Throwable e) {
            return new PositionException(message, e);
        }

    }

    /**
     * 无效位置异常
     */
    public static class InvalidPositionException extends JournalException {
        public InvalidPositionException() {
            super(INVALID_POSITION);
        }

        public InvalidPositionException(String message) {
            super(message);
        }

        public static InvalidPositionException build() {
            return new InvalidPositionException();
        }

        public static InvalidPositionException build(String message) {
            return new InvalidPositionException(message);
        }
    }

    /**
     * 文件容量不够
     */
    public static class CapacityException extends JournalException {
        public CapacityException() {
            super(CAPACITY);
        }

        public CapacityException(String message) {
            super(message);
        }

        public static CapacityException build() {
            return new CapacityException();
        }

        public static CapacityException build(String message) {
            return new CapacityException(message);
        }

    }

}
