package com.ipd.jmq.common.model;

import java.io.File;
import java.util.Date;

/**
 * 文件基本信息
 *
 * @author hexiaofeng
 */
public class FileInfo extends BaseModel {
    /**
     * 名称
     */
    protected String name;
    /**
     * 原始文件名称
     */
    protected String path;
    /**
     * 大小（字节）
     */
    protected long size;
    /**
     * 扩展名
     */
    protected String extension;

    // 类型
    protected String type;

    public FileInfo() {
    }

    public FileInfo(String name) {
        this.name = name;
    }

    /**
     * 转化成文件信息
     *
     * @param file 文件
     * @param name 文件名称
     * @param id   序号
     * @return 文件信息
     */
    public static FileInfo getFileInfo(File file, String name, long id) {
        if (file == null) {
            throw new IllegalArgumentException("file is null.");
        }
        if (name == null || name.isEmpty()) {
            if (id > 0) {
                name = String.valueOf(id);
            } else {
                name = file.getName();
                int pos = name.indexOf('.');
                if (pos > 0) {
                    name = name.substring(0, pos);
                }
            }
        }
        if (id <= 0 && name != null && !name.isEmpty()) {
            try {
                id = Long.parseLong(name);
            } catch (NumberFormatException e) {
            }
        }
        String ext = file.getName();
        FileInfo info = new FileInfo();
        info.setStatus(FileInfo.ENABLED);
        info.setId(id);
        info.setPath(file.getPath());
        info.setName(name);
        if (ext != null) {
            int pos = ext.lastIndexOf('.');
            if (pos > 0 && pos < ext.length() - 1) {
                info.setExtension(ext.substring(pos + 1));
            }
        }
        if (file.exists()) {
            info.setSize(file.length());
            info.setCreateTime(new Date(file.lastModified()));
        }
        return info;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getExtension() {
        return extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}