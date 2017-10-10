package com.example.kv.filecompare.model;

import java.io.Serializable;

public class FileSizeHolder implements Serializable {
    private static final long serialVersionUID = 1L;

    private String fileName;
    private Long size;

    public FileSizeHolder() {

    }
    public FileSizeHolder(String fileName, Long size) {
        this.fileName = fileName;
        this.size = size;
    }
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }
}
