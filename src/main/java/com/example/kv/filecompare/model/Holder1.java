package com.example.kv.filecompare.model;

import java.io.Serializable;

public class Holder1 implements Serializable {
    private static final long serialVersionUID = 1L;

    private String fileName;
    private Boolean isDirectory;

    public Holder1() {

    }
    public Holder1(String fileName, Boolean isDirectory) {
        this.fileName = fileName;
        this.isDirectory = isDirectory;
    }
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Boolean getDirectory() {
        return isDirectory;
    }

    public void setDirectory(Boolean directory) {
        isDirectory = directory;
    }
}
