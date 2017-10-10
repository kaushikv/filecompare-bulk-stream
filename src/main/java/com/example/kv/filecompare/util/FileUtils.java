package com.example.kv.filecompare.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class FileUtils implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

    public static int MAX_FILE_SIZE = 1000000; // 1 MB
    
    public String getMD5ForFile(String fileName) {
        try (InputStream is = Files.newInputStream(Paths.get(fileName));
        )
        {
            String md5 = DigestUtils.md5Hex(is);
            return md5;
        } catch (IOException e) {
            LOGGER.error("IOException reading file stream " + fileName, e);
        }
        return null;
    }

    private String getMD5ForFileByParts(String fileName, long size) {
        StringBuilder sb = new StringBuilder();
        try (InputStream is = Files.newInputStream(Paths.get(fileName));
             BufferedInputStream bis=new BufferedInputStream(is);
        ) {
            for (int offset =0 ; offset <= size; offset+= MAX_FILE_SIZE) {
                int bytesToRead = (int) ( (size-offset) >= MAX_FILE_SIZE? MAX_FILE_SIZE : (size-offset) );
                byte[] bytes = new byte[bytesToRead];
                try {
                    int bytesRead = bis.read(bytes);
                } catch (IOException e) {
                    LOGGER.error("IOException reading file bytes. Dropped.", e);
                }
                String md5 = DigestUtils.md5Hex(bytes);
                sb.append(md5);
            }
            String md5 = DigestUtils.md5Hex(sb.toString().getBytes());
            return md5;
        } catch (IOException e) {
            LOGGER.error("IOException reading file stream " + fileName, e);
        }
        return null;
    }

    public List<String> listDirectoryContents(Path dirPath) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath)) {
            List<String> pathList = new ArrayList<>();
            Iterator it = stream.iterator();
            while (it.hasNext()) {
                   pathList.add(it.next().toString());
            }
            // List<String> pathList = Lists.newArrayList(stream.iterator());
            return pathList;
        } catch (IOException e) {
            LOGGER.error("IOException listing files in " + dirPath, e);
        }
        return null;
    }

    public boolean isDirectory(Path path) {
        return Files.isDirectory(path);
    }

    public boolean isSymLink(Path path) {
        return Files.isSymbolicLink(path);
    }

    public Optional<Path> getValidPathForSymLink(Path path) {
        if (Files.isSymbolicLink(path)) {
            try {
                Path p = Files.readSymbolicLink(path);
                if (Files.isSymbolicLink(p)) {
                    Optional<Path> optionalPath = getValidPathForSymLink(p);
                    if (optionalPath.isPresent()) {
                        p = optionalPath.get();
                    }
                }
                if (Files.exists(p)) {
                    return Optional.of(p);
                }
            } catch (IOException e) {
                LOGGER.error("IOException reading symlink " + path.toString(), e);
            }
        } else {
            if (Files.exists(path)) {
                return Optional.of(path);
            }
        }
        return Optional.empty();
    }

    public Long getSize(String fileName) {
        try {
            return Files.size(Paths.get(fileName));
        } catch (IOException e) {
            LOGGER.error("Error getting size for file {}", fileName, e);
            return null;
        }
    }

    public String getSizeHash(String fileName, Long size) {
        String md5;
        // Bypass MD5 checks if file size = 0
        if (size == 0) {
            md5 = "0";
        } else {
            md5 = this.getMD5ForFile(fileName);
        }

        return size + "-" + md5;
    }

    public String getSizeHashInParts(String fileName, Long size) {
        String md5;
        // Bypass MD5 checks if file size = 0
        if (size == 0) {
            md5 = "0";
        } else if (size <= MAX_FILE_SIZE) {
            md5 = this.getMD5ForFile(fileName);
        } else { // chunk the large file to get composite MD5
            md5 = this.getMD5ForFileByParts(fileName, size);
        }

        return size + "-" + md5;
    }

}
