package com.example.kv.filecompare.mapper;

import com.example.kv.filecompare.model.Holder1;
import com.example.kv.filecompare.util.FileUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.List;

public class DirFlagMapFunction implements FlatMapFunction<String, Holder1> {
    private static final Logger LOG = LoggerFactory.getLogger(DirFlagMapFunction.class);

    private FileUtils fileUtils;

    public DirFlagMapFunction(FileUtils fileUtils) {
        this.fileUtils = fileUtils;
    }

    public void flatMap(String dirName, Collector<Holder1> out) {
        List<String> directoryContents = fileUtils.listDirectoryContents(Paths.get(dirName));
        for (String path: directoryContents) {
            boolean dirFlag = false;
            if (fileUtils.isDirectory(Paths.get(path))) {
                dirFlag = true;
            }
            out.collect(new Holder1(path.toString(), dirFlag));
        }
    }
}
