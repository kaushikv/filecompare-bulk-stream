package com.example.kv.filecompare.mapper;

import com.example.kv.filecompare.util.FileUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class SymLinkResolverFunction implements FlatMapFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(SymLinkResolverFunction.class);

    private FileUtils fileUtils;

    public SymLinkResolverFunction(FileUtils fileUtils) {
        this.fileUtils = fileUtils;
    }

    public void flatMap(String symLink, Collector<String> out) {
        Optional<Path> resolvedPath = fileUtils.getValidPathForSymLink(Paths.get(symLink));
        if (resolvedPath.isPresent()) {
            // out.collect(new Holder1(resolvedPath.get().toString(), false));
            out.collect(resolvedPath.get().toString().toString());
        }
    }
}
