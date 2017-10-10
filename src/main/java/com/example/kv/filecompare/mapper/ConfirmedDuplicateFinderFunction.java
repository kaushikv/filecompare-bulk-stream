package com.example.kv.filecompare.mapper;

import com.example.kv.filecompare.model.FileSizeHolder;
import com.example.kv.filecompare.util.FileUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConfirmedDuplicateFinderFunction implements FlatMapFunction<FileSizeHolder, Tuple2<String,String>> {
    private static final Logger LOG = LoggerFactory.getLogger(ConfirmedDuplicateFinderFunction.class);

    private Map<String,Tuple2<String,Boolean>> sizeHashCompositeKeyMap = new ConcurrentHashMap<>();

    private String loneFileInGroup = null;
    private boolean sizeHashPartitionHasDuplicates = false;

    private FileUtils fileUtils;

    public ConfirmedDuplicateFinderFunction(FileUtils fileUtils) {
        this.fileUtils = fileUtils;
    }

    public void flatMap(FileSizeHolder fileSizeHolder, Collector<Tuple2<String,String>> out) {
        String sizeHashCompositeKey = fileUtils.getSizeHash(fileSizeHolder.getFileName(), fileSizeHolder.getSize());
        Tuple2<String,Boolean> sizeStatusTuple = sizeHashCompositeKeyMap.get(sizeHashCompositeKey);
        if (sizeStatusTuple == null) {
            String loneFileInGroup = fileSizeHolder.getFileName();
            sizeHashCompositeKeyMap.put(sizeHashCompositeKey, Tuple2.of(loneFileInGroup, false));
        } else {
            if (!sizeStatusTuple.f1) {
                out.collect(Tuple2.of(sizeHashCompositeKey, fileSizeHolder.getFileName()));
                out.collect(Tuple2.of(sizeHashCompositeKey, fileSizeHolder.getFileName()));
                sizeHashCompositeKeyMap.put(sizeHashCompositeKey, Tuple2.of(null, true));
            } else {
                out.collect(Tuple2.of(sizeHashCompositeKey, fileSizeHolder.getFileName()));
            }
        }
    }
}
