package com.example.kv.filecompare.mapper;

import com.example.kv.filecompare.model.FileSizeHolder;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PotentialDuplicateFinderFunction implements FlatMapFunction<FileSizeHolder, FileSizeHolder> {
    private static final Logger LOG = LoggerFactory.getLogger(PotentialDuplicateFinderFunction.class);

    private Map<Long,Tuple2<String,Boolean>> sizeStatusMap = new ConcurrentHashMap<>();

    private String loneFileInGroup = null;
    private boolean sizePartitionHasDuplicates = false;

    public void flatMap(FileSizeHolder fileSizeHolder, Collector<FileSizeHolder> out) {
        Tuple2<String,Boolean> sizeStatusTuple = sizeStatusMap.get(fileSizeHolder.getSize());
        if (sizeStatusTuple == null) {
            String loneFileInGroup = fileSizeHolder.getFileName();
            sizeStatusMap.put(fileSizeHolder.getSize(), Tuple2.of(loneFileInGroup, false));
        } else {
            if (!sizeStatusTuple.f1) {
                out.collect(new FileSizeHolder(sizeStatusTuple.f0, fileSizeHolder.getSize()));
                out.collect(fileSizeHolder);
                sizeStatusMap.put(fileSizeHolder.getSize(), Tuple2.of(null, true));
            } else {
                out.collect(fileSizeHolder);
            }
        }
    }
}
