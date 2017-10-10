package com.example.kv.filecompare;

import com.example.kv.filecompare.mapper.ConfirmedDuplicateFinderFunction;
import com.example.kv.filecompare.mapper.DirFlagMapFunction;
import com.example.kv.filecompare.mapper.PotentialDuplicateFinderFunction;
import com.example.kv.filecompare.mapper.SymLinkResolverFunction;
import com.example.kv.filecompare.model.FileSizeHolder;
import com.example.kv.filecompare.model.Holder1;
import com.example.kv.filecompare.util.FileUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class FileCompareBulkStream implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FileCompareBulkStream.class);

    private final FileUtils fileUtils = new FileUtils();

    private static final int PARALLEL_EXECUTORS = 4;

    public static void main(String[] args) throws Exception
    {
        String usage = "$0 <rootDir> [MAX_FILE_CHUNK]\n\t rootDir => The parent directory to scan the files and subdirectories.\n\tMAX_FILE_CHUNK => The largest byte chunk size in which a large file should be read to prevent OOM Error. [Default 1000000 bytes]";
        if (args.length <1 ) {
            LOG.error("Invalid args. Usage: {}", usage);
            System.exit(-1);
        }
        if (args.length > 1) {
            try {
                int maxFileChunk = Integer.parseInt(args[1]);
                FileUtils.MAX_FILE_SIZE = maxFileChunk;
            } catch (NumberFormatException e) {
                LOG.error("Invalid args. Usage: {}. Args {}", usage, args, e);
                System.exit(-1);
            }
        }
        new FileCompareBulkStream().run(args[0]);
    }

    private FileCompareBulkStream() {}

    private void run(String rootDirName) throws Exception {
        LOG.info("Preparing to run FileCompareBulkStream");

        // Set up the execution streaming environment
        LOG.info("Setting up the execution streaming environment");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLEL_EXECUTORS);

        List<String> rootDirList = Arrays.asList(rootDirName);

        DataStream<String> dirStream = env.fromCollection(rootDirList).rebalance();
        IterativeStream<String> dirIteration = dirStream.iterate();
        DataStream<Holder1> dirIterationBody = dirIteration.setParallelism(PARALLEL_EXECUTORS)
                .flatMap(new SymLinkResolverFunction(fileUtils))
                .flatMap(new DirFlagMapFunction(fileUtils));

        dirIteration.closeWith(dirIterationBody.filter(pathBooleanTuple2 -> pathBooleanTuple2.getDirectory() ).map(dirPathBooleanTuple2 -> dirPathBooleanTuple2.getFileName()));
        DataStream<FileSizeHolder> fileSizeStream = dirIterationBody.filter(pathBooleanTuple2 -> !pathBooleanTuple2.getDirectory()).map(dirPathBooleanTuple2 -> (new FileSizeHolder(dirPathBooleanTuple2.getFileName(), fileUtils.getSize(dirPathBooleanTuple2.getFileName())) ));
        fileSizeStream
            .filter(holder -> holder.getSize()!= null)
            .keyBy("size")
            .flatMap(new PotentialDuplicateFinderFunction())
            .flatMap(new ConfirmedDuplicateFinderFunction(fileUtils))
            .addSink(new SinkFunction<Tuple2<String, String>>() {

                private static final long serialVersionUID = 1L;

                public void invoke(Tuple2<String, String> sizeHashFileNameTuple) {
                    LOG.info("path={} size-Hash = {}", sizeHashFileNameTuple.f1, sizeHashFileNameTuple.f0);
                }
            });
        env.execute("FileCompareBulkStream");
    }

}
