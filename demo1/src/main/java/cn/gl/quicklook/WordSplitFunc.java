package cn.gl.quicklook;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordSplitFunc implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = line.split("\\W+");
        Arrays.stream(words)
            .map(word -> Tuple2.of(word, 1))
            .forEach(collector::collect);
    }
}
