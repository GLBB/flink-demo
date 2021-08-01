package cn.gl.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringSteam = env.fromCollection(Arrays.asList("a", "b", "c", "d", "e"));
        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5).setParallelism(1);
        stringSteam.print("string");
        intStream.print("int");
        env.execute("SourceDemo");
    }
}
