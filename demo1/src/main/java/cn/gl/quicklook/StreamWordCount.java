package cn.gl.quicklook;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        String dataPath = "/home/gl/code/learn/flink-learn/flink-demo/data/demo1/wordCountData1.txt";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.readTextFile(dataPath);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordsCountStream = streamSource.flatMap(new WordSplitFunc())
            .keyBy(0)
            .sum(1);
        wordsCountStream.print();
        env.execute();
        

    }
}
