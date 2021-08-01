package cn.gl.quicklook;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamNCWordCount {

    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");
        DataStreamSource<String> streamSource = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordsCountStream = streamSource.flatMap(new WordSplitFunc())
            .keyBy(0)
            .sum(1);
        wordsCountStream.print();
        env.execute();
        

    }
}
