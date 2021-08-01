package cn.gl.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {

    public static void main(String[] args) throws Exception {
        String path = "/home/gl/code/learn/flink-learn/flink-demo/data/demo1/sensorData.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> fileStream = env.readTextFile(path);
        fileStream.print("line");
        env.execute("FileSourceDemo");
    }
}
