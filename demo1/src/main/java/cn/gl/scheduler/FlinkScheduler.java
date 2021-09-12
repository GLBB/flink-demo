package cn.gl.scheduler;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkScheduler {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("/home/gl/code/learn/flink-learn/flink-demo/data/demo1/sensorData.txt");
        source.print("sensor: ");
        
        env.execute();
    }
}
