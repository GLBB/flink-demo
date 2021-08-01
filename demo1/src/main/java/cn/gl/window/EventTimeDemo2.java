package cn.gl.window;

import cn.gl.bean.SensorRecord;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class EventTimeDemo2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 8888);

        // 转换成SensorReading类型，分配时间戳和watermark
        DataStream<SensorRecord> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorRecord(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorRecord>(Time.seconds(2)) {
                @Override
                public long extractTimestamp(SensorRecord element) {
                    return element.getEventTime() * 1000L;
                }
            });

        OutputTag<SensorRecord> outputTag = new OutputTag<SensorRecord>("late") {
        };

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorRecord> minTempStream = dataStream.keyBy("id")
            .timeWindow(Time.seconds(15))
            .allowedLateness(Time.minutes(1))
            .sideOutputLateData(outputTag)
            .minBy("temperature");

        minTempStream.print("minTemp");
//        minTempStream.addSink(new SinkFunction<SensorRecord>() {
//            @Override
//            public void invoke(SensorRecord value) throws Exception {
//                System.out.println("output: " + value);
//            }
//        });
//        minTempStream.writeAsText("/home/gl/code/learn/flink-learn/flink-demo/data/demo1/output/sensorEvent");
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
