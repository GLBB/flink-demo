package cn.gl.window;

import cn.gl.bean.SensorTemperature;
import cn.gl.common.SensorRecordMapper;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TumblingWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineSource = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<SensorTemperature> sensorStream = lineSource.map(SensorRecordMapper::lineToSensorRecord);
        KeyedStream<SensorTemperature, String> keyedStream = sensorStream.keyBy(new KeySelector<SensorTemperature, String>() {
            @Override
            public String getKey(SensorTemperature sensorRecord) throws Exception {
                return sensorRecord.getId();
            }
        });

//        SingleOutputStreamOperator<SensorRecord> sumStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//            .sum("temperature");
//        sumStream.print("sumStream: ");
        
        SingleOutputStreamOperator<Tuple3<LocalDateTime, String, Double>> avgStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .apply(new WindowFunction<SensorTemperature, Tuple3<LocalDateTime, String, Double>, String, TimeWindow>() {
                @Override
                public void apply(String key, TimeWindow window, Iterable<SensorTemperature> input, Collector<Tuple3<LocalDateTime, String, Double>> out) throws Exception {
                    long start = window.getStart();
                    long end = window.getEnd();
                    Double sum = 0.0;
                    int count = 0;
                    for (SensorTemperature sensorRecord : input) {
                        sum += sensorRecord.getTemperature();
                        count += 1;
                    }
                    Double avg = sum / count;
                    LocalDateTime startDateTime = Instant.ofEpochMilli(start).atOffset(ZoneOffset.of("+8")).toLocalDateTime();
                    out.collect(Tuple3.of(startDateTime, key, avg));
                }
            });
        avgStream.print("avg");

        env.execute();
    }
}
