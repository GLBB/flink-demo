package cn.gl.state;

import cn.gl.bean.SensorTemperature;
import lombok.Data;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class AlertDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorTemperature> sensorStream = env.addSource(new SourceFunction<SensorTemperature>() {
            @Override
            public void run(SourceContext<SensorTemperature> ctx) throws Exception {
                while (true) {
                    SensorTemperature sensorRecord = new SensorTemperature();
                    Random random = new Random();
                    sensorRecord.setId(String.valueOf(random.nextInt(100)));
                    sensorRecord.setEventTime(System.currentTimeMillis());
                    sensorRecord.setTemperature((double) random.nextInt(60));
                    ctx.collect(sensorRecord);
                    TimeUnit.MILLISECONDS.sleep(200);
                    System.out.println("source: " + sensorRecord);
                }
                
            }

            @Override
            public void cancel() {
                System.out.println("SourceFunction#cancel");
            }
        });
        KeyedStream<SensorTemperature, String> sensorKeyStream = sensorStream.keyBy(new KeySelector<SensorTemperature, String>() {
            @Override
            public String getKey(SensorTemperature sensorRecord) throws Exception {
                return sensorRecord.getId();
            }
        });

        DataStream<SensorAlert> alertStream = sensorKeyStream.flatMap(new TemperatureAlertFunc(10D));
        alertStream.print("alert");

        env.execute();
    }
    
    public static class TemperatureAlertFunc extends RichFlatMapFunction<SensorTemperature, SensorAlert> {
        
        private ValueState<Double> lastTempState;
        private Double threshold;
        
        public TemperatureAlertFunc(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> lastTempStateDesc = new ValueStateDescriptor<Double>("lastTemperature", Double.class);
            lastTempState = getRuntimeContext().getState(lastTempStateDesc);
        }

        @Override
        public void flatMap(SensorTemperature sensor, Collector<SensorAlert> out) throws Exception {
            Double lastTemp = lastTempState.value();
            if (lastTemp == null) {
                lastTempState.update(sensor.getTemperature());
                return;
            }
            Double abs = Math.abs(sensor.getTemperature() - lastTemp);
            if (abs > threshold) {
                SensorAlert sensorAlert = new SensorAlert();
                sensorAlert.setId(sensor.getId());
                sensorAlert.setCurrentTemperature(sensor.getTemperature());
                sensorAlert.setLastTemperature(lastTemp);
                sensorAlert.setDiff(abs);
                out.collect(sensorAlert);
            }
            lastTempState.update(sensor.getTemperature());
        }
    }
    
    @Data
    public static class SensorAlert {
        private String id;
        private Double currentTemperature;
        private Double lastTemperature;
        private Double diff;
    }
    
}
