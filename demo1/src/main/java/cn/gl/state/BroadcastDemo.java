package cn.gl.state;

import cn.gl.bean.City;
import cn.gl.bean.SensorInfo;
import cn.gl.bean.SensorPosition;
import cn.gl.bean.SensorTemperature;
import cn.gl.source.SensorSource;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class BroadcastDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
//        Integer idBound = 10;
        Integer idBound = Integer.MAX_VALUE;

        DataStream<SensorPosition> sensorPositionSource = env.addSource(new SourceFunction<SensorPosition>() {
            @Override
            public void run(SourceContext<SensorPosition> ctx) throws Exception {
                while (true) {
                    Random random = new Random();
                    SensorPosition sensorPosition = new SensorPosition();
                    sensorPosition.setId(String.valueOf(random.nextInt(idBound)));
                    int length = City.values().length;
                    int cityIdx = ThreadLocalRandom.current().nextInt(length);
                    sensorPosition.setPosition(City.values()[cityIdx].toString());
                    sensorPosition.setTimestamp(System.currentTimeMillis());
                    ctx.collect(sensorPosition);
//                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                System.out.println("SensorPosition#cancel");
            }
        });
        sensorPositionSource.print("sensorPos");
        MapStateDescriptor<String, SensorPosition> sensorPosBroadcastDescriptor = 
            new MapStateDescriptor<String, SensorPosition>("sensorPositions", String.class, SensorPosition.class);
        BroadcastStream<SensorPosition> sensorPosBroadcast = sensorPositionSource.broadcast(sensorPosBroadcastDescriptor);

        DataStreamSource<SensorTemperature> sensorTemperatureSource = env.addSource(new SensorSource(100, idBound, 60));
        sensorTemperatureSource.print("sensorTemperature");
        KeyedStream<SensorTemperature, String> sensorKeyStream = sensorTemperatureSource.keyBy(new KeySelector<SensorTemperature, String>() {
            @Override
            public String getKey(SensorTemperature value) throws Exception {
                return value.getId();
            }
        });

        SingleOutputStreamOperator<SensorInfo> sensorInfoStream = sensorKeyStream.connect(sensorPosBroadcast)
            .process(new SensorTempAndPosFunction());
        
        sensorInfoStream.print("sensorInfo");
        
        env.execute();

    }
    
    public static class SensorTempAndPosFunction extends KeyedBroadcastProcessFunction<String, SensorTemperature, SensorPosition, SensorInfo> {
        
        private MapStateDescriptor<String, SensorPosition> sensorPosMapStateDesc = 
            new MapStateDescriptor<String, SensorPosition>("sensorPositions", String.class, SensorPosition.class);
        
        
        @Override
        public void processElement(SensorTemperature sensorTemperature, ReadOnlyContext ctx, Collector<SensorInfo> out) throws Exception {
            ReadOnlyBroadcastState<String, SensorPosition> sensorPosState = ctx.getBroadcastState(sensorPosMapStateDesc);
            SensorPosition sensorPosition = sensorPosState.get(sensorTemperature.getId());
            SensorInfo sensorInfo = new SensorInfo();
            sensorInfo.setId(sensorTemperature.getId());
            sensorInfo.setTemperature(sensorTemperature.getTemperature());
            if (sensorPosition != null) {
                sensorInfo.setPosition(sensorPosition.getPosition());
            }
            sensorInfo.setTimestamp(sensorTemperature.getEventTime());
            out.collect(sensorInfo);
        }

        @Override
        public void processBroadcastElement(SensorPosition sensorPosition, Context ctx, Collector<SensorInfo> out) throws Exception {
            BroadcastState<String, SensorPosition> sensorPosState = ctx.getBroadcastState(sensorPosMapStateDesc);
            sensorPosState.put(sensorPosition.getId(),  sensorPosition);
        }
    }
}
