package cn.gl.window;


/**
 * data:
 * sensor_1,1627829642,37.2
 * sensor_1,1627829642,37
 * sensor_1,1627829642,37
 * 
 */
public class EventWindowDemo {

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        SingleOutputStreamOperator<SensorRecord> sensorStream = env.socketTextStream("localhost", 8888)
//            .map(SensorRecordMapper::lineSecondsToSensorRecord)
//            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorRecord>(Time.seconds(3)) {
//                @Override
//                public long extractTimestamp(SensorRecord element) {
//                    System.out.println(element.getEventTime());
//                    return element.getEventTime();
//                }
//            });
//
//        KeyedStream<SensorRecord, String> keyedStream = sensorStream.keyBy(new KeySelector<SensorRecord, String>() {
//            @Override
//            public String getKey(SensorRecord sensorRecord) throws Exception {
//                return sensorRecord.getId();
//            }
//        });
//
//        WindowedStream<SensorRecord, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(15)));
//        SingleOutputStreamOperator<SensorRecord> minTemperatureStream = windowStream.min("temperature");
//        minTemperatureStream.print("min");
//
//        env.execute();


    }
    
    
}
