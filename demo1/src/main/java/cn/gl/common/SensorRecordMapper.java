package cn.gl.common;

import cn.gl.bean.SensorRecord;

public class SensorRecordMapper {
    
    public static SensorRecord lineToSensorRecord(String line) {
        String[] segments = line.split(",");
        return new SensorRecord(segments[0], Long.valueOf(segments[1]), Double.valueOf(segments[2]));
    }
    
    public static SensorRecord lineSecondsToSensorRecord(String line) {
        String[] segments = line.split(",");
        SensorRecord sensorRecord = new SensorRecord(segments[0], Long.parseLong(segments[1]) * 1000, Double.valueOf(segments[2]));
        System.out.println("input: " + sensorRecord);
        return sensorRecord;
    }
}
