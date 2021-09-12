package cn.gl.common;

import cn.gl.bean.SensorTemperature;

public class SensorRecordMapper {
    
    public static SensorTemperature lineToSensorRecord(String line) {
        String[] segments = line.split(",");
        return new SensorTemperature(segments[0], Long.valueOf(segments[1]), Double.valueOf(segments[2]));
    }
    
    public static SensorTemperature lineSecondsToSensorRecord(String line) {
        String[] segments = line.split(",");
        SensorTemperature sensorRecord = new SensorTemperature(segments[0], Long.parseLong(segments[1]) * 1000, Double.valueOf(segments[2]));
        System.out.println("input: " + sensorRecord);
        return sensorRecord;
    }
}
