package cn.gl.bean;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SensorRecord {
    
    private String id;
    private Long eventTime;
    private Double temperature;

    public SensorRecord(String id, Long eventTime, Double temperature) {
        this.id = id;
        this.eventTime = eventTime;
        this.temperature = temperature;
    }
}
