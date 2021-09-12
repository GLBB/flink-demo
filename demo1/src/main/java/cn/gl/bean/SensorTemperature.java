package cn.gl.bean;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SensorTemperature {
    
    private String id;
    private Long eventTime;
    private Double temperature;

    public SensorTemperature(String id, Long eventTime, Double temperature) {
        this.id = id;
        this.eventTime = eventTime;
        this.temperature = temperature;
    }
}
