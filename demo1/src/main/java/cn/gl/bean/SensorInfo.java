package cn.gl.bean;

import lombok.Data;

@Data
public class SensorInfo {
    private String id;
    private Double temperature;
    private String position;
    private Long timestamp;
}
