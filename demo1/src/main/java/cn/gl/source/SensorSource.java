package cn.gl.source;

import cn.gl.bean.SensorTemperature;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SensorSource implements SourceFunction<SensorTemperature> {
    
    private Integer waitTime;
    
    private Integer idBound;
    
    private Integer temperatureBound;

    public SensorSource() {
        this(200);
    }

    public SensorSource(Integer waitTime) {
        this(waitTime, 100, 60);
    }

    public SensorSource(Integer waitTime, Integer idBound, Integer temperatureBound) {
        this.waitTime = waitTime;
        this.idBound = idBound;
        this.temperatureBound = temperatureBound;
    }

    @Override
    public void run(SourceContext<SensorTemperature> ctx) throws Exception {
        while (true) {
            SensorTemperature sensorRecord = new SensorTemperature();
            Random random = new Random();
            sensorRecord.setId(String.valueOf(random.nextInt(idBound)));
            sensorRecord.setEventTime(System.currentTimeMillis());
            sensorRecord.setTemperature((double) random.nextInt(60));
            ctx.collect(sensorRecord);
//            TimeUnit.MILLISECONDS.sleep(waitTime);
        }
    }

    @Override
    public void cancel() {
        System.out.println("SensorSource#cancel");
    }
}
