package cn.gl.state;

import cn.gl.bean.SensorTemperature;
import cn.gl.common.SensorRecordMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        
        SingleOutputStreamOperator<SensorTemperature> sensorStream = env.readTextFile("/home/gl/code/learn/flink-learn/flink-demo/data/demo1/sensorData.txt")
            .map(SensorRecordMapper::lineToSensorRecord);
        SingleOutputStreamOperator<Integer> countStream = sensorStream.map(new CountFunction());
        
        
        countStream.print();

        env.execute();
    }
    
    public static class CountFunction implements MapFunction<SensorTemperature, Integer>, CheckpointedFunction {

        private Integer count = 0;
        private ListState<Integer> countState;
        
        @Override
        public Integer map(SensorTemperature value) throws Exception {
            return ++count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            countState.clear();
            countState.add(this.count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            countState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("count", Integer.class));
            if (countState.get() == null) {
                return;
            }
            for (Integer value : countState.get()) {
                count += value;
            }
        }
    }
    
    
}
