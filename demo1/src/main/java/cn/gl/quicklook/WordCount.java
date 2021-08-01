package cn.gl.quicklook;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCount {

    public static void main(String[] args) throws Exception {
        
        String dataPath = "/home/gl/code/learn/flink-learn/flink-demo/data/demo1/wordCountData1.txt";
        
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = executionEnvironment.readTextFile(dataPath);
        AggregateOperator<Tuple2<String, Integer>> wordsCount = dataSource.flatMap(new WordSplitFunc())
            .groupBy(0)
            .sum(1);
        wordsCount.print();
        
        
    }
}
