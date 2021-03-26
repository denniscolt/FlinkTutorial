package com.blinkdemo.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception{
        //Create env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Read data from input
        String inputPath = "src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        //Process data set, separate by space, and calculate result
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0) //group by first location in tuples
                .sum(1); //sum second location in tuples

        resultSet.print();
    }

    //FlatMapFunc interface
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //Separate by space
            String[] words = value.split(" ");
            //Iterate all words, pack as tuples
            for(String word:words){
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
