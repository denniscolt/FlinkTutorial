package com.blinkdemo.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        //Create stream process env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //set parallelism
//        env.setParallelism(3);

//        //Read data from input
//        String inputPath = "src/main/resources/hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        //Use parameterTool extract config from startup file
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //Read input from socket
        DataStream<String> inputDataStream = env.socketTextStream(host,port);

        //Process data set, separate by space, and calculate result
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1)
                .setParallelism(2);

        resultStream.print().setParallelism(1);

        //Execute
        env.execute();
    }
}
