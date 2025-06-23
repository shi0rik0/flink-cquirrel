package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class DataStreamJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.fromElements("Hello", "Flink", "World", "from", "Java!");
        text.print();
        System.out.println("Executing Flink job: Hello Flink World");
        env.execute("Hello Flink World Job");
    }
}