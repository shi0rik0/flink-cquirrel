package com.example.flink;

import java.util.Arrays;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TPCHQuery3JobV2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.fromElements("Hello", "Flink", "World", "from", "Java!");
        text.print();
        System.out.println("Executing Flink job: Hello Flink World");
        env.execute("Hello Flink World Job");
    }
}
