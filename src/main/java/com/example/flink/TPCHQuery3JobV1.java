package com.example.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TPCHQuery3JobV1 {

    public static void main(String[] args) throws Exception {
        boolean isBatchMode = false;
        String dataPathArg = null;
        String outputPathArg = null;

        for (String arg : args) {
            if (arg.equals("-b")) {
                isBatchMode = true;
            } else if (dataPathArg == null) {
                dataPathArg = arg;
            } else if (outputPathArg == null) {
                outputPathArg = arg;
            } else {
                throw new IllegalArgumentException(
                        "Usage: TPCHQuery3JobV1 [-b] <path-to-data> <output-file-path>");
            }
        }

        if (dataPathArg == null || outputPathArg == null) {
            throw new IllegalArgumentException(
                    "Usage: TPCHQuery3JobV1 [-b] <path-to-data> <output-file-path>");
        }

        Path dataPath = Paths.get(Utils.convertAndNormalizePath(dataPathArg));
        String customerPath = dataPath.resolve("customer.tbl").toString();
        String ordersPath = dataPath.resolve("orders.tbl").toString();
        String lineitemPath = dataPath.resolve("lineitem.tbl").toString();
        String customerURI = Utils.getFileURI(customerPath);
        String ordersURI = Utils.getFileURI(ordersPath);
        String lineitemURI = Utils.getFileURI(lineitemPath);

        Path outputPath = Paths.get(Utils.convertAndNormalizePath(outputPathArg));
        String outputURI = Utils.getFileURI(outputPath.toString());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (isBatchMode) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            System.out.println("Flink environment set to BATCH mode.");
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            System.out.println("Flink environment set to STREAMING mode.");
        }

        // Set the job name
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().set("pipeline.name", "TPC-H Query 3 Job V1");

        tableEnv.executeSql(
                "CREATE TABLE Customer (" +
                        "  C_CUSTKEY BIGINT," +
                        "  C_NAME VARCHAR(25)," +
                        "  C_ADDRESS VARCHAR(40)," +
                        "  C_NATIONKEY BIGINT," +
                        "  C_PHONE CHAR(15)," +
                        "  C_ACCTBAL DECIMAL(12, 2)," +
                        "  C_MKTSEGMENT CHAR(10)," +
                        "  C_COMMENT VARCHAR(117)" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + customerURI + "'," +
                        "  'format' = 'csv'," +
                        "  'csv.field-delimiter' = '|'," +
                        "  'csv.disable-quote-character' = 'true'" +
                        ")");

        tableEnv.executeSql(
                "CREATE TABLE Orders (" +
                        "  O_ORDERKEY BIGINT," +
                        "  O_CUSTKEY BIGINT," +
                        "  O_ORDERSTATUS CHAR(1)," +
                        "  O_TOTALPRICE DECIMAL(12, 2)," +
                        "  O_ORDERDATE DATE," +
                        "  O_ORDERPRIORITY CHAR(15)," +
                        "  O_CLERK CHAR(15)," +
                        "  O_SHIPPRIORITY INTEGER," +
                        "  O_COMMENT VARCHAR(79)" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + ordersURI + "'," +
                        "  'format' = 'csv'," +
                        "  'csv.field-delimiter' = '|'," +
                        "  'csv.disable-quote-character' = 'true'" +
                        ")");

        tableEnv.executeSql(
                "CREATE TABLE Lineitem (" +
                        "  L_ORDERKEY BIGINT," +
                        "  L_PARTKEY BIGINT," +
                        "  L_SUPPKEY BIGINT," +
                        "  L_LINENUMBER INTEGER," +
                        "  L_QUANTITY DECIMAL(12, 2)," +
                        "  L_EXTENDEDPRICE DECIMAL(12, 2)," +
                        "  L_DISCOUNT DECIMAL(12, 2)," +
                        "  L_TAX DECIMAL(12, 2)," +
                        "  L_RETURNFLAG CHAR(1)," +
                        "  L_LINESTATUS CHAR(1)," +
                        "  L_SHIPDATE DATE," +
                        "  L_COMMITDATE DATE," +
                        "  L_RECEIPTDATE DATE," +
                        "  L_SHIPINSTRUCT CHAR(25)," +
                        "  L_SHIPMODE CHAR(10)," +
                        "  L_COMMENT VARCHAR(44)" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + lineitemURI + "'," +
                        "  'format' = 'csv'," +
                        "  'csv.field-delimiter' = '|'," +
                        "  'csv.disable-quote-character' = 'true'" +
                        ")");

        String sqlQuery = "SELECT " +
                "  C_CUSTKEY, " +
                "  O_ORDERKEY, " +
                "  L_LINENUMBER " +
                "FROM " +
                "  Lineitem, " +
                "  Customer, " +
                "  Orders " +
                "WHERE " +
                "  C_CUSTKEY = O_CUSTKEY AND " +
                "  L_ORDERKEY = O_ORDERKEY AND " +
                "  C_MKTSEGMENT = 'AUTOMOBILE' AND " +
                "  O_ORDERDATE < DATE '1995-03-13' AND " +
                "  L_SHIPDATE > DATE '1995-03-13' ";

        Table resultTable = tableEnv.sqlQuery(sqlQuery);

        tableEnv.executeSql(
                "CREATE TABLE Query3ResultOutput (" +
                        "  C_CUSTKEY BIGINT," +
                        "  O_ORDERKEY BIGINT," +
                        "  L_LINENUMBER INTEGER" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + outputURI + "'," +
                        "  'format' = 'csv'," +
                        "  'csv.field-delimiter' = '|'," +
                        "  'sink.rolling-policy.rollover-interval' = '1 min'," +
                        "  'sink.rolling-policy.check-interval' = '10 s'" +
                        ")");
        resultTable.executeInsert("Query3ResultOutput").await();
    }
}
