package com.example.flink.model;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

public class Customer implements Serializable {
    private long c_custkey;
    private String c_name;
    private String c_address;
    private long c_nationkey;
    private String c_phone;
    private BigDecimal c_acctbal;
    private String c_mktsegment;
    private String c_comment;

    public Customer() {
    }

    public Customer(long c_custkey, String c_name, String c_address, long c_nationkey,
            String c_phone, BigDecimal c_acctbal, String c_mktsegment, String c_comment) {
        this.c_custkey = c_custkey;
        this.c_name = c_name;
        this.c_address = c_address;
        this.c_nationkey = c_nationkey;
        this.c_phone = c_phone;
        this.c_acctbal = c_acctbal;
        this.c_mktsegment = c_mktsegment;
        this.c_comment = c_comment;
    }

    // Getters
    public long getC_custkey() {
        return c_custkey;
    }

    public String getC_name() {
        return c_name;
    }

    public String getC_address() {
        return c_address;
    }

    public long getC_nationkey() {
        return c_nationkey;
    }

    public String getC_phone() {
        return c_phone;
    }

    public BigDecimal getC_acctbal() {
        return c_acctbal;
    }

    public String getC_mktsegment() {
        return c_mktsegment;
    }

    public String getC_comment() {
        return c_comment;
    }

    // Setters
    public void setC_custkey(long c_custkey) {
        this.c_custkey = c_custkey;
    }

    public void setC_name(String c_name) {
        this.c_name = c_name;
    }

    public void setC_address(String c_address) {
        this.c_address = c_address;
    }

    public void setC_nationkey(long c_nationkey) {
        this.c_nationkey = c_nationkey;
    }

    public void setC_phone(String c_phone) {
        this.c_phone = c_phone;
    }

    public void setC_acctbal(BigDecimal c_acctbal) {
        this.c_acctbal = c_acctbal;
    }

    public void setC_mktsegment(String c_mktsegment) {
        this.c_mktsegment = c_mktsegment;
    }

    public void setC_comment(String c_comment) {
        this.c_comment = c_comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Customer customer = (Customer) o;
        return c_custkey == customer.c_custkey &&
                c_nationkey == customer.c_nationkey &&
                Objects.equals(c_name, customer.c_name) &&
                Objects.equals(c_address, customer.c_address) &&
                Objects.equals(c_phone, customer.c_phone) &&
                Objects.equals(c_acctbal, customer.c_acctbal) &&
                Objects.equals(c_mktsegment, customer.c_mktsegment) &&
                Objects.equals(c_comment, customer.c_comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
                c_mktsegment, c_comment);
    }

    @Override
    public String toString() {
        return "Customer{" +
                "c_custkey=" + c_custkey +
                ", c_name='" + c_name + '\'' +
                ", c_address='" + c_address + '\'' +
                ", c_nationkey=" + c_nationkey +
                ", c_phone='" + c_phone + '\'' +
                ", c_acctbal=" + c_acctbal +
                ", c_mktsegment='" + c_mktsegment + '\'' +
                ", c_comment='" + c_comment + '\'' +
                '}';
    }

    public static DataStream<Customer> createCustomerStream(StreamExecutionEnvironment env, String customerPath) {
        FileSource<String> source = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(StandardCharsets.UTF_8.name()),
                new org.apache.flink.core.fs.Path(customerPath))
                .build();

        DataStream<String> lineStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "customer.tbl");

        DataStream<Customer> customerStream = lineStream
                .filter(line -> !line.trim().isEmpty())
                .map(new MapFunction<String, Customer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Customer map(String line) throws Exception {
                        String[] parts = line.split("\\|");

                        if (parts.length != 8) {
                            System.err
                                    .println("Skipping malformed line (wrong number of fields for Customer): " + line);
                            return null;
                        }

                        try {
                            long c_custkey = Long.parseLong(parts[0].trim());
                            String c_name = parts[1].trim();
                            String c_address = parts[2].trim();
                            long c_nationkey = Long.parseLong(parts[3].trim());
                            String c_phone = parts[4].trim();
                            BigDecimal c_acctbal = new BigDecimal(parts[5].trim());
                            String c_mktsegment = parts[6].trim();
                            String c_comment = parts[7].trim();

                            return new Customer(c_custkey, c_name, c_address, c_nationkey,
                                    c_phone, c_acctbal, c_mktsegment, c_comment);

                        } catch (NumberFormatException e) {
                            System.err.println(
                                    "Skipping line due to number format error for Customer: " + line + " - "
                                            + e.getMessage());
                            return null;
                        } catch (Exception e) {
                            System.err.println(
                                    "Skipping line due to unexpected parsing error for Customer: " + line + " - "
                                            + e.getMessage());
                            return null;
                        }
                    }
                })
                .returns(Customer.class)
                .filter(Objects::nonNull);

        return customerStream;
    }
}