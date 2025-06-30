package com.example.flink.model;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Objects;
import java.io.Serializable;

public class Order implements Serializable {
    private long o_orderkey;
    private long o_custkey;
    private String o_orderstatus;
    private BigDecimal o_totalprice;
    private LocalDate o_orderdate;
    private String o_orderpriority;
    private String o_clerk;
    private int o_shippriority;
    private String o_comment;

    public Order() {
    }

    public Order(long o_orderkey, long o_custkey, String o_orderstatus, BigDecimal o_totalprice,
            LocalDate o_orderdate, String o_orderpriority, String o_clerk, int o_shippriority,
            String o_comment) {
        this.o_orderkey = o_orderkey;
        this.o_custkey = o_custkey;
        this.o_orderstatus = o_orderstatus;
        this.o_totalprice = o_totalprice;
        this.o_orderdate = o_orderdate;
        this.o_orderpriority = o_orderpriority;
        this.o_clerk = o_clerk;
        this.o_shippriority = o_shippriority;
        this.o_comment = o_comment;
    }

    // Getters
    public long getO_orderkey() {
        return o_orderkey;
    }

    public long getO_custkey() {
        return o_custkey;
    }

    public String getO_orderstatus() {
        return o_orderstatus;
    }

    public BigDecimal getO_totalprice() {
        return o_totalprice;
    }

    public LocalDate getO_orderdate() {
        return o_orderdate;
    }

    public String getO_orderpriority() {
        return o_orderpriority;
    }

    public String getO_clerk() {
        return o_clerk;
    }

    public int getO_shippriority() {
        return o_shippriority;
    }

    public String getO_comment() {
        return o_comment;
    }

    // Setters
    public void setO_orderkey(long o_orderkey) {
        this.o_orderkey = o_orderkey;
    }

    public void setO_custkey(long o_custkey) {
        this.o_custkey = o_custkey;
    }

    public void setO_orderstatus(String o_orderstatus) {
        this.o_orderstatus = o_orderstatus;
    }

    public void setO_totalprice(BigDecimal o_totalprice) {
        this.o_totalprice = o_totalprice;
    }

    public void setO_orderdate(LocalDate o_orderdate) {
        this.o_orderdate = o_orderdate;
    }

    public void setO_orderpriority(String o_orderpriority) {
        this.o_orderpriority = o_orderpriority;
    }

    public void setO_clerk(String o_clerk) {
        this.o_clerk = o_clerk;
    }

    public void setO_shippriority(int o_shippriority) {
        this.o_shippriority = o_shippriority;
    }

    public void setO_comment(String o_comment) {
        this.o_comment = o_comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Order order = (Order) o;
        return o_orderkey == order.o_orderkey &&
                o_custkey == order.o_custkey &&
                o_shippriority == order.o_shippriority &&
                Objects.equals(o_orderstatus, order.o_orderstatus) &&
                Objects.equals(o_totalprice, order.o_totalprice) &&
                Objects.equals(o_orderdate, order.o_orderdate) &&
                Objects.equals(o_orderpriority, order.o_orderpriority) &&
                Objects.equals(o_clerk, order.o_clerk) &&
                Objects.equals(o_comment, order.o_comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
                o_orderpriority, o_clerk, o_shippriority, o_comment);
    }

    @Override
    public String toString() {
        return "Order{" +
                "o_orderkey=" + o_orderkey +
                ", o_custkey=" + o_custkey +
                ", o_orderstatus='" + o_orderstatus + '\'' +
                ", o_totalprice=" + o_totalprice +
                ", o_orderdate=" + o_orderdate +
                ", o_orderpriority='" + o_orderpriority + '\'' +
                ", o_clerk='" + o_clerk + '\'' +
                ", o_shippriority=" + o_shippriority +
                ", o_comment='" + o_comment + '\'' +
                '}';
    }
}