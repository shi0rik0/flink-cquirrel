package com.example.flink.model;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Objects;
import java.io.Serializable;

public class LineItem implements Serializable {
    private long l_orderkey;
    private long l_partkey;
    private long l_suppkey;
    private int l_linenumber;
    private BigDecimal l_quantity;
    private BigDecimal l_extendedprice;
    private BigDecimal l_discount;
    private BigDecimal l_tax;
    private String l_returnflag;
    private String l_linestatus;
    private LocalDate l_shipdate;
    private LocalDate l_commitdate;
    private LocalDate l_receiptdate;
    private String l_shipinstruct;
    private String l_shipmode;
    private String l_comment;

    public LineItem() {
    }

    public LineItem(long l_orderkey, long l_partkey, long l_suppkey, int l_linenumber,
            BigDecimal l_quantity, BigDecimal l_extendedprice, BigDecimal l_discount,
            BigDecimal l_tax, String l_returnflag, String l_linestatus,
            LocalDate l_shipdate, LocalDate l_commitdate, LocalDate l_receiptdate,
            String l_shipinstruct, String l_shipmode, String l_comment) {
        this.l_orderkey = l_orderkey;
        this.l_partkey = l_partkey;
        this.l_suppkey = l_suppkey;
        this.l_linenumber = l_linenumber;
        this.l_quantity = l_quantity;
        this.l_extendedprice = l_extendedprice;
        this.l_discount = l_discount;
        this.l_tax = l_tax;
        this.l_returnflag = l_returnflag;
        this.l_linestatus = l_linestatus;
        this.l_shipdate = l_shipdate;
        this.l_commitdate = l_commitdate;
        this.l_receiptdate = l_receiptdate;
        this.l_shipinstruct = l_shipinstruct;
        this.l_shipmode = l_shipmode;
        this.l_comment = l_comment;
    }

    // Getters
    public long getL_orderkey() {
        return l_orderkey;
    }

    public long getL_partkey() {
        return l_partkey;
    }

    public long getL_suppkey() {
        return l_suppkey;
    }

    public int getL_linenumber() {
        return l_linenumber;
    }

    public BigDecimal getL_quantity() {
        return l_quantity;
    }

    public BigDecimal getL_extendedprice() {
        return l_extendedprice;
    }

    public BigDecimal getL_discount() {
        return l_discount;
    }

    public BigDecimal getL_tax() {
        return l_tax;
    }

    public String getL_returnflag() {
        return l_returnflag;
    }

    public String getL_linestatus() {
        return l_linestatus;
    }

    public LocalDate getL_shipdate() {
        return l_shipdate;
    }

    public LocalDate getL_commitdate() {
        return l_commitdate;
    }

    public LocalDate getL_receiptdate() {
        return l_receiptdate;
    }

    public String getL_shipinstruct() {
        return l_shipinstruct;
    }

    public String getL_shipmode() {
        return l_shipmode;
    }

    public String getL_comment() {
        return l_comment;
    }

    // Setters
    public void setL_orderkey(long l_orderkey) {
        this.l_orderkey = l_orderkey;
    }

    public void setL_partkey(long l_partkey) {
        this.l_partkey = l_partkey;
    }

    public void setL_suppkey(long l_suppkey) {
        this.l_suppkey = l_suppkey;
    }

    public void setL_linenumber(int l_linenumber) {
        this.l_linenumber = l_linenumber;
    }

    public void setL_quantity(BigDecimal l_quantity) {
        this.l_quantity = l_quantity;
    }

    public void setL_extendedprice(BigDecimal l_extendedprice) {
        this.l_extendedprice = l_extendedprice;
    }

    public void setL_discount(BigDecimal l_discount) {
        this.l_discount = l_discount;
    }

    public void setL_tax(BigDecimal l_tax) {
        this.l_tax = l_tax;
    }

    public void setL_returnflag(String l_returnflag) {
        this.l_returnflag = l_returnflag;
    }

    public void setL_linestatus(String l_linestatus) {
        this.l_linestatus = l_linestatus;
    }

    public void setL_shipdate(LocalDate l_shipdate) {
        this.l_shipdate = l_shipdate;
    }

    public void setL_commitdate(LocalDate l_commitdate) {
        this.l_commitdate = l_commitdate;
    }

    public void setL_receiptdate(LocalDate l_receiptdate) {
        this.l_receiptdate = l_receiptdate;
    }

    public void setL_shipinstruct(String l_shipinstruct) {
        this.l_shipinstruct = l_shipinstruct;
    }

    public void setL_shipmode(String l_shipmode) {
        this.l_shipmode = l_shipmode;
    }

    public void setL_comment(String l_comment) {
        this.l_comment = l_comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        LineItem lineItem = (LineItem) o;
        return l_orderkey == lineItem.l_orderkey &&
                l_partkey == lineItem.l_partkey &&
                l_suppkey == lineItem.l_suppkey &&
                l_linenumber == lineItem.l_linenumber &&
                Objects.equals(l_quantity, lineItem.l_quantity) &&
                Objects.equals(l_extendedprice, lineItem.l_extendedprice) &&
                Objects.equals(l_discount, lineItem.l_discount) &&
                Objects.equals(l_tax, lineItem.l_tax) &&
                Objects.equals(l_returnflag, lineItem.l_returnflag) &&
                Objects.equals(l_linestatus, lineItem.l_linestatus) &&
                Objects.equals(l_shipdate, lineItem.l_shipdate) &&
                Objects.equals(l_commitdate, lineItem.l_commitdate) &&
                Objects.equals(l_receiptdate, lineItem.l_receiptdate) &&
                Objects.equals(l_shipinstruct, lineItem.l_shipinstruct) &&
                Objects.equals(l_shipmode, lineItem.l_shipmode) &&
                Objects.equals(l_comment, lineItem.l_comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
                l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
                l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode,
                l_comment);
    }

    @Override
    public String toString() {
        return "LineItem{" +
                "l_orderkey=" + l_orderkey +
                ", l_partkey=" + l_partkey +
                ", l_suppkey=" + l_suppkey +
                ", l_linenumber=" + l_linenumber +
                ", l_quantity=" + l_quantity +
                ", l_extendedprice=" + l_extendedprice +
                ", l_discount=" + l_discount +
                ", l_tax=" + l_tax +
                ", l_returnflag='" + l_returnflag + '\'' +
                ", l_linestatus='" + l_linestatus + '\'' +
                ", l_shipdate=" + l_shipdate +
                ", l_commitdate=" + l_commitdate +
                ", l_receiptdate=" + l_receiptdate +
                ", l_shipinstruct='" + l_shipinstruct + '\'' +
                ", l_shipmode='" + l_shipmode + '\'' +
                ", l_comment='" + l_comment + '\'' +
                '}';
    }
}