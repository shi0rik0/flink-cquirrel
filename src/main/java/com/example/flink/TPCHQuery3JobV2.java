package com.example.flink;

import com.example.flink.model.Order;
import com.example.flink.model.Customer;
import com.example.flink.model.LineItem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

public class TPCHQuery3JobV2 {

    public static class CustomerJoinOrder {
        private final Customer customer;
        private final Order order;

        public CustomerJoinOrder(Customer customer, Order order) {
            this.customer = customer;
            this.order = order;
        }

        public Customer getCustomer() {
            return customer;
        }

        public Order getOrder() {
            return order;
        }
    }

    public static class CustomerJoinOrderFunction
            extends KeyedCoProcessFunction<Long, Customer, Order, CustomerJoinOrder> {
        private ValueState<Customer> customerState;
        private ListState<Order> orderState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            customerState = getRuntimeContext().getState(new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    "customerState", Customer.class));
            orderState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>(
                    "orderState", Order.class));
        }

        @Override
        public void processElement1(Customer value,
                KeyedCoProcessFunction<Long, Customer, Order, CustomerJoinOrder>.Context ctx,
                Collector<CustomerJoinOrder> out) throws Exception {
            // If the customer is already set, emit a warning
            if (customerState.value() != null) {
                System.err.println("Warning: Customer with key " + value.getC_custkey()
                        + " already exists in state. Omitting.");
                return;
            }
            customerState.update(value);

            // Iterate through all orders in the state and emit the join result
            for (Order order : orderState.get()) {
                out.collect(new CustomerJoinOrder(value, order));
            }
        }

        @Override
        public void processElement2(Order value,
                KeyedCoProcessFunction<Long, Customer, Order, CustomerJoinOrder>.Context ctx,
                Collector<CustomerJoinOrder> out) throws Exception {
            orderState.add(value);
            Customer customer = customerState.value();
            if (customer != null) {
                out.collect(new CustomerJoinOrder(customer, value));
            }
        }
    }

    public static class CustomerJoinOrdersJoinLineItem {
        private final CustomerJoinOrder customerJoinOrder;
        private final LineItem lineItem;

        public CustomerJoinOrdersJoinLineItem(CustomerJoinOrder customerJoinOrder, LineItem lineItem) {
            this.customerJoinOrder = customerJoinOrder;
            this.lineItem = lineItem;
        }

        public CustomerJoinOrder getCustomerJoinOrder() {
            return customerJoinOrder;
        }

        public LineItem getLineItem() {
            return lineItem;
        }
    }

    public static class CustomerJoinOrdersJoinLineItemFunction
            extends KeyedCoProcessFunction<Long, CustomerJoinOrder, LineItem, CustomerJoinOrdersJoinLineItem> {
        private ValueState<CustomerJoinOrder> customerJoinOrderState;
        private ListState<LineItem> lineItemState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            customerJoinOrderState = getRuntimeContext()
                    .getState(new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                            "customerJoinOrderState", CustomerJoinOrder.class));
            lineItemState = getRuntimeContext()
                    .getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>(
                            "lineItemState", LineItem.class));
        }

        @Override
        public void processElement1(CustomerJoinOrder value,
                KeyedCoProcessFunction<Long, CustomerJoinOrder, LineItem, CustomerJoinOrdersJoinLineItem>.Context ctx,
                Collector<CustomerJoinOrdersJoinLineItem> out) throws Exception {
            // If the customer join order is already set, emit a warning
            if (customerJoinOrderState.value() != null) {
                System.err.println("Warning: CustomerJoinOrder with key " + value.getOrder().getO_orderkey()
                        + " already exists in state. Omitting.");
                return;
            }

            customerJoinOrderState.update(value);

            for (LineItem lineItem : lineItemState.get()) {
                out.collect(new CustomerJoinOrdersJoinLineItem(value, lineItem));
            }
        }

        @Override
        public void processElement2(LineItem value,
                KeyedCoProcessFunction<Long, CustomerJoinOrder, LineItem, CustomerJoinOrdersJoinLineItem>.Context ctx,
                Collector<CustomerJoinOrdersJoinLineItem> out) throws Exception {
            lineItemState.add(value);
            CustomerJoinOrder customerJoinOrder = customerJoinOrderState.value();
            if (customerJoinOrder != null) {
                out.collect(new CustomerJoinOrdersJoinLineItem(customerJoinOrder, value));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: TPCHQuery3JobV2 <path-to-data> <output-path>");
        }
        Path dataPath = Paths.get(Utils.convertAndNormalizePath(args[0]));
        String outputPath = Paths.get(Utils.convertAndNormalizePath(args[1])).toString();

        String orderPath = dataPath.resolve("orders.tbl").toString();
        String customerPath = dataPath.resolve("customer.tbl").toString();
        String lineItemPath = dataPath.resolve("lineitem.tbl").toString();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set checkpointing for fault-tolerant file sink
        env.enableCheckpointing(5000);

        DataStream<Order> orderStream = Order.createOrderStream(env, orderPath);
        DataStream<Customer> customerStream = Customer.createCustomerStream(env,
                customerPath);
        DataStream<LineItem> lineItemStream = LineItem.createLineItemStream(env,
                lineItemPath);

        // O_ORDERDATE < DATE '1995-03-13'
        orderStream = orderStream.filter(order -> order.getO_orderdate().isBefore(LocalDate.of(1995, 3, 13)));
        // C_MKTSEGMENT = 'AUTOMOBILE'
        customerStream = customerStream.filter(customer -> "AUTOMOBILE".equals(customer.getC_mktsegment()));
        // L_SHIPDATE > DATE '1995-03-13'
        lineItemStream = lineItemStream.filter(lineItem -> lineItem.getL_shipdate().isAfter(LocalDate.of(1995, 3, 13)));

        KeyedStream<Customer, Long> keyedCustomerStream = customerStream
                .keyBy(Customer::getC_custkey);

        KeyedStream<Order, Long> keyedOrderStream = orderStream
                .keyBy(Order::getO_custkey);

        DataStream<CustomerJoinOrder> joinedStream = keyedCustomerStream.connect(keyedOrderStream)
                .process(new CustomerJoinOrderFunction());

        KeyedStream<LineItem, Long> keyedLineItemStream = lineItemStream
                .keyBy(LineItem::getL_orderkey);

        KeyedStream<CustomerJoinOrder, Long> keyedJoinedStream = joinedStream
                .keyBy(joined -> joined.getOrder().getO_orderkey());

        DataStream<CustomerJoinOrdersJoinLineItem> finalJoinedStream = keyedJoinedStream
                .connect(keyedLineItemStream)
                .process(new CustomerJoinOrdersJoinLineItemFunction());

        DataStream<String> resultStringStream = finalJoinedStream.map(
                item -> String.join("|",
                        String.valueOf(item.getCustomerJoinOrder().getCustomer().getC_custkey()),
                        String.valueOf(item.getCustomerJoinOrder().getOrder().getO_orderkey()),
                        String.valueOf(item.getLineItem().getL_linenumber())));

        FileSink<String> fileSink = FileSink
                .forRowFormat(new org.apache.flink.core.fs.Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(1024 * 1024 * 10)
                                .withInactivityInterval(Duration.ofMinutes(1))
                                .build())
                .build();

        resultStringStream.sinkTo(fileSink);

        env.execute("TPC-H Query 3 Job V2");
    }
}
