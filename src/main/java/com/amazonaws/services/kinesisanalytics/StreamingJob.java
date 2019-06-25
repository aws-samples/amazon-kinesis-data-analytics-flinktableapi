package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.Table;
import java.util.Properties;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Set to input data Timestamp indicating the time of the event to process the data sequentially.
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, "us-west-2");
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        //Leverage JsonNodeDeserializationSchema to convert incoming JSON to generic ObjectNode
        DataStream<ObjectNode> orderStreamObject =  env.addSource(
                new FlinkKinesisConsumer<>(
                        "flinkjoin-order", 
                        new JsonNodeDeserializationSchema(), 
                        inputProperties));

        DataStream<ObjectNode> exchangeRateStreamObject =  env.addSource(
                new FlinkKinesisConsumer<>(
                        "flinkjoin-exchangerate", 
                        new JsonNodeDeserializationSchema(), 
                        inputProperties));
        
        //Map incomming data from the generic Object Node to a POJO object
        //Set if TimeCharacteristic = "EventTime" to determine how the the Time Attribute rowtime will be determined from the incoming data
        DataStream<Order> orderStream = orderStreamObject
                .map((ObjectNode object) -> {
                    //For debugging input
                    //System.out.println(object.toString());
                    ObjectMapper mapper = new ObjectMapper();
                    Order order = mapper.readValue(object.toString(), Order.class);
                    return order;
                });

        DataStream<ExchangeRate> exchangeRateStream = exchangeRateStreamObject
                .map((ObjectNode object) -> {
                    //For debugging input
                    //System.out.println(object.toString());
                    ObjectMapper mapper = new ObjectMapper();
                    ExchangeRate exchangeRate = mapper.readValue(object.toString(), ExchangeRate.class);
                    return exchangeRate;
                });

        DataStream<Order> orderStreamWithTime = orderStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
                    @Override
                    public long extractAscendingTimestamp(Order element) {
                        return element.orderTime.getTime();
                    }});

        DataStream<ExchangeRate> exchangeRateStreamWithTime = exchangeRateStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ExchangeRate>() {
                    @Override
                    public long extractAscendingTimestamp(ExchangeRate element) {
                        return element.exchangeRateTime.getTime();
                    }});

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        //Convert from DataStream to Table, replacing replacing incoming field with "Time Attribute" rowtime and aliasing it to orderTime and exchangeRateTime respectively.
        Table orderTable = tableEnv.fromDataStream(orderStreamWithTime, "id, orderTime, amount, currency, eventtime.rowtime");
        Table exchangeRateTable = tableEnv.fromDataStream(exchangeRateStreamWithTime, "exchangeRateTime, currency, rate, eventtime.rowtime");

        //Register the table for use in SQL queries
        tableEnv.registerTable("Orders", orderTable);
        tableEnv.registerTable("ExchangeRates", exchangeRateTable);

        //Register UDF for display of Timestamp in output records as a formatted string instead of a number
        tableEnv.registerFunction("TimestampToString", new TimestampToString());

        //Define a new Dynamic Table as the results of a SQL Query.
        Table resultTable = tableEnv.sqlQuery(""+
                "SELECT o.id, " +
                "  TimestampToString(o.orderTime) orderTime, " +
                "  o.amount originalAmount, " +
                "  (o.amount*r.rate) convertedAmount " +
                "FROM Orders o " +
                "LEFT JOIN ExchangeRates r ON " +
                "      o.currency = r.currency " +
                "  AND o.eventtime >= r.eventtime " +
                "  AND r.eventtime > (o.eventtime - INTERVAL '5' SECOND)"
        );

        //Convert the Dynamic Table to a DataStream
        DataStream<Result> resultSet = tableEnv.toAppendStream(resultTable,Result.class);

        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, "us-west-2");

        //Convert the stream of Objects to a stream of Byte[] which can be fed to the FlinkKensisProducer output Stream
        FlinkKinesisProducer<Result> streamSink = new FlinkKinesisProducer<Result>(new SerializationSchema<Result>() {
            @Override
            public byte[] serialize(Result element) {

                ObjectMapper mapper = new ObjectMapper();
                byte[] output;
                try {
                    output = mapper.writeValueAsString(element).getBytes();
                    //For debugging output
                    System.out.println(mapper.writeValueAsString(element));
                } catch (Exception e) {
                    output = "".getBytes();
                }
                return output;
            }
        }, outputProperties);
        streamSink.setDefaultStream("flinkjoin-output");
        streamSink.setDefaultPartition("0");

        //Set output destination (sink) for the resultSet Dynamic Table.
        resultSet.addSink(streamSink);

        //Execute the application;
        env.execute("Flink Streaming - Orders w/ Exchange Rates");

    }
}
