package com.popupd.Weights;

//import com.popupd.Visualize.InfluxDBSink;
import com.popupd.util.BourseDataDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.popupd.util.BourseData;
import com.popupd.util.*;

import java.time.Duration;

public class WeightsUpdate {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Use event time for the application

        // Configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000);

        KafkaSource<BourseData> priceSource = KafkaSource.<BourseData>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("intraday-stock-prices")
                .setGroupId("flink-prices-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new BourseDataDeserializationSchema())
                .build();

        KafkaSource<WeightStockSchema> weightSource = KafkaSource.<WeightStockSchema>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("stock_weights")
                .setGroupId("flink-weights-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new WeightStockDeserializationSchema())
                .build();

        //KafkaSource<PortfolioStatsSchema> portfolioStatsSource = KafkaSource.<PortfolioStatsSchema>builder()
                //.setBootstrapServers("localhost:9092")
                //.setTopics("portf_stats")
                //.setGroupId("flink-weights-group")
                //.setStartingOffsets(OffsetsInitializer.latest())
                //.setDeserializer(new PortfolioStatsDeserializationSchema())
                //.build();


        WatermarkStrategy<BourseData> watermarkPriceStrategy = WatermarkStrategy
                .<BourseData>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, timestamp) ->
                        java.time.OffsetDateTime.parse(event.getFieldLastTradedTime())
                                .toInstant()
                                .toEpochMilli()
                );


        WatermarkStrategy<WeightStockSchema> watermarkWeightStrategy = WatermarkStrategy
                .<WeightStockSchema>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, timestamp) -> {
                    String timestampStr = event.getTime().toString();

                    // Check if the timestamp contains a time zone information, if not, add 'Z' for UTC
                    if (!timestampStr.contains("T") || !timestampStr.contains("Z")) {
                        // Assuming the timestamp should be in UTC if it does not have a time zone
                        timestampStr = timestampStr.replace(" ", "T") + "Z";  // Adding 'Z' to indicate UTC
                    }

                    // Parse using OffsetDateTime
                    return java.time.OffsetDateTime.parse(timestampStr)
                            .toInstant()
                            .toEpochMilli();
                });



        //WatermarkStrategy<PortfolioStatsSchema> watermarkPortfolioStatsStrategy = WatermarkStrategy
                //.<PortfolioStatsSchema>forBoundedOutOfOrderness(Duration.ofMinutes(1))  // max lateness
                //.withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        DataStreamSource<BourseData> pricesStream = (DataStreamSource<BourseData>) env.fromSource(
                priceSource,
                watermarkPriceStrategy,
                "Kafka Prices Source"
        ).returns(BourseData.class);

        DataStreamSource<WeightStockSchema> weightStream = (DataStreamSource<WeightStockSchema>) env.fromSource(
                weightSource,
                watermarkWeightStrategy,
                "Kafka Weights Source"
        ).returns(WeightStockSchema.class);

        KeyedStream<BourseData, String> keyedPricesStream = pricesStream
                .keyBy(data -> data.getTicker().toString());

        KeyedStream<WeightStockSchema, String> keyedWeightsStream = weightStream
                .keyBy(data -> data.getTicker().toString());

        KeyedStream<StockReturn, String> logReturnsStream = keyedPricesStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new LogReturnWindowFunction()).keyBy(StockReturn::getTicker);

        KeyedStream<WeightedReturn, String> weightedReturns =  keyedWeightsStream
                .connect(logReturnsStream)
                .process(new WeightReturnMultiplicationFunction()).keyBy(WeightedReturn::getTicker);

        SingleOutputStreamOperator<PortfolioCumulativeReturn> portfolioReturn = weightedReturns
                .process(new CumulativeWeightedReturnFunction());

        portfolioReturn.print();




        //keyedPricesStream
                //.window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5))) // Slide every minute
                //.process(new LogReturnWindowFunction())
                //.addSink(new InfluxDBSink());












        //pricesStream.print();
        //weightStream.print();

        env.execute("Weights update job");


















    }
}
