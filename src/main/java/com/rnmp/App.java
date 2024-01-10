package com.rnmp;


import com.rnmp.kafka_data.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;


import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.file.sink.FileSink;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import java.util.stream.StreamSupport;


public class App {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // X and Y are the window sizes in milliseconds
        // define X and Y
        final long X = 2500;
        final long Y = 500;

        // kafka config vars
        // if running locally with IDE then we use localhost, otherwise if running in flink
        // we use docker host name. I use the env variable FLINK_PROPERTIES to determine
        final String brokers = System.getenv("FLINK_PROPERTIES") != null ? "kafka:29092" : "localhost:9092";


        final String kafka_consumer_group = "homework-1";


        KafkaSource<SensorData> source = KafkaSource.<SensorData>builder()
                // these are just kafka configs, we can just do fine with first three
                // but the rest of them are just for tuning the performance
                .setBootstrapServers(brokers) // Set your Kafka bootstrap server
                .setGroupId(kafka_consumer_group) // Set your Kafka consumer group
                .setTopics("sensors") // Set your Kafka topic
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SensorDataDeserializer())
                .build();

        DataStream<SensorData> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");



//        FileSink<CountResult> sink1 = FileSink
//            .forRowFormat(new Path("output"), new CountResultNewLineJsonEncoder())
//            .build();
//
//        FileSink<AggregateResult> sink2 = FileSink
//                .forRowFormat(new Path("output2"), new AggregateResultEncoder())
//                .build();

        KafkaSink<CountResult> counts_kafka_sink = KafkaSink.<CountResult>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                            .setTopic("counts")
                            .setValueSerializationSchema(new CountResultSerializationSchema())
                            .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<AggregateResult> aggregate_kafka_sink = KafkaSink.<AggregateResult>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("aggregates")
                                .setValueSerializationSchema(new AggregateResultSerializationSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        // https://github.com/apache/flink/blob/release-1.18.1-rc2/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/windowing/GroupedProcessingTimeWindowExample.java

        WindowedStream<SensorData, String, TimeWindow> window = stream
            .keyBy(SensorData::getKey)
            .window(SlidingProcessingTimeWindows.of(Time.milliseconds(X), Time.milliseconds(Y)));

        window
            .apply(new CountResultFunction())
            .sinkTo(counts_kafka_sink);

        window.apply(new AggregateResultFunction())
            .sinkTo(aggregate_kafka_sink);

//        FileSink<CountResult> sink1 = FileSink
//            .forRowFormat(new Path("output"), new CountResultNewLineJsonEncoder())
//            .build();
//
//        FileSink<AggregateResult> sink2 = FileSink
//                .forRowFormat(new Path("output2"), new AggregateResultEncoder())
//                .build();
//        window
//                .apply(new CountResultFunction())
//                .sinkTo(sink1);
//
//        window.apply(new AggregateResultFunction())
//                .sinkTo(sink2);


        env.execute("Flink Kafka to File Sink Job");
    }

    public static class CountResultFunction implements WindowFunction<SensorData, CountResult, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow timeWindow, Iterable<SensorData> iterable, Collector<CountResult> collector) throws Exception {
            long count = StreamSupport.stream(iterable.spliterator(), false).count();
            CountResult result = new CountResult(key, timeWindow.getStart(), timeWindow.getEnd(), count);
            collector.collect(result);
        }
    }

    public static class AggregateResultFunction implements WindowFunction<SensorData, AggregateResult, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow timeWindow, Iterable<SensorData> iterable, Collector<AggregateResult> collector) throws Exception {
            long count = StreamSupport.stream(iterable.spliterator(), false).count();
            double average = StreamSupport.stream(iterable.spliterator(), false).mapToDouble(SensorData::getValue).average().orElse(0);
            double min_value = StreamSupport.stream(iterable.spliterator(), false).mapToDouble(SensorData::getValue).min().orElse(0);
            double max_value = StreamSupport.stream(iterable.spliterator(), false).mapToDouble(SensorData::getValue).max().orElse(0);
            AggregateResult result = new AggregateResult(key, timeWindow.getStart(), timeWindow.getEnd(), count, average, min_value, max_value);
            collector.collect(result);
        }
    }


}
