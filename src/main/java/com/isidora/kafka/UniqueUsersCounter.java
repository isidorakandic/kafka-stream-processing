package com.isidora.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class UniqueUsersCounter {

    static public final class JsonSerde extends Serdes.WrapperSerde<JsonNode> {
        public JsonSerde() {
            super(new JsonSerializer(), new JsonDeserializer());
        }
    }

    public static void main(String[] args) {

        final String inputTopic = args[0];
        final String outputTopic = args[1];

        Properties streamsProperties = new Properties();
        // https://kafka.apache.org/20/documentation/#streamsconfigs
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe"); // check if this name needs to be different
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");    // assuming that the Kafka broker this application is talking to runs on local machine with port 9092
        streamsProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);  // to minimize the error
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<JsonNode> jsonSerde = new JsonSerde();
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        KStream<String, JsonNode> source = builder.stream(inputTopic, Consumed.with(Serdes.String(), jsonSerde));
        // this results in this:
        // key: null value: {"uid":"dd804e1c0499d5f4beb","ts":1468244590...
        // key: null value: {"uid":"03cb408c6a6b4a795a0","ts":1468244591...

        KStream<String, String> sourceIDs = source.mapValues((value) -> value.get("uid").toString()); // the only input data we need is uid
        // this results in this:
        // key: null value: "c947412ea2be8b34391"
        // key: null value: "b7e4ef7a91f18c0f3d1"


        KGroupedStream<String, String> groupedStream = sourceIDs.groupBy((key, value) -> value);
        // grouped is done so we get KGroupedStream object on which we can all windowing. we group by uid so we can get unique users (questionable logic since we may be losing this property during further transformations)

        TimeWindowedKStream<String, String> windowedStream = groupedStream.windowedBy(TimeWindows.of(Duration.ofMinutes(1)));
        // this is done to get partitions of input data by the minute (time used for this evaluation is the time the event entered the input stream)

        KTable<Windowed<String>, String> aggregated = windowedStream.aggregate(() -> "", (aggKey, newValue, aggValue) -> "");
        // aggregate only provides us with timestamp and uid in the key and gives us a KTable object

        KStream<String, String> mapTsId = aggregated.toStream().map((key, value) -> new KeyValue<>(key.window().startTime().toString(), value));
        // finally, we get a stream that looks like this:
        // key: timestamp value: uid

        KTable<String, Long> uniqueUsersPerMinute = mapTsId.groupByKey().count();
        // grouping by timestamp and counting the number of values associated with each key gives us the number of unique users per timestamp (minute)

        KStream<String, String> finalResult = uniqueUsersPerMinute.toStream().map((key, value) -> new KeyValue<>(key, key + " " + value));
        // value field is enriched with timestamp so timestamp can be visible when consuming the data from the output topic in kafka directly

        finalResult.to(outputTopic);

        final Topology topology = builder.build();
        System.out.println(topology.describe()); // to see what kind of topology is created


        final KafkaStreams streams = new KafkaStreams(topology, streamsProperties);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.cleanUp(); // https://docs.confluent.io/current/streams/developer-guide/app-reset-tool.html#step-2-reset-the-local-environments-of-your-application-instances
            streams.start(); // this starts the processing

            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}
