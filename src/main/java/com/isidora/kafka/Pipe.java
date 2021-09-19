package com.isidora.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe"); // check if this name needs to be different
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");    // assuming that the Kafka broker this application is talking to runs on local machine with port 9092

        final StreamsBuilder builder = new StreamsBuilder();


        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        KStream<String, JsonNode> source = builder.stream("mytopic", Consumed.with(Serdes.String(), jsonSerde));

        source.map((key, value) -> KeyValue.pair("uid", value.get("uid")));
        source.groupBy((key, value) -> value.get("uid")).count(Named.as("count per user")); // .filter(count==1)

        // 1. reference - https://stackoverflow.com/questions/46607435/how-to-count-unique-users-in-a-fixed-time-window-in-a-kafka-stream-app
//        source.groupBy(/* put a KeyValueMapper that return the grouping key */)
//                .aggregate(... TimeWindow.of(TimeUnit.HOURS.toMillis(1))
//                .groupBy(/* put a KeyValueMapper that return the new grouping key */)
//                .count();

         // 2. reference (in Kotlin) - https://chrzaszcz.dev/2019/09/kafka-streams-store-basics/
//        source
//        .groupBy { _, value -> value.customerId }
//        .count(Materialized
//                .`as`<String, Long, KeyValueStore<Bytes, ByteArray>> ("TotalPizzaOrdersStore")
//                .withCachingDisabled())
//        .toStream()
//                .to("TotalPizzaOrders", Produced.with(Serdes.String(), Serdes.Long()));


        // my attempt
        source.groupBy((key, value) -> value.get("uid")).windowedBy(TimeWindows.of(Duration.ofMinutes(1))).count();
        // logic for extracting unique IDs needs to be added

        source.to("quickstart-events"); // some random topic I have
        final Topology topology = builder.build();
        System.out.println(topology.describe()); // to see what kind of topology is created


        final KafkaStreams streams = new KafkaStreams(topology, properties);
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
            streams.start(); // this starts the processing

            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}
