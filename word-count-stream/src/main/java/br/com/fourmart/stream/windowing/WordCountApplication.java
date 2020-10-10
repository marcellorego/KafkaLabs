package br.com.fourmart.stream.windowing;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

public class WordCountApplication {

    static final Logger log = LoggerFactory.getLogger(WordCountApplication.class);
    static final Pattern WORD_PATTERN = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
    static final String inputTopic = "streams-plaintext-input";
    static final String outputTopic = "streams-wordcount-output";

    private final Properties prop;

    public WordCountApplication(Properties prop) {
        this.prop = prop;
    }

    private Properties getStreamsConfiguration(Properties properties) {

        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-lambda-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        // Consumer configuration
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // streamsConfiguration.putAll(properties);
        return streamsConfiguration;
    }

    private Topology createWordCountTopology() {

        final StreamsBuilder builder = new StreamsBuilder();

        // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).  The default key and value serdes will be used.
        final KStream<String, String> textLines = builder.stream(inputTopic);

        final KTable<String, Long> wordCounts = textLines
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                // Split each text line, by whitespace, into words.  The text lines are the record
                // values, i.e. we can ignore whatever data is in the record keys and thus invoke
                // `flatMapValues()` instead of the more generic `flatMap()`.
                .flatMapValues(value -> Arrays.asList(WORD_PATTERN.split(value)))
                .selectKey((key, value) -> value)
                // Group the split data by word so that we can subsequently count the occurrences per word.
                // This step re-keys (re-partitions) the input data, with the new record key being the words.
                // Note: No need to specify explicit serdes because the resulting key and value types
                // (String and String) match the application's default serdes.
                .groupByKey()
                // Count the occurrences of each word (record key).
                .count(Named.as("wordcount"));

        // Write the `KTable<String, Long>` to the output topic.
        wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    private void start() {

        // Configure the Streams application.
        final Properties streamsConfiguration = getStreamsConfiguration(this.prop);

        // Define the processing topology of the Streams application.
        final Topology topology = createWordCountTopology();
        log.info(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);


        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }

    public static void main(final String[] args) throws Exception {

        final Properties prop = new Properties();

        if (args != null && args.length > 0) {

            final String bootstrapProperties = args[0];

            try (InputStream input = new FileInputStream(bootstrapProperties)) {

                // load a properties file
                prop.load(input);

            } catch (IOException ex) {
                log.error(ex.getMessage());
                System.exit(1);
            }
        }

        new WordCountApplication(prop).start();
    }
}
