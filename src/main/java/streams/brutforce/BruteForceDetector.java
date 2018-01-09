package streams.brutforce;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BruteForceDetector implements Runnable{

    private final String inputTopic;
    private final int numOfMaxLoggingAttemptsPerTimeInterval;
    private final long timeInterval; // specified in seconds

    public BruteForceDetector(String inputTopic, int numOfMaxLoggingAttemptsPerTimeInterval, int timeInterval) {
        this.inputTopic = inputTopic;
        this.numOfMaxLoggingAttemptsPerTimeInterval = numOfMaxLoggingAttemptsPerTimeInterval;
        this.timeInterval = timeInterval;
    }

    @Override
    public void run() {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "bruteforce-detector");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> loggingAttempts = builder.stream(inputTopic);


        KTable<Windowed<String>, Long> anomalousLogging = loggingAttempts
                .groupByKey()
                //.windowedBy(JoinWindows.of(timeInterval * 1000L))
                .windowedBy(TimeWindows.of(timeInterval * 1000L).advanceBy(100L))
                .count()
                .filter((login, count) -> count >= numOfMaxLoggingAttemptsPerTimeInterval);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStream<String, Long> anomalousLoggingForConsole = anomalousLogging
                .toStream()
                // removing empty windows
                .filter((windowedLogin, count) -> count != null)
                .map((windowedLogin, count) -> new KeyValue<>(windowedLogin.toString(), count));

        // write to the result topic
        anomalousLoggingForConsole.to("streams-wordcount-output2", Produced.with(stringSerde, longSerde));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
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
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}
