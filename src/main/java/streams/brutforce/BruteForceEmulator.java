package streams.brutforce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class BruteForceEmulator implements Runnable{

    private final String topic;
    private final int throughput; // meassured in logging attemets per second

    public BruteForceEmulator(String topic, int throughput) {
        this.topic = topic;
        this.throughput = throughput;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        while (true) {
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                try {
                    Thread.sleep(1000 / throughput);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                producer.send(new ProducerRecord<String, String>(topic, "login1", new User("login1","pass" + i).toString()));
            }
        }

        //producer.close();
    }
}
