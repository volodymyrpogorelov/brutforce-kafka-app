package streams.brutforce;

public class BrutForceDetection {

    public static void main(String[] args){

        String topic = "my-topic";
        int loggingAttemptsPerSecond = 10; // logging attempts per second


        Thread tprocducer = new Thread(new BruteForceEmulator(topic, loggingAttemptsPerSecond));
        Thread tconsumer = new Thread(new ColsoleConsumer(topic));
        Thread tdetector = new Thread(new BruteForceDetector("my-topic", 2, 1));

        tprocducer.start();
        tconsumer.start();
        tdetector.start();

        try {
            tprocducer.join();
            tconsumer.join();
            tdetector.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}
