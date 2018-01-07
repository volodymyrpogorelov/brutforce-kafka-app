package streams.brutforce;

public class BrutForceDetection {

    public static void main(String[] args){

        String topic = "my-topic";
        int loggingAttemptsPerSecond = 2; // logging attempts per second


        Thread tprocducer = new Thread(new BruteForceEmulator(topic, loggingAttemptsPerSecond));
        Thread tconsumer = new Thread(new ColsoleConsumer(topic));

        tprocducer.start();
        tconsumer.start();

        try {
            tprocducer.join();
            tconsumer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}
