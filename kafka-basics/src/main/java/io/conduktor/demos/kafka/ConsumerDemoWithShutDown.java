package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutDown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "optimum-parakeet-12027-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"b3B0aW11bS1wYXJha2VldC0xMjAyNyTRv3wqUilg-oHMFrT3auLSbtcdHsgVm2A\" password=\"OWQzNTU1ZmMtNDVlOS00YjM1LTg5OGYtMWFkODI5YmFjNGRh\";");

        // create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread
        // 获取main的进程instance
        final Thread mainThread = Thread.currentThread();

        // add a shutdown hook
        // shutdownHook的参数是一个thread instance，是定义主线程结束时执行操作的线程吗？？
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                // wakeup的作用是，在下次执行consumer.poll的时候，会抛出wakeup exception
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                // mainThread.join()的作用是等待主线程结束吗
                try {
                    mainThread.join();
                    // 这里的catch具体捕获的是什么exception？
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe to a topic
            consumer.subscribe(List.of(topic));

            // poll for data
            while (true) {
                log.info("Polling");

                // 如果consumer立即得到了message，那么会返回records；如果没有，会等待一秒。
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // 对于records里的每个record
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            // WakeupException是预期中的，所以只是打印info
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in consumer", e);
        } finally {
            consumer.close();   // close consumer同样会commit offsets
            log.info("Consumer is gracefully shut down");
        }

    }
}
