package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        // create Producer Properties
        // connect to upstash cluster, 配置和playground.config一致
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "optimum-parakeet-12027-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"b3B0aW11bS1wYXJha2VldC0xMjAyNyTRv3wqUilg-oHMFrT3auLSbtcdHsgVm2A\" password=\"OWQzNTU1ZmMtNDVlOS00YjM1LTg5OGYtMWFkODI5YmFjNGRh\";");

        // set producer properties key value的序列化方式
        // 含义是，producer产生的键值对类型都为string，也会用kafka提供的serializer序列化成byte
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a Producer Record
        // 产生一条message。指定message发送的topic name，value是message内容。没有提供key的话则为null，没有提供partition的话由kafka loadbalancer指定
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        // send data
        // 发送数据是异步的，所以如果不执行flush/close，程序会在发送数据前直接中止
        producer.send(producerRecord);

        // flush and close the producer
        // 阻塞当前线程，直至所有消息刷新至kafka cluster。用于在kafka程序终止前执行，防止message丢失
        producer.flush();

        // 其实直接调用close也会执行flush，flush不需要特别执行，但这个API是存在的
        producer.close();
    }
}
