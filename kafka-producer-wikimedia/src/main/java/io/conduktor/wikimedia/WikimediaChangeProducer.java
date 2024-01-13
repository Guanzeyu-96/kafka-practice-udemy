package io.conduktor.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangeProducer {
  public static void main(String[] args) throws InterruptedException {
    String bootstrapServers = "optimum-parakeet-12027-eu2-kafka.upstash.io:9092";

    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
    properties.setProperty("security.protocol", "SASL_SSL");
    properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"b3B0aW11bS1wYXJha2VldC0xMjAyNyTRv3wqUilg-oHMFrT3auLSbtcdHsgVm2A\" password=\"OWQzNTU1ZmMtNDVlOS00YjM1LTg5OGYtMWFkODI5YmFjNGRh\";");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    String topic = "wikimedia.recentchange";

    // 来自okhttp-eventsource库，作用是让我们处理stream中的event，发送给producer。4.0版本移除了EventHandler，现在得用BackgroundEventHandler
    // 需要实现BackgroundEventHandler这个类
    BackgroundEventHandler backgroundEventHandler = new WikimediaChangeHandler(producer, topic);
    String url = "https://stream.wikimedia.org/v2/stream/recentchange";
    // okhttp-eventsource知识，用handler和指定url去构造一个build工具，从而build出event source
    BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(backgroundEventHandler, new EventSource.Builder(URI.create(url)));
    BackgroundEventSource eventSource = builder.build();

    // start the producer in another thread
    eventSource.start();

    // 如果这里什么也不做，主线程会直接结束，从而eventSource线程也结束。所以必须让主线程不立即结束
    // 意思是持续produce 10分钟，然后停止
    TimeUnit.MINUTES.sleep(10);

  }
}
