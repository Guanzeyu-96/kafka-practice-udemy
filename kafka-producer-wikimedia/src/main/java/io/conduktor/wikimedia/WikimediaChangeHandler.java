package io.conduktor.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WikimediaChangeHandler implements BackgroundEventHandler {
  // 不是必须override所有方法
  // 需要把kafka producer instance传进来，因为在onMessage回调中，需要调用producer.send()方法去produce

  KafkaProducer<String, String> kafkaProducer;
  String topic;
  private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

  // 构造器
  public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
    this.kafkaProducer = kafkaProducer;
    this.topic = topic;
  }

  @Override
  public void onOpen() {
    // steam打开的时候执行
    // nothing
  }

  @Override
  public void onClosed() {
    // stream关闭执行
    kafkaProducer.close();
  }

  @Override
  public void onMessage(String s, MessageEvent messageEvent) {
    log.info(messageEvent.getData());

    // 每当从stream得到数据的时候，调用kafka producer异步方法发送到kafka client（指定topic）
    kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
  }

  @Override
  public void onComment(String s) {
    // nothing
  }

  @Override
  public void onError(Throwable throwable) {
    log.error("Error in Stream Reading", throwable);
  }
}
