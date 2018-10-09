
package shenzhen.teamway.pdms.mq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;

public class MQClient implements Runnable {

    Logger log = LoggerFactory.getLogger(MQClient.class);

    private static MQClient mqClient = new MQClient();
    private LinkedList<MessageInboundHandler> handlers = new LinkedList<>();
    private static byte[] lock = new byte[0];

    private String brokerIP = null;
    private int brokerPort = 0;
    private String subscribeTopic = null;

    private Properties props = new Properties();
    private Producer<String, String> producer = null;

    public static MQClient instance() {
        return mqClient;
    }

    public void init(String brokerIP, int brokerPort) {

        log.info("初始化消息代理客户端。");

        this.brokerIP = brokerIP;
        this.brokerPort = brokerPort;

        props.clear();
        props.put("bootstrap.servers", brokerIP + ':' + brokerPort);
        props.put("acks", "1");
        props.put("retry.backoff.ms", 500);
        props.put("retries", 2);
        props.put("batch.size", 16348);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }

    public void subscribe(String topic) {

        log.info("添加订阅消息主题: " + topic);

        synchronized (lock) {
            subscribeTopic = topic;
        }
    }

    public boolean send(String topic, String key, String message) {

        log.info("发送消息到消息代理，主题: " + topic);

        if (producer == null || brokerPort == 0 || brokerIP == null) {
            log.info("发送消息到消息代理失败，初始化失败。");
            return false;
        }

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
            producer.send(record).get();
        } catch (Exception e) {
            log.info("发送消息到消息代理失败。");
            return false;
        }

        return true;
    }

    public void addHandle(MessageInboundHandler handler) {
        synchronized (lock) {
            handlers.add(handler);
        }
    }

    public void run() {
        props.put("group.id", "shenzhen.teamway.nettyServer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        log.info("消息代理模块启动。");

        String topic = null;
        do {
            synchronized (lock) {
                if (subscribeTopic != null) {
                    topic = subscribeTopic;
                    break;
                }
            }

            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {

            }
        } while (true);

        log.info("订阅消息。");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(500);
                for (ConsumerRecord<String, String> record : records) {
                    log.info("从代理模块收到订阅消息。");
                    for (MessageInboundHandler handler : handlers) {
                        handler.messageReceived(record.value());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
