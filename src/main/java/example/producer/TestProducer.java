package example.producer;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;


public class TestProducer {

    public static void main(String[] args) {
        long events = 10l; //Long.parseLong(args[0]);
        Random rnd = new Random();

        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:9092,127.0.0.1:9093");
        props.put("serializer.class", "kafka.serializer.StringEncoder"); //默认字符串编码消息
        props.put("partitioner.class", "example.producer.CustomizePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            String ip = "127.0.0.1:909" + (rnd.nextInt(2)+2);
            String msg = runtime + ",www.example.com," + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
            producer.send(data);
        }
        producer.close();

    }
}
