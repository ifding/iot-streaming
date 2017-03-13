package org.ifding.iot.kafka;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * Created by ifding on 3/5/17.
 */

public class Consumer {

    @SuppressWarnings("resource")
    public static void main(String[] args) throws IOException {

        // kafka consumer
        KafkaConsumer<String, String> consumer;
        try(InputStream inputs = Resources.getResource("consumer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(inputs);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            consumer = new KafkaConsumer<String, String>(properties);
        }
        consumer.subscribe(Arrays.asList("iot-streaming","others"));

        // infinite loop
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record : records) {
                switch (record.topic()) {
                    case "iot-streaming":
                        JSONObject msg = new JSONObject(record.value());
                        System.out.println("Received Message: " + msg.toString());
                        break;
                    case "others":
                        break;
                    default:
                        throw new IllegalStateException("You get message on topic: " + record.topic());
                }
            }
        }
    }

}
