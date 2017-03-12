package org.feiware.iot.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;


/**
 * Created by feiware on 12/30/16.
 */

public class KafkaCassandra {

    private static String HOST = "localhost";

    private static Logger logger = LoggerFactory.getLogger(KafkaCassandra.class);

    final static SimpleDateFormat dft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");

    public static void writeToCassandra() throws Exception {

        Cluster cluster = Cluster.builder().addContactPoints(HOST).build();
        Session session = cluster.connect();

        Initialization.init(session);

        Properties props = new Properties();

        props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("group.id", "my-group");
        props.put("zookeeper.session.timeout.ms", 6000);
        props.put("auto.commit.interval.ms", 4000);
        props.put("auto.offset.reset", "smallest");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("enable.auto.commit", true);
        props.put("session.timeout.ms", 10000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("iot-log"));

        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                try {

                    ConsumerRecords<String, String> records = consumer.poll(1000);

                    for (ConsumerRecord<String, String> record : records) {

                        logger.info(String.format("offset = %d, key = %s, value = %s",
                                record.offset(), record.key(), record.value()));
                        String[] arr = record.value().split(",");
                        session.execute("INSERT INTO tiger.iotdata(datatime, dataid, datavalue, datatype, datarank) VALUES (?,?,?,?,?);",
                                dft.parse(arr[0]),arr[1],Double.parseDouble(arr[2]),arr[3],Integer.parseInt(arr[4]));
                    }

                } finally {

                }
            }
        });

    }
}
