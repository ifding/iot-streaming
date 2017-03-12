package org.feiware.iot.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import com.esotericsoftware.yamlbeans.YamlReader;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.feiware.iot.utils.IoTDataConf;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Simulate to produce the IoT data.
 * IoT data event producer class which uses Kafka producer for events.
 *
 * Created by feiware on 12/28/16.
 */

public class Producer {

    private static final String CONFIG_FILE_PATH = "/config.yml";

    private static KafkaProducer<String,String> producer;

    public static void main(String[] args) throws IOException {

        init();

        YamlReader reader = new YamlReader(
                new InputStreamReader(Producer.class.getResourceAsStream(CONFIG_FILE_PATH))
        );
        final IoTDataConf ioTDataConf = reader.read(IoTDataConf.class);

        for (int i = 1; i < 61; i++) {
            System.out.println("Ready to send message:");
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            publishMessage(ioTDataConf.TOPIC, createData());
        }
        producer.close();
    }

    private static void init() throws IOException {

        InputStream inputStream = Producer.class.getResourceAsStream("/producer.properties");
        Properties properties = new Properties();
        properties.load(inputStream);
        producer = new KafkaProducer<String, String>(properties);
    }

    /**
     *  Method generates random IoT data in JSON with below format.
     *  {"dataId":"6f3a4407-5501-48cc-a874-b8d75d186452","dataTime":"2017-03-10 09:36:05","dataType":"CPU",
     *  "dataRank":8,"dataValue":2.5017078077089483}
     *
     */

    private static String createData() {

        List<String> dataTypeList = Arrays.asList(new String[]{"CPU", "Memory","IO", "Network", "Power"});

        try {
            Random random = new Random();
            String dataId = UUID.randomUUID().toString();
            double dataValue = 1.0 + (100.0 - 1.0) * random.nextDouble();
            String dataType = dataTypeList.get(random.nextInt(5));
            int dataRank = random.nextInt(10-1) + 1;
            Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dataTime = format.format(new Date());

            JSONObject data = new JSONObject();
            data.put("dataTime", dataTime);
            data.put("dataId", dataId);
            data.put("dataValue", dataValue);
            data.put("dataType", dataType);
            data.put("dataRank", dataRank);
            return data.toString();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    private static void publishMessage(String topic, String message) {

        System.out.println(message);
        producer.send(new ProducerRecord<String, String>(topic, message));
    }
}