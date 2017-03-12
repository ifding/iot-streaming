package org.feiware.iot.flink;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.feiware.iot.utils.IoTDataConf;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by feiware on 12/29/16.
 */

public class KafkaFlink {

    public static DataStreamSource<ObjectNode> readFromKafka(StreamExecutionEnvironment env) throws Exception {

        // use the measurement timestamp of the event
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        YamlReader reader = new YamlReader(
                new InputStreamReader(Processor.class.getResourceAsStream("/config.yml"))
        );

        final IoTDataConf ioTDataConf = reader.read(IoTDataConf.class);

        // construct window with default size of 1 minute and slide of 10 seconds
        Time window = Time.seconds(ioTDataConf.WINDOW);
        Time slide = Time.seconds(ioTDataConf.SLIDE);

        Properties properties = null;

        try (InputStream props = Resources.getResource("consumer.properties").openStream()){

            properties = new Properties();
            properties.load(props);
        }

        DataStreamSource<ObjectNode> sourceStream = env.addSource(
                new FlinkKafkaConsumer010<ObjectNode>(ioTDataConf.TOPIC,
                        new JSONDeserializationSchema(),
                        properties));

        return sourceStream;
    }

    public static void writeToKafka(DataStream<String> inStream) throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                inStream,
                "iot-log",
                new SimpleStringSchema(),
                properties);

    }
}
