package org.feiware.iot.elastic;


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by feiware on 1/1/17.
 */

public class FlinkElastic {

    public static void writeToElastic(DataStream<String> input) {

        Map<String, String> config = new HashMap<String, String>();

        // this instructs the sink to emit after every element, otherwise they would be buffered
        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, "1");

        try {
            // add elastic hosts on startup
            List<InetSocketAddress> transports = new ArrayList<>();

            // port is 9300 for elastic transportClient
            transports.add(new InetSocketAddress("127.0.0.1",9300));

            ElasticsearchSinkFunction<String> indexLog = new ElasticsearchSinkFunction<String>() {
                public IndexRequest createIndexRequest(String element) {
                    String[] logContent = element.trim().split(",");
                    Map<String, String> esJson = new HashMap<>();
                    esJson.put("id", logContent[0]);
                    esJson.put("info", logContent[2]);

                    return Requests
                            .indexRequest()
                            .index("iot-test")
                            .type("iot-log")
                            .source(esJson);
                }
                @Override
                public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                    requestIndexer.add(createIndexRequest(s));

                }
            };

            ElasticsearchSink esSink = new ElasticsearchSink(config, transports, indexLog);
            input.addSink(esSink);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}