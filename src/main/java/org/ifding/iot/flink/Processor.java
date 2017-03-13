package org.ifding.iot.flink;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.ifding.iot.cassandra.KafkaCassandra;
import org.ifding.iot.data.IoTData;
import org.ifding.iot.data.IoTDataKey;
import org.ifding.iot.elastic.FlinkElastic;
import org.ifding.iot.utils.IoTDataUtil;
import org.ifding.iot.utils.IoTDataUniqueState;


/**
 * Created by ifding on 12/29/16.
 */

public class Processor {

    public static void main(String[] args) throws Exception {

        // setup stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<ObjectNode> sourceStream = KafkaFlink.readFromKafka(env);

        // process the duplicated data
        SingleOutputStreamOperator<IoTData> filteredIoTDataStream = filterProcess(sourceStream);

        // process the total data
        processTotalIoTData(filteredIoTDataStream);

        // process the window stream
        processWindowIoTData(filteredIoTDataStream);

        // convert IoTData SingleOutStreamOperator to String DataStream
        DataStream<String> stringDataStream = filteredIoTDataStream
                .map(new MapFunction<IoTData, String>() {
                    @Override
                    public String map(IoTData ioTData) throws Exception {
                        return ioTData.toString();
                    }
                });

        // from flink write to kafka
        KafkaFlink.writeToKafka(stringDataStream);

        // from kafka write to Cassandra
        KafkaCassandra.writeToCassandra();

        // it also can write to Elastic
        FlinkElastic.writeToElastic(stringDataStream);

        env.execute("Running Flink Processor to process data");
    }

    public static SingleOutputStreamOperator<IoTData> filterProcess(DataStreamSource<ObjectNode> stream ) throws Exception {

        // source and map OdjectNode to IoTData
        SingleOutputStreamOperator<IoTData> nonFilteredIoTDataStream = stream
                .map(IoTDataUtil.streamToIoTDataMap());


        // partition stream by data Id for deduplication.
        SingleOutputStreamOperator<IoTData> reduceKeyedStream = nonFilteredIoTDataStream
                .keyBy(IoTDataUtil.ioTDataKeySelector())
                .reduce((a, b) -> a);

        // passing Keyed stream  through a stateful mapper to deduplicate from the same data Id.
        SingleOutputStreamOperator<Tuple2<IoTData,Boolean>> dedupedKeyedStream = reduceKeyedStream
                .keyBy(IoTDataUtil.ioTDataKeySelector())
                .map(new IoTDataUniqueState());

        SingleOutputStreamOperator<Tuple2<IoTData, Boolean>> uniqueDataStream = dedupedKeyedStream
                .filter(p -> p.f1);

        SingleOutputStreamOperator<IoTData> filteredIoTDataStream = uniqueDataStream
                .map(new MapFunction<Tuple2<IoTData, Boolean>, IoTData>() {
                    @Override
                    public IoTData map(Tuple2<IoTData, Boolean> ioTDataBooleanTuple2) throws Exception {
                        return ioTDataBooleanTuple2.f0;
                    }
                });

        return filteredIoTDataStream;
    }

    public static void processTotalIoTData(SingleOutputStreamOperator<IoTData> filteredIoTDataStream) {
        SingleOutputStreamOperator<Tuple2<IoTDataKey, Long>> groupedStream =
                getKeyedStreamForCounting(filteredIoTDataStream)
                .reduce(new ReduceFunction<Tuple2<IoTDataKey, Long>>() {
                    @Override
                    public Tuple2<IoTDataKey, Long> reduce(Tuple2<IoTDataKey, Long> t1, Tuple2<IoTDataKey, Long> t2) throws Exception {
                        return new Tuple2<IoTDataKey, Long>(t1.f0, t1.f1 + t2.f1);
                    }
                });

        groupedStream.map(new MapFunction<Tuple2<IoTDataKey, Long>,Tuple2<IoTDataKey, Long>>() {
            @Override
            public Tuple2<IoTDataKey, Long> map(Tuple2<IoTDataKey, Long> input) throws Exception {
                System.out.println("IoTDataKey: " + input.f0.toString() + " Count: " + input.f1.toString());
                return input;
            }
        })
        .setParallelism(4);
    }

    public static void processWindowIoTData(SingleOutputStreamOperator<IoTData> filteredIoTDataStream) {

        // build a tumble windowed keyedStream of 10 sec.
        WindowedStream<Tuple2<IoTDataKey, Long>, IoTDataKey, TimeWindow> tumbleWindowedStream =
                getKeyedStreamForCounting(filteredIoTDataStream)
                        .timeWindow(Time.seconds(10));

        SingleOutputStreamOperator<Tuple2<IoTDataKey, Long>> reduceWindowStream =
                tumbleWindowedStream
                        .reduce(new ReduceFunction<Tuple2<IoTDataKey, Long>>() {
                            @Override
                            public Tuple2<IoTDataKey, Long> reduce(Tuple2<IoTDataKey, Long> t1, Tuple2<IoTDataKey, Long> t2) throws Exception {
                                return new Tuple2<IoTDataKey, Long>(t1.f0, t1.f1.longValue() + t1.f1.longValue());
                            }
                        });

        reduceWindowStream.map(new MapFunction<Tuple2<IoTDataKey,Long>, Tuple2<IoTDataKey,Long>>() {
            @Override
            public Tuple2<IoTDataKey,Long> map(Tuple2<IoTDataKey,Long> input) throws Exception {
                System.out.println("Stream Per Window for IoTData Key: " + input.f0.toString() + " Count: " + input.f1.toString());
                return input;
            }
        });
    }

    private static KeyedStream<Tuple2<IoTDataKey, Long>,IoTDataKey> getKeyedStreamForCounting(
            SingleOutputStreamOperator<IoTData> filteredIoTDataStream) {
        return filteredIoTDataStream
                .map(new MapFunction<IoTData, Tuple2<IoTDataKey, Long>>() {
                    @Override
                    public Tuple2<IoTDataKey, Long> map(IoTData ioTData) throws Exception {
                        return new Tuple2<IoTDataKey, Long>(
                                new IoTDataKey(ioTData.getDataType(),ioTData.getDataValue()), new Long(1));
                    }
                })
                .keyBy(IoTDataUtil.getIoTDataKey());
    }
}
