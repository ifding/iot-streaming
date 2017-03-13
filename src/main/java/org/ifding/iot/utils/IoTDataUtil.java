package org.ifding.iot.utils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.ifding.iot.data.IoTData;
import org.ifding.iot.data.IoTDataKey;

import java.io.Serializable;
import java.text.ParseException;
import java.util.TimeZone;

/**
 * Created by ifding on 12/29/16.
 */

public class IoTDataUtil implements Serializable {

    private static final FastDateFormat formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss",
            TimeZone.getTimeZone("EST"));

    public static MapFunction<ObjectNode, IoTData> streamToIoTDataMap() {
        return new MapFunction<ObjectNode, IoTData>() {
            @Override
            public IoTData map(ObjectNode jsonNodes) throws Exception {
                return IoTDataUtil.jsonNodeToIoTData(jsonNodes);
            }
        };
    }

    public static IoTData jsonNodeToIoTData(ObjectNode jsonNode) throws ParseException {
        return new IoTData(formatter.parse(jsonNode.get("dataTime").asText()),
                jsonNode.get("dataId").asText(),
                jsonNode.get("dataValue").asDouble(),
                jsonNode.get("dataType").asText(),
                jsonNode.get("dataRank").asInt());
    }

    public static KeySelector<IoTData, String> ioTDataKeySelector() {
        return new KeySelector<IoTData, String>() {
            @Override
            public String getKey(IoTData ioTData) throws Exception {
                return ioTData.getDataId();
            }
        };
    }

    public static KeySelector<Tuple2<IoTDataKey, Long>, IoTDataKey> getIoTDataKey() {
        return new KeySelector<Tuple2<IoTDataKey, Long>, IoTDataKey>() {
            @Override
            public IoTDataKey getKey(Tuple2<IoTDataKey, Long> input) throws Exception {
                return input.f0;
            }
        };
    }

}