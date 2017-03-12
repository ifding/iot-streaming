package org.feiware.iot.utils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.feiware.iot.data.IoTData;


/**
 * the class of iot data unique state
 *
 * Created by feiware on 12/29/16.
 */

public class IoTDataUniqueState extends RichMapFunction<IoTData, Tuple2<IoTData,Boolean>> {

    /**
     * the valueState handle, the first field is the count, the second field is a running sum.
     */
    private transient ListState<String> uniqueDataId;

    @Override
    public Tuple2<IoTData,Boolean> map(IoTData ioTData) throws Exception {
        if (!Iterables.contains(uniqueDataId.get(),ioTData.getDataId())) {
            uniqueDataId.add(ioTData.getDataId());
            return new Tuple2<>(ioTData,true);
        }
        return new Tuple2<>(ioTData,false);
    }

    @Override
    public void open(Configuration config) {
        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<String>("unique_data", String.class); // default value of the state
        uniqueDataId = getRuntimeContext().getListState(descriptor);
    }
}
