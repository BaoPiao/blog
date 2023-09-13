package com.test.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

@Slf4j
public class BroadcastProcessFunction extends org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction<String, Tuple2<String, Integer>, String> {
    private final MapStateDescriptor<String, Integer> broadcastStateDescriptor;

    public BroadcastProcessFunction(MapStateDescriptor<String, Integer> broadcastStateDescriptor) {
        this.broadcastStateDescriptor = broadcastStateDescriptor;
    }

    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> out) throws Exception {
        ReadOnlyBroadcastState<String, Integer> broadcastState = readOnlyContext.getBroadcastState(broadcastStateDescriptor);
        Integer key = broadcastState.get("key");
        log.info("broad cast state is {}", key);
        out.collect(s + "_" + key);
    }

    @Override
    public void processBroadcastElement(Tuple2<String, Integer> stringIntegerTuple2, Context context, Collector<String> collector) throws Exception {
        BroadcastState<String, Integer> broadcastState = context.getBroadcastState(broadcastStateDescriptor);
        Integer key = broadcastState.get("key");
        log.info("broad cast state change {} to {}", key, stringIntegerTuple2.f1);
        broadcastState.put("key", stringIntegerTuple2.f1);
    }


}
