package com.test.map;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.ArrayList;
import java.util.Map;

@Slf4j
public class TestOperatorStateSpanBroadCastMap implements MapFunction<String, String>, CheckpointedFunction {


    private transient BroadcastState broadcastState;

    @Override
    public String map(String str) throws Exception {
        broadcastState.put(str, str);
        ArrayList<String> strings = new ArrayList<>();
        broadcastState.entries().forEach(
                a -> {
                    strings.add(String.valueOf(((Map.Entry) a).getKey()));
                }
        );
        log.info("broadcast state  {}", String.join(",", strings));
        return String.valueOf(str).split("_")[0];
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        ArrayList<String> strings = new ArrayList<>();
        broadcastState.entries().forEach(
                a -> {
                    strings.add(String.valueOf(((Map.Entry) a).getKey()));
                }
        );
        log.info("snapshot broadcast state  {}", String.join(",", strings));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //初始化描述符
        MapStateDescriptor sourceState = new MapStateDescriptor(
                "BroadCastMapState", StringSerializer.INSTANCE,
                StringSerializer.INSTANCE
        );
        broadcastState = context.getOperatorStateStore().getBroadcastState(sourceState);
        ArrayList<String> strings = new ArrayList<>();
        broadcastState.entries().forEach(
                a -> {
                    strings.add(String.valueOf(((Map.Entry) a).getKey()));
                }
        );
        log.info("init broadcast state  {}", String.join(",", strings));
    }
}
