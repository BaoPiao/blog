package com.test.map;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.ArrayList;

@Slf4j
public class TestOperatorStateSpanListMap implements MapFunction<String, String>, CheckpointedFunction {

    private transient ListState listState;

    @Override
    public String map(String str) throws Exception {

        listState.add(str);
        return String.valueOf(str).split("_")[0];
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        log.info("snapshot listState  {}", String.join(",", (ArrayList) listState.get()));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor mapListState = new ListStateDescriptor(
                "mapListState", StringSerializer.INSTANCE);
        listState = context.getOperatorStateStore().getListState(mapListState);
        log.info("init listState  {}", String.join(",", (ArrayList) listState.get()));
    }
}
