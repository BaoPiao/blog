package com.test.map;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class KeyStateMap extends KeyedProcessFunction<String, String, String> implements CheckpointedFunction {
    //值状态
    private transient ValueState<Integer> intState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("lastDataValue", Integer.class);

        //根据描述符初始化intState值
        intState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(String value, Context context, Collector<String> collector) throws Exception {
        if (null == intState.value()) {
            intState.update(0);
            log.info("init state to {}", 0);
        }
        log.info("value is {}, before update state is {}", value, intState.value());
        //更新值状态
        intState.update(Integer.valueOf(value));
        log.info("value is {} after update state is {}", value, intState.value());
//        if (Integer.valueOf(value) % 30 == 0) {
//            try {
//                int i = 1 / 0;
//            } catch (Exception e) {
//                log.error("异常", e.getStackTrace());
//                throw e;
//            }
//        }
        collector.collect(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("snapshot key value state {} ", intState.value());

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
