package com.test.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

//想使用算子状态必须配合实现CheckpointedFunction接口
//算子状态在每个运行子任务之间不是共享的！每一个算子都维护自己的状态！
@Slf4j
public class BroadCastStateSource extends RichSourceFunction<String> implements CheckpointedFunction {

    private transient BroadcastState integerValueState;
    private volatile boolean isRunning = true;

    private volatile int offset = 0;

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //快照时更新算子状态
        log.info("snapshot source map {}", offset);
        integerValueState.put("key", offset);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        //初始化描述符
        MapStateDescriptor sourceState = new MapStateDescriptor(
                "sourceState", StringSerializer.INSTANCE,
                IntSerializer.INSTANCE
        );
        //可通过如下方式是在过期时间：sourceState.enableTimeToLive();
        integerValueState = functionInitializationContext.getOperatorStateStore().getBroadcastState(sourceState);
        //算子状态还可以通过如下方式获得List类型的状态
//        functionInitializationContext.getOperatorStateStore().getUnionListState()
//        functionInitializationContext.getOperatorStateStore().getListState()
        Integer key = (Integer) integerValueState.get("key");
        if (null != key) {
            log.info("init BroadCastStateSource offset {}", key);
            //通过状态初始化offset
            offset = key;
        }
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning) {
            //更新offset
            offset += 1;
            log.info("生成" + String.valueOf(offset));
            sourceContext.collect(String.valueOf(offset));
            Thread.sleep(1000);  // 每隔一段时间发送一条测试数据  要pressure
        }
    }

    @Override
    public void cancel() {
        isRunning = Boolean.FALSE;
    }
}
