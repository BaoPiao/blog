package com.test.source;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleSource implements SourceFunction<Tuple2<String, Integer>> {
    private volatile Boolean isRunning = Boolean.TRUE;

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
        while (isRunning) {

            Tuple2<String, Integer> data = new Tuple2("key", Long.valueOf(System.currentTimeMillis()).intValue());
            sourceContext.collect(data);

            Thread.sleep(10000);  // 每隔一段时间发送一条测试数据  要pressure
        }
    }

    @Override
    public void cancel() {
        isRunning = Boolean.FALSE;
    }
}
