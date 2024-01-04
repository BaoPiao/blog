package com.test.Utils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvUtils {
    public static StreamExecutionEnvironment ofEnv(String checkPointPath, Configuration configuration) {
        //设置web ui端口
        if (null == configuration) {
            configuration = new Configuration();
        }
        configuration.setString("rest.port", "18081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        //注意这里再次设置，会导致CheckpointingOptions.MAX_RETAINED_CHECKPOINTS设置失效
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(checkPointPath);
        //开启checkpoint
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100L));
        env.setParallelism(1);
        return env;
    }
}
