package com.test.operator.state.key;

import com.test.Utils.EnvUtils;
import com.test.Utils.PathUtils;
import com.test.map.KeyStateMap;
import com.test.source.BroadCastStateSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import static com.test.Utils.ExecuteUtils.execute;

@Slf4j
public class TestKeyState {
    //snapshot key value state 2
    //snapshot key value state 3
    static final String P1_KEY_STATE = "/ck/keyState1P/chk-7";
    ;

    @Test
    public void generateP1KeyState() throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(1, env);
    }

    @Test
    public void useP1KeyStateUseToP1() throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        String savePointPath = checkPointPath + P1_KEY_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(1, env);
    }

    @Test
    public void useP1KeyStateToP2() throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        String savePointPath = checkPointPath + P1_KEY_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(2, env);
    }

    private void setOperatorAndExecute(Integer parallelism, StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> source = env.addSource(new BroadCastStateSource());
        //注意这里不指定uid，会根据输入和输出来确定uid（如果输入和输出是可以chainAble则会使用边来一起生成）
        //而这里会改变并行度，可能会导致uid发生变化，所有一定要指定uid
        source.name("operator_state_source").uid("operator_state_source");
        source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return String.valueOf(Integer.valueOf(s) % 2);
            }
        }).process(new KeyStateMap())
                .name("KeyStateMap").uid("KeyStateMap")
                .setParallelism(parallelism);
        execute(env);
    }

}
