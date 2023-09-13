package com.test.operator.state.list.union;

import com.test.Utils.EnvUtils;
import com.test.Utils.PathUtils;
import com.test.map.TestOperatorStateSpanListMap;
import com.test.map.TestOperatorStateSpanUnionListMap;
import com.test.source.BroadCastStateSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import static com.test.Utils.ExecuteUtils.execute;

@Slf4j
public class TestUnionListState {
    //07:24:31.709 [TestOperatorStateSpanUnionListMap (1/2)#0] INFO - snapshot unionListState  1,3
    //07:24:31.709 [TestOperatorStateSpanUnionListMap (2/2)#0] INFO - snapshot unionListState  2,4
    private static String P2_UNION_LIST_STATE = "/ck/p2UnionStateList/chk-4";

    @Test
    public void generate2PUnionListState() throws Exception {
        generateUnionListState(2);
    }

    @Test
    public void executorWith2PUnionListState() throws Exception {
        //初始化后
        //07:39:57.435 [TestOperatorStateSpanUnionListMap (2/2)#0] INFO init unionListState  2,4,1,3
        //07:39:57.435 [TestOperatorStateSpanUnionListMap (1/2)#0] INFO init unionListState  2,4,1,3
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        String savePointPath = checkPointPath + P2_UNION_LIST_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(2, env);
    }

    @Test
    public void executorWith3PUnionListState() throws Exception {
        //07:40:52.740 [TestOperatorStateSpanUnionListMap (1/3)#0] INFO - init unionListState  2,4,1,3
        //07:40:52.740 [TestOperatorStateSpanUnionListMap (2/3)#0] INFO - init unionListState  2,4,1,3
        //07:40:52.740 [TestOperatorStateSpanUnionListMap (3/3)#0] INFO - init unionListState  2,4,1,3
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        String savePointPath = checkPointPath + P2_UNION_LIST_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(3, env);
    }


    private void generateUnionListState(Integer parallelism) throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(parallelism, env);
    }

    private void setOperatorAndExecute(Integer parallelism, StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> source = env.addSource(new BroadCastStateSource());
        //注意这里不指定uid，会根据输入和输出来确定uid（如果输入和输出是可以chainAble则会使用边来一起生成）
        //而这里会改变并行度，可能会导致uid发生变化，所有一定要指定uid
        source.name("operator_state_source").uid("operator_state_source");
        source.map(new TestOperatorStateSpanUnionListMap())
                .name("TestOperatorStateSpanUnionListMap").uid("TestOperatorStateSpanUnionListMap")
                .setParallelism(parallelism);
        execute(env);
    }

}
