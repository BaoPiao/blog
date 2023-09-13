package com.test.operator.state.list.ordinary;

import com.test.Utils.EnvUtils;
import com.test.Utils.PathUtils;
import com.test.map.TestOperatorStateSpanListMap;
import com.test.source.BroadCastStateSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FlinkException;
import org.junit.Assert;
import org.junit.Test;

import static com.test.Utils.ExecuteUtils.execute;

@Slf4j
public class TestOrdinaryListState {
    //09:11:20.504 [TestOperatorStateSpanListMap (1/2)#0] INFO  c.t.map - snapshot listState  2,4,6
    //09:11:20.504 [TestOperatorStateSpanListMap (2/2)#0] INFO  c.t.map - snapshot listState  1,3,5
    private static String P2_LIST_STATE = "/ck/p2ListState/chk-6";

    //09:13:09.770 [TestOperatorStateSpanListMap (2/3)#0] INFO  c.t.map. - snapshot listState  1,6,8
    //09:13:09.770 [TestOperatorStateSpanListMap (3/3)#0] INFO  c.t.map. - snapshot listState  3,5,9
    //09:13:09.770 [TestOperatorStateSpanListMap (1/3)#0] INFO  c.t.map. - snapshot listState  2,4,7
    private static String P3_LIST_STATE_FROM_P2 = "/ck/p3ListStateFromP2/chk-9";


    @Test
    public void generate2PListState() throws Exception {
        generateListState(2);
    }


    //扩容
    @Test
    public void startWith2PListStateTo3P() throws Exception {
        executorWith2PListState(3);
        //生成 P3_LIST_STATE
    }

    //缩容
    @Test
    public void startWith2PListStateTo1P() throws Exception {
        executorWith2PListState(1);
    }

    //缩容
    @Test
    public void startWith3PListStateFromP2To2P() throws Exception {

        executorWith3PListStateFromP2(2);
    }

    //缩容
    @Test
    public void startWith3PListStateFromP2To1P() throws Exception {
        executorWith3PListStateFromP2(1);
    }


    private void generateListState(Integer parallelism) throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        DataStreamSource<String> source = env.addSource(new BroadCastStateSource());
        //注意这里不指定uid，会根据输入和输出来确定uid（如果输入和输出是可以chainAble则会使用边来一起生成）
        //而这里会改变并行度，可能会导致uid发生变化，所有一定要指定uid
        source.name("operator_state_source").uid("operator_state_source");
        source.map(new TestOperatorStateSpanListMap())
                .name("TestOperatorStateSpanListMap").uid("TestOperatorStateSpanListMap")
                .setParallelism(parallelism);
        execute(env);
    }

    private void executorWith2PListState(int parallelism) throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        String savePointPath = checkPointPath + P2_LIST_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        DataStreamSource<String> source = env.addSource(new BroadCastStateSource());
        source.name("operator_state_source").uid("operator_state_source");
        source.map(new TestOperatorStateSpanListMap())
                .name("TestOperatorStateSpanListMap").uid("TestOperatorStateSpanListMap")
                .setParallelism(parallelism);

        execute(env);
    }

    private void executorWith3PListStateFromP2(int parallelism) throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        String savePointPath = checkPointPath + P3_LIST_STATE_FROM_P2;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        DataStreamSource<String> source = env.addSource(new BroadCastStateSource());
        source.name("operator_state_source").uid("operator_state_source");
        source.map(new TestOperatorStateSpanListMap())
                .name("TestOperatorStateSpanListMap").uid("TestOperatorStateSpanListMap")
                .setParallelism(parallelism);
        execute(env);
    }


//    @Test
//    public void startWith3PListStateTo1P() throws Exception {
//        String checkPointPath = PathUtils.getCurrentPath(this);
//        log.info("checkpoint path is {}", checkPointPath);
//        Configuration configuration = new Configuration();
//        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
//        String savePointPath = checkPointPath + P2_LIST_STATE;
//        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
//        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
//        DataStreamSource<String> source = env.addSource(new BroadCastStateSource());
//        source.map(new TestOperatorStateSpanListMap()).setParallelism(1);
//        execute(env);
//    }
}
