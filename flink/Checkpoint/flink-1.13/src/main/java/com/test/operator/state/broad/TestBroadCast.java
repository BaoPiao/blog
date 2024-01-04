package com.test.operator.state.broad;

import com.test.source.BroadCastStateSource;
import com.test.process.BroadcastProcessFunction;
import com.test.source.SimpleSource;
import com.test.map.TestOperatorStateSpanBroadCastMap;
import com.test.Utils.EnvUtils;
import com.test.Utils.PathUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FlinkException;
import org.junit.Assert;
import org.junit.Test;

import static com.test.Utils.ExecuteUtils.execute;


//测试算子状态-广播类型，在重启和修改并行度时状态变化情况
@Slf4j
public class TestBroadCast {



    //06:39:02.930 [BroadcastProcessFunction (1/2)#0] INFO  broad cast state change null to -1953739007
    //06:39:02.930 [BroadcastProcessFunction (2/2)#0] INFO  broad cast state change null to -1953739007
    private String P2_CONNECT_STATE = "/ck/connectBroadcast2P/chk-8";

    private void setOperatorAndExecute(StreamExecutionEnvironment env, Integer parallelism) throws Exception {
        MapStateDescriptor<String, Integer> broadcastDescriptor = new MapStateDescriptor<>("broadcast", String.class, Integer.class);
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = env.addSource(new SimpleSource());
        dataStreamSource.uid("simple_source").name("simple_source");
        //获得广播流
        BroadcastStream broadcast1 = dataStreamSource.broadcast(broadcastDescriptor);
        env.addSource(new BroadCastStateSource()).name("operator_state_source").uid("operator_state_source")
                .connect(broadcast1)
                .process(new BroadcastProcessFunction(broadcastDescriptor))
                .uid("BroadcastProcessFunction")
                .name("BroadcastProcessFunction").setParallelism(parallelism);

        execute(env);
    }


    //生成2P的CK
    @Test
    public void OnConnectCreate2PCK() throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();

        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(env, 2);
    }


    //正常启动，状态正常恢复
    @Test
    public void OnConnectUseP2_CONNECT_STATEWith2P() throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        String savePointPath = checkPointPath + P2_CONNECT_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(env, 2);

    }

    //正常启动，状态正常恢复
    @Test
    public void OnConnectUseP2_CONNECT_STATEWith1P() throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        String savePointPath = checkPointPath + P2_CONNECT_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(env, 1);
    }

    //扩容-正常启动，状态正常恢复
    @Test
    public void OnConnectUseP2_CONNECT_STATEWith3P() throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        String savePointPath = checkPointPath + P2_CONNECT_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(env, 3);
    }

    //扩容-正常启动，状态正常恢复
    @Test
    public void OnConnectUseP2_CONNECT_STATEWith4P() throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        String savePointPath = checkPointPath + P2_CONNECT_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setOperatorAndExecute(env, 4);
    }

    /**
     * ---------------------  将BroadcastState注解作为算子状态，不通过connect传入更新
     */
    private void setBroadCastOperatorAndExecutor(StreamExecutionEnvironment env, int parallelism) throws Exception {
        DataStreamSource<String> source = env.addSource(new BroadCastStateSource());
        source.name("operator_state_source").uid("operator_state_source");
        source.map(new TestOperatorStateSpanBroadCastMap())
                .name("TestOperatorStateSpanBroadCastMap").uid("TestOperatorStateSpanBroadCastMap")
                .setParallelism(parallelism);
        execute(env);
    }

    //获得初始化状态 2b702e1991b8ec7162530ba24a67e56e/chk-4 并行度是2
    //06:48:22.158 [TestOperatorStateSpanBroadCastMap (2/2)#0] INFO  snapshot broadcast state  1,3
    //06:48:22.158 [TestOperatorStateSpanBroadCastMap (1/2)#0] INFO  snapshot broadcast state  2,4
    private String P2_STATE = "/ck/broadcast2P/chk-4";

    //生成CK
    @Test
    public void testGet2PCheckPoint() throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setBroadCastOperatorAndExecutor(env, 2);
    }


    @Test
    public void useP2_STATEWith1P() throws Exception {
        //正常启动，会丢失状态
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 0);
        String savePointPath = checkPointPath + P2_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setBroadCastOperatorAndExecutor(env, 1);

    }

    //使用4并行度从P2_STATE启动获得ck  3cc3072ef71c178f19fa9225bf3ed0dd/ck-12
    //06:59:54.817 [TestOperatorStateSpanBroadCastMap (2/4)#0] INFO  snapshot broadcast state  1,3
    //06:59:54.817 [TestOperatorStateSpanBroadCastMap (4/4)#0] INFO  snapshot broadcast state  1,3,5
    //06:59:54.817 [TestOperatorStateSpanBroadCastMap (3/4)#0] INFO  snapshot broadcast state  2,4
    //06:59:54.817 [TestOperatorStateSpanBroadCastMap (1/4)#0] INFO  snapshot broadcast state  2,4
    private String P4_STATE = "/ck/broadcast4P/chk-5";

    @Test
    public void useP2_STATEWith4P() throws Exception {
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        String savePointPath = checkPointPath + P2_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setBroadCastOperatorAndExecutor(env, 4);
    }

    @Test
    public void useP4_STATEWith2P() throws Exception {
        //可以正常启动，并丢失状态
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        String savePointPath = checkPointPath + P4_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        setBroadCastOperatorAndExecutor(env, 2);
    }

    @Test
    public void useP4_STATEWith1P() throws Exception {
        //可以正常启动，并丢失状态
        String checkPointPath = PathUtils.getCurrentPath(this);
        log.info("checkpoint path is {}", checkPointPath);
        Configuration configuration = new Configuration();
        String savePointPath = checkPointPath + P4_STATE;
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savePointPath);
        StreamExecutionEnvironment env = EnvUtils.ofEnv(checkPointPath, configuration);
        DataStreamSource<String> dataStreamSource = env.addSource(new BroadCastStateSource());
        setBroadCastOperatorAndExecutor(env, 1);
    }
    //结论，如果使用BroadCast状态，并行度无法降低到初始并行度以下（第一次运行并行度为2；第二次修改为4正常运行，修改为1异常；第三次修改2正常，1会报错）


}
