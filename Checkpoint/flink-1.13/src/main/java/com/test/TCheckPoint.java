package com.test;

import com.test.map.KeyStateMap;
import com.test.map.TestOperatorStateSpanBroadCastMap;
import com.test.map.TestOperatorStateSpanListMap;
import com.test.map.TestOperatorStateSpanUnionListMap;
import com.test.process.BroadcastProcessFunction;
import com.test.source.BroadCastStateSource;
import com.test.source.SimpleSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;

@Slf4j
public class TCheckPoint {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("rest.port", "8081");
        // 测试TestOperatorStateSpanBroadCastMap中的broadcastState变量
        //[Map (1/2)#0]   1_1,3_1,5_1
        //[Map (2/2)#0]   2_1,4_1,6_0
//        configuration.setString("execution.savepoint.path",
//                "D:/我的坚果云/java-Base/bigdata/flink-1.13/target/classes/45aeeacf6e73a5c7c146364e40c4ae3b/chk-585");

        //并行度设置2改为1时  无法启动！

        //2并行度改为4 没运行几分钟 没变！！！ 调为2还能运行
//        configuration.setString("execution.savepoint.path",
//                "D:\\我的坚果云\\java-Base\\bigdata\\flink-1.13\\target\\classes\\6f95fbde45d790e83dcbf6b24c04d788\\chk-598");

        //2并行度改为4 并运行几分钟让状态改变！  现在恢复至原先2并行度启动！
        //[Map (4/4)#0]   2_1,4_1,6_0
        //[Map (3/4)#0]   1_1,3_1,5_1
        //[Map (1/4)#0]   1_1,3_1,5_1
        //[Map (2/4)#0]   2_1,4_1,6_0
        //thread 4 broadcast State  2_1,4_1,6_0,7_1
        //thread 1 broadcast State  1_1,3_1,5_1,8_0
        //thread 2 broadcast State  2_1,4_1,6_0,9_1

        //将并行度调整为2,丢弃了两个状态，丢失统计数据，启动成功
        //[Map (2/2)#0] init broadcast State  2_1,4_1,6_0,9_1
        //[Map (1/2)#0] init broadcast State  1_1,3_1,5_1,8_0
        //将并行度设置为1，无法启动
        configuration.setString("execution.savepoint.path",
                "D:\\我的坚果云\\java-Base\\bigdata\\flink-1.13\\target\\classes\\0b14d1b29ec782d4049b48594faa5157\\chk-877");

        URI uri = TCheckPoint.class.getClassLoader().getResource("").toURI();
        String path = uri
                .getPath();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置存储类型  RocksDBStateBackend HashMapStateBackend
        env.setStateBackend(new HashMapStateBackend());
//        env.setStateBackend(new MemoryStateBackend());
        //设置存储：存储或者文件系统  FileSystemCheckpointStorage JobManagerCheckpointStorage
        //设置为内存存储
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env.getCheckpointConfig().setCheckpointStorage("file://" + path);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //开启checkpoint
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100L));
        env.setParallelism(1);
        //创建描述符
        MapStateDescriptor<String, Integer> broadcastDescriptor = new MapStateDescriptor<>("broadcast", String.class, Integer.class);
        DataStream dataStreamSource = env.addSource(new SimpleSource());
        //获得广播流
        BroadcastStream broadcast1 = dataStreamSource.broadcast(broadcastDescriptor);
        //将广播流和普通流连接（使用算子状态存储输入初始值，以便在失败时能从上次位置继续处理 ）
        SingleOutputStreamOperator map = env.addSource(new BroadCastStateSource()).name("operator_state_source")
                .connect(broadcast1)
                //处理广播连接流BroadcastConnectedStream
                .process(new BroadcastProcessFunction(broadcastDescriptor));
        map.print();
        //测试算子状态之间状态是否有影响，没有影响（Span 跨越）每个算子子任务都有自己的独立状态；所谓算子状态和键值分区状态主要区别在于，恢复重启时状态信息切分的力度
        map.map(new TestOperatorStateSpanListMap()).setParallelism(2);
        map.map(new TestOperatorStateSpanBroadCastMap()).setParallelism(1);

        //测试算子状态之间状态是否有影响，没有影响和List表现一样
        map.map(new TestOperatorStateSpanUnionListMap()).setParallelism(2)
                //获得KeyedStream流
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return "0";
                    }
                })
                //处理keyStream流数据，KeyStateMap中使用值状态来维护一个计算值，以便恢复时能和源端保持一致！
                .process(new KeyStateMap()).name("key process").setParallelism(2)
                .print();

        log.info("----执行流图----");
        log.info(env.getExecutionPlan());
        log.info("----执行流图----");

        env.execute("name_test");
    }
}
