#### Flink的非源节点是如何与checkpoint串行执行的？

```java
MailboxProcessor->runMailboxLoop()方法

MailboxDefaultAction -> runDefaultAction()方法
```


从源码和注释中可得出Task是执行最小单位，核心执行流程在doRun方法中，下面代码是从

```java
//StreamTask的inputProcessor是在oneInputStreamTask中被初始化为：StreamOneInputProcessor
OneInputStreamTask extends StreamTask
{
    //init方法中初始化input
    public void init() throws Exception {
        ...
        StreamTaskInput<IN> input = createTaskInput(inputGate);
        //初始化inputProcessor
        inputProcessor = new StreamOneInputProcessor<>(input, output, operatorChain);
        ...
    }
    //追踪input的初始化
    //createTaskInput调用如下方法
    private StreamTaskInput<IN> createTaskInput(CheckpointedInputGate inputGate) {
        return StreamTaskNetworkInputFactory.create(
        inputGate,
        inSerializer,...)
    }
    
}    
public class StreamTaskNetworkInputFactory {
    public static <T> StreamTaskInput<T> create(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            IOManager ioManager,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            InflightDataRescalingDescriptor rescalingDescriptorinflightDataRescalingDescriptor,
            Function<Integer, StreamPartitioner<?>> gatePartitioners,
            TaskInfo taskInfo) {
        //create方法返回如下
        return rescalingDescriptorinflightDataRescalingDescriptor.equals(
                        InflightDataRescalingDescriptor.NO_RESCALE)
                ? new StreamTaskNetworkInput<>(
                        checkpointedInputGate,
                        inputSerializer,
                        ioManager,
                        statusWatermarkValve,
                        inputIndex)
                : new RescalingStreamTaskNetworkInput<>(
                        checkpointedInputGate,
                        inputSerializer,
                        ioManager,
                        statusWatermarkValve,
                        inputIndex,
                        rescalingDescriptorinflightDataRescalingDescriptor,
                        gatePartitioners,
                        taskInfo);
    }

}

public final class StreamTaskNetworkInput<T>
        extends AbstractStreamTaskNetworkInput<
                T,
                SpillingAdaptiveSpanningRecordDeserializer<
                        DeserializationDelegate<StreamElement>>>{} 

StreamOneInputProcessor  input在OneInputStreamTask的init方法中初始化

//重点关注AbstractStreamTaskNetworkInput类中的emitNext方法

```



