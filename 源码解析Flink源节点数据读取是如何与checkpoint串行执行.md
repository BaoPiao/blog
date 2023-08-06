#### 源码解析Flink源节点数据读取是如何与checkpoint串行执行

Flink版本：1.13.6

前置知识：源节点的Checkpoint是由Checkpointcoordinate触发，具体是通过RPC调用TaskManager中对应的Task的StreamTask类的performChecpoint方法执行Checkpoint。

本文思路：本文将先分析checkpoint阶段，然后再分析数据读取阶段，最后得出结论：源节点Checkpoint时和源节点发送数据时，都需要抢SourceStreamTask类中lock变量的锁，最终实现串行执行checkpoint与发送数据

##### Checkpoint阶段

Checkpoint执行在StreamTask的performCheckpoint方法中执行，该方法调用过程如下

```java
// 在StreamTask类中 执行checkpoint操作
private boolean performCheckpoint(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics )
            throws Exception {
        if (isRunning) {
            //使用actionExecutor 同步触发checkpoint
            actionExecutor.runThrowing(
                    () -> {
    					....//进过一些列检查
                        subtaskCheckpointCoordinator.checkpointState(
                                checkpointMetaData,
                                checkpointOptions,
                                checkpointMetrics,
                                operatorChain,
                                this::isRunning);
                    });
            return true;
        } else {
    		....
        }
    }

```

从上述代码可以看出，Checkpoint执行是由actionExecutor执行器执行

###### StreamTask变量actionExecutor的实现和初始化

> StreamTask变量actionExecution的实现

通过代码注释可以知道该执行器的实现是StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor；从SynchronizedStreamTaskActionExecutor源代码可知，该执行器每次执行都需要获得mutex对象锁

```java
  /**
     * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another
     * thread) must be executed through this executor to ensure that we don't have concurrent method
     * calls that void consistent checkpoints.
     *
     * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with {@link
     * StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor
     * SynchronizedStreamTaskActionExecutor} to provide lock to {@link SourceStreamTask}.
     */
private final StreamTaskActionExecutor actionExecutor;


class SynchronizedStreamTaskActionExecutor implements StreamTaskActionExecutor {
    private final Object mutex;

    public SynchronizedStreamTaskActionExecutor(Object mutex) {
        this.mutex = mutex;
    }

    @Override
    public void run(RunnableWithException runnable) throws Exception {
        synchronized (mutex) {
            runnable.run();
        }
    }

}
```

> StreamTask变量actionExecution初始化

actionExecutor变量在StreamTask中定义，在构造方法中初始化；该构造方法由SourceStreamTask调用，并传入SynchronizedStreamTaskActionExecutor对象，代码如下所示

```java
//   SourceStreamTask的方法
private SourceStreamTask(Environment env, Object lock) throws Exception {
    //调用的StreamTask构造函数，传入SynchronizedStreamTaskActionExecutor对象
    super(
            env,
            null,
            FatalExitExceptionHandler.INSTANCE,
            //初始化actionExecutor
            StreamTaskActionExecutor.synchronizedExecutor(lock));
    //将lock对象赋值给类变量lock
    this.lock = Preconditions.checkNotNull(lock);
    this.sourceThread = new LegacySourceFunctionThread();

    getEnvironment().getMetricGroup().getIOMetricGroup().setEnableBusyTime(false);
}

//  StreamTask的方法
protected StreamTask(
        Environment environment,
        @Nullable TimerService timerService,
        Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
    	//初始化actionExecutor
        StreamTaskActionExecutor actionExecutor)
        throws Exception {
    this(
            environment,
            timerService,
            uncaughtExceptionHandler,
            actionExecutor,
            new TaskMailboxImpl(Thread.currentThread()));
}

protected StreamTask(
        Environment environment,
        @Nullable TimerService timerService,
        Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
        StreamTaskActionExecutor actionExecutor,
        TaskMailbox mailbox)
        throws Exception {
    super(environment);
    this.configuration = new StreamConfig(getTaskConfiguration());
    this.recordWriter = createRecordWriterDelegate(configuration, environment);
    //初始化actionExecutor
    this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
    this.mailboxProcessor = new MailboxProcessor(this::processInput, mailbox, actionExecutor);
    .......}
```

> 小结

actionExecutor执行器每次执行都需要获得mutex对象，mutex对象就是SourceStreamTask类中的lock对象；即**算子每次执行Checkpoint时都需要获得SourceStreamTask类中lock对象锁才能进行**

##### 数据读取阶段

在执行Checkpoint时控制读取源端，则控制点必定是在调用SourceContext的collect方法时

```java
@Override
public void run(SourceContext<String> ctx) throws Exception {
    int i = 0;
    while (true) {
		//在这个方法里处理
        ctx.collect(String.valueOf(i));
    }
}
```

点击collection查看实现，选择NonTimestampContext查看代码，collect()实现如下

```java
@Override
public void collect(T element) {
    synchronized (lock) {
        output.collect(reuse.replace(element));
    }
}
```

所以这里控制数据读取发送是通过lock来控制，lock是如何初始化的？

通过NonTimestampContext构造方法可以定位到StreamSourceContexts->getSourceContext方法；

```java
public static <OUT> SourceFunction.SourceContext<OUT> getSourceContext(
        TimeCharacteristic timeCharacteristic,
        ProcessingTimeService processingTimeService,
        Object checkpointLock,
        StreamStatusMaintainer streamStatusMaintainer,
        Output<StreamRecord<OUT>> output,
        long watermarkInterval,
        long idleTimeout) {

    final SourceFunction.SourceContext<OUT> ctx;
    switch (timeCharacteristic) {
		....
        case ProcessingTime:
            //初始化NonTimestampContext
            ctx = new NonTimestampContext<>(checkpointLock, output);
            break;
        default:
            throw new IllegalArgumentException(String.valueOf(timeCharacteristic));
    }
    return ctx;
}
```

向上追踪，在StreamSource类中调用getSourceContext：

```java
public void run(
        final Object lockingObject,
        final StreamStatusMaintainer streamStatusMaintainer,
        final Output<StreamRecord<OUT>> collector,
        final OperatorChain<?, ?> operatorChain)
        throws Exception {
        ....
        this.ctx =
        
        StreamSourceContexts.getSourceContext(
                timeCharacteristic,
                getProcessingTimeService(),
                lockingObject,
                streamStatusMaintainer,
                collector,
                watermarkInterval,
                -1);
        ....
        }
// 再向上最终run方法的调用点->是由内部方法run调用
public void run(
        final Object lockingObject,
        final StreamStatusMaintainer streamStatusMaintainer,
        final OperatorChain<?, ?> operatorChain)
        throws Exception {

    run(lockingObject, streamStatusMaintainer, output, operatorChain);
}

//再向上最终run方法的调用点->SourceStreamTask 调用run 然后再代用mainOpterator run方法
@Override
public void run() {
    try {
        // 使用的是类变量lock
        mainOperator.run(lock, getStreamStatusMaintainer(), operatorChain);
        if (!wasStoppedExternally && !isCanceled()) {
            synchronized (lock) {
                operatorChain.setIgnoreEndOfInput(false);
            }
        }
        completionFuture.complete(null);
    } catch (Throwable t) {
        // Note, t can be also an InterruptedException
        completionFuture.completeExceptionally(t);
    }
}
```

###### 小结

所以在源端写数据时，必须**获得SourceStreamTask中的类变量lock的锁才能进行写数据**；类变量lock刚好和执行器时同一个对象

##### 总结

flink的source算子在Checkpoint时，是通过锁对象SourceStreamTask.lock，来控制源端数据产生和Checkpoint的有序进行
