## Flink CDC 3.0 表结构变更的处理流程

​	表结构变更主要涉及到三个类`SchemaOperator`、`DataSinkWriterOperator`（`Sink`端）和`SchemaRegistry`（协调器）；`SchemaOperator`接收结构变更消息时会通知`sink`端和协调器，并等待结构变更操作在协调器执行完毕后在处理后续数据，具体流程参考如下。

## 前提条件

cdc版本：Flink-cdc 3.0

Flink版本：Flink 1.18

## `SchemaOperator`类

​	`Source`抓表结构变更事件推送到`SchemaOperator`时，`SchemaOperator`会向协调器（也就是`SchemaRegistry`）发起变更请求；如果是表结构变更，则向`Sink`发送`flushEvent`，让其（`Sink`）`flush`内存中数据（`Sink`是经过`DataSinkWriterOperator`包装），最后阻塞数据流；

### `SchemaOperator`处理表结构变更事件

```java
# SchemaOperator
@Override
public void processElement(StreamRecord<Event> streamRecord) {
    Event event = streamRecord.getValue();
    //如果是schame change事件
    if (event instanceof SchemaChangeEvent) {
        TableId tableId = ((SchemaChangeEvent) event).tableId();
        LOG.info(
                "Table {} received SchemaChangeEvent and start to be blocked.",
                tableId.toString());
        //处理schame change事件
        handleSchemaChangeEvent(tableId, (SchemaChangeEvent) event);
        return;
    }
    output.collect(streamRecord);
}

private void handleSchemaChangeEvent(TableId tableId, SchemaChangeEvent schemaChangeEvent) {
    // The request will need to send a FlushEvent or block until flushing finished
    //向协调节点（SchemaRegistry）发送表结构变更请求，是表结构变更会返回true 如果是建表则返回false
    SchemaChangeResponse response = requestSchemaChange(tableId, schemaChangeEvent);
    if (response.isShouldSendFlushEvent()) {
        LOG.info(
                "Sending the FlushEvent for table {} in subtask {}.",
                tableId,
                getRuntimeContext().getIndexOfThisSubtask());
        //向sink发送 flush事件和schame信息
        output.collect(new StreamRecord<>(new FlushEvent(tableId)));
        output.collect(new StreamRecord<>(schemaChangeEvent));
        // The request will block until flushing finished in each sink writer
        // 这个请求查询协调器，当前schame是否执行完毕，如果没有则阻塞等待，直到协调器完成schame change操作
        requestReleaseUpstream();
    }
}
```

## `Sink`端

​	`Sink`端`flush`掉变更前的数据，并上报给协调器（`SchemaRegistry`）缓存刷新完成

### `Sink`端处理表结构变更事件，并上报给协调器

```java
# DataSinkWriterOperator 创建sink时会使用DataSinkWriterOperator包装，用于处理FlushEvent和CreateTableEvent事件
@Override
public void processElement(StreamRecord<Event> element) throws Exception {
    Event event = element.getValue();

    // 处理FlushEvent事件
    if (event instanceof FlushEvent) {
        handleFlushEvent(((FlushEvent) event));
        return;
    }

    // CreateTableEvent marks the table as processed directly
    if (event instanceof CreateTableEvent) {
        processedTableIds.add(((CreateTableEvent) event).tableId());
        this.<OneInputStreamOperator<Event, CommittableMessage<CommT>>>getFlinkWriterOperator()
                .processElement(element);
        return;
    }

    // Check if the table is processed before emitting all other events, because we have to make
    // sure that sink have a view of the full schema before processing any change events,
    // including schema changes.
    ChangeEvent changeEvent = (ChangeEvent) event;
    if (!processedTableIds.contains(changeEvent.tableId())) {
        emitLatestSchema(changeEvent.tableId());
        processedTableIds.add(changeEvent.tableId());
    }
    processedTableIds.add(changeEvent.tableId());
    this.<OneInputStreamOperator<Event, CommittableMessage<CommT>>>getFlinkWriterOperator()
            .processElement(element);
}

# handleFlushEvent 向协调节点（SchemaRegistry）发送`FlushSuccess`请求
private void handleFlushEvent(FlushEvent event) throws Exception {
    copySinkWriter.flush(false);
    schemaEvolutionClient.notifyFlushSuccess(
            getRuntimeContext().getIndexOfThisSubtask(), event.getTableId());
}
```

## 协调器

​	协调节点收到所有`Sink`的`flush`完成通知后，然后执行结构变更操作，最后通知完成给等待的`requestReleaseUpstream`请求。

### 协调节点处理`FlushSuccess`请求

```java
public void flushSuccess(TableId tableId, int sinkSubtask) {
    flushedSinkWriters.add(sinkSubtask);
    //所有节点都处理完成
    if (flushedSinkWriters.equals(activeSinkWriters)) {
        LOG.info(
                "All sink subtask have flushed for table {}. Start to apply schema change.",
                tableId.toString());
        PendingSchemaChange waitFlushSuccess = pendingSchemaChanges.get(0);
        //执行表结构变更操作
        applySchemaChange(tableId, waitFlushSuccess.getChangeRequest().getSchemaChangeEvent());
        //通知等待的SchemaOperator，结构变更完成！
        waitFlushSuccess.getResponseFuture().complete(wrap(new ReleaseUpstreamResponse()));
        if (RECEIVED_RELEASE_REQUEST.equals(waitFlushSuccess.getStatus())) {
            startNextSchemaChangeRequest();
        }
    }
}
```

