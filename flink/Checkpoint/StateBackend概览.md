## 关于StateBackend

StateBackend作为状态后端，提供创建 KeyedStateBackend和OperatorStateBackend能力，并通过CheckpointStorage实现对状态的持久化。StateBackend提供了 resolveCheckpoint()、createCheckpointStorage()、createKeyedStateBackend()、createOperatorStateBackend() 等方法；

- resolveCheckpoint()获取 Checkpoint 的 Location信息；
- createCheckpointStorage()方法为Job创建CheckpointStorage对象，CheckpointStorage提供写入Checkpoint数据和元数据信息的能力;
- createKeyedStateBackend()方法用于创建 KeyedStateBackend，KeyedStateBackend 提供创建和管理KeyedState的能力;
- createOperatorStateBackend()方法主要用于创建OperatorStateBackend，通过OperatorStateBackend可以创建和管理 OperatorState 状态数据。

### StateBackend接口详情

StateBackend接口有RocksDBStateBackend和AbstractFileStateBackend两种实现类，其中AbstractFileStateBackend有MemoryStateBackend（内存存储）和FsStateBackend（文件存储）两种实现类；RocksDB对比较大状态支持良好（因为采用了LSM-Tree（日志合并树））

#### StateBackend创建方法

通过StateBackendFactory创建，StateBackendFactory主要有MemoryStateBackendFactory、FsStateBackendFactory和RocksDBStateBackendFactory三种实现；其中Factory是通过java SPI技术加载相应的Factory。

### 关于状态创建和恢复

1. 创建KeyStateBackend时，会根据每个Task实例的Key分组范围创建；

2. KeyStateBackend、OperatorStateBackend恢复时，会使用OperatorId来恢复KeyState

   