# Flink状态简介

Flink中除框架自身状态外，用户可自定义状态；其中状态包括keyed状态和算子状态，keyed状态只能用于`KeyedStream`流，算子状态一般用于没有主键的场景，例如source、sink等。

## keyed状态（Keyed State）

keyed状态支持的状态类型如下所示：

- `ValueState<T>`: 保存一个可以更新和检索的值（如上所述，每个值都对应到当前的输入数据的 key，因此算子接收到的每个 key 都可能对应一个值）。 这个值可以通过 `update(T)` 进行更新，通过 `T value()` 进行检索。
- `ListState<T>`: 保存一个元素的列表。可以往这个列表中追加数据，并在当前的列表上进行检索。可以通过 `add(T)` 或者 `addAll(List<T>)` 进行添加元素，通过 `Iterable<T> get()` 获得整个列表。还可以通过 `update(List<T>)` 覆盖当前的列表。
- `ReducingState<T>`: 保存一个单值，表示添加到状态的所有值的聚合。接口与 `ListState` 类似，但使用 `add(T)` 增加元素，会使用提供的 `ReduceFunction` 进行聚合。
- `AggregatingState<IN, OUT>`: 保留一个单值，表示添加到状态的所有值的聚合。和 `ReducingState` 相反的是, 聚合类型可能与 添加到状态的元素的类型不同。 接口与 `ListState` 类似，但使用 `add(IN)` 添加的元素会用指定的 `AggregateFunction` 进行聚合。
- `MapState<UK, UV>`: 维护了一个映射列表。 你可以添加键值对到状态中，也可以获得反映当前所有映射的迭代器。使用 `put(UK，UV)` 或者 `putAll(Map<UK，UV>)` 添加映射。 使用 `get(UK)` 检索特定 key。 使用 `entries()`，`keys()` 和 `values()` 分别检索映射、键和值的可迭代视图。你还可以通过 `isEmpty()` 来判断是否包含任何键值对。

## 算子状态（Operator State）

算子状态支持的状态类型如下所示：

- `ListState<T>`: 保存一个元素的列表。`ListState`可以分为两种类型**Even-split redistribution**和**Union redistribution**，用于恢复时是全分配还是部分分配
- `MapState<UK, UV>`: 维护了一个映射列表。也叫广播状态（`Broadcast State`）

## 状态有效期

状态可配置有效期，但暂时只支持基于 *processing time* 的 TTL