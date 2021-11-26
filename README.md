# atsdb

An in-memory experimental TSDB.

## TSDB 特性
1. 在时间维度，时序数据是局部稠密、整体稀疏的；
2. 写入数据的时间戳大部分发生在近期而非远期；
3. 近期的数据查询要求高精度、低延迟，远期的数据查询容忍低精度、高延迟，要求高吞吐；
4. 容忍局部乱序写入，要求整体时序写入；
5. Label cardinality 在大时间尺度上是发散的，在小时间尺度上是收敛的；
6. 对事务没有要求；

## 设计目标
1. 只允许修改近期的可变数据 chunk，远期数据逐步按 chunk 转存到 immutable arrow (memory)、parquet (disk) 上；
2. 利用 arrow parquet 的生态加速聚合，shm ipc 导出进行离线 / 科学计算；
3. 利用 [datafusion](https://github.com/apache/arrow-datafusion) 查询 arrow parquet，datafusion 下推 projection / limit / filter 到可变 chunk，返回 arrow 查询；
4. sharding 下推到绑定 core affinity 线程的 coroutine executor 上，让对同 shard 的查询与写入在同 core 上完成，实现 wait-free 的查询与写入： [ntroducing-glommio](https://www.datadoghq.com/blog/engineering/i/)；
5. Label key / value 元数据绑定到 chunk，支持在同 metrics name 下 Label cardinality 发散；
6. 不做数据压缩以支持 shm ipc 与内存对齐；
