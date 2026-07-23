# CDC 数据入湖（pyspark）

该项目用于实现将CDC的数据实时摄入 S3（以 Iceberg/Huid/DeltaLake的格式存放）

该项目通过 pyspark 实现，消费存放于kafka的cdc数据，解析处理insert/update/delete等事务操作，并且通过spark dataframe的schema推断，支持数据实时同步的同时更新表的schema。

项目支持两种模式的数据实时入湖

1. [Aamzon EMR Serverless](/aws-emr-serverless)
2. [AWS Glue](/aws-glue)

## 已知问题跟踪

### Spark 3.5+ 下 `write.spark.accept-any-schema` 导致 MERGE INTO 报 UNRESOLVED_COLUMN（等待社区修复）

- **现象**：在 Glue 5.0 / EMR 7.x（Spark 3.5+）上，对设置了 `'write.spark.accept-any-schema'='true'` 的 Iceberg 表执行 MERGE INTO 时，报 `AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION]`，且报错中"无法解析的列"就在建议列表里。Glue 4.0（Spark 3.3）不受影响。
- **原因**：该表属性会让 Spark 跳过 MERGE 的列解析（SPARK-30609）。Spark 3.4 及以前由 Iceberg 扩展自行完成解析；Spark 3.5 起 MERGE 移入 Spark core（SPARK-46207），导致双方都不做解析。详见 [apache/iceberg#9827](https://github.com/apache/iceberg/issues/9827)。
- **当前规避**：`transaction_log_util.py` 与 `WriteIcebergTable.py` 的 MergeIntoDataLake 中，在执行 MERGE 前临时 `UNSET TBLPROPERTIES ('write.spark.accept-any-schema')`，MERGE 结束后恢复（insert 路径的 schema 自动演进依赖该属性）。代价是每个 update batch 增加两次轻量的表属性提交。
- **后续**：Iceberg 1.11+ / Spark 4.1 支持 `MERGE WITH SCHEMA EVOLUTION` 语法（见 [apache/iceberg#5556](https://github.com/apache/iceberg/issues/5556)），待 Glue / EMR 升级支持后，可移除此规避逻辑并改用新语法。
