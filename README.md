# CDC 数据入湖（pyspark）

该项目用于实现将CDC的数据实时摄入 S3（以 Iceberg/Huid/DeltaLake的格式存放）

该项目通过 pyspark 实现，消费存放于kafka的cdc数据，解析处理insert/update/delete等事务操作，并且通过spark dataframe的schema推断，支持数据实时同步的同时更新表的schema。