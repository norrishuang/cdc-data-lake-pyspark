# AWS Glue 实时入湖的实现



1. Iceberg
./iceberg
该目录下的代码，提供通过 AWS Glue 运行 pyspark 完成数据CDC入湖到Iceberg的方法。

update 2024-07-12 
* 升级到 V2 版本，支持 dms。
* 提取数据写入的公共类 writeIcebergTable，同时支持 emr serverless 和 glue 的写入。

   | File                              | 简介                                                         |
   |-----------------------------------| ------------------------------------------------------------ |
   | **kafka-iceberg-streaming-glue.py | pyspark代码实现消费MSK Serverless的 CDC 数据，写入Iceberg。支持多表，支持Schema变更。支持I/U/D。 |

**调用参数：**

| 参数                              | 说明                                                                                                                                                                                                                                                                                                    |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| jobname                         | 自定义Job名称，此参数会影响 checkpointpath 参数，作为checkpointpath中的一部分定义。                                                                                                                                                                                                                                            |
| starting_offsets_of_kafka_topic | 消费Kafka的偏移量设置：latest，earliest<br />latest：当集群中存在消费者之前提交的offset记录时 队列集群会从之前记录的offset点开始发送 「记录的offset点，+无穷」。 而当消费者之前在集群中不存在offset记录点时 会从队列中最新的记录开始消费。<br />earliest：如果一个消费者之前提交过offset。 假设这个消费者中途断过，那当它恢复之后重新连接到队列集群 此时应该是从 它在集群中之前提交的offset点开始继续消费，而不是从头消费。 而一个消费者如果之前没有offset记录并设置earliest ，此时才会从头消费。 |
| topics                          | 指定消费 Kafka 的 Topic，多个Topic，用逗号（，）分隔符。                                                                                                                                                                                                                                                                 |
| icebergdb                       | 指定Iceberg表对应数据库的名称                                                                                                                                                                                                                                                                                    |
| warehouse                       | 指定Iceberg Warehouse                                                                                                                                                                                                                                                                                   |
| tablejsonfile                   | 设置Iceberg表的属性配置文件。配置文件说明参考[xxx]                                                                                                                                                                                                                                                                       |
| region                          | 程序运行的Region（us-east-1,us-west-1, etc.）                                                                                                                                                                                                                                                                |
| kafkaserver                     | 指定Kafka Bootstrap Server                                                                                                                                                                                                                                                                              |
| checkpointpath                  | 指定Spark运行的checkpoint路径                                                                                                                                                                                                                                                                                |
| cdcformat                       | 指定接入数据源的cdc格式，目前支持的是 debezium/dms 两种                                                                                                                                                                                                                                                                  |


**table 配置参数：**
在数据实时入湖时我们还需要注意以下几点：
- 对数据中Update/Delete的场景，在数据入湖时，需要根据表的主键字段来更新数据或者删除数据。
- 时间字段的处理。
- 从消息队列消费数据时，可能出现的乱序情况。
  因此，还需要有一个配置文件来设置这些参数。
  配置文件采用Json的格式，示例如下：

```json
[
  {
    "db":"<sink_database_name>",
    "table":"sink_table_name",
    "primary_key":"<primary_key>",
    "format-version": 2,
    "write.merge.mode": "<merge-on-read or copy-on-write>",
    "write.update.mode": "merge-on-read or copy-on-write",
    "write.delete.mode": "merge-on-read or copy-on-write",
    "timestamp.fields": ["<timestamp-col1>","timestamp-col2"],
    "precombine_key": "<precombine-key>"
  }
]
```
参数说明：


| 参数                             | 描述                                                                                                                                                                                                                                                                                                    |
|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| db                             | 写入到 Iceberg 的库名|
| table | 同步的表名，自动建表，因此需要保证这个表名和同步的数据源的表名一致|
| primary_key                    | 用于UPDATE/DELETE的主键字段              |
| write.merge.mode               | Iceberg 建表参数【copy-on-write/merge-on-read】                                                                                                                                                                                                                                                                                    |
| write.update.mode              | Iceberg 建表参数【copy-on-write/merge-on-read】                                                                                                                                                                                                                                                                                  |
| write.delete.mode              | Iceberg 建表参数【copy-on-write/merge-on-read】                                                                                                                                                                                                                                                                       |
| timestamp.fields               | 如果表中存在 timestamp 的字段，将字段名配置在这里，数据处理过程中将会将这个字段在spark中设置成 timestamp 类型。                                                                                                                                                                                                    |
| precombine_key                 | 如果一批数据中同一条记录发生了多次更改，可以选择一个能从多次变化记录中取到最新的那条记录的字段，例如 updatetime 。                                                                                                                                                                                                                                                                            |


**需要依赖的Jar**

[spark-sql-kafka-0-10_2.12-3.3.1.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.1/spark-sql-kafka-0-10_2.12-3.3.1.jar)

[kafka-clients-3.3.1.jar](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar)

[spark-token-provider-kafka-0-10_2.12-3.3.1.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.1/spark-token-provider-kafka-0-10_2.12-3.3.1.jar)

[commons-pool2-2.11.1.jar](https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar)

[aws-msk-iam-auth-2.1.1-all.jar](https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/2.1.1/aws-msk-iam-auth-2.1.1-all.jar)	  

代码 `./Iceberg/kafka-iceberg-streaming-glue.py`

1. 由于代码有依赖公共方法，因此需要在这个项目的根目录下打包，首先获取代码。
```shell
git clone https://github.com/norrishuang/cdc-data-lake-pyspark.git

cd cdc-data-lake-pyspark

# 打包公共类
pip3 install wheel 
python3 setup.py bdist_wheel

# 上传到S3
aws s3 cp ./dist/transaction_log_venv-0.6-py3-none-any.whl s3://<s3-bucket>/pyspark/

aws s3 cp ./aws-emr-serverless/iceberg/kafka-iceberg-streaming-glue.py s3://<s3-bucket>/pyspark/
```

2. 创建一个 Glue Job
> 注意替换参数，例如 s3-bucket 为当前环境的服务地址
Kafka 的连接需要先在 Glue Connection 中创建，然后在 Glue Job 中引用。

```shell
MAIN_PYTHON_CODE_FILE=s3://<s3-bucket>/kafka-iceberg-streaming-glue.py
ADDITIONAL_PYTHON_MODULES=s3://<s3-bucket>/transaction_log_venv-0.6-py3-none-any.whl
TABLECONFFILE='s3://<s3-bucket>/config/<table-conf>.json'
AWS_REGION="us-east-1"
ICEBERG_WAREHOUSE=s3://<s3-bucket>/data/iceberg-folder/
TEMP_DIR=s3://<s3-bucket>/temporary/

aws glue create-job \
    --name msk-to-iceberg \
    --role my-glue-role \
    --command '{ 
        "Name": "gluestreaming", 
        "PythonVersion": "3", 
        "ScriptLocation": "'$MAIN_PYTHON_CODE_FILE'" 
    }' \
    --region us-east-1 \
    --connections '{"Connections":["<glue coneciton of kafka>"]}' \
    --output json \
    --default-arguments '{ 
        "--job-language": "python",
        "--additional-python-modules":"'$ADDITIONAL_PYTHON_MODULES'", 
        "--aws_region": "'$AWS_REGION'", 
        "--TempDir": "'$TEMP_DIR'",
        "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions", 
        "--config_s3_path": "'$TABLECONFFILE'", 
        "--datalake-formats": "iceberg",
        "--icebergdb": "<iceberg-database-name>",
        "--glueconnect": "<glue coneciton of kafka>",
        "--tableconffile": "'$TABLECONFFILE'",
        "--region": "'$AWS_REGION'",
        "--warehouse": "'$ICEBERG_WAREHOUSE'",
        "--starting_offsets_of_kafka_topic": "earliest",
        "--user-jars-first": "true",
        "--topics": "<topic-name>"
    }' \
    --glue-version 4.0 \
    --number-of-workers 3 \
    --worker-type G.1X
```
