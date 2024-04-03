# Python Sample

./iceberg

该目录下的代码，提供通过EMR Serverless 运行 pyspark 完成数据CDC入湖到Iceberg的方法


1. EMR Serverless 数据湖的实现

   | File                                          | 简介                                                         |
   |-----------------------------------------------| ------------------------------------------------------------ |
   | **kafka-iceberg-streaming-emrserverless.py**  | pyspark代码实现消费MSK Serverless的 CDC 数据，写入Iceberg。支持多表，支持Schema变更。支持I/U/D。 |

**调用参数：**

| 参数                            | 说明                                                         |
| ------------------------------- | ------------------------------------------------------------ |
| jobname                         | 自定义Job名称，此参数会影响 checkpointpath 参数，作为checkpointpath中的一部分定义。 |
| starting_offsets_of_kafka_topic | 消费Kafka的偏移量设置：latest，earliest<br />latest：当集群中存在消费者之前提交的offset记录时 队列集群会从之前记录的offset点开始发送 「记录的offset点，+无穷」。 而当消费者之前在集群中不存在offset记录点时 会从队列中最新的记录开始消费。<br />earliest：如果一个消费者之前提交过offset。 假设这个消费者中途断过，那当它恢复之后重新连接到队列集群 此时应该是从 它在集群中之前提交的offset点开始继续消费，而不是从头消费。 而一个消费者如果之前没有offset记录并设置earliest ，此时才会从头消费。 |
| topics                          | 指定消费 Kafka 的 Topic，多个Topic，用逗号（，）分隔符。     |
| icebergdb                       | 指定Iceberg表对应数据库的名称                                |
| warehouse                       | 指定Iceberg Warehouse                                        |
| tablejsonfile                   | 设置Iceberg表的属性配置文件。配置文件说明参考[xxx]           |
| region                          | 程序运行的Region（us-east-1,us-west-1, etc.）                |
| kafkaserver                     | 指定Kafka Bootstrap Server                                   |
| checkpointpath                  | 指定Spark运行的checkpoint路径                                |


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

[spark-sql-kafka-0-10_2.12-3.3.1.jar](https://s3.console.aws.amazon.com/s3/object/emr-hive-us-east-1-812046859005?region=us-east-1&prefix=pyspark/spark-sql-kafka-0-10_2.12-3.3.1.jar)

[kafka-clients-3.3.1.jar](https://s3.console.aws.amazon.com/s3/object/emr-hive-us-east-1-812046859005?region=us-east-1&prefix=pyspark/kafka-clients-3.3.1.jar)

[spark-token-provider-kafka-0-10_2.12-3.3.1.jar](https://s3.console.aws.amazon.com/s3/object/emr-hive-us-east-1-812046859005?region=us-east-1&prefix=pyspark/spark-token-provider-kafka-0-10_2.12-3.3.1.jar)

[commons-pool2-2.11.1.jar](https://s3.console.aws.amazon.com/s3/object/emr-hive-us-east-1-812046859005?region=us-east-1&prefix=pyspark/commons-pool2-2.11.1.jar)

[aws-msk-iam-auth-1.1.6-all.jar](https://s3.console.aws.amazon.com/s3/object/emr-hive-us-east-1-812046859005?region=us-east-1&prefix=pyspark/aws-msk-iam-auth-1.1.6-all.jar)	  

代码 `./Iceberg/kafka-iceberg-streaming-emrserverless.py`

1. 由于代码有依赖公共方法，因此需要在这个项目的根目录下打包，首先获取代码。
```shell
git clone https://github.com/norrishuang/cdc-data-lake-pyspark.git
cd cdc-data-lake-pyspark
```

2. 打包公共类
```shell
pip3 install wheel
python3 setup.py bdist_wheel
```

3. 打包 python 环境，并且上传到 S3 指定目录下。
```shell
export S3_PATH=<s3-path>
# python lib
python3 -m venv transaction_logs_venv

source transaction_logs_venv/bin/activate
pip3 install --upgrade pip
pip3 install boto3

pip3 install ./dist/transaction_log_process-0.6-py3-none-any.whl --force-reinstall

pip3 install venv-pack
venv-pack -f -o transaction_log_process.tar.gz

# upload s3
aws s3 cp transaction_log_process.tar.gz $S3_PATH
aws s3 cp aws-emr-serverless/iceberg/kafka-iceberg-streaming-emrserverless.py $S3_PATH
```

**EMR Serverless 提交**

```shell
SPARK_APPLICATION_ID=<applicationid>
JOB_ROLE_ARN=arn:aws:iam::812046859005:role/<ROLE>
S3_BUCKET=<s3-buclet-name>

STARTING_OFFSETS_OF_KAFKA_TOPIC='earliest'
TOPICS='\"<topic-name>\"'
TABLECONFFILE='s3://'$S3_BUCKET'/pyspark/config/tables.json'
REGION='us-east-1'
DATABASE_NAME='emr_icebergdb'
WAREHOUSE='s3://'$S3_BUCKET'/data/iceberg-folder/'
KAFKA_BOOSTRAPSERVER='<msk-bootstrap-server>'
CHECKPOINT_LOCATION='s3://'$S3_BUCKET'/checkpoint/'
JOBNAME="MSKServerless-TO-Iceberg-20240301"

# 第三步上传的文件
PYTHON_ENV=$S3_PATH/cdc_venv.tar.gz

aws emr-serverless start-job-run \
  --application-id $SPARK_APPLICATION_ID \
  --execution-role-arn $JOB_ROLE_ARN \
  --name $JOBNAME \
  --job-driver '{
      "sparkSubmit": {
          "entryPoint": "s3://'${S3_BUCKET}'/pyspark/kafka-iceberg-streaming-emrserverless.py",
          "entryPointArguments":["--jobname","'${JOBNAME}'","--starting_offsets_of_kafka_topic","'${STARTING_OFFSETS_OF_KAFKA_TOPIC}'","--topics","'${TOPICS}'","--tablejsonfile","'${TABLECONFFILE}'","--region","'${REGION}'","--icebergdb","'${DATABASE_NAME}'","--warehouse","'${WAREHOUSE}'","--kafkaserver","'${KAFKA_BOOSTRAPSERVER}'","--checkpointpath","'${CHECKPOINT_LOCATION}'"],
          "sparkSubmitParameters": "--jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar,s3://emr-hive-us-east-1-812046859005/pyspark/*.jar --conf spark.executor.instances=10 --conf spark.driver.cores=2 --conf spark.driver.memory=4G --conf spark.executor.memory=4G --conf spark.executor.cores=2 --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.archives='$PYTHON_ENV'#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
        }
     }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": "s3://'${S3_BUCKET}'/sparklogs/"
        }
    }
}'
```
