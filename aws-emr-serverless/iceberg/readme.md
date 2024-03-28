# Python Sample

./iceberg

该目录下的代码，提供通过EMR Serverless 运行 pyspark 完成数据CDC入湖到Iceberg的方法


1. EMR Serverless 数据湖的实现

   | File                                          | 简介                                                         |
   |-----------------------------------------------| ------------------------------------------------------------ |
   | **kafka-iceberg-streaming-emrserverless.py**  | pyspark代码实现消费MSK Serverless的 CDC 数据，写入Iceberg。支持多表，支持Schema变更。支持I/U/D。 |

​      调用参数：

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



​      **需要依赖的Jar**

​      [spark-sql-kafka-0-10_2.12-3.3.1.jar](https://s3.console.aws.amazon.com/s3/object/emr-hive-us-east-1-812046859005?region=us-east-1&prefix=pyspark/spark-sql-kafka-0-10_2.12-3.3.1.jar)

​	  [kafka-clients-3.3.1.jar](https://s3.console.aws.amazon.com/s3/object/emr-hive-us-east-1-812046859005?region=us-east-1&prefix=pyspark/kafka-clients-3.3.1.jar)

​      [spark-token-provider-kafka-0-10_2.12-3.3.1.jar](https://s3.console.aws.amazon.com/s3/object/emr-hive-us-east-1-812046859005?region=us-east-1&prefix=pyspark/spark-token-provider-kafka-0-10_2.12-3.3.1.jar)

​      [commons-pool2-2.11.1.jar](https://s3.console.aws.amazon.com/s3/object/emr-hive-us-east-1-812046859005?region=us-east-1&prefix=pyspark/commons-pool2-2.11.1.jar)

​      [aws-msk-iam-auth-1.1.6-all.jar](https://s3.console.aws.amazon.com/s3/object/emr-hive-us-east-1-812046859005?region=us-east-1&prefix=pyspark/aws-msk-iam-auth-1.1.6-all.jar)	  

​     

​      **EMR Serverless 提交**

```shell
SPARK_APPLICATION_ID=<applicationid>
JOB_ROLE_ARN=arn:aws:iam::<accountid>:role/<EMR-Serverless-Role>
S3_BUCKET=<s3bucket name>

STARTING_OFFSETS_OF_KAFKA_TOPIC='earliest'
TOPICS='\"<topic-name>\"'
TABLECONFFILE='s3://<s3bucket name>/pyspark/config/tables.json'
REGION='us-east-1'
DATABASE_NAME='emr_icebergdb'
WAREHOUSE='s3://<s3bucket name>/data/iceberg-folder/'
KAFKA_BOOSTRAPSERVER='<kafka bootstrap server>'
CHECKPOINT_LOCATION='s3://<s3bucket name>/'
JOBNAME="MSKServerless-TO-Iceberg"

aws emr-serverless start-job-run \
	--application-id $SPARK_APPLICATION_ID \
  --execution-role-arn $JOB_ROLE_ARN \
  --name $JOBNAME \
  --job-driver '{
      "sparkSubmit": {
          "entryPoint": "s3://'${S3_BUCKET}'/pyspark/CDC_Kafka_Iceberg.py",
          "entryPointArguments":["--jobname","'${JOBNAME}'","--starting_offsets_of_kafka_topic","'${STARTING_OFFSETS_OF_KAFKA_TOPIC}'","--topics","'${TOPICS}'","--tablejsonfile","'${TABLECONFFILE}'","--region","'${REGION}'","--icebergdb","'${DATABASE_NAME}'","--warehouse","'${WAREHOUSE}'","--kafkaserver","'${KAFKA_BOOSTRAPSERVER}'","--checkpointpath","'${CHECKPOINT_LOCATION}'"],
          "sparkSubmitParameters": "--jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar,s3://emr-hive-us-east-1-812046859005/pyspark/*.jar --conf spark.executor.instances=20 --conf spark.driver.cores=2 --conf spark.driver.memory=4G --conf spark.executor.memory=4G --conf spark.executor.cores=2 --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
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
