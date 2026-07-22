import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
import json
from awsglue.job import Job
from urllib.parse import urlparse
import boto3
from transaction_log_process.transaction_log_util import TransctionLogProcessDebeziumCDC
from transaction_log_process.transcation_log_dms import TransctionLogProcessDMSCDC

from msg.KafkaConnector import KafkaConnector

'''
Glue -> Kafka -> Iceberg -> S3 Tables
通过 Glue 消费 MSK/MSK Serverless 的数据，写S3 Tables（Iceberg）。多表，支持I U D

1. 支持多表，通过MSK Connect 将数据库的数据CDC到MSK后，使用 [topics] 配置参数，可以接入多个topic的数据。
2. 支持MSK Serverless IAM认证，需要提前在Glue Connection配置MSK的connect。MSK Connect 配置在私有子网中，私有子网配置NAT访问公网
3. Job 参数说明
    (1). starting_offsets_of_kafka_topic: 'latest', 'earliest'
    (2). topics: 消费的Topic名称，如果消费多个topic，之间使用逗号分割（,）,例如 kafka1.db1.topica,kafka1.db2.topicb
    (3). icebergdb: 数据写入的iceberg database(namespace)名称
    (4). warehouse: S3 Tables bucket ARN，例如 arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket
    (5). datalake-formats: iceberg 指定使用哪一种datalake技术
    (6). glueconnect: Glue Connection 名称，用以获取MSK Serverless的数据
    (7). user-jars-first: True
    (8). cdcformat: cdc 的格式，目前支持的是 debezium, dms
    (9). catalogname: Spark 中注册的 catalog 名称，默认 s3tablesCatalog
4. Glue 需要使用 5.0 引擎，5.0 支持 S3 Tables 集成。
5. MSK Serverless 认证只支持IAM，因此在Kafka连接的时候需要包含IAM认证相关的代码。
'''

'''
读取表配置文件
'''


def load_tables_config(aws_region, config_s3_path):
    o = urlparse(config_s3_path, allow_fragments=False)
    client = boto3.client('s3', region_name=aws_region)
    data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
    file_content = data['Body'].read().decode("utf-8")
    json_content = json.loads(file_content)
    return json_content


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'starting_offsets_of_kafka_topic',
                                     'topics',
                                     'icebergdb',
                                     'warehouse',
                                     'tableconffile',
                                     'region',
                                     'glueconnect',
                                     'cdcformat',
                                     'catalogname'])

'''
获取Glue Job参数
'''
STARTING_OFFSETS_OF_KAFKA_TOPIC = args.get('starting_offsets_of_kafka_topic', 'latest')
TOPICS = args.get('topics')
DATABASE_NAME = args.get('icebergdb')
WAREHOUSE = args.get('warehouse')
TABLECONFFILE = args.get('tableconffile')
REGION = args.get('region')
GLUE_CONNECT = args.get('glueconnect')
CDCFORMAT = args.get('cdcformat')
CATALOG_NAME = args.get('catalogname', 's3tablesCatalog')

config = {
    "database_name": DATABASE_NAME,
    "warehouse": WAREHOUSE
}

tables_ds = load_tables_config(REGION, TABLECONFFILE)

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE) \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", True) \
    .getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

JOB_NAME = args['JOB_NAME']

logger = glueContext.get_logger()

logger.info("Init...")

logger.info("starting_offsets_of_kafka_topic:" + STARTING_OFFSETS_OF_KAFKA_TOPIC)
logger.info("topics:" + TOPICS)
logger.info("DATABASE_NAME:" + DATABASE_NAME)
logger.info("warehouse:" + WAREHOUSE)
logger.info("glue-connect:" + GLUE_CONNECT)
logger.info("table-config-file:" + TABLECONFFILE)
logger.info("catalog-name:" + CATALOG_NAME)


def writeJobLogger(logs):
    logger.info(args['JOB_NAME'] + " [CUSTOM-LOG]:{0}".format(logs))


### Check Parameter
if TABLECONFFILE == '':
    logger.info("Need Parameter [table-config-file]")
    sys.exit(1)
elif GLUE_CONNECT == '':
    logger.info("Need Parameter [glue-connect]")
    sys.exit(1)
elif WAREHOUSE == '':
    logger.info("Need Parameter [warehouse]")
    sys.exit(1)

checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" + "20250101" + "/"


def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)


kafka_options = KafkaConnector(topics=TOPICS, job_name=JOB_NAME,
                               starting_offset=STARTING_OFFSETS_OF_KAFKA_TOPIC,
                               glue_msk_connect=GLUE_CONNECT).get_glue_connect_options()

# Script generated for node Apache Kafka
dataframe_ApacheKafka_source = glueContext.create_data_frame.from_options(
    connection_type="kafka",
    connection_options=kafka_options
)

process = None

if CDCFORMAT == 'dms':
    process = TransctionLogProcessDMSCDC(spark,
                                               REGION,
                                               TABLECONFFILE,
                                               logger,
                                               JOB_NAME,
                                               DATABASE_NAME,
                                               warehouse=WAREHOUSE,
                                               isglue=True,
                                               catalog_name=CATALOG_NAME)
else:
    process = TransctionLogProcessDebeziumCDC(spark,
                                       REGION,
                                       TABLECONFFILE,
                                       logger,
                                       JOB_NAME,
                                       DATABASE_NAME,
                                       isglue=True,
                                       catalog_name=CATALOG_NAME)


glueContext.forEachBatch(frame=dataframe_ApacheKafka_source,
                         batch_function=process.processBatch,
                         options={
                             "windowSize": "120 seconds",
                             "recordPollingLimit": "200000",
                             "checkpointLocation": checkpoint_location,
                             "batchMaxRetries": 1
                         })

job.commit()
