import time

from pyspark.sql.functions import col, from_json, schema_of_json, to_timestamp, to_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
from urllib.parse import urlparse
import boto3
import json

from transaction_log_process.WriteIcebergTable import WriteIcebergTableClass


def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)


### created by norris 2024-06-18
### 该方法用于解析 DMS 输出的数据格式
class TransctionLogProcessDMSCDC:

    def __init__(self,
                 spark,
                 region,
                 tableconffile,
                 logger,
                 jobname,
                 databasename,
                 isglue=False,
                 WriteIcebergTableClass=None):
        self.region = region
        self.spark = spark
        self.tableconffile = tableconffile
        self.logger = logger
        self.jobname = jobname
        self.isglue = isglue

        self.tables_ds = self._load_tables_config(region, tableconffile)

        self.config = {
            "database_name": databasename,
        }

        WriteIcebergTableClass.__init__(spark=spark,
                                        region=self.region,
                                        tableconffile=self.tableconffile,
                                        logger=self.logger,
                                        jobname=self.jobname,
                                        databasename=databasename,
                                        isglue=self.isglue)

    def _writeJobLogger(self, logs):
        WriteIcebergTableClass.WriteJobLogger(self, logs)

    def _load_tables_config(self, aws_region, config_s3_path):
        self._writeJobLogger("table config file path" + config_s3_path)
        o = urlparse(config_s3_path, allow_fragments=False)
        client = boto3.client('s3', region_name=aws_region)
        data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
        file_content = data['Body'].read().decode("utf-8")
        json_content = json.loads(file_content)
        return json_content

    def processBatch(self, data_frame_batch, batchId):
        if data_frame_batch.count() > 0:

            data_frame = data_frame_batch.cache()

            schema = StructType([
                StructField("data", StringType(), True),
                StructField("metadata", StringType(), True)
            ])

            self._writeJobLogger("## Source Data from Kafka Batch\r\n + " + getShowString(data_frame, truncate=False))

            if self.isglue:
                # glue kafka connect
                dataJsonDF = data_frame.select(
                    from_json(col("$json$data_infer_schema$_temporary$").cast("string"), schema).alias(
                        "origdata")).select(col("origdata.*"))
            else:
                dataJsonDF = data_frame.select(from_json(col("value").cast("string"), schema).alias("origdata")).select(
                    col("origdata.*"))
            self._writeJobLogger("## Create DataFrame \r\n" + getShowString(dataJsonDF, truncate=False))

            # 获取元数据信息
            ####
            #"metadata":     {
            #    "timestamp":    "2024-06-18T05:52:17.637628Z",
            #    "record-type":  "data",
            #    "operation":    "load",
            #    "partition-key-type":   "primary-key",
            #    "schema-name":  "norrisdb",
            #    "table-name":   "user_order_list"
            #}
            ###
            metadataschema = StructType([
                StructField("timestamp", StringType(), True),
                StructField("record-type", StringType(), True),
                StructField("record-type", StringType(), True),
                StructField("operation", StringType(), True),
                StructField("partition-key-type", StringType(), True),
                StructField("schema-name", StringType(), True),
                StructField("table-name", StringType(), True),
                StructField("transaction-id", LongType(), True),
                StructField("transaction-record-id", LongType(), True),
                StructField("prev-transaction-id", LongType(), True),
                StructField("prev-transaction-record-id", LongType(), True),
                StructField("commit-timestamp", StringType(), True),
                StructField("stream-position", StringType(), True)
            ])

            rootschema = StructType([
                StructField("data", StringType(), True),
                StructField("metadata", StringType(), True)
            ])

            if self.isglue:
                # glue kafka connect
                dataJsonDF = data_frame.select(
                    from_json(col("$json$data_infer_schema$_temporary$").cast("string"), rootschema).alias(
                        "origdata")).select(col("origdata.data"),
                                            from_json(
                                                col("origdata.metadata"),
                                                metadataschema).alias(
                                                "metadata"))
            else:
                dataJsonDF = data_frame.select(
                    from_json(col("value").cast("string"), rootschema).alias("origdata")).select(col("origdata.data"),
                                                                                                 from_json(
                                                                                                     col("origdata.metadata"),
                                                                                                     metadataschema).alias(
                                                                                                     "metadata"))


            '''
            由于Iceberg没有主键，需要通过SQL来处理upsert的场景，需要识别CDC log中的 I/U/D 分别逻辑处理
            '''
            dataInsert = dataJsonDF.filter("metadata.operation in ('load','insert')")
            # 过滤 区分 insert upsert delete
            dataUpsert = dataJsonDF.filter("metadata.operation in ('update')")

            dataDelete = dataJsonDF.filter("metadata.operation in ('delete')")

            if dataInsert.count() > 0:
                #### 分离一个topics多表的问题。
                # dataInsert = dataInsertDYF.toDF()
                # sourceJson = dataInsert.select('data').first()
                # schemaSource = schema_of_json(sourceJson[0])

                # 获取多表
                datatables = dataInsert.select(col("metadata.schema-name"), col("metadata.table-name")).distinct()
                # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
                rowtables = datatables.collect()

                for cols in rowtables:
                    databaseName = cols[0]
                    tableName = cols[1]
                    self._writeJobLogger("Insert Table [%],Counts[%]".format(tableName, str(dataInsert.count())))
                    dataDF = dataInsert.select(col("data")) \
                        .filter(
                        "metadata.`table-name` = '" + tableName + "' and metadata.`schema-name` = '" + databaseName + "'")

                    datajson = dataDF.select('data').first()
                    schemadata = schema_of_json(datajson[0])

                    self._writeJobLogger("############  Insert Into-GetSchema-FirstRow:" + datajson[0])

                    '''识别时间字段'''

                    dataDFOutput = dataDF.select(
                        from_json(col("data").cast("string"), schemadata).alias("DFADD")).select(col("DFADD.*"))

                    # logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                    WriteIcebergTableClass.InsertDataLake(self, tableName, dataDFOutput)

            if dataUpsert.count() > 0:
                #### 分离一个topics多表的问题。
                # sourcejson = dataUpsert.select('source').first()
                # schemasource = schema_of_json(sourcejson[0])

                # 获取多表
                datatables = dataInsert.select(col("metadata.schema-name"), col("metadata.table-name")).distinct()
                # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
                rowTables = datatables.collect()
                self._writeJobLogger("MERGE INTO Table Names \r\n" + getShowString(datatables, truncate=False))

                for cols in rowTables:
                    databaseName = cols[0]
                    tableName = cols[1]
                    self._writeJobLogger("Upsert Table [%],Counts[%]".format(tableName, str(dataUpsert.count())))
                    dataDF = dataUpsert.select(col("data"), to_timestamp(col("metadata.timestamp")).alias("ts_ms")) \
                        .filter(
                        "metadata.`table-name` = '" + tableName + "' and metadata.`schema-name` = '" + databaseName + "'")

                    datajson = dataDF.select('data').first()
                    schemadata = schema_of_json(datajson[0])

                    self._writeJobLogger(
                        "MERGE INTO Table [" + tableName + "]\r\n" + getShowString(dataDF, truncate=False))
                    ##由于merge into schema顺序的问题，这里schema从表中获取（顺序问题待解决）
                    database_name = self.config["database_name"]

                    refreshtable = True
                    if refreshtable:
                        self.spark.sql(f"REFRESH TABLE glue_catalog.{database_name}.{tableName}")
                        self._writeJobLogger("Refresh table - True")

                    schemadata = self.spark.table(f"glue_catalog.{database_name}.{tableName}").schema
                    print(schemadata)
                    dataDFOutput = dataDF.select(
                        from_json(col("data").cast("string"), schemadata).alias("DFADD")).select(col("DFADD.*"),
                                                                                                 col("ts_ms"))

                    self._writeJobLogger(
                        "############  MERGE INTO  ############### \r\n" + getShowString(dataDFOutput, truncate=False))
                    WriteIcebergTableClass.MergeIntoDataLake(self, tableName, dataDFOutput, batchId)

            if dataDelete.count() > 0:

                # 获取多表
                datatables = dataInsert.select(col("metadata.schema-name"), col("metadata.table-name")).distinct()
                # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
                rowTables = datatables.collect()

                for cols in rowTables:
                    databaseName = cols[0]
                    tableName = cols[1]
                    self._writeJobLogger("Delete Table [%],Counts[%]".format(tableName, str(dataDelete.count())))
                    dataDF = dataDelete.select(col("data"), to_timestamp(col("metadata.timestamp")).alias("ts_ms")) \
                        .filter(
                        "metadata.`table-name` = '" + tableName + "' and metadata.`schema-name` = '" + databaseName + "'")
                    dataJson = dataDF.select('data').first()

                    schemaData = schema_of_json(dataJson[0])
                    dataDFOutput = dataDF.select(
                        from_json(col("data").cast("string"), schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                    WriteIcebergTableClass.DeleteDataFromDataLake(self, tableName, dataDFOutput, batchId)
