
import time

from pyspark.sql.functions import col, from_json, schema_of_json, to_timestamp, to_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
from urllib.parse import urlparse
import boto3
import json


def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

class WriteIcebergTableClass:
    def __init__(self,
                 spark,
                 region,
                 tableconffile,
                 logger,
                 jobname,
                 databasename,
                 isglue=False):

        self.logger = logger
        self.spark = spark
        self.region = region
        self.tableconffile = tableconffile
        self.logger = logger
        self.jobname = jobname
        self.isglue = isglue

        self.tables_ds = self.Load_tables_config(region, tableconffile)

        self.config = {
            "database_name": databasename,
        }

    def WriteJobLogger(self, logs):
        self.logger.info(self.jobname + " [CUSTOM-LOG]:{0}".format(logs))


    def Load_tables_config(self, aws_region, config_s3_path):
        self._writeJobLogger("table config file path" + config_s3_path)
        o = urlparse(config_s3_path, allow_fragments=False)
        client = boto3.client('s3', region_name=aws_region)
        data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
        file_content = data['Body'].read().decode("utf-8")
        json_content = json.loads(file_content)
        return json_content


    def InsertDataLake(self, tableName, dataFrame, warehouse):

        database_name = self.config["database_name"]
        # partition as id
        ###如果表不存在，创建一个空表
        '''
        如果表不存在，新建。解决在 writeto 的时候，空表没有字段的问题。
        write.spark.accept-any-schema 用于在写入 DataFrame 时，Spark可以自适应字段。
        format-version 使用iceberg v2版本
        '''
        format_version = "2"
        write_merge_mode = "copy-on-write"
        write_update_mode = "copy-on-write"
        write_delete_mode = "copy-on-write"
        timestamp_fields = ""

        for item in self.tables_ds:
            if item['db'] == database_name and item['table'] == tableName:
                format_version = item['format-version']
                write_merge_mode = item['write.merge.mode']
                write_update_mode = item['write.update.mode']
                write_delete_mode = item['write.delete.mode']
                if 'timestamp.fields' in item:
                    timestamp_fields = item['timestamp.fields']

        if timestamp_fields != "":
            ##Timestamp字段转换
            for cols in dataFrame.schema:
                if cols.name in timestamp_fields:
                    dataFrame = dataFrame.withColumn(cols.name, to_timestamp(col(cols.name)))
                    self._writeJobLogger("Covert time type-Column:" + cols.name)

        #dyDataFrame = dataFrame.repartition(4, col("id"))

        creattbsql = f"""CREATE TABLE IF NOT EXISTS glue_catalog.{database_name}.{tableName} 
              USING iceberg 
              LOCATION '{warehouse}/{database_name}.db/{tableName}/'
              TBLPROPERTIES ('write.distribution-mode'='hash',
              'format-version'='{format_version}',
              'write.merge.mode'='{write_merge_mode}',
              'write.update.mode'='{write_update_mode}',
              'write.delete.mode'='{write_delete_mode}',
              'write.metadata.delete-after-commit.enabled'='true',
              'write.metadata.previous-versions-max'='10',
              'write.spark.accept-any-schema'='true')"""

        self._writeJobLogger( "####### IF table not exists, create it:" + creattbsql)
        self.spark.sql(creattbsql)

        dataFrame.writeTo(f"glue_catalog.{database_name}.{tableName}") \
            .option("merge-schema", "true") \
            .option("check-ordering", "false").append()

    def MergeIntoDataLake(self, tableName, dataFrame, batchId):

        database_name = self.config["database_name"]
        primary_key = 'ID'
        timestamp_fields = ''
        precombine_key = ''
        for item in self.tables_ds:
            if item['db'] == database_name and item['table'] == tableName:
                if 'primary_key' in item:
                    primary_key = item['primary_key']
                if 'precombine_key' in item:# 控制一批数据中对数据做了多次修改的情况，取最新的一条记录
                    precombine_key = item['precombine_key']
                if 'timestamp.fields' in item:
                    timestamp_fields = item['timestamp.fields']


        # dataMergeFrame = spark.range(1)
        if timestamp_fields != '':
            ##Timestamp字段转换
            for cols in dataFrame.schema:
                if cols.name in timestamp_fields:
                    dataFrame = dataFrame.withColumn(cols.name, to_timestamp(col(cols.name)))
                    self._writeJobLogger("Covert time type-Column:" + cols.name)

        self._writeJobLogger("############  TEMP TABLE batch {}  ############### {}\r\n".format(str(batchId),
                                                                                                getShowString(dataFrame, truncate=False)))
        t = time.time()  # 当前时间
        ts = (int(round(t * 1000000)))  # 微秒级时间戳
        TempTable = "tmp_" + tableName + "_u_" + str(batchId) + "_" + str(ts)
        dataFrame.createOrReplaceGlobalTempView(TempTable)

        MergeTempTable = ''
        ##dataFrame.sparkSession.sql(f"REFRESH TABLE {TempTable}")
        # 修改为全局试图OK，为什么？[待解决]
        if precombine_key == '':
            query = f"""MERGE INTO glue_catalog.{database_name}.{tableName} t USING (SELECT * FROM global_temp.{TempTable}) u
                ON t.{primary_key} = u.{primary_key}
                    WHEN MATCHED THEN UPDATE
                        SET *
                    WHEN NOT MATCHED THEN INSERT * """
        else:

            queryTemp = f"""
                SELECT a.* FROM global_temp.{TempTable} a join 
                (SELECT {primary_key},ts_ms,
                    row_number() over(PARTITION BY {primary_key} ORDER BY ts_ms DESC) AS rank 
                    FROM global_temp.{TempTable}) b 
                        ON a.{primary_key} = b.{primary_key} 
                        and a.ts_ms = b.ts_ms
                        WHERE b.rank = 1
            """
            self.logger.info("####### Execute SQL({}):{}".format(TempTable, queryTemp))
            tmpDF = spark.sql(queryTemp)

            ### DUBEG 查看更新的数据是否存在重复数据
            DebugTable = "debug_merge_" + tableName + "_u_" + str(batchId) + "_" + str(ts)
            tmpDF.createOrReplaceGlobalTempView(DebugTable)
            debugQuery = f"""
                SELECT {primary_key},ts_ms FROM global_temp.{DebugTable} a GROUP BY {primary_key},ts_ms HAVING count(*) > 1
            """
            debugDF = spark.sql(debugQuery)
            self._writeJobLogger(f"############ DEBUG MERGE TEMP {DebugTable} ############### \r\n" + getShowString(debugDF, truncate=False))
            spark.catalog.dropGlobalTempView(DebugTable)

            # 移除字段 ts_ms
            mergeDF = tmpDF.drop("ts_ms")
            MergeTempTable = "tmp_merge_" + tableName + "_u_" + str(batchId) + "_" + str(ts)

            self._writeJobLogger(f"############ MERGE TEMP TABLE {MergeTempTable} ############### \r\n" + getShowString(mergeDF, truncate=False))

            mergeDF.createOrReplaceGlobalTempView(MergeTempTable)
            query = f"""MERGE INTO glue_catalog.{database_name}.{tableName} t USING 
                (SELECT * FROM global_temp.{MergeTempTable}) u 
                  ON t.{primary_key} = u.{primary_key}
                     WHEN MATCHED THEN UPDATE
                         SET *
                     WHEN NOT MATCHED THEN INSERT * """

            # query_test = f"""SELECT * FROM global_temp.{TempTable} a join (
            #                 SELECT {primary_key},{precombine_key} FROM global_temp.{TempTable}
            #                 GROUP BY {primary_key},{precombine_key} having count(*) > 1
            #                 ) b on
            #                     a.{primary_key} = b.{primary_key} and
            #                     a.{precombine_key} = b.{precombine_key}"""
            #
            # self.logger.info("####### Execute SQL:" + query_test)
            # dfTest = self.spark.sql(query_test)
            # self._writeJobLogger("############  MERGE INTO  ############### \r\n" + getShowString(dfTest, truncate=False))
            #
            # query = f"""MERGE INTO glue_catalog.{database_name}.{tableName} t USING
            # (SELECT distinct a.* FROM global_temp.{TempTable} a join (SELECT {primary_key},{precombine_key},
            #     row_number() over(PARTITION BY {primary_key},{precombine_key} ORDER BY {precombine_key} DESC) AS rank
            #         from global_temp.{TempTable}) b on
            #     a.{primary_key} = b.{primary_key} and a.{precombine_key} = b.{precombine_key} and rank = 1) u
            #     ON t.{primary_key} = u.{primary_key}
            #         WHEN MATCHED THEN UPDATE
            #             SET *
            #         WHEN NOT MATCHED THEN INSERT * """

        self.logger.info("####### Execute SQL:" + query)
        try:
            spark.sql(query)
        except Exception as err:
            self.logger.error("Error of MERGE INTO")
            self.logger.error(err)
            pass
        spark.catalog.dropGlobalTempView(TempTable)
        if MergeTempTable != '':
            spark.catalog.dropGlobalTempView(MergeTempTable)


    def DeleteDataFromDataLake(self, tableName, dataFrame, batchId):

        database_name = self.config["database_name"]
        primary_key = 'ID'
        for item in self.tables_ds:
            if item['db'] == database_name and item['table'] == tableName:
                primary_key = item['primary_key']

        database_name = self.config["database_name"]
        t = time.time()  # 当前时间
        ts = (int(round(t * 1000000)))  # 微秒级时间戳
        TempTable = "tmp_" + tableName + "_d_" + str(batchId) + "_" + str(ts)
        dataFrame.createOrReplaceGlobalTempView(TempTable)
        query = f"""DELETE FROM glue_catalog.{database_name}.{tableName} AS t1 
             where EXISTS (SELECT {primary_key} FROM global_temp.{TempTable} WHERE t1.{primary_key} = {primary_key})"""
        try:
            spark.sql(query)
        except Exception as err:
            self.logger.error("Error of DELETE")
            self.logger.error(err)
            pass
        spark.catalog.dropGlobalTempView(TempTable)