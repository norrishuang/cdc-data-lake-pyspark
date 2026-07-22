#!/usr/bin/env bash
# =============================================================
# 提交 CDC 入湖 Job 到 EMR Serverless
# 支持两种目标 catalog：
#   CATALOG_TYPE=glue     写 Glue Data Catalog + S3 (Iceberg)
#   CATALOG_TYPE=s3table  写 S3 Tables (Iceberg)
#
# 用法：
#   ./deploy/deploy-emr-serverless.sh
# 先决条件：
#   1. 已执行 ./deploy/build-and-upload.sh emr
#   2. 已在 deploy/config.env 中完成配置
# =============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CONFIG_FILE="${SCRIPT_DIR}/config.env"
if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "[ERROR] 未找到 ${CONFIG_FILE}"
    echo "        请先执行: cp deploy/config.env.example deploy/config.env 并修改配置"
    exit 1
fi
# shellcheck source=/dev/null
source "${CONFIG_FILE}"

S3_BASE="s3://${S3_BUCKET}/${S3_DEPLOY_PREFIX}"
PYTHON_ENV="${S3_BASE}/transaction_log_process.tar.gz"

# 根据 catalog 类型选择入口脚本与依赖 JAR
if [[ "${CATALOG_TYPE}" == "s3table" ]]; then
    ENTRY_POINT="${S3_BASE}/kafka-iceberg-streaming-emrserverless-s3table.py"
    # EMR 7.5+ 内置 S3 Tables catalog jar
    JARS="/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar,/usr/share/aws/s3tables-catalog/s3-tables-catalog-for-iceberg-runtime.jar,${KAFKA_JARS_S3_PATH}/*.jar"
    if [[ "${WAREHOUSE}" != arn:aws:s3tables:* ]]; then
        echo "[ERROR] CATALOG_TYPE=s3table 时 WAREHOUSE 必须是 S3 Tables bucket ARN"
        echo "        例如 arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket"
        exit 1
    fi
else
    ENTRY_POINT="${S3_BASE}/kafka-iceberg-streaming-emrserverless-v2.py"
    JARS="/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar,${KAFKA_JARS_S3_PATH}/*.jar"
fi

echo "==> 提交 EMR Serverless Job"
echo "    Application : ${EMR_APPLICATION_ID}"
echo "    Entry Point : ${ENTRY_POINT}"
echo "    CatalogType : ${CATALOG_TYPE}"
echo "    CatalogName : ${CATALOG_NAME}"
echo "    Warehouse   : ${WAREHOUSE}"
echo "    Topics      : ${TOPICS}"
echo "    CDC Format  : ${CDC_FORMAT}"

SUBMIT_RESULT=$(aws emr-serverless start-job-run \
  --region "${AWS_REGION}" \
  --application-id "${EMR_APPLICATION_ID}" \
  --execution-role-arn "${EMR_JOB_ROLE_ARN}" \
  --name "${JOB_NAME}" \
  --job-driver '{
      "sparkSubmit": {
          "entryPoint": "'"${ENTRY_POINT}"'",
          "entryPointArguments":[
              "--jobname","'"${JOB_NAME}"'",
              "--starting_offsets_of_kafka_topic","'"${STARTING_OFFSETS_OF_KAFKA_TOPIC}"'",
              "--topics","'"${TOPICS}"'",
              "--tablejsonfile","'"${TABLECONFFILE}"'",
              "--region","'"${AWS_REGION}"'",
              "--icebergdb","'"${DATABASE_NAME}"'",
              "--warehouse","'"${WAREHOUSE}"'",
              "--cdcformat","'"${CDC_FORMAT}"'",
              "--catalogtype","'"${CATALOG_TYPE}"'",
              "--catalogname","'"${CATALOG_NAME}"'",
              "--kafkaserver","'"${KAFKA_BOOSTRAPSERVER}"'",
              "--checkpointpath","'"${CHECKPOINT_LOCATION}"'"
          ],
          "sparkSubmitParameters": "--jars '"${JARS}"' --conf spark.executor.instances='"${SPARK_EXECUTOR_INSTANCES}"' --conf spark.driver.cores='"${SPARK_DRIVER_CORES}"' --conf spark.driver.memory='"${SPARK_DRIVER_MEMORY}"' --conf spark.executor.memory='"${SPARK_EXECUTOR_MEMORY}"' --conf spark.executor.cores='"${SPARK_EXECUTOR_CORES}"' --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.archives='"${PYTHON_ENV}"'#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
        }
     }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": "s3://'"${S3_BUCKET}"'/sparklogs/"
        }
    }
}')

echo "${SUBMIT_RESULT}"

JOB_RUN_ID=$(echo "${SUBMIT_RESULT}" | python3 -c "import sys,json;print(json.load(sys.stdin)['jobRunId'])")
echo ""
echo "==> 提交成功。JobRunId: ${JOB_RUN_ID}"
echo "==> 查看状态："
echo "    aws emr-serverless get-job-run --region ${AWS_REGION} --application-id ${EMR_APPLICATION_ID} --job-run-id ${JOB_RUN_ID}"
