#!/usr/bin/env bash
# =============================================================
# 创建（或更新）CDC 入湖的 Glue Streaming Job
# 支持两种目标 catalog：
#   CATALOG_TYPE=glue     写 Glue Data Catalog + S3 (Iceberg)，Glue 4.0
#   CATALOG_TYPE=s3table  写 S3 Tables (Iceberg)，Glue 5.0
#
# 用法：
#   ./deploy/deploy-glue.sh <glue-job-name>
# 先决条件：
#   1. 已执行 ./deploy/build-and-upload.sh glue
#   2. 已在 deploy/config.env 中完成配置
#   3. 已在 Glue Console 创建 Kafka/MSK 的 Glue Connection
#   4. CATALOG_TYPE=s3table 时，已上传 s3-tables-catalog jar（见文档）
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

GLUE_JOB_NAME="${1:-msk-to-iceberg}"
S3_BASE="s3://${S3_BUCKET}/${S3_DEPLOY_PREFIX}"
WHEEL_VERSION=$(python3 -c "import re;print(re.search(r'version=\"([^\"]+)\"', open('${SCRIPT_DIR}/../setup.py').read()).group(1))")
ADDITIONAL_PYTHON_MODULES="${S3_BASE}/transaction_log_venv-${WHEEL_VERSION}-py3-none-any.whl"

# 根据 catalog 类型选择入口脚本 / Glue 版本 / 额外 JAR
if [[ "${CATALOG_TYPE}" == "s3table" ]]; then
    MAIN_PYTHON_CODE_FILE="${S3_BASE}/kafka-iceberg-streaming-glue-s3table.py"
    GLUE_VERSION="5.0"
    EXTRA_JARS="${S3_BASE}/jars/s3-tables-catalog-for-iceberg-runtime-${S3TABLES_CATALOG_JAR_VERSION}.jar"
    if [[ "${WAREHOUSE}" != arn:aws:s3tables:* ]]; then
        echo "[ERROR] CATALOG_TYPE=s3table 时 WAREHOUSE 必须是 S3 Tables bucket ARN"
        echo "        例如 arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket"
        exit 1
    fi
else
    MAIN_PYTHON_CODE_FILE="${S3_BASE}/kafka-iceberg-streaming-glue.py"
    GLUE_VERSION="4.0"
    EXTRA_JARS=""
fi

echo "==> 部署 Glue Streaming Job"
echo "    Job Name    : ${GLUE_JOB_NAME}"
echo "    Glue Version: ${GLUE_VERSION}"
echo "    Script      : ${MAIN_PYTHON_CODE_FILE}"
echo "    CatalogType : ${CATALOG_TYPE}"
echo "    CatalogName : ${CATALOG_NAME}"
echo "    Warehouse   : ${WAREHOUSE}"
echo "    Connection  : ${GLUE_CONNECT}"

# 组装 default-arguments
DEFAULT_ARGS=$(cat <<EOF
{
    "--job-language": "python",
    "--additional-python-modules": "${ADDITIONAL_PYTHON_MODULES}",
    "--TempDir": "${GLUE_TEMP_DIR}",
    "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--datalake-formats": "iceberg",
    "--icebergdb": "${DATABASE_NAME}",
    "--glueconnect": "${GLUE_CONNECT}",
    "--tableconffile": "${TABLECONFFILE}",
    "--region": "${AWS_REGION}",
    "--warehouse": "${WAREHOUSE}",
    "--starting_offsets_of_kafka_topic": "${STARTING_OFFSETS_OF_KAFKA_TOPIC}",
    "--user-jars-first": "true",
    "--topics": "${TOPICS}",
    "--cdcformat": "${CDC_FORMAT}",
    "--catalogtype": "${CATALOG_TYPE}",
    "--catalogname": "${CATALOG_NAME}"$(if [[ -n "${EXTRA_JARS}" ]]; then echo ",
    \"--extra-jars\": \"${EXTRA_JARS}\""; fi)
}
EOF
)

COMMAND_JSON='{"Name": "gluestreaming", "PythonVersion": "3", "ScriptLocation": "'"${MAIN_PYTHON_CODE_FILE}"'"}'
CONNECTIONS_JSON='{"Connections":["'"${GLUE_CONNECT}"'"]}'

# 已存在则更新，否则创建
if aws glue get-job --job-name "${GLUE_JOB_NAME}" --region "${AWS_REGION}" >/dev/null 2>&1; then
    echo "==> Job 已存在，执行更新 ..."
    aws glue update-job \
        --job-name "${GLUE_JOB_NAME}" \
        --region "${AWS_REGION}" \
        --job-update '{
            "Role": "'"${GLUE_JOB_ROLE}"'",
            "Command": '"${COMMAND_JSON}"',
            "Connections": '"${CONNECTIONS_JSON}"',
            "DefaultArguments": '"${DEFAULT_ARGS}"',
            "GlueVersion": "'"${GLUE_VERSION}"'",
            "NumberOfWorkers": '"${GLUE_NUMBER_OF_WORKERS}"',
            "WorkerType": "'"${GLUE_WORKER_TYPE}"'"
        }' --output json
else
    echo "==> 创建新 Job ..."
    aws glue create-job \
        --name "${GLUE_JOB_NAME}" \
        --role "${GLUE_JOB_ROLE}" \
        --command "${COMMAND_JSON}" \
        --region "${AWS_REGION}" \
        --connections "${CONNECTIONS_JSON}" \
        --output json \
        --default-arguments "${DEFAULT_ARGS}" \
        --glue-version "${GLUE_VERSION}" \
        --number-of-workers "${GLUE_NUMBER_OF_WORKERS}" \
        --worker-type "${GLUE_WORKER_TYPE}"
fi

echo ""
echo "==> 部署完成。启动 Job："
echo "    aws glue start-job-run --job-name ${GLUE_JOB_NAME} --region ${AWS_REGION}"
