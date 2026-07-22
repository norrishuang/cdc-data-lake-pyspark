#!/usr/bin/env bash
# =============================================================
# 打包公共类并上传代码 / 依赖 / 配置文件到 S3
# 用法：
#   ./deploy/build-and-upload.sh [emr|glue|all]
# 默认 all（同时准备 emr serverless 和 glue 需要的产物）
# =============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET="${1:-all}"

CONFIG_FILE="${SCRIPT_DIR}/config.env"
if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "[ERROR] 未找到 ${CONFIG_FILE}"
    echo "        请先执行: cp deploy/config.env.example deploy/config.env 并修改配置"
    exit 1
fi
# shellcheck source=/dev/null
source "${CONFIG_FILE}"

S3_BASE="s3://${S3_BUCKET}/${S3_DEPLOY_PREFIX}"
WHEEL_VERSION=$(python3 -c "import re;print(re.search(r'version=\"([^\"]+)\"', open('${PROJECT_ROOT}/setup.py').read()).group(1))")
WHEEL_FILE="transaction_log_venv-${WHEEL_VERSION}-py3-none-any.whl"

echo "==> 项目目录: ${PROJECT_ROOT}"
echo "==> 上传目标: ${S3_BASE}"

cd "${PROJECT_ROOT}"

# ---------- 1. 打包公共类 wheel ----------
echo "==> [1/4] 打包公共类 wheel ..."
pip3 install --quiet wheel
rm -rf build dist ./*.egg-info
python3 setup.py bdist_wheel >/dev/null
echo "    生成 dist/${WHEEL_FILE}"

# ---------- 2. EMR Serverless: 打包 python venv ----------
if [[ "${TARGET}" == "emr" || "${TARGET}" == "all" ]]; then
    echo "==> [2/4] 打包 EMR Serverless python 环境 (venv) ..."
    VENV_DIR="${PROJECT_ROOT}/.venv-package"
    rm -rf "${VENV_DIR}" transaction_log_process.tar.gz
    python3 -m venv --copies "${VENV_DIR}"
    # shellcheck source=/dev/null
    source "${VENV_DIR}/bin/activate"
    pip3 install --quiet --upgrade pip
    pip3 install --quiet boto3 venv-pack
    pip3 install --quiet "./dist/${WHEEL_FILE}" --force-reinstall
    venv-pack -f -p "${VENV_DIR}" -o transaction_log_process.tar.gz
    deactivate
    echo "    生成 transaction_log_process.tar.gz"
else
    echo "==> [2/4] 跳过 EMR venv 打包 (target=${TARGET})"
fi

# ---------- 3. 上传到 S3 ----------
echo "==> [3/4] 上传产物到 S3 ..."

if [[ "${TARGET}" == "emr" || "${TARGET}" == "all" ]]; then
    aws s3 cp transaction_log_process.tar.gz "${S3_BASE}/" --region "${AWS_REGION}"
    aws s3 cp aws-emr-serverless/iceberg/kafka-iceberg-streaming-emrserverless-v2.py "${S3_BASE}/" --region "${AWS_REGION}"
    aws s3 cp aws-emr-serverless/iceberg/kafka-iceberg-streaming-emrserverless-s3table.py "${S3_BASE}/" --region "${AWS_REGION}"
fi

if [[ "${TARGET}" == "glue" || "${TARGET}" == "all" ]]; then
    aws s3 cp "dist/${WHEEL_FILE}" "${S3_BASE}/" --region "${AWS_REGION}"
    aws s3 cp aws-glue/iceberg/kafka-iceberg-streaming-glue.py "${S3_BASE}/" --region "${AWS_REGION}"
    aws s3 cp aws-glue/iceberg/kafka-iceberg-streaming-glue-s3table.py "${S3_BASE}/" --region "${AWS_REGION}"
fi

# ---------- 4. 上传表配置文件（如果本地存在） ----------
echo "==> [4/4] 上传表配置文件 ..."
LOCAL_TABLES_JSON="aws-emr-serverless/iceberg/config/tables.json"
if [[ -f "${LOCAL_TABLES_JSON}" ]]; then
    aws s3 cp "${LOCAL_TABLES_JSON}" "${TABLECONFFILE}" --region "${AWS_REGION}"
    echo "    已上传 ${LOCAL_TABLES_JSON} -> ${TABLECONFFILE}"
else
    echo "    [WARN] 未找到 ${LOCAL_TABLES_JSON}，请确认 ${TABLECONFFILE} 已存在"
fi

echo ""
echo "==> 完成。产物清单："
aws s3 ls "${S3_BASE}/" --region "${AWS_REGION}"
