# 文档目录

| 文档 | 说明 |
|---|---|
| [deployment-emr-serverless.md](deployment-emr-serverless.md) | EMR Serverless 部署文档（支持 Glue Catalog + S3 / S3 Tables 两种写入目标） |
| [deployment-glue.md](deployment-glue.md) | AWS Glue Streaming Job 部署文档（支持 Glue Catalog + S3 / S3 Tables 两种写入目标） |

## 快速开始

```bash
# 1. 准备配置
cp deploy/config.env.example deploy/config.env
vi deploy/config.env

# 2. 打包并上传（emr / glue / all）
./deploy/build-and-upload.sh all

# 3a. 提交 EMR Serverless Job
./deploy/deploy-emr-serverless.sh

# 3b. 或创建/更新 Glue Job
./deploy/deploy-glue.sh my-cdc-job
aws glue start-job-run --job-name my-cdc-job --region <region>
```

写入 S3 Tables 时，将 `deploy/config.env` 中的 `CATALOG_TYPE` 设置为 `s3table`，并将 `WAREHOUSE` 设置为 S3 Tables bucket ARN，详见各部署文档。
