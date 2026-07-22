# EMR Serverless 部署文档

本文档介绍如何将 Kafka CDC 数据入湖 Job 部署到 EMR Serverless，支持两种写入目标：

| 目标 | CATALOG_TYPE | Warehouse 格式 | 入口脚本 |
|---|---|---|---|
| Glue Data Catalog + S3 (Iceberg) | `glue` | `s3://<bucket>/path/` | `kafka-iceberg-streaming-emrserverless-v2.py` |
| S3 Tables (Iceberg) | `s3table` | `arn:aws:s3tables:<region>:<account-id>:bucket/<name>` | `kafka-iceberg-streaming-emrserverless-s3table.py` |

## 1. 前置条件

### 1.1 基础资源

- 一个 EMR Serverless Application（Spark 类型）
  - 写 Glue Catalog：EMR 6.9+（建议 7.x）
  - 写 S3 Tables：**EMR 7.5+**（内置 S3 Tables catalog jar）
  - Application 需要配置 VPC 网络，且子网可以访问 MSK 集群
- 一个 MSK / MSK Serverless 集群，CDC 数据已通过 MSK Connect（Debezium）或 DMS 写入 topic
- 一个 S3 Bucket，用于存放代码、依赖、checkpoint、日志
- 写 S3 Tables 时：一个 S3 Tables bucket
  ```bash
  aws s3tables create-table-bucket --region us-east-1 --name my-table-bucket
  ```

### 1.2 IAM 执行角色

Job 执行角色（`EMR_JOB_ROLE_ARN`）需要以下权限：

- S3：对代码 / checkpoint / 数据 bucket 的读写权限
- MSK：`kafka-cluster:Connect`、`kafka-cluster:DescribeTopic`、`kafka-cluster:ReadData`、`kafka-cluster:DescribeGroup`、`kafka-cluster:AlterGroup`
- 写 Glue Catalog 时：`glue:GetTable`、`glue:CreateTable`、`glue:UpdateTable`、`glue:GetDatabase`、`glue:CreateDatabase` 等
- 写 S3 Tables 时：
  ```json
  {
      "Effect": "Allow",
      "Action": ["s3tables:*"],
      "Resource": [
          "arn:aws:s3tables:<region>:<account-id>:bucket/<table-bucket-name>",
          "arn:aws:s3tables:<region>:<account-id>:bucket/<table-bucket-name>/table/*"
      ]
  }
  ```
  > 生产环境建议按最小权限收敛为 `s3tables:GetTable`、`s3tables:CreateTable`、`s3tables:UpdateTableMetadataLocation`、`s3tables:GetTableData`、`s3tables:PutTableData`、`s3tables:GetNamespace`、`s3tables:CreateNamespace` 等。

### 1.3 准备依赖 JAR

将 Kafka 相关依赖 JAR 上传到 S3（`KAFKA_JARS_S3_PATH` 指向的目录）：

```bash
S3_JARS=s3://<your-s3-bucket>/pyspark/jars

wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.1/spark-sql-kafka-0-10_2.12-3.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.1/spark-token-provider-kafka-0-10_2.12-3.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
wget https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/2.1.1/aws-msk-iam-auth-2.1.1-all.jar

aws s3 cp . $S3_JARS/ --recursive --exclude "*" --include "*.jar"
```

> 注意：EMR 7.x 自带 Spark 3.5，如使用 EMR 7.x，请将上述 JAR 替换为对应 Spark 版本（如 `spark-sql-kafka-0-10_2.12-3.5.x.jar`）。S3 Tables catalog jar 在 EMR 7.5+ 中位于 `/usr/share/aws/s3tables-catalog/`，无需额外上传。

### 1.4 写 S3 Tables 时创建 Namespace

S3 Tables 中的 namespace 相当于 database，代码不会自动创建，需提前创建（名称与 `DATABASE_NAME` 一致）：

```bash
aws s3tables create-namespace \
    --table-bucket-arn arn:aws:s3tables:<region>:<account-id>:bucket/<table-bucket-name> \
    --namespace <database-name>
```

## 2. 配置

复制配置模板并按实际环境修改：

```bash
cp deploy/config.env.example deploy/config.env
vi deploy/config.env
```

写 S3 Tables 时的关键配置：

```bash
export CATALOG_TYPE="s3table"
export CATALOG_NAME="s3tablesCatalog"
export WAREHOUSE="arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket"
```

写 Glue Catalog 时的关键配置：

```bash
export CATALOG_TYPE="glue"
export CATALOG_NAME="glue_catalog"
export WAREHOUSE="s3://<your-s3-bucket>/data/iceberg-folder/"
```

表配置文件（`tables.json`）的格式参考 [aws-emr-serverless/readme.md](../aws-emr-serverless/readme.md)。

## 3. 打包与上传

在项目根目录执行：

```bash
./deploy/build-and-upload.sh emr
```

该脚本会：

1. 将公共类（`msg`、`transaction_log_process`）打包成 wheel
2. 创建 venv 并用 `venv-pack` 打包成 `transaction_log_process.tar.gz`（EMR Serverless 的 python 运行环境）
3. 上传 tar.gz、入口 py 脚本、表配置文件到 S3

> 注意：打包环境的 Python 版本需与 EMR 运行时匹配（EMR 7.x 为 Python 3.9+）。建议在 Amazon Linux 环境（如 EC2/CloudShell/容器）中打包，避免 macOS 与 Linux 的二进制不兼容。

## 4. 提交 Job

```bash
./deploy/deploy-emr-serverless.sh
```

脚本会根据 `CATALOG_TYPE` 自动选择入口脚本与依赖 JAR，并提交 `start-job-run`。提交成功后会输出 JobRunId 及查询状态的命令。

## 5. 验证

### 5.1 查看 Job 状态

```bash
aws emr-serverless get-job-run \
    --application-id <application-id> \
    --job-run-id <job-run-id> \
    --region <region>
```

状态为 `RUNNING` 后，向源数据库写入数据触发 CDC。

### 5.2 验证数据（S3 Tables）

```bash
# 确认表已自动创建
aws s3tables list-tables \
    --table-bucket-arn arn:aws:s3tables:<region>:<account-id>:bucket/<table-bucket-name> \
    --namespace <database-name>
```

如果 S3 Tables 已与 Analytics Services 集成（Lake Formation / Glue Data Catalog federation），可直接用 Athena 查询：

```sql
SELECT * FROM "s3tablescatalog/<table-bucket-name>"."<database-name>"."<table-name>" LIMIT 10;
```

### 5.3 验证数据（Glue Catalog）

用 Athena 直接查询 `<database-name>.<table-name>` 即可。

## 6. 常见问题

| 问题 | 原因与解决 |
|---|---|
| `ClassNotFoundException: software.amazon.s3tables.iceberg.S3TablesCatalog` | EMR 版本低于 7.5，或未在 `--jars` 中带上 s3tables catalog jar。升级 EMR Application 版本。 |
| `AccessDeniedException` (s3tables) | 执行角色缺少 `s3tables:*` 相关权限，参考 1.2 节。 |
| `NoSuchNamespaceException` | S3 Tables 的 namespace 未创建，参考 1.4 节。 |
| Kafka 连接超时 | EMR Serverless Application 的 VPC/子网/安全组无法访问 MSK；检查安全组入站规则（9098 端口 IAM 认证）。 |
| Python 依赖 ImportError | venv 打包环境与 EMR 运行时 OS/Python 版本不匹配，参考第 3 节注意事项。 |
