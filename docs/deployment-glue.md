# AWS Glue 部署文档

本文档介绍如何将 Kafka CDC 数据入湖 Job 部署为 Glue Streaming Job，支持两种写入目标：

| 目标 | CATALOG_TYPE | Glue 版本 | Warehouse 格式 | 入口脚本 |
|---|---|---|---|---|
| Glue Data Catalog + S3 (Iceberg) | `glue` | 4.0 | `s3://<bucket>/path/` | `kafka-iceberg-streaming-glue.py` |
| S3 Tables (Iceberg) | `s3table` | **5.0** | `arn:aws:s3tables:<region>:<account-id>:bucket/<name>` | `kafka-iceberg-streaming-glue-s3table.py` |

## 1. 前置条件

### 1.1 基础资源

- 一个 MSK / MSK Serverless 集群，CDC 数据已通过 MSK Connect（Debezium）或 DMS 写入 topic
- 一个 Glue Connection（Kafka 类型），指向 MSK 集群，配置在能访问 MSK 的私有子网中（私有子网需配置 NAT 访问公网）
- 一个 S3 Bucket，用于存放代码、依赖、临时目录
- 写 S3 Tables 时：一个 S3 Tables bucket
  ```bash
  aws s3tables create-table-bucket --region us-east-1 --name my-table-bucket
  ```

### 1.2 IAM 角色

Glue Job Role（`GLUE_JOB_ROLE`）需要：

- 托管策略 `AWSGlueServiceRole`（或等价自定义策略）
- S3：对代码 / 数据 / 临时目录 bucket 的读写权限
- MSK：`kafka-cluster:Connect`、`kafka-cluster:DescribeTopic`、`kafka-cluster:ReadData`、`kafka-cluster:DescribeGroup`、`kafka-cluster:AlterGroup`（MSK Serverless IAM 认证）
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
  > 生产环境建议按最小权限收敛。

### 1.3 写 S3 Tables 时准备 catalog JAR

Glue 5.0 未内置 S3 Tables catalog，需要下载并上传到 S3（版本号与 `config.env` 中 `S3TABLES_CATALOG_JAR_VERSION` 一致）：

```bash
VERSION=0.1.5
wget https://repo1.maven.org/maven2/software/amazon/s3tables/s3-tables-catalog-for-iceberg-runtime/${VERSION}/s3-tables-catalog-for-iceberg-runtime-${VERSION}.jar

aws s3 cp s3-tables-catalog-for-iceberg-runtime-${VERSION}.jar s3://<your-s3-bucket>/pyspark/jars/
```

部署脚本会自动将该 JAR 加入 Job 的 `--extra-jars` 参数。

### 1.4 写 S3 Tables 时创建 Namespace

S3 Tables 中的 namespace 相当于 database，需提前创建（名称与 `DATABASE_NAME` 一致）：

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
export GLUE_CONNECT="<glue-connection-of-kafka>"
export S3TABLES_CATALOG_JAR_VERSION="0.1.5"
```

写 Glue Catalog 时的关键配置：

```bash
export CATALOG_TYPE="glue"
export CATALOG_NAME="glue_catalog"
export WAREHOUSE="s3://<your-s3-bucket>/data/iceberg-folder/"
export GLUE_CONNECT="<glue-connection-of-kafka>"
```

表配置文件（`tables.json`）的格式参考 [aws-glue/readme.md](../aws-glue/readme.md)。

## 3. 打包与上传

在项目根目录执行：

```bash
./deploy/build-and-upload.sh glue
```

该脚本会：

1. 将公共类（`msg`、`transaction_log_process`）打包成 wheel
2. 上传 wheel、入口 py 脚本、表配置文件到 S3

Glue 通过 `--additional-python-modules` 参数直接安装 wheel，无需打包 venv。

## 4. 创建 / 更新 Glue Job

```bash
./deploy/deploy-glue.sh <glue-job-name>
```

脚本行为：

- 根据 `CATALOG_TYPE` 自动选择入口脚本、Glue 版本（glue→4.0，s3table→5.0）和额外 JAR
- Job 不存在时执行 `create-job`，已存在时执行 `update-job`（幂等，可重复运行）

启动 Job：

```bash
aws glue start-job-run --job-name <glue-job-name> --region <region>
```

## 5. 验证

### 5.1 查看 Job 状态与日志

```bash
aws glue get-job-runs --job-name <glue-job-name> --region <region> --max-results 1
```

日志位于 CloudWatch Logs 的 `/aws-glue/jobs/` 日志组，自定义日志可搜索 `[CUSTOM-LOG]`。

**监控 Kafka 消费延迟**：Job 已开启 `emitConsumerLagMetrics`（Kafka source 选项）和 `--enable-metrics`（Job 参数，Glue 4.0+），每个 batch 会向 CloudWatch 上报消费延迟指标：

- 位置：CloudWatch → Metrics → `Glue` 命名空间，按 JobName 维度筛选
- 指标名：`glue.driver.streaming.maxConsumerLagInMs`，表示 topic 中最老记录与到达 Glue 时间的差值（毫秒）
- 该值持续下降说明在追赶积压；持续上涨说明消费速度跟不上写入速度，需要调大 `maxOffsetsPerTrigger` 或增加 Worker 数量

### 5.2 验证数据（S3 Tables）

```bash
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
| `ClassNotFoundException: software.amazon.s3tables.iceberg.S3TablesCatalog` | 未上传 s3tables catalog jar 或 `--extra-jars` 路径不对，参考 1.3 节。 |
| `AccessDeniedException` (s3tables) | Job Role 缺少 `s3tables:*` 相关权限，参考 1.2 节。 |
| `NoSuchNamespaceException` | S3 Tables 的 namespace 未创建，参考 1.4 节。 |
| Kafka 连接超时 | Glue Connection 所在子网无法访问 MSK；检查子网路由、安全组（9098 端口 IAM 认证）、NAT。 |
| `ModuleNotFoundError: transaction_log_process` | wheel 未上传或 `--additional-python-modules` 路径错误；重新执行 `./deploy/build-and-upload.sh glue`。 |
| Glue 5.0 下 Kafka connector 行为变化 | Glue 5.0 基于 Spark 3.5，如遇 `create_data_frame.from_options` 兼容性问题，检查 Glue Connection 配置及 `user-jars-first` 参数。 |
