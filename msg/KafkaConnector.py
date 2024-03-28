

class KafkaConnector:
    def __init__(self,
                 topics,
                 job_name,
                 starting_offset,
                 kafka_boostrapserver='',
                 glue_msk_connect=''):

        self.topics = topics
        self.kafka_boostrapserver = kafka_boostrapserver
        self.job_name = job_name
        self.starting_offset = starting_offset
        self.glue_msk_connect = glue_msk_connect

    def get_kafka_options(self):
        kafka_options = {
            "kafka.bootstrap.servers": self.kafka_boostrapserver,
            "subscribe": self.topics,
            "kafka.consumer.commit.groupid": "group-" + self.job_name,
            "inferSchema": "true",
            "classification": "json",
            "failOnDataLoss": "false",
            "maxOffsetsPerTrigger": 10000,
            "max.partition.fetch.bytes": 10485760,
            "startingOffsets": self.starting_offset,
            "connectionName": self.glue_msk_connect
            # "kafka.security.protocol": "SASL_SSL",
            # "kafka.sasl.mechanism": "AWS_MSK_IAM",
            # "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            # "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        }
        return kafka_options

    def get_glue_connect_options(self):
        kafka_options = {
            "connectionName": self.glue_msk_connect,
            "topicName": self.topics,
            "inferSchema": "true",
            "classification": "json",
            "failOnDataLoss": "false",
            "maxOffsetsPerTrigger": 10000,
            "max.partition.fetch.bytes": 10485760,
            "startingOffsets": self.starting_offset
            # "kafka.security.protocol": "SASL_SSL",
            # "kafka.sasl.mechanism": "AWS_MSK_IAM",
            # "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            # "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        }
        return kafka_options