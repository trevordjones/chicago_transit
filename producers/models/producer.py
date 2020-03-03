"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)
BROKER_URL = 'PLAINTEXT://localhost:9092'


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.client = AdminClient({"bootstrap.servers": BROKER_URL})

        self.broker_properties = {
            # TODO
            # TODO
            # TODO
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            {
                'bootstrap.servers': BROKER_URL,
                'schema.registry.url': 'http://localhost:8081',
            },
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        topic = NewTopic(self.topic_name, num_partitions=10, replication_factor=1)
        if self.topic_does_exist():
            if self.topic_name not in Producer.existing_topics:
                Producer.existing_topics.add(self.topic_name)
        else:
            futures = self.client.create_topics(
                [
                    topic,
                ]
            )

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f'Topic created {topic}')
                except Exception as e:
                    logger.error(f'Could not create topic {topic} because: {e}')

    def topic_does_exist(self):
        topic_metadata = self.client.list_topics(self.topic_name)
        return topic_metadata.topics[self.topic_name].error is None

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
