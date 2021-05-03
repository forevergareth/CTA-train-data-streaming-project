"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


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


        self.broker_properties = {
            'bootstrap.servers': 'PLAINTEXT://localhost:9092',
            'schema.registry.url' : 'http://localhost:8081',
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema = self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        client = AdminClient(
            {
                "bootstrap.servers": self.broker_properties["bootstrap.servers"]
            }
        )

        # Get broker topics
        broker_topic_metadata = client.list_topics(timeout=5)

        # Does the topic exist on the broker?
        exists_in_broker = self.topic_name in set(t.topic for t in iter(broker_topic_metadata.topics.values()))

        if exists_in_broker:
            logger.info(f"topic exits on broker {self.topic_name}")
            return

        # Create topic on broker
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=1,
                    replication_factor=1,
                    config={
                        "compression.type": "lz4",
                    },
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                print("topic created on broker")
            except Exception as e:
                print(f"failed to create topic {topic_name}: {e}")


        logger.info("topic creation complete")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.info("Cleaning up producer")
        if self.topic_name is not None:
            self.producer.flush()
            logger.info("Producer is cleaned up")
        
    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
