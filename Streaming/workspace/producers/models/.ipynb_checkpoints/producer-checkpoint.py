"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

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
            "bootstrap.servers": BROKER_URL,
            "group.id": "stations"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            # Tracks existing topics across all Producer instances
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL),
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        try:
            admin_client = AdminClient(self.broker_properties)  

            # Checks if topic exists in broker
            topic_metadata = admin_client.list_topics()
            if topic_metadata.topics.get(self.topic_name) is None:
                admin_client.create_topics([NewTopic(f"{self.topic_name}", 
                                                     num_partitions=self.num_partitions,
                                                     replication_factor=self.num_replicas)])
            
            logger.info(f"topic creation kafka integration complete - {self.topic_name}")
        except Exception as e:
            logger.info("topic creation kafka integration incomplete - skipping - " + str(e))
        

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        try:
            self.producer.flush()
            logger.info("\tProducer close complete")   
        except Exception as e:
            logger.info("\tProducer close incomplete - skipping - " + str(e))   

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
