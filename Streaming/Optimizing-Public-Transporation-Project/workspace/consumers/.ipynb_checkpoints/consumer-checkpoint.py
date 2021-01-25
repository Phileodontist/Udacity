"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "group.id": "stations",
            "auto.offset.reset" : "latest"
        }
        
        if (offset_earliest):
            self.broker_properties["auto.offset.reset"] = "earliest"

        # Determine which API to use. Since data is encoded in Avro, this will be true
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)
            pass

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        # If offset_earliest is set, assign the partition to consume from the very beginning
        for partition in partitions:
            try:
                if self.offset_earliest is True:
                    partition.offset = confluent_kafka.OFFSET_BEGINNING
                    logger.info(f"Set offset partition to OFFSET_BEGINNING")
            except Exception as e:
                logger.info("on_assign is incomplete - skipping - " + str(e))
                
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            # Continues on consuming until an error is found
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            message = self.consumer.poll(timeout=self.consume_timeout)
        except Exception as e:
            logger.error("_consume is incomplete - skipping")
            return 0
        
        if not message:
            logger.info("No messages received yet.")
            return 0
        elif message.error() is not None:
            logger.error("Error encountered -" + str(e))
            return 0
        else:
            self.message_handler(message)
            return 1
        

    def close(self):
        """Cleans up any open kafka consumers"""
        logger.info("Closing consumer...")
        self.consumer.close()
