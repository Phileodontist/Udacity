"""Creates a turnstile data producer"""
import logging
import traceback
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    # Loads key & value schemas 
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        # Normalizes station names
        self.station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )
        
        self.topic_name = "turnstiles"
        # Uses the Producer parent class to create a new topic
        super().__init__(
            self.topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,
            num_replicas=1,
        )
        
        # Assign this turnstile instance to a station
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""

        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        
        key = {"timestamp": self.time_millis()}
        value = {
            "station_id": self.station.station_id,
            "station_name": self.station.name,
            "line": self.station.color.name
        }
        
        try:
            for entry in range(num_entries):
                # Produce an event to Kafka -> topic
                self.producer.produce(
                    topic=self.topic_name,
                    key=key,
                    value=value
                )
            logger.info(f"Turnstile Data written to {self.topic_name}")
        except Exception as e:
            traceback.print_exc()
            logger.info("turnstile kafka integration incomplete - skipping - " + str(e))            

        
