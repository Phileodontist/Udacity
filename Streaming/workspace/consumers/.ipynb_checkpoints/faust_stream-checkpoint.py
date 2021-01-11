"""Defines trends calculations for stations"""
import logging
import faust
import asyncio


logger = logging.getLogger(__name__)

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

# Create a stream to consume from the stations topic, feeding into another topic
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# Read events from the specificed topic -> prefix + stations
topic = app.topic("jdbc_stations", value_type=Station)
# Filters data from stations topic and writes to new topic
out_topic = app.topic("stations.table", partitions=1)
table = app.Table(
   "stations.table",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)

# Assigns Line color to transform record
def lineColor(record):
    """Determines which line the station record belongs in"""
    if record.red:
        line = 'red'
    elif record.blue:
        line = 'blue'
    elif record.green:
        line = 'green'
    else:
        line = 'N/A'
    return line

@app.agent(topic)
async def stationRecords(stationRecords):
    """Transform station records to consolidate line field"""
    async for record in stationRecords:
        # Determine which line of the station
        recLine = lineColor(record)
        
        # Transform station record to transformed record
        table[record.station_id] = TransformedStation(
                                    station_id=record.station_id,
                                    station_name=record.station_name,
                                    order=record.order,
                                    line=recLine)
        logger.info(table[record.station_id])
    

if __name__ == "__main__":
    app.main()
