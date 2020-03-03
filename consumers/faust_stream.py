"""Defines trends calculations for stations"""
import logging

import faust


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


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("psql.stations", value_type=Station)
out_topic = app.topic("chicago_transit.stations.table", partitions=1)
table = app.Table(
    "chicago_transit.stations.table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

@app.agent(topic)
async def station_stream(stations):
    async for station in stations:
        line = "red" if station.red else "blue" if station.blue else "green"
        transformed_station = TransformedStation(station.station_id, station.station_name, station.order, line)
        table[transformed_station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
