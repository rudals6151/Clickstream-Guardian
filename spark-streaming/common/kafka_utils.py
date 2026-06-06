"""
Kafka utilities for Spark Streaming
"""
from pyspark.sql.types import StructType, StructField, LongType, TimestampType, StringType, IntegerType


def get_click_schema():
    """Get Spark schema for click events"""
    return StructType([
        StructField("session_id", LongType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("item_id", LongType(), False),
        StructField("category", StringType(), True),
        StructField("event_type", StringType(), False)
    ])


def get_purchase_schema():
    """Get Spark schema for purchase events"""
    return StructType([
        StructField("session_id", LongType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("item_id", LongType(), False),
        StructField("price", IntegerType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("event_type", StringType(), False)
    ])


def get_kafka_options(bootstrap_servers, topic, group_id=None):
    """Get Kafka connection options for Spark"""
    options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "10000"
    }
    if group_id:
        options["kafka.group.id"] = group_id
    return options
