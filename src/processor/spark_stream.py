import logging
from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from src.config import KAFKA_SERVER, TOPIC_TRADES
from src.logger import setup_logging

logger = logging.getLogger(__name__)

TRADE_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("trade_time", LongType(), True)
])

def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName('Crypto Pulse Pipeline') \
        .master('local[*]') \
        .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.sql.shuffle.partitions', '3') \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_from_kafka(spark: SparkSession) -> DataFrame:
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", TOPIC_TRADES) \
        .option("settingOffsets", "latest") \
        .load()

def parse_trades(raw_df: DataFrame) -> DataFrame:
    return raw_df \
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("trade")) \
        .select("trade.*") \
        .withColumn("event_time", (F.col("trade_time") / 1000).cast("timestamp"))

def aggregate_per_minute(trades_df: DataFrame) -> DataFrame:
    return trades_df \
        .withWatermark("event_time", "30 seconds") \
        .groupBy(F.window("event_time", "1 minute"), "symbol") \
        .agg(
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count")
        ) \
        .select(
            F.date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("minute_start"),
            "symbol", "avg_price", "min_price", "max_price", "trade_count"
        )

def main():
    setup_logging()
    spark = create_spark_session()
    raw_df = read_from_kafka(spark)
    trades_df = parse_trades(raw_df)
    candles_df = aggregate_per_minute(trades_df)

    query = candles_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    query.awaitTermination()


if __name__ == "__main__":
    main()