import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from src.alerts.telegram import send_anomaly_alert
from src.config import KAFKA_SERVER, TOPIC_TRADES
from src.logger import setup_logging
from src.ml.anomaly_detector import check_candle, load_models
from src.storage.db_client import get_db_connection, save_candle, save_anomaly

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
        .option("startingOffsets", "latest") \
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
            "symbol", "avg_price", "min_price", "max_price", "volume", "trade_count"
        )

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = batch_df.collect()
    if len(rows) == 0:
        return

    new_anomalies = []
    conn = get_db_connection()
    try:
        for row in rows:
            candle = row.asDict()
            save_candle(conn, candle)
            anomaly = check_candle(conn, candle)
            if anomaly is not None and save_anomaly(conn, anomaly):
                new_anomalies.append(anomaly)
        conn.commit()
    finally:
        conn.close()

    for anomaly in new_anomalies:
        logger.warning(f"Anomaly detected: {anomaly}")
        send_anomaly_alert(anomaly)

    logger.info(f"Batch {batch_id}: {len(rows)} candles, {len(new_anomalies)} anomalies")

def main():
    setup_logging()
    load_models()
    spark = create_spark_session()
    raw_df = read_from_kafka(spark)
    trades_df = parse_trades(raw_df)
    candles_df = aggregate_per_minute(trades_df)

    query = candles_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "checkpoints/candles") \
        .trigger(processingTime = "20 seconds") \
        .start()
    query.awaitTermination()

if __name__ == "__main__":
    main()