import os

from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, BooleanType
from sklearn.ensemble import IsolationForest
import pandas as pd

def create_spark_session() -> SparkSession | None:
    try:
        spark = SparkSession.builder \
            .appName('Crypto Pulse Pipeline') \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.13") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(e)
        return None

def read_from_kafka(spark: SparkSession, bootstrap_servers: str, topic_name: str) -> DataFrame:
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic_name) \
        .load()

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
    return df

def parse_and_clean_stream(raw_df) -> DataFrame:
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("volume", FloatType(), True),
        StructField("timestamp", LongType(), True)
    ])

    parsed_df = raw_df \
        .withColumn("parsed_value", from_json(col("value"), schema)) \
        .select("parsed_value.*", "timestamp")

    return parsed_df

@pandas_udf(BooleanType())
def predict_isolation_forest(price_series: pd.Series) -> pd.Series:
    model = IsolationForest(contamination=0.01, random_state=42)

    reshaped_prices = price_series.values.reshape(-1, 1)

    if len(reshaped_prices) < 5:
        return pd.Series([False] * len(price_series))

    model.fit(reshaped_prices)
    predictions = model.predict(reshaped_prices)

    is_anom = (predictions == -1)
    return pd.Series(is_anom)

def detect_anomalies(parsed_df: DataFrame) -> DataFrame:
    return parsed_df.withColumn("is_anomaly", predict_isolation_forest(col("price")))

def write_stream_to_sinks(processed_df: DataFrame, checkpoint_path: str, delta_path: str):
    delta_query = processed_df \
        .writeStream() \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", os.path.join(checkpoint_path, "delta")) \
        .start(delta_path)

    anomalies_df = processed_df.filter(col("is_anomaly") == True)

    console_query = anomalies_df \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", os.path.join(checkpoint_path, "console")) \
        .option("truncate", "false") \
        .start()

    return delta_query, console_query