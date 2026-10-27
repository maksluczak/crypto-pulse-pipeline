from datetime import datetime, timedelta
from airflow.decorators import dag, task
from delta import DeltaTable
from pyspark.sql.connect.session import SparkSession
from src.storage.db_client import get_db_connection

@dag(
    dag_id = 'crypto_maintenance_pipeline',
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes = 5)
    },
    description = 'Daily Delta Lake and database maintenance, anomalies summary',
    schedule_interval = '@daily',
    start_date = datetime(2026, 1, 1),
    catchup = False,
    tags = ['crypto', 'maintenance', 'spark', 'postgres']
)
def crypto_maintenance_pipeline():

    @task(task_id = 'check_db_health')
    def check_database_health():
        conn = None
        try:
            conn = get_db_connection()
            if not conn:
                return

            cur = conn.cursor()
            cur.execute('SELECT 1')
            cur.close()
            return "DB is healthy"
        except Exception as e:
            print(e)
            raise
        finally:
            if conn:
                conn.close()

    @task(task_id = 'optimize_delta_storage')
    def optimize_delta_lake_storage():
        try:
            spark = SparkSession.builder \
                .appName("Airflow Delta Maintenance") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
            delta_path = "./tmp/delta_crypto"

            deltaTable = DeltaTable.forPath(spark, delta_path)
            deltaTable.optimize().executeCompaction()
            deltaTable.vacuum(retentionHours = 168)

            spark.stop()
            return "Delta Lake optimized"
        except Exception as e:
            print(e)

    @task(task_id = 'log_anomalies_summary')
    def summarize_daily_anomalies():
        conn = None
        try:
            conn = get_db_connection()
            if not conn:
                return

            cur = conn.cursor()
            cur.execute("""
                SELECT symbol, COUNT(*) AS anomaly_count FROM crypto_anomalies
                WHERE detected_at >= NOW() - INTERVAL '1 day'
                GROUP BY symbol;
            """)
            rows = cur.fetchall()
            summary_dict = {row[0]: row[1] for row in rows}
            cur.close()
            return f"Summary from last 24 hours: {summary_dict}"
        except Exception as e:
            print(e)
        finally:
            if conn:
                conn.close()

    db_status = check_database_health()
    delta_status = optimize_delta_lake_storage()
    summary = summarize_daily_anomalies()

    db_status >> delta_status >> summary