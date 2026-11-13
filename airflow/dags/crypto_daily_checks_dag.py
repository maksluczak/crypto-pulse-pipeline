from datetime import timedelta, datetime
from airflow.decorators import dag, task
from src.alerts.telegram import send_telegram_message
from src.monitoring.checks import find_stale_symbols, build_daily_summary
from src.storage.db_client import get_db_connection

@dag(
    dag_id = "daily_checks",
    schedule = "@daily",
    start_date = datetime(2026, 1, 1),
    catchup = False,
    default_args = {"retries": 1, "retry_daily": timedelta(minutes = 5)},
    tags = ["crypto"]
)
def crypto_daily_checks():

    @task(task_id = "check_data_is_fresh")
    def check_data_is_fresh():
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT symbol, MAX(minute_start) FROM crypto_candles GROUP BY symbol;
                """
            )
            rows = cursor.fetchall()
            cursor.close()
        finally:
            conn.close()
        stale_symbols = find_stale_symbols(rows, datetime.utcnow())
        if len(stale_symbols) == 0:
            raise ValueError(f"No fresh data for: {stale_symbols}")

    @task(task_id = "delete_old_data")
    def delete_old_data():
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM crypto_candles WHERE minute_start < NOW() - INTERVAL '90 days';
                """
            )
            conn.commit()
            cursor.close()
        finally:
            conn.close()

    @task(task_id = "send_daily_summary", trigger_rule = "all_done")
    def send_daily_summary():
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT symbol, COUNT(*) FROM crypto_anomalies 
                WHERE detected_at >= NOW() - INTERVAL '1 day';
                GROUP BY symbol;
                """
            )
            rows = cursor.fetchall()
            cursor.close()
        finally:
            conn.close()
        send_telegram_message(build_daily_summary(rows))

crypto_daily_checks()