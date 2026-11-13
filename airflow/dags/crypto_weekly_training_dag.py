from datetime import datetime
from airflow.decorators import dag, task
from src.config import SYMBOLS
from src.ml.download_history import download_history
from src.ml.train_models import train_model_for_symbol

@dag(
    dag_id = "weekly_training",
    schedule = "@weekly",
    start_date = datetime(2026, 1, 1),
    catchup = False,
    tags = ["crypto", "ml"]
)
def crypto_weekly_training():

    @task(task_id = "download_recent_history")
    def download_recent_history():
        for symbol in SYMBOLS:
            download_history(symbol, days = 7)

    @task(task_id = "train_models")
    def train_models():
        for symbol in SYMBOLS:
            train_model_for_symbol(symbol)

    download_recent_history() >> train_models()

crypto_weekly_training()