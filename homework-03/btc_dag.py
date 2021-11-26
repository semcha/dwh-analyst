from datetime import datetime, timedelta
from decimal import Decimal
import logging
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook


with DAG(
    dag_id="btc_dag",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    schedule_interval="*/30 * * * *",
    start_date=datetime(2021, 11, 25),
    catchup=False,
) as dag:

    @task()
    def request_data():
        url = "https://api.coincap.io/v2/rates/bitcoin"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception("Invalid Response!")
        json_data = response.json()
        if "data" not in json_data:
            raise Exception("Invalid JSON!")
        return json_data

    @task()
    def insert_data(btc_json):
        pg_hook = PostgresHook(postgres_conn_id="otus_postgresql")
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        json_data = btc_json.get("data")
        try:
            rate_usd = Decimal(json_data.get("rateUsd"))
        except Exception:
            logging.error("Can't convert rateUsd to Decimal!")
        row = (
            json_data.get("symbol"),
            rate_usd,
            btc_json.get("timestamp"),
            datetime.utcnow(),
        )
        cursor.execute(
            """insert into btc_data (symbol, rate_usd, site_timestamp, updated_at) values (%s, %s, %s, %s)""",
            row,
        )
        pg_conn.commit()

    btc_data = request_data()
    insert_data(btc_data)
