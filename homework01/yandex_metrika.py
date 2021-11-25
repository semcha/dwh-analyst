import io
from datetime import datetime, date, timedelta

import requests
import pandas as pd
from loguru import logger


def get_stat_data(metrics, dimensions, filters, sort, start_date, end_date):
    # token = ""  # токен
    counter_id = "44147844"  # id счетчика
    url = "https://api-metrika.yandex.ru/stat/v1/data.csv"
    parameters = {
        "ids": counter_id,
        "metrics": metrics,
        "dimensions": dimensions,
        "filters": filters,
        "sort": sort,
        "date1": str(start_date),
        "date2": str(end_date),
        "accuracy": "full",
        "proposed_accuracy": False,
        "limit": 100000,
        "lang": "en",
    }
    # headers = {"Authorization": "OAuth " + token}
    response = requests.get(url, params=parameters)  # , headers=headers)
    df = pd.read_csv(io.StringIO(response.content.decode("utf-8")))
    df = df.drop(df.index[range(1)])  # Удаление Total'а
    return df
