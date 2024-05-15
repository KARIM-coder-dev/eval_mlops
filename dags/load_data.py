"""
Module pour charger des données depuis une API et les stocker dans une base de données PostgreSQL.
"""

import os
import json
from datetime import datetime, timedelta
import time
import requests
from urllib.parse import urlparse, parse_qs
import re
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from db_init import Database
import pandas as pd

logger = logging.getLogger(__name__)

DATA_PATH = f"/opt/airflow/data/"
URL_FILE = os.path.join(DATA_PATH, "api", "url.json")
RESULTS_FILE = os.path.join(DATA_PATH, "api", "results.json")


def rename_columns(columns):
    """
    Renomme les colonnes.
    """
    columns = [col.lower() for col in columns]

    rgxs = [
        (r"[°|/|']", "_"),
        (r"²", "2"),
        (r"[(|)]", ""),
        (r"é|è", "e"),
        (r"â", "a"),
        (r"^_", "dpe_"),
        (r"_+", "_"),
    ]

    for rgx in rgxs:
        columns = [re.sub(rgx[0], rgx[1], col) for col in columns]
    return columns


def check_environment_setup():
    """
    Vérifie la configuration de l'environnement.
    """
    logger.info("--" * 20)
    logger.info(f"[info logger] cwd: {os.getcwd()}")
    assert os.path.isfile(URL_FILE)
    logger.info(f"[info logger] URL_FILE: {URL_FILE}")
    logger.info("--" * 20)


def interrogate_api():
    """
    Interroge l'API pour obtenir des données.
    """
    # test url file exists
    assert os.path.isfile(URL_FILE)
    # open url file
    with open(URL_FILE, encoding="utf-8") as file:
        url = json.load(file)
    assert url.get("url") is not None
    assert url.get("payload") is not None

    # make GET requests
    results = requests.get(url.get("url"), params=url.get("payload"), timeout=5)
    assert results.raise_for_status() is None

    data = results.json()

    # save results to file
    with open(RESULTS_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


def save_postgresdb():
    """
    Sauvegarde les données dans la base de données PostgreSQL.
    """
    assert os.path.isfile(RESULTS_FILE)
    logger.info("openning result file")
    # read previous API call output
    with open(RESULTS_FILE, encoding="utf-8") as file:
        data = json.load(file)

    logger.info("dataframe creation")
    data = pd.DataFrame(data["results"])

    # set columns
    logger.info("dataframe creation")
    new_columns = rename_columns(data.columns)
    data.columns = new_columns
    logger.info(data.columns.tolist())
    data = data.astype(str).replace("nan", "")
    logger.info("dataframe cleaning")
    # now check that the data does not have columns not already in the table
    # store data into table
    logger.info("dataframe storing")
    data["payload"] = data.apply(lambda row: json.dumps(row.to_dict()), axis=1)

    # Create a new DataFrame with necessary columns
    df = data[["n_dpe", "payload"]]
    db = Database()

    try:
        df.to_sql(name="dpe_logement", con=db.engine, if_exists="append", index=False)
        logger.info("Data successfully stored to the database.")
    except Exception as e:
        logger.error(f"An error occurred while storing data: {e}")
    finally:
        db.close()

    logger.info("Database operation completed")


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "check_interrogate_save",
    default_args=default_args,
    description="Get & store ademe data to database",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    check_environment_setup_task = PythonOperator(
        task_id="check_environment_setup",
        python_callable=check_environment_setup,
    )

    interrogate_api_task = PythonOperator(
        task_id="interrogate_api",
        python_callable=interrogate_api,
    )

    # Define the task to store results to database
    store_results_task = PythonOperator(
        task_id="save_postgresdb",
        python_callable=save_postgresdb,
    )

    check_environment_setup_task >> interrogate_api_task >> store_results_task
