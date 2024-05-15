from datetime import datetime, timedelta
import logging
import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from db_init import Database
import numpy as np
import json

logger = logging.getLogger(__name__)

# ---------------------------------------------
# instanciate class
# ---------------------------------------------

input_columns = [
    "etiquette_dpe",
    "etiquette_ges",
    "version_dpe",
    "type_energie_principale_chauffage",
    "type_energie_n_1",
]


def transform():
    db = Database()
    query = "select * from dpe_logement"
    data = pd.read_sql(query, con=db.engine)
    # handled by the DAG
    print(data.head())
    new_rows = []
    # Parcourir les lignes du DataFrame original
    for _, row in data.iterrows():
        n_dpe = row["n_dpe"]
        # Assurez-vous que le payload est un dictionnaire
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]

        # Création d'un dictionnaire pour les nouvelles entrées
        new_row = {"n_dpe": row["n_dpe"]}

        # Extraire les valeurs des clés désirées
        filtered_payload = {key: payload[key] for key in input_columns if key in payload}

        # Ajouter la nouvelle ligne à la liste
        new_rows.append({"n_dpe": n_dpe, "payload": json.dumps(filtered_payload)})

    df_new = pd.DataFrame(new_rows)

    # Stocker les nouvelles données dans une autre table
    df_new.to_sql("dpe_training", con=db.engine, if_exists="replace", index=False)

    db.close()


# ---------------------------------------------
#  DAG
# ---------------------------------------------
with DAG(
    "evalmlops_transform_data",
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=10),
    },
    description="Get raw data from dpe-tertiaire, transform and store into training_data",
    schedule="*/3 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ademe"],
) as dag:
    transform_data_task = PythonOperator(task_id="transform_data_task", python_callable=transform)

    transform_data_task
