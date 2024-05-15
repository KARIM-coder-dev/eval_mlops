# MLOPS Ynov

Repo pour le cours de MLops

Construire un pipeline de prediction sur les données DPE tertiaire, calcul 3CL, de l'Ademe

- VM azure pour faire tourner Airflow et MLflow
- DAGS Airflow pour
    - extraire les données de l'API ADEME et les stocker brutes dans une base postgresql
    - transformer les données brutes en données de training (ints)
    - regulierement entrainer un modele (challenger) et promouvoir le meilleur modele en production (champion)
    - Creer une API avec FAstAPI pour faire des predictions
    - UI streamlit de saisie des données utilisateur

L'utilisateur obtient une prediction de chaque etiquette DPE
