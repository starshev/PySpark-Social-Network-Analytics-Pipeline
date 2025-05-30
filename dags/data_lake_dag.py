import os
from airflow.decorators import dag
from datetime import datetime as dt
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# переменные окружения
os.environ['HADOOP_CONF_DIR'] = '/.../hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/.../hadoop/conf'
os.environ['JAVA_HOME'] = '/...'
os.environ['SPARK_HOME'] = '/.../lib/spark'
os.environ['PYTHONPATH'] = '/.../local/lib/python3.8'

stg_path_events = '/.../data/geo/events'   # корневая директория таблицы событий в STG
ods_path_events = '/.../data/events'   # корневая директория таблицы событий в ODS
ods_path_geo_handbook = '/.../data/analytics/geo_handbook'   # директория гео-справочника городов
cdm_path_users_in_cities = '/.../data/analytics/users_in_cities'   # корневая директория витрины о нахождении юзеров в городах
cdm_path_events_in_cities = '/.../data/analytics/events_in_cities'   # корневая директория витрины о событиях в городах
cdm_path_users_matches = '/.../data/analytics/users_matches'   # корневая директория витрины для рекомендации друзей

default_args = {
    'owner' : 'airflow',
    'start_date' : dt(2025, 5, 30),
    'end_date' : dt(2025, 5, 30)
    }

@dag(default_args = default_args, schedule_interval = '0 0 * * *')
def data_lake_dag():

    # ежедневно переносим инкремент событий из STG в ODS
    events_stg_to_ods = SparkSubmitOperator(
        task_id = 'events_stg_to_ods',
        application = '/scripts/events_stg_to_ods.py',
        conn_id = 'yarn_spark',
        application_args = ['{{ ds }}', stg_path_events, ods_path_events]
        )

    # ежедневно рассчитывем витрину о нахождении юзеров в городах
    users_in_cities = SparkSubmitOperator(
        task_id = 'users_in_cities',
        application = '/scripts/users_in_cities.py',
        conn_id = 'yarn_spark',
        application_args = ['{{ ds }}', ods_path_events, ods_path_geo_handbook, cdm_path_users_in_cities]
        )

    # ежедневно рассчитывем витрину о кол-ве событий в городах
    events_in_cities = SparkSubmitOperator(
        task_id = 'events_in_cities',
        application = '/scripts/events_in_cities.py',
        conn_id = 'yarn_spark',
        application_args = ['{{ ds }}', ods_path_events, ods_path_geo_handbook, cdm_path_events_in_cities]
        )

    # ежедневно рассчитывем витрину для рекомендации друзей
    users_matches = SparkSubmitOperator(
        task_id = 'users_matches',
        application = '/scripts/users_matches.py',
        conn_id = 'yarn_spark',
        application_args = ['{{ ds }}', ods_path_events, ods_path_geo_handbook, cdm_path_users_matches]
        )

    events_stg_to_ods >> users_in_cities >> events_in_cities >> users_matches

data_lake_dag()