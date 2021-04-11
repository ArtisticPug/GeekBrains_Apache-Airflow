import datetime as dt
import json

import pandas as pd
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'vladimir',
    'retries': 1,
    'depends_on_past': False,
    'catchup': False
}


@dag(default_args=default_args, schedule_interval=None, start_date=dt.datetime(2021, 1, 1), tags=['Lesson_4'])
def lesson_4_DAG():

    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )

    # Загрузка датасета
    @task()
    def download_data():
        url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
        df = pd.read_csv(url)
        df = df.to_json()
        # df = json.loads(df)
        return df

    # Чтение, преобразование и запись датасета
    @task()
    def pivot_data(df):
        df = pd.read_json(df)
        df = df.pivot_table(index=['Sex'],
                            columns=['Pclass'],
                            values='Name',
                            aggfunc='count').reset_index()
        df = df.to_json()
        table_name = 'titanic_pivot'
        return table_name, df

    # Подсчет средней цены билета
    @task()
    def mean_fares(df):
        df = pd.read_json(df)
        df = df.groupby(['Pclass']).agg({
            'Fare': 'mean',  # Средняя цена билета на каждый из классов
        }).reset_index()
        df = df.to_json()
        table_name = 'mean_fares'
        return table_name, df

    # Загрузка данных в postgres с помощью hook
    @task()
    def hook(input):  # input = (table_name, df)
        # data_warehouse
        hook = BaseHook.get_hook(conn_id='data_warehouse')
        postgreSQLConnection = hook.get_sqlalchemy_engine()
        postgreSQLTable = input[0]
        df = pd.read_json(input[1])
        try:
            df.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail')
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
        else:
            print("PostgreSQL Table %s has been created successfully." %
                  postgreSQLTable)

    # BashOperator, закрывающий
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ ds }}"'
    )

    data = download_data()
    mean = mean_fares(data)
    pivot = pivot_data(data)
    mean_psg = hook(mean)
    pivot_psg = hook(pivot)

    first_task >> data >> (pivot, mean)
    (mean_psg, pivot_psg) >> last_task


DAG_4 = lesson_4_DAG()
