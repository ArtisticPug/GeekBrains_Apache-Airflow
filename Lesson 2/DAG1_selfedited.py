import os
import datetime as dt
import pandas as pd
# import logging
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


settings = {
    'dag_id': 'titanic_pivot_self_edited',  # Имя DAG
    'schedule_interval': None,  # Периодичность запуска, например, "00 15 * * *"
    'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
    'default_args': {  # базовые аргументы DAG
        'retries': 1,  # Количество повторений в случае неудач
        'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
        }
}

# Функция чтобы логировать с использованием тумплейта не получилась
# def finish():
#     logging.info(f'Pipeline finished! Execution date is {{ ds }}')


def make_folder():
    try:
        os.mkdir(f"{os.path.expanduser('~')}/dag_data")
    except FileExistsError:
        pass


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), f'dag_data/{file_name}')


def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')


def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))


def mean_fare_per_class():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.groupby(['Pclass']).agg({
        'Fare': 'mean',  # Средняя цена билета на каждый из классов
        'Survived': 'sum',  # Кол-во выживших (ради интереса)
        'Age': 'mean'  # Средний возраст (ради интереса)
        }).reset_index()
    df.to_csv(get_path('titanic_mean_fares.csv'))


# В контексте DAG'а зададим набор task'ок
# Объект-инстанс Operator'а - это и есть task
with DAG(**settings) as dag:
    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )

    create_titanic_folder = PythonOperator(
        task_id='make_directory',
        python_callable=make_folder
    )
    # Загрузка датасета
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset
    )
    # Чтение, преобразование и запись датасета
    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_dataset',
        python_callable=pivot_dataset
    )
    # Чтение, аггрегация, подсчет средней стоимости билета и запись датасета
    mean_fares_titanic_dataset = PythonOperator(
        task_id='mean_fares_dataset',
        python_callable=mean_fare_per_class
    )
    # STDOUT
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ ds }}"'
    )
    # Вот как jinja темплейты типа {{ ds }} реализовать в питоне, пока не понял
    # last_task = PythonOperator(
    #     task_id='last_task',
    #     python_callable=finish
    # )
    # Порядок выполнения тасок
    first_task >> create_titanic_folder >> create_titanic_dataset >> (pivot_titanic_dataset, mean_fares_titanic_dataset)
    (pivot_titanic_dataset, mean_fares_titanic_dataset) >> last_task
