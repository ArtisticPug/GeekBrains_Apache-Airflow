import os
import json
import pandas as pd
from sqlalchemy import create_engine


def to_postgres(table_name, df):
    alchemyEngine = create_engine('postgresql+psycopg2://vladimir:1@127.0.0.1:5432/data_warehouse', pool_recycle=3600)
    postgreSQLConnection = alchemyEngine.connect()
    postgreSQLTable = table_name
    try:
        df.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail')
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("PostgreSQL Table %s has been created successfully." % postgreSQLTable)
    finally:
        postgreSQLConnection.close()


def make_folder():
    try:
        os.mkdir(f"{os.path.expanduser('~')}/dag_data")
    except FileExistsError:
        pass


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), f'dag_data/{file_name}')


def download_titanic_dataset(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    # df.to_csv(get_path('titanic.csv'), encoding='utf-8')
    df = df.to_json()
    # df = json.loads(df)
    # df.to_csv(get_path('titanic.csv'), encoding='utf-8')
    context['task_instance'].xcom_push(key='dataframe', value=df)


def pivot_dataset(**context):
    # df = pd.read_csv(get_path('titanic.csv'))
    df = context['task_instance'].xcom_pull(task_ids='download_titanic_dataset', key='dataframe')
    df = pd.read_json(df)
    df = df.pivot_table(index=['Sex'],
                        columns=['Pclass'],
                        values='Name',
                        aggfunc='count').reset_index()
    # context['task_instance'].xcom_push(key='pivotdataframe', value=df)
    to_postgres('titanic_pivot', df)
    # df.to_csv(get_path('titanic_pivot.csv'))


def mean_fare_per_class(**context):
    # df = pd.read_csv(get_path('titanic.csv'))
    df = context['task_instance'].xcom_pull(task_ids='download_titanic_dataset', key='dataframe')
    df = pd.read_json(df)
    df = df.groupby(['Pclass']).agg({
        'Fare': 'mean',  # Средняя цена билета на каждый из классов
        'Survived': 'sum',  # Кол-во выживших (ради интереса)
        'Age': 'mean'  # Средний возраст (ради интереса)
        }).reset_index()
    # context['task_instance'].xcom_push(key='meandataframe', value=df)
    to_postgres('mean_fares', df)
    # df.to_csv(get_path('titanic_mean_fares.csv'))
