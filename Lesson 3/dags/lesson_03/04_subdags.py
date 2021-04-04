import logging
import time

from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

from util.settings import default_settings
from util.dummy import dummy


def subdag(parent_dag_name, child_dag_name, default_args):
    dag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=default_args,
        start_date=days_ago(2),
        schedule_interval="@daily",
    )

    for i in range(5):
        dummy('{}-task-{}'.format(child_dag_name, i + 1), dag=dag)

    return dag


with DAG(**default_settings()) as dag:
    section_1 = SubDagOperator(
        task_id='section-1',
        subdag=subdag('04_subdags', 'section-1', {}),
        dag=dag,
    )
