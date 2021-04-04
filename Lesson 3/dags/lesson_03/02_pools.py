import logging
import time

from airflow.models import DAG

from util.settings import default_settings
from util.deco import python_operator


@python_operator()
def sleepy_operator(**context):
    time.sleep(30)


with DAG(**default_settings()) as dag:
    sleepy_operator(task_id='first_tired_task')
    sleepy_operator(task_id='second_tired_task')
    sleepy_operator(task_id='third_tired_task')
    sleepy_operator(task_id='fourth_tired_task')
