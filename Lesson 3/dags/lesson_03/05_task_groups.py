from airflow.models import DAG
from airflow.utils.task_group import TaskGroup

from util.settings import default_settings
from util.dummy import dummy


with DAG(**default_settings()) as dag:
    with TaskGroup('first_group') as first_group:
        task1 = dummy('first')
        task2 = dummy('second')

    with TaskGroup('second_group') as first_group:
        task3 = dummy('third')

    with TaskGroup('third_group') as first_group:
        task4 = dummy('fourth')
        task5 = dummy('fifth')

    (task1, task2) >> task3 >> (task4, task5)
