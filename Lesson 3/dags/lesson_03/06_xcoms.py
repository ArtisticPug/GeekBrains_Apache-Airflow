import logging
from airflow.models import DAG

from util.settings import default_settings
from util.deco import python_operator


@python_operator()
def returns_something(**context):
    return "RESULT"


@python_operator()
def pushes_something_to_xcom(**context):
    context['task_instance'].xcom_push('my_xcom_key', 'my_xcom_value')


@python_operator()
def pulls_something_from_xcom(**context):
    xcom_value = context['task_instance'].xcom_pull(task_ids="pushes_something_to_xcom", key='my_xcom_key')
    logging.info('XCom value: "%s"', xcom_value)


with DAG(**default_settings()) as dag:
    returns_something()
    pushes_something_to_xcom() >> pulls_something_from_xcom()
