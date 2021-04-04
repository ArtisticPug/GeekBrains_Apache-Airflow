import logging

from airflow.models import DAG, Variable

from util.settings import default_settings
from util.deco import python_operator


@python_operator()
def please_echo_my_variable_unsafe(**context):
    test_variable = Variable.get("test_variable")
    logging.info('My test variable equals %s', test_variable)


@python_operator()
def please_echo_my_variable_safe(**context):
    try:
        test_variable = Variable.get("test_variable")
    except KeyError:
        logging.warning('My test variable is undefined')
    else:
        logging.info('My test variable equals %s', test_variable)


@python_operator()
def please_echo_my_macro(value, **context):
    logging.info('Received value: %s', value)


with DAG(**default_settings()) as dag:
    please_echo_my_variable_unsafe()
    please_echo_my_variable_safe()
    please_echo_my_macro(value='{{ var.value.test_variable }}')
