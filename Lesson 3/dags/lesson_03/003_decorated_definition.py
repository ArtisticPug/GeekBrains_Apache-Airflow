from airflow.models import DAG

from util.settings import default_settings
from util.deco import python_operator


@python_operator()
def decorated_python_operator_definition(value, **context):
    print('Hey I\'m a decorated operator. Value = ', value)


with DAG(**default_settings()) as dag:
    decorated_python_operator_definition()
