from airflow.models import DAG
from util.settings import default_settings
from util.dummy import dummy


with DAG(**default_settings()) as dag:
    dummy('hello-world')


