from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from my_util.my_funcs import make_folder, download_titanic_dataset, pivot_dataset, mean_fare_per_class
from my_util.settings import default_settings


with DAG(**default_settings()) as dag:
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
    # Порядок выполнения тасок
    first_task >> create_titanic_folder >> create_titanic_dataset >> (pivot_titanic_dataset, mean_fares_titanic_dataset)
    (pivot_titanic_dataset, mean_fares_titanic_dataset) >> last_task
