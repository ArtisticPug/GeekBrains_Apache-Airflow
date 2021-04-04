from airflow.operators.dummy_operator import DummyOperator


def dummy(task_id, **kwargs):
    return DummyOperator(task_id=task_id, **kwargs)
