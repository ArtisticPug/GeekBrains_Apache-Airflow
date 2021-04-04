import datetime as dt
import inspect
import pathlib


def default_settings():
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    filename = module.__file__

    settings = {
        'dag_id': pathlib.Path(filename).stem,  # Имя DAG достаем из имени файла
        'schedule_interval': '@daily',  # Периодичность запуска, например, "00 15 * * *"
        'catchup': False,  # Выполняем только последний запуск
        'default_args': {  # Базовые аргументы для каждого оператора
            'owner': 'vladimir',  # Информация о владельце DAG
            'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
            'retries': 1,  # Количество повторений в случае неудач
            'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
            'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
        }
    }

    return settings
