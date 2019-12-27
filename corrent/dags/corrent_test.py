"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta
from typing import List

from airflow import DAG

from corrent.decorators import operation, copy_operation
from corrent.core import inject

# Injecting corrent code to Airflow operators to enable functional API
inject()

default_args = {
    'owner': 'Corrent',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


@operation
def generate_list(length: int = 5) -> List[int]:
  return list(range(length))


@operation
def add_one_list(int_list: List[int]) -> List[int]:
  return [i + 1 for i in int_list]


@operation
def print_result(result: List[int]) -> None:
  print(result)


with DAG(
    'corrent_test', default_args=default_args, schedule_interval=None
) as dag:
  l = generate_list()
  print_generated = copy_operation(print_result, "print_generated")
  print_generated(l)
  l1 = add_one_list(l)
  print_result(l1)
