import functools
from typing import Callable, Optional

from airflow.operators.python_operator import PythonOperator

from corrent.core import inject


def operation(f):
  """Python wrapper to generate PythonOperators out of simple python functions.
  Used for Airflow functional interface
  """
  @functools.wraps(f)
  def python_operator_generate(
      *args, __operator_name_internal: Optional[str] = None, **kwargs
  ):
    inject()
    op = PythonOperator(
        python_callable=f, task_id=__operator_name_internal or f.__name__
    )
    return op(op_args=args, op_kwargs=kwargs)

  return python_operator_generate


def copy_operation(task: Callable, name: str):
  """Helper function to copy tasks created using the task decorator.
  """
  return functools.partial(task, __operator_name_internal=name)