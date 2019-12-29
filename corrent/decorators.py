import functools
from typing import Callable, Optional

from airflow.operators.python_operator import PythonOperator

from corrent.core import inject
from corrent.operators import PythonFunctionalOperator

def operation(*args, **kwargs):
  if len(args)>1 and not callable(args[0]):
    raise Exception('No args allowed to specify arguments for PythonOperator')

  def wrapper(f):
    """Python wrapper to generate PythonOperators out of simple python functions.
    Used for Airflow functional interface
    """
    inject()
    return PythonFunctionalOperator(python_callable=f, task_id=f.__name__, **kwargs)
  if len(args) == 1 and callable(args[0]):
    return wrapper(args[0])
  return wrapper