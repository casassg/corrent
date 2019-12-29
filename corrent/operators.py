from copy import deepcopy

from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from corrent.functional_operator_mixin import FunctionalOperatorMixin


class PythonFunctionalOperator(PythonOperator):
  def __call__(self, *args, **kwargs):
    return super(PythonFunctionalOperator,
                 self).__call__(op_args=args, op_kwargs=kwargs)

  def copy(self, task_id: str):
    return PythonFunctionalOperator(
        python_callable=self.python_callable, task_id=task_id
    )
