from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.models.xcom import XCOM_RETURN_KEY


class XComArg:
  """Class that represents a XCom push from a previous operator. Defaults to "return_value" as key.
  If you need a different key, use the dictionary accessor. Ex:

  arg = XComArg(op)
  arg2 = arg["pushed_value"]
  """

  def __init__(self, operator: BaseOperator, key: str = XCOM_RETURN_KEY):
    self._operator = operator
    self._key = key

  def add_dependent(self, downstream_operator: BaseOperator) -> None:
    """Add an operator to the downstream dependencies of the XCom implicit operator
    """
    self._operator.set_downstream(downstream_operator)

  def resolve(self, context: dict) -> Any:
    """Pull XCom value for the existing arg.
    """
    return self._operator.xcom_pull(
        context=context,
        task_ids=self._operator.task_id,
        key=self._key,
        dag_id=self._operator.dag.dag_id
    )

  def __getitem__(self, key: str) -> 'XComArg':
    """Return an XComArg for the specified key and the same operator as the current one.
    """
    return XComArg(self._operator, key)
