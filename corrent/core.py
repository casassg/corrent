from airflow.models.baseoperator import BaseOperator, LoggingMixin

from corrent.functional_operator_mixin import FunctionalOperatorMixin

def inject():
  """Injection method. It adds FunctionalOperatorMixin as a base of the BaseOperator.
  This enables the functional API for Airflow.
  """
  if CallOperatorMixin not in BaseOperator.__bases__:
    BaseOperator.__bases__ = (CallOperatorMixin, *BaseOperator.__bases__)