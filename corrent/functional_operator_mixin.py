from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow import settings

from corrent.xcom_arg import XComArg


def _resolve(context: dict, value: Any, recursion=True):
  """Given an object "value", it resolves if it's a XComArg or does recursion if list or tuple.
  """
  if isinstance(value, XComArg):
    return value.resolve(context)
  elif recursion and isinstance(value, tuple):
    return tuple(_resolve(context, i, False) for i in value)
  elif recursion and isinstance(value, dict):
    return {k: _resolve(context, v, False) for k, v in value.items()}
  else:
    return value


def _add_dependent(value: Any, dependent: BaseOperator, recursion: bool = True):
  """Given an object "value", it adds a dependent if it's an XComArg.

  Recurses tuples and dictionaries.
  """
  if isinstance(value, XComArg):
    value.add_dependent(dependent)
  elif recursion and isinstance(value, tuple):
    [_add_dependent(i, dependent, False) for i in value]
  elif recursion and isinstance(value, dict):
    [_add_dependent(v, dependent, False) for v in value.values()]
  else:
    pass


class FunctionalOperatorMixin:
  """Mixin to enable functional behaviour on operators. 

  It enables on execution injection of variables based on XCom and it parses for data dependencies.
  """
  def __call__(self, *args, **kwargs):
    """Implements call operation for operators. Only accepts kwargs. This kwargs will be added to the instance
    as attributes before pre_execute and execute methods. If they are XComArgs, they will be resolved.

    Returns:
      XComArg for the current operator. Defaults to "return_value". To specify a different XCom use res["xcom_2"]
    """
    if args:
      raise Exception('No arguments are accepted, use kwargs only')
    if getattr(self, '_has_been_called', False):
      raise Exception('Operators can only be called once')
    setattr(self, '_has_been_called', True)
    [_add_dependent(v, self) for v in kwargs.values()]
    original_pre_exec = self.pre_execute
    original_exec = self.execute

    def custom_pre_execute(context):
      resolved = {k: _resolve(context, v) for k, v in kwargs.items()}
      self.__dict__.update(resolved)
      return original_pre_exec(context)

    def custom_execute(context):
      resolved = {k: _resolve(context, v) for k, v in kwargs.items()}
      self.__dict__.update(resolved)
      return original_exec(context)

    self.execute = custom_execute
    self.pre_execute = custom_pre_execute

    return XComArg(self)
