from typing import Any, List, Dict, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.models.xcom import XCOM_RETURN_KEY


class XComArg:
    """
    Class that represents a XCom push from a previous operator. Defaults to "return_value" as only key.
    """
    def __init__(self, operator: BaseOperator, keys: Optional[List[str]] = None):
        self._operator = operator
        self._keys = keys or [XCOM_RETURN_KEY]

    def set_upstream(self, t):
        self._operator.set_upstream(t)

    def set_downstream(self, t):
        self._operator.set_downstream(t)

    def __lshift__(self, other):
        self.set_upstream(other)

    def __rshift__(self, other):
        self.set_downstream(other)

    def resolve(self, context: dict):
        """
        Pull XCom value for the existing arg.
        """
        resolved_value = self._operator.xcom_pull(context=context, task_ids=[self._operator.task_id], key=self._keys[0], dag_id=self._operator.dag.dag_id)
        if not resolved_value:
            raise
        return resolved_value[0]
        # dict_result = {}
        # i = dict_result
        # for key in self._keys:
        #     # Always set resolved value - will be overridden until last iteration
        #     i[key] = resolved_value
        #     i = i[key]
        #
        # return dict_result

    # def __getitem__(self, key: str) -> 'XComArg':
    #     """Return an XComArg for the specified key and the same operator as the current one.
    # """
    #     return XComArg(self._operator, [key])


#  __iter__
#   return x[1]
#
#
#
# xcom[restul]["a"]["b"]    xcom (keys= "result, a, ,b") -> 3
# return {"a": {"b": 3}}
#
#
# XcomCombine( a= Xcom, b=Xcom)
