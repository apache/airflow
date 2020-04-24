# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Any, Dict, List, Optional, Union

from airflow.models.baseoperator import BaseOperator
from airflow.models.xcom import XCOM_RETURN_KEY


class XComArg:
    """
    Class that represents a XCom push from a previous operator.
    Defaults to "return_value" as only key.
    """

    def __init__(self, operator: BaseOperator, keys: Optional[List[str]] = None):
        self._operator = operator
        self._keys = keys or [XCOM_RETURN_KEY]

    def __eq__(self, other):
        return self._operator == other.operator and self.keys == other.keys

    def __lshift__(self, other):
        self.set_upstream(other)
        return self

    def __rshift__(self, other):
        self.set_downstream(other)
        return self

    @property
    def operator(self):
        """Operator"""
        return self._operator

    @property
    def keys(self):
        return self._keys

    def set_upstream(self, task_or_task_list: Union[BaseOperator, List[BaseOperator]]):
        """
        Proxy to underlying operator set_upstream method
        """
        self._operator.set_upstream(task_or_task_list)

    def set_downstream(
        self, task_or_task_list: Union[BaseOperator, List[BaseOperator]]
    ):
        """
        Proxy to underlying operator set_downstream method
        """
        self._operator.set_downstream(task_or_task_list)

    def resolve(self, context: Dict) -> Any:
        """
        Pull XCom value for the existing arg.
        """
        resolved_value = self._operator.xcom_pull(
            context=context,
            task_ids=[self._operator.task_id],
            key=self._keys[0],
            dag_id=self._operator.dag.dag_id,
        )
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
