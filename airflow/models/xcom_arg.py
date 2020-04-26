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

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.models.xcom import XCOM_RETURN_KEY


class XComArg:
    """
    Class that represents a XCom push from a previous operator.
    Defaults to "return_value" as only key.

    Current implementations supports
      xcomarg >> op
      xcomarg << op
      op >> xcomarg   (by BaseOperator code)
      op << xcomarg   (by BaseOperator code)

    **Example**:
        The moment you got result from any op (functional or regular one) you can ::
            xcomarg = ...
            my_op = MyOperator()
            my_op >> xcomarg

    This object can be used in legacy Operators via Jinja:
    **Example**:
        You can make this result to be part of any generated string ::
            xcomarg = ...
            op1 = MyOperator(my_text_message=f"{xcomarg}")
            op2 = MyOperator(my_text_message=f"{xcomarg['topic']}")


    """

    def __init__(self, operator: BaseOperator, key: Optional[str] = XCOM_RETURN_KEY):
        self._operator = operator
        # _key is used for xcom pull (key in the XCom table)
        self._key = key

    def __eq__(self, other):
        return (self._operator == other.operator
                and self.key == other.key)

    def __lshift__(self, other):
        """
        Implements xcomresult << op
        """
        self.set_upstream(other)
        return self

    def __rshift__(self, other):
        """
        Implements xcomresult << op
        """
        self.set_downstream(other)
        return self

    @property
    def operator(self) -> BaseOperator:
        """Operator"""
        return self._operator

    @property
    def key(self):
        return self._key

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
        this function likely to run during at op.execute() context
        """
        resolved_value = self._operator.xcom_pull(
            context=context,
            task_ids=[self._operator.task_id],
            key=self.key,
            dag_id=self._operator.dag.dag_id,
        )
        if not resolved_value:
            raise AirflowException(
                f'XComArg result from {self._operator.task_id} at {self._operator.dag.dag_id} '
                f'with key="{self.key}"" is not found!')
        resolved_value = resolved_value[0]

        return resolved_value

    def __str__(self):
        """
        Backward compatibility for old-style jinja used in Airflow Operators
        **Example**: to use XArg at BashOperator::
            BashOperator(cmd=f"... { xcomarg } ...")
        :return:
        """
        xcom_pull_kwargs = [f"task_ids='{self._operator.task_id}'",
                            f"dag_id='{self._operator.dag.dag_id}'",
                            ]
        if self.key is not None:
            xcom_pull_kwargs.append(f"key='{self.key}'")

        xcom_pull_kwargs = ", ".join(xcom_pull_kwargs)
        xcom_pull = f"task_instance.xcom_pull({xcom_pull_kwargs})"
        return xcom_pull
