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
from typing import TYPE_CHECKING, Any, Iterator, List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models.abstractoperator import AbstractOperator
from airflow.models.taskmixin import DAGNode, DependencyMixin
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.utils.context import Context
from airflow.utils.edgemodifier import EdgeModifier
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.operator import Operator


class XComArg(DependencyMixin):
    """
    Class that represents a XCom push from a previous operator.
    Defaults to "return_value" as only key.

    Current implementation supports
        xcomarg >> op
        xcomarg << op
        op >> xcomarg   (by BaseOperator code)
        op << xcomarg   (by BaseOperator code)

    **Example**: The moment you get a result from any operator (decorated or regular) you can ::

        any_op = AnyOperator()
        xcomarg = XComArg(any_op)
        # or equivalently
        xcomarg = any_op.output
        my_op = MyOperator()
        my_op >> xcomarg

    This object can be used in legacy Operators via Jinja.

    **Example**: You can make this result to be part of any generated string ::

        any_op = AnyOperator()
        xcomarg = any_op.output
        op1 = MyOperator(my_text_message=f"the value is {xcomarg}")
        op2 = MyOperator(my_text_message=f"the value is {xcomarg['topic']}")

    :param operator: operator to which the XComArg belongs to
    :param key: key value which is used for xcom_pull (key in the XCom table)
    """

    def __init__(self, operator: "Operator", key: str = XCOM_RETURN_KEY):
        self.operator = operator
        self.key = key

    def __eq__(self, other):
        return self.operator == other.operator and self.key == other.key

    def __getitem__(self, item: str) -> "XComArg":
        """Implements xcomresult['some_result_key']"""
        if not isinstance(item, str):
            raise ValueError(f"XComArg only supports str lookup, received {type(item).__name__}")
        return XComArg(operator=self.operator, key=item)

    def __iter__(self):
        """Override iterable protocol to raise error explicitly.

        The default ``__iter__`` implementation in Python calls ``__getitem__``
        with 0, 1, 2, etc. until it hits an ``IndexError``. This does not work
        well with our custom ``__getitem__`` implementation, and results in poor
        DAG-writing experience since a misplaced ``*`` expansion would create an
        infinite loop consuming the entire DAG parser.

        This override catches the error eagerly, so an incorrectly implemented
        DAG fails fast and avoids wasting resources on nonsensical iterating.
        """
        raise TypeError(f"{self.__class__.__name__!r} object is not iterable")

    def __str__(self):
        """
        Backward compatibility for old-style jinja used in Airflow Operators

        **Example**: to use XComArg at BashOperator::

            BashOperator(cmd=f"... { xcomarg } ...")

        :return:
        """
        xcom_pull_kwargs = [
            f"task_ids='{self.operator.task_id}'",
            f"dag_id='{self.operator.dag.dag_id}'",
        ]
        if self.key is not None:
            xcom_pull_kwargs.append(f"key='{self.key}'")

        xcom_pull_kwargs = ", ".join(xcom_pull_kwargs)
        # {{{{ are required for escape {{ in f-string
        xcom_pull = f"{{{{ task_instance.xcom_pull({xcom_pull_kwargs}) }}}}"
        return xcom_pull

    @property
    def roots(self) -> List[DAGNode]:
        """Required by TaskMixin"""
        return [self.operator]

    @property
    def leaves(self) -> List[DAGNode]:
        """Required by TaskMixin"""
        return [self.operator]

    def set_upstream(
        self,
        task_or_task_list: Union[DependencyMixin, Sequence[DependencyMixin]],
        edge_modifier: Optional[EdgeModifier] = None,
    ):
        """Proxy to underlying operator set_upstream method. Required by TaskMixin."""
        self.operator.set_upstream(task_or_task_list, edge_modifier)

    def set_downstream(
        self,
        task_or_task_list: Union[DependencyMixin, Sequence[DependencyMixin]],
        edge_modifier: Optional[EdgeModifier] = None,
    ):
        """Proxy to underlying operator set_downstream method. Required by TaskMixin."""
        self.operator.set_downstream(task_or_task_list, edge_modifier)

    @provide_session
    def resolve(self, context: Context, session: "Session" = NEW_SESSION) -> Any:
        """
        Pull XCom value for the existing arg. This method is run during ``op.execute()``
        in respectable context.
        """
        result = context["ti"].xcom_pull(
            task_ids=self.operator.task_id, key=str(self.key), default=NOTSET, session=session
        )
        if result is NOTSET:
            raise AirflowException(
                f'XComArg result from {self.operator.task_id} at {context["ti"].dag_id} '
                f'with key="{self.key}" is not found!'
            )
        return result

    @staticmethod
    def iter_xcom_args(arg: Any) -> Iterator["XComArg"]:
        """Return XComArg instances in an arbitrary value.

        This recursively traverse ``arg`` and look for XComArg instances in any
        collection objects, and instances with ``template_fields`` set.
        """
        if isinstance(arg, XComArg):
            yield arg
        elif isinstance(arg, (tuple, set, list)):
            for elem in arg:
                yield from XComArg.iter_xcom_args(elem)
        elif isinstance(arg, dict):
            for elem in arg.values():
                yield from XComArg.iter_xcom_args(elem)
        elif isinstance(arg, AbstractOperator):
            for elem in arg.template_fields:
                yield from XComArg.iter_xcom_args(elem)

    @staticmethod
    def apply_upstream_relationship(op: "Operator", arg: Any):
        """Set dependency for XComArgs.

        This looks for XComArg objects in ``arg`` "deeply" (looking inside
        collections objects and classes decorated with ``template_fields``), and
        sets the relationship to ``op`` on any found.
        """
        for ref in XComArg.iter_xcom_args(arg):
            op.set_upstream(ref.operator)
