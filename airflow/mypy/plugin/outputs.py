#
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

from __future__ import annotations

from typing import Callable

from mypy.plugin import AttributeContext, MethodContext, Plugin
from mypy.types import AnyType, Type, TypeOfAny

OUTPUT_PROPERTIES = {
    "airflow.models.baseoperator.BaseOperator.output",
    "airflow.models.mappedoperator.MappedOperator.output",
}

TASK_CALL_FUNCTIONS = {
    "airflow.decorators.base.Task.__call__",
}


class OperatorOutputPlugin(Plugin):
    """Plugin to convert XComArg to the runtime type.

    This allows us to pass an *XComArg* to a downstream task, such as::

        @task
        def f(a: str) -> int:
            return len(a)

        f(op.output)  # "op" is an operator instance.
        f(g())  # "g" is a taskflow task.

    where the *a* argument of ``f`` should accept a *str* at runtime, but can be
    provided with an *XComArg* in the DAG.

    In the long run, it is probably a good idea to make *XComArg* a generic that
    carries information about the task's return type, and build the entire XCom
    mechanism into the type checker. But Python's type system is still limiting
    in this regard now, and (using the above example) we yet to have a good way
    to convert ``f``'s argument list from ``[str]`` to ``[XComArg[str] | str]``.
    Perhaps *ParamSpec* will be extended enough one day to accommodate this.
    """

    @staticmethod
    def _treat_as_any(context: AttributeContext | MethodContext) -> Type:
        """Pretend *XComArg* is actually *typing.Any*."""
        return AnyType(TypeOfAny.special_form, line=context.context.line, column=context.context.column)

    def get_attribute_hook(self, fullname: str) -> Callable[[AttributeContext], Type] | None:
        if fullname not in OUTPUT_PROPERTIES:
            return None
        return self._treat_as_any

    def get_method_hook(self, fullname: str) -> Callable[[MethodContext], Type] | None:
        if fullname not in TASK_CALL_FUNCTIONS:
            return None
        return self._treat_as_any


def plugin(version: str):
    return OperatorOutputPlugin
