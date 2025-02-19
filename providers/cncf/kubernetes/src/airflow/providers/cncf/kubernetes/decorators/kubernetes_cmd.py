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

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Callable

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


logger = logging.getLogger(__name__)


class _KubernetesCmdDecoratedOperator(DecoratedOperator, KubernetesPodOperator):
    custom_operator_name = "@task.kubernetes_cmd"

    # `cmds` and `arguments` are used internally by the operator
    template_fields: Sequence[str] = tuple(
        {"op_args", "op_kwargs", *KubernetesPodOperator.template_fields} - {"cmds", "arguments"}
    )

    # Since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    def __init__(self, namespace: str | None = None, args_only: bool = False, **kwargs) -> None:
        self.args_only = args_only

        # If the name was not provided, we generate operator name from the python_callable
        # we also instruct operator to add a random suffix to avoid collisions by default
        op_name = kwargs.pop("name", f"k8s-airflow-pod-{kwargs['python_callable'].__name__}")
        random_name_suffix = kwargs.pop("random_name_suffix", True)
        if kwargs.pop("cmds", None) is not None:
            logger.warning(
                "The 'cmds' and 'arguments' are unused in @task.kubernetes_cmd decorator. "
                "You should return a list of commands or default cmd arguments with "
                "args_only=True from the python_callable."
            )

        super().__init__(
            namespace=namespace,
            name=op_name,
            random_name_suffix=random_name_suffix,
            cmds=["placeholder-command"],
            arguments=["placeholder-argument"],
            **kwargs,
        )

    def _generate_cmds(self) -> list[str]:
        generated_cmds = self.python_callable(*self.op_args, **self.op_kwargs)
        func_name = self.python_callable.__name__
        if not isinstance(generated_cmds, list):
            raise TypeError(
                f"Expected python_callable to return a list of strings, but got {type(generated_cmds)}"
            )
        if not all(isinstance(cmd, str) for cmd in generated_cmds):
            raise TypeError(f"Expected {func_name} to return a list of strings, but got {generated_cmds}")
        if not generated_cmds:
            raise ValueError(f"The {func_name} returned an empty list of commands")

        return generated_cmds

    def execute(self, context: Context):
        if self.args_only:
            self.cmds = []
            self.arguments = self._generate_cmds()
        else:
            self.cmds = self._generate_cmds()
            self.arguments = []
        return super().execute(context)


def kubernetes_task_cmd(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Kubernetes cmd operator decorator.

    This wraps a function which should return command to be executed
    in K8s using KubernetesPodOperator. The function should return a list of strings.
    If args_only is set to True, the function should return a list of arguments for
    container default command. Also accepts any argument that KubernetesPodOperator
    will via ``kwargs``. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with
        keys as XCom keys. Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_KubernetesCmdDecoratedOperator,
        **kwargs,
    )
