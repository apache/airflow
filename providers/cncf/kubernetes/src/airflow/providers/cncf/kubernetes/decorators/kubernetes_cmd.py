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

import warnings
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    TaskDecorator,
    context_merge,
    task_decorator_factory,
)
from airflow.utils.operator_helpers import determine_kwargs

if TYPE_CHECKING:
    from airflow.utils.context import Context


class _KubernetesCmdDecoratedOperator(DecoratedOperator, KubernetesPodOperator):
    custom_operator_name = "@task.kubernetes_cmd"

    template_fields: Sequence[str] = tuple({"op_args", "op_kwargs", *KubernetesPodOperator.template_fields})

    overwrite_rtif_after_execution: bool = True

    def __init__(self, *, python_callable: Callable, args_only: bool = False, **kwargs) -> None:
        self.args_only = args_only

        cmds = kwargs.pop("cmds", None)
        arguments = kwargs.pop("arguments", None)

        if cmds is not None or arguments is not None:
            warnings.warn(
                f"The `cmds` and `arguments` are unused in {self.custom_operator_name} decorator. "
                "You should return a list of commands or image entrypoint arguments with "
                "args_only=True from the python_callable.",
                UserWarning,
                stacklevel=3,
            )

        # If the name was not provided, we generate operator name from the python_callable
        # we also instruct operator to add a random suffix to avoid collisions by default
        op_name = kwargs.pop("name", f"k8s-airflow-pod-{python_callable.__name__}")
        random_name_suffix = kwargs.pop("random_name_suffix", True)

        super().__init__(
            python_callable=python_callable,
            name=op_name,
            random_name_suffix=random_name_suffix,
            cmds=None,
            arguments=None,
            **kwargs,
        )

    def execute(self, context: Context):
        self.render_template_fields(context)

        generated = self._generate_cmds(context)
        if self.args_only:
            self.cmds = []
            self.arguments = generated
        else:
            self.cmds = generated
            self.arguments = []
        context["ti"].render_templates()  # type: ignore[attr-defined]
        return super().execute(context)

    def _generate_cmds(self, context: Context) -> list[str]:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        generated_cmds = self.python_callable(*self.op_args, **kwargs)
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


def kubernetes_cmd_task(
    python_callable: Callable | None = None,
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
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_KubernetesCmdDecoratedOperator,
        **kwargs,
    )
