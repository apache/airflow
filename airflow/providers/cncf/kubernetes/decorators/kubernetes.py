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

import base64
import inspect
import os
import pickle
import uuid
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import TYPE_CHECKING, Callable, Sequence

import dill

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.python_kubernetes_script import (
    remove_task_decorator,
    write_python_script,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context

_FILENAME_IN_CONTAINER = "/tmp/script.py"
_INPUT_FILENAME_IN_CONTAINER = "/tmp/script.in"
_OUTPUT_FILENAME_IN_CONTAINER = "/airflow/xcom/return.json"


def _generate_decoded_command(val: str, file: str) -> str:
    return (
        f'python -c "import base64;'
        rf"x = base64.b64decode(\"{val}\");"
        rf'f = open(\"{file}\", \"wb\"); f.write(x); f.close()"'
    )


def _read_file_contents(filename: str) -> str:
    with open(filename, "rb") as script_file:
        return base64.b64encode(script_file.read()).decode("utf-8")


class _KubernetesDecoratedOperator(DecoratedOperator, KubernetesPodOperator):
    custom_operator_name = "@task.kubernetes"

    # `cmds` and `arguments` are used internally by the operator
    template_fields: Sequence[str] = tuple(
        {"op_args", "op_kwargs", *KubernetesPodOperator.template_fields} - {"cmds", "arguments"}
    )

    # Since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    def __init__(self, namespace: str = "default", use_dill: bool = False, **kwargs) -> None:
        self.pickling_library = dill if use_dill else pickle
        super().__init__(
            namespace=namespace,
            name=kwargs.pop("name", f"k8s_airflow_pod_{uuid.uuid4().hex}"),
            cmds=["dummy-command"],
            **kwargs,
        )

    def _get_python_source(self):
        raw_source = inspect.getsource(self.python_callable)
        res = dedent(raw_source)
        res = remove_task_decorator(res, "@task.kubernetes")
        return res

    def _generate_cmds(self, encoded_script_contents: str, encoded_input_contents: str) -> list[str]:
        return [
            "bash",
            "-cx",
            (
                f"{_generate_decoded_command('%s', '%s')} && "
                % (encoded_script_contents, _FILENAME_IN_CONTAINER)
                + f"{_generate_decoded_command('%s', '%s')} && "
                % (encoded_input_contents, _INPUT_FILENAME_IN_CONTAINER)
                + "mkdir -p /airflow/xcom && python "
                + f"{_FILENAME_IN_CONTAINER} {_INPUT_FILENAME_IN_CONTAINER} {_OUTPUT_FILENAME_IN_CONTAINER}"
            ),
        ]

    def execute(self, context: Context):
        with TemporaryDirectory(prefix="venv") as tmp_dir:
            script_filename = os.path.join(tmp_dir, "script.py")
            input_filename = os.path.join(tmp_dir, "script.in")

            with open(input_filename, "wb") as file:
                if self.op_args or self.op_kwargs:
                    self.pickling_library.dump({"args": self.op_args, "kwargs": self.op_kwargs}, file)

            py_source = self._get_python_source()
            jinja_context = {
                "op_args": self.op_args,
                "op_kwargs": self.op_kwargs,
                "pickling_library": self.pickling_library.__name__,
                "python_callable": self.python_callable.__name__,
                "python_callable_source": py_source,
                "string_args_global": False,
            }
            write_python_script(jinja_context=jinja_context, filename=script_filename)

            self.cmds = self._generate_cmds(
                _read_file_contents(script_filename), _read_file_contents(input_filename)
            )
            return super().execute(context)


def kubernetes_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """Kubernetes operator decorator.

    This wraps a function to be executed in K8s using KubernetesPodOperator.
    Also accepts any argument that DockerOperator will via ``kwargs``. Can be
    reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with
        keys as XCom keys. Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_KubernetesDecoratedOperator,
        **kwargs,
    )
