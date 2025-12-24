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
import os
import pickle
from collections.abc import Callable, Sequence
from shlex import quote
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import dill
from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.python_kubernetes_script import (
    write_python_script,
)
from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context

_PYTHON_SCRIPT_ENV = "__PYTHON_SCRIPT"
_PYTHON_INPUT_ENV = "__PYTHON_INPUT"


def _generate_decoded_command(env_var: str, file: str) -> str:
    return (
        f'python -c "import base64, os;'
        rf"x = base64.b64decode(os.environ[\"{env_var}\"]);"
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

    def __init__(self, namespace: str | None = None, use_dill: bool = False, **kwargs) -> None:
        self.use_dill = use_dill

        # If the name was not provided, we generate operator name from the python_callable
        # we also instruct operator to add a random suffix to avoid collisions by default
        op_name = kwargs.pop("name", f"k8s-airflow-pod-{kwargs['python_callable'].__name__}")
        random_name_suffix = kwargs.pop("random_name_suffix", True)
        super().__init__(
            namespace=namespace,
            name=op_name,
            random_name_suffix=random_name_suffix,
            cmds=["placeholder-command"],
            **kwargs,
        )

    def _generate_cmds(self) -> list[str]:
        script_filename = "/tmp/script.py"
        input_filename = "/tmp/script.in"

        if getattr(self, "do_xcom_push", False):
            output_filename = "/airflow/xcom/return.json"
            make_xcom_dir_cmd = "mkdir -p /airflow/xcom"
        else:
            output_filename = "/dev/null"
            make_xcom_dir_cmd = ":"  # shell no-op

        write_local_script_file_cmd = (
            f"{_generate_decoded_command(quote(_PYTHON_SCRIPT_ENV), quote(script_filename))}"
        )
        write_local_input_file_cmd = (
            f"{_generate_decoded_command(quote(_PYTHON_INPUT_ENV), quote(input_filename))}"
        )
        exec_python_cmd = f"python {script_filename} {input_filename} {output_filename}"
        return [
            "bash",
            "-cx",
            (
                f"{write_local_script_file_cmd} && "
                f"{write_local_input_file_cmd} && "
                f"{make_xcom_dir_cmd} && "
                f"{exec_python_cmd}"
            ),
        ]

    def execute(self, context: Context):
        with TemporaryDirectory(prefix="venv") as tmp_dir:
            pickling_library = dill if self.use_dill else pickle
            script_filename = os.path.join(tmp_dir, "script.py")
            input_filename = os.path.join(tmp_dir, "script.in")

            with open(input_filename, "wb") as file:
                pickling_library.dump({"args": self.op_args, "kwargs": self.op_kwargs}, file)

            py_source = self.get_python_source()
            jinja_context = {
                "op_args": self.op_args,
                "op_kwargs": self.op_kwargs,
                "pickling_library": pickling_library.__name__,
                "python_callable": self.python_callable.__name__,
                "python_callable_source": py_source,
                "string_args_global": False,
            }
            write_python_script(jinja_context=jinja_context, filename=script_filename)

            self.env_vars: list[k8s.V1EnvVar] = [
                *self.env_vars,
                k8s.V1EnvVar(name=_PYTHON_SCRIPT_ENV, value=_read_file_contents(script_filename)),
                k8s.V1EnvVar(name=_PYTHON_INPUT_ENV, value=_read_file_contents(input_filename)),
            ]

            self.cmds = self._generate_cmds()
            return super().execute(context)


def kubernetes_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Kubernetes operator decorator.

    This wraps a function to be executed in K8s using KubernetesPodOperator.
    Also accepts any argument that KubernetesPodOperator will via ``kwargs``. Can be
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
