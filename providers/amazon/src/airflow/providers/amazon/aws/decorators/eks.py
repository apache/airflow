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

from airflow.providers.amazon.aws.operators.eks import EksPodOperator
from airflow.providers.cncf.kubernetes.python_kubernetes_script import write_python_script
from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)

if TYPE_CHECKING:
    from airflow.sdk import Context

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


class _EksPodDecoratedOperator(DecoratedOperator, EksPodOperator):
    """Wrap a Python callable to run inside a pod on Amazon EKS via ``EksPodOperator``."""

    custom_operator_name = "@task.eks_pod"

    # `cmds` and `arguments` are used internally by the operator
    template_fields: Sequence[str] = tuple(
        {"op_args", "op_kwargs", *EksPodOperator.template_fields} - {"cmds", "arguments"}
    )

    # Since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    def __init__(self, *, cluster_name: str, use_dill: bool = False, **kwargs) -> None:
        self.use_dill = use_dill

        # Accept the EKS ``pod_name`` or the K8s-style ``name``, otherwise derive one from the callable.
        pod_name = (
            kwargs.pop("pod_name", None)
            or kwargs.pop("name", None)
            or f"eks-airflow-pod-{kwargs['python_callable'].__name__}"
        )
        random_name_suffix = kwargs.pop("random_name_suffix", True)
        super().__init__(
            cluster_name=cluster_name,
            pod_name=pod_name,
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


def eks_pod_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Run a Python function in a pod on an Amazon EKS cluster.

    Wraps :class:`~airflow.providers.amazon.aws.operators.eks.EksPodOperator`, which builds the
    cluster kubeconfig and a short-lived token from the AWS connection (``aws_conn_id``), so the
    worker needs no ``aws`` CLI or kubeconfig. Any ``EksPodOperator`` argument is accepted via
    ``kwargs``.

    :param python_callable: Function to decorate
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with
        keys as XCom keys. Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_EksPodDecoratedOperator,
        **kwargs,
    )
