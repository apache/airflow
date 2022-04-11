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

import inspect
import os
import pickle
import uuid
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import TYPE_CHECKING, Callable, Optional, Sequence, TypeVar

from kubernetes.client import models as k8s

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.python_virtualenv import remove_task_decorator, write_python_script

if TYPE_CHECKING:
    from airflow.utils.context import Context


def _generate_decode_command(env_var, file):
    # We don't need `f.close()` as the interpreter is about to exit anyway
    return (
        f'python -c "import base64, os;'
        rf'x = os.environ[\"{env_var}\"];'
        rf'f = open(\"{file}\", \"w\"); f.write(x);"'
    )


def _read_file_contents(filename):
    with open(filename) as script_file:
        return script_file.read()


class _KubernetesDecoratedOperator(DecoratedOperator, KubernetesPodOperator):
    """
    Wraps a Python callable and executes in a kubernetes pod

    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    """

    template_fields: Sequence[str] = ('op_args', 'op_kwargs')

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ('python_callable',)

    def __init__(
        self,
        **kwargs,
    ) -> None:
        self.pickling_library = pickle

        # Set defaults for name and namespace.
        if 'name' not in kwargs:
            kwargs['name'] = f'k8s_airflow_pod_{uuid.uuid4().hex}'

        if 'namespace' not in kwargs:
            kwargs['namespace'] = 'default'

        super().__init__(**kwargs)

    def execute(self, context: 'Context'):

        with TemporaryDirectory(prefix='venv') as tmp_dir:
            script_filename = os.path.join(tmp_dir, 'script.py')
            py_source = self._get_python_source()

            jinja_context = dict(
                op_args=self.op_args,
                op_kwargs=self.op_kwargs,
                pickling_library=self.pickling_library.__name__,
                python_callable=self.python_callable.__name__,
                python_callable_source=py_source,
                string_args_global=False,
            )
            write_python_script(
                jinja_context=jinja_context,
                filename=script_filename,
                template_file='python_kubernetes_script.jinja2',
            )

            self.env_vars.append(
                k8s.V1EnvVar(name="__PYTHON_SCRIPT", value=_read_file_contents(script_filename))
            )

            self.cmds.append("bash")

            self.arguments.append("-cx")
            self.arguments.append(
                f'{_generate_decode_command("__PYTHON_SCRIPT", "/tmp/script.py")} && python /tmp/script.py'
            )

            return super().execute(context)

    def _get_python_source(self):
        raw_source = inspect.getsource(self.python_callable)
        res = dedent(raw_source)
        res = remove_task_decorator(res, "@task.kubernetes")
        return res


T = TypeVar("T", bound=Callable)


def kubernetes_task(
    python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs
) -> TaskDecorator:
    """
    Kubernetes operator decorator. Wraps a function to be executed in K8s using KubernetesPodOperator.
    Also accepts any argument that DockerOperator will via ``kwargs``. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_KubernetesDecoratedOperator,
        **kwargs,
    )
