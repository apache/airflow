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
from textwrap import dedent
from typing import TYPE_CHECKING, Callable, Optional, Sequence, TypeVar

import dill
from kubernetes.client import models as k8s

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
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


def _b64_encode_file(filename):
    with open(filename) as file_to_encode:
        return file_to_encode.read()


class _KubernetesDecoratedOperator(DecoratedOperator, KubernetesPodOperator):
    """
    Wraps a Python callable and executes in a kubernetes pod

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :type op_kwargs: dict
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :type op_args: list
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    :type multiple_outputs: bool
    """

    template_fields: Sequence[str] = ('op_args', 'op_kwargs')

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ('python_callable',)

    def __init__(
        self,
        use_dill=False,
        **kwargs,
    ) -> None:
        print("KUBERNETES EXECUTOR" + str(kwargs))
        command = "dummy command"
        self.pickling_library = dill if use_dill else pickle

        # Image, name and namespace are all required.
        if not 'image' in kwargs:
            kwargs['image'] = 'python'

        if not 'name' in kwargs:
            kwargs['name'] = 'test'

        if not 'namespace' in kwargs:
            kwargs['namespace'] = 'default'

        super().__init__(**kwargs)

    def write_python_script(
        jinja_context: dict,
        filename: str,
        render_template_as_native_obj: bool = False,
    ):
        """
        Renders the python script to a file to execute in the virtual environment.

        :param jinja_context: The jinja context variables to unpack and replace with its placeholders in the
            template file.
        :type jinja_context: dict
        :param filename: The name of the file to dump the rendered script to.
        :type filename: str
        :param render_template_as_native_obj: If ``True``, rendered Jinja template would be converted
            to a native Python object
        """
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.dirname(__file__))
        template_env: jinja2.Environment
        if render_template_as_native_obj:
            template_env = jinja2.nativetypes.NativeEnvironment(
                loader=template_loader, undefined=jinja2.StrictUndefined
            )
        else:
            template_env = jinja2.Environment(loader=template_loader, undefined=jinja2.StrictUndefined)
        template = template_env.get_template('python_kubernetes_script.jinja2')

        template.stream(**jinja_context).dump(filename)

    def execute(self, context: 'Context'):

        with TemporaryDirectory(prefix='venv') as tmp_dir:
            script_filename = os.path.join(tmp_dir, 'script.py')
            py_source = self._get_python_source()

            write_python_script(
                jinja_context=dict(
                    op_args=self.op_args,
                    op_kwargs=self.op_kwargs,
                    pickling_library=self.pickling_library.__name__,
                    python_callable=self.python_callable.__name__,
                    python_callable_source=py_source,
                    string_args_global=False,
                ),
                filename=script_filename,
            )

            self.env_vars.append(
                k8s.V1EnvVar(name="__PYTHON_SCRIPT", value=_b64_encode_file(script_filename))
            )

            self.cmds.append("bash")
            self.arguments.append("-cx")

            self.arguments.append(
                f'{_generate_decode_command("__PYTHON_SCRIPT", "/tmp/script.py")} && python /tmp/script.py && sleep 1000'
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
):
    """
    Kubernetes operator decorator. Wraps a function to be executed in K8s using KubernetesPodOperator.
    Also accepts any argument that DockerOperator will via ``kwargs``. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :type python_callable: Optional[Callable]
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. List/Tuples will unroll to xcom values
        with index as key. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    :type multiple_outputs: bool
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_KubernetesDecoratedOperator,
        **kwargs,
    )
