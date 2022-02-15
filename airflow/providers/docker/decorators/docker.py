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

import base64
import inspect
import os
import pickle
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import TYPE_CHECKING, Callable, Optional, Sequence, TypeVar

import dill

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.python_virtualenv import remove_task_decorator, write_python_script

if TYPE_CHECKING:
    from airflow.decorators.base import TaskDecorator
    from airflow.utils.context import Context


def _generate_decode_command(env_var, file):
    # We don't need `f.close()` as the interpreter is about to exit anyway
    return (
        f'python -c "import base64, os;'
        rf'x = base64.b64decode(os.environ[\"{env_var}\"]);'
        rf'f = open(\"{file}\", \"wb\"); f.write(x);"'
    )


def _b64_encode_file(filename):
    with open(filename, "rb") as file_to_encode:
        return base64.b64encode(file_to_encode.read())


class _DockerDecoratedOperator(DecoratedOperator, DockerOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

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
        use_dill=False,
        **kwargs,
    ) -> None:
        command = "dummy command"
        self.pickling_library = dill if use_dill else pickle
        super().__init__(
            command=command, retrieve_output=True, retrieve_output_path="/tmp/script.out", **kwargs
        )

    def execute(self, context: 'Context'):
        with TemporaryDirectory(prefix='venv') as tmp_dir:
            input_filename = os.path.join(tmp_dir, 'script.in')
            script_filename = os.path.join(tmp_dir, 'script.py')

            with open(input_filename, 'wb') as file:
                if self.op_args or self.op_kwargs:
                    self.pickling_library.dump({'args': self.op_args, 'kwargs': self.op_kwargs}, file)
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

            # Pass the python script to be executed, and the input args, via environment variables. This is
            # more than slightly hacky, but it means it can work when Airflow itself is in the same Docker
            # engine where this task is going to run (unlike say trying to mount a file in)
            self.environment["__PYTHON_SCRIPT"] = _b64_encode_file(script_filename)
            if self.op_args or self.op_kwargs:
                self.environment["__PYTHON_INPUT"] = _b64_encode_file(input_filename)
            else:
                self.environment["__PYTHON_INPUT"] = ""

            self.command = (
                f"""bash -cx  '{_generate_decode_command("__PYTHON_SCRIPT", "/tmp/script.py")} &&"""
                f'{_generate_decode_command("__PYTHON_INPUT", "/tmp/script.in")} &&'
                f'python /tmp/script.py /tmp/script.in /tmp/script.out\''
            )
            return super().execute(context)

    def _get_python_source(self):
        raw_source = inspect.getsource(self.python_callable)
        res = dedent(raw_source)
        res = remove_task_decorator(res, "@task.docker")
        return res


T = TypeVar("T", bound=Callable)


def docker_task(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    **kwargs,
) -> "TaskDecorator":
    """
    Python operator decorator. Wraps a function into an Airflow operator.
    Also accepts any argument that DockerOperator will via ``kwargs``. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. List/Tuples will unroll to xcom values
        with index as key. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_DockerDecoratedOperator,
        **kwargs,
    )
