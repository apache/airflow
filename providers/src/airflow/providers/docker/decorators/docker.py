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
import warnings
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Callable, Literal, Sequence

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.python_virtualenv import write_python_script

if TYPE_CHECKING:
    from airflow.decorators.base import TaskDecorator
    from airflow.utils.context import Context

    Serializer = Literal["pickle", "dill", "cloudpickle"]

try:
    from airflow.providers.standard.core.operators.python import _SERIALIZERS
except ImportError:
    import logging

    import lazy_object_proxy

    log = logging.getLogger(__name__)

    def _load_pickle():
        import pickle

        return pickle

    def _load_dill():
        try:
            import dill
        except ModuleNotFoundError:
            log.error("Unable to import `dill` module. Please please make sure that it installed.")
            raise
        return dill

    def _load_cloudpickle():
        try:
            import cloudpickle
        except ModuleNotFoundError:
            log.error(
                "Unable to import `cloudpickle` module. "
                "Please install it with: pip install 'apache-airflow[cloudpickle]'"
            )
            raise
        return cloudpickle

    _SERIALIZERS: dict[Serializer, Any] = {  # type: ignore[no-redef]
        "pickle": lazy_object_proxy.Proxy(_load_pickle),
        "dill": lazy_object_proxy.Proxy(_load_dill),
        "cloudpickle": lazy_object_proxy.Proxy(_load_cloudpickle),
    }


def _generate_decode_command(env_var, file, python_command):
    # We don't need `f.close()` as the interpreter is about to exit anyway
    return (
        f'{python_command} -c "import base64, os;'
        rf"x = base64.b64decode(os.environ[\"{env_var}\"]);"
        rf'f = open(\"{file}\", \"wb\"); f.write(x);"'
    )


def _b64_encode_file(filename):
    with open(filename, "rb") as file_to_encode:
        return base64.b64encode(file_to_encode.read())


class _DockerDecoratedOperator(DecoratedOperator, DockerOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :param python: Python binary name to use
    :param expect_airflow: whether to expect airflow to be installed in the docker environment. if this
          one is specified, the script to run callable will attempt to load Airflow macros.
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    :param serializer: Which serializer use to serialize the args and result. It can be one of the following:

        - ``"pickle"``: (default) Use pickle for serialization. Included in the Python Standard Library.
        - ``"cloudpickle"``: Use cloudpickle for serialize more complex types,
          this requires to include cloudpickle in your requirements.
        - ``"dill"``: Use dill for serialize more complex types,
          this requires to include dill in your requirements.
    :param use_dill: Deprecated, use ``serializer`` instead. Whether to use dill to serialize
        the args and result (pickle is default). This allows more complex types
        but requires you to include dill in your requirements.
    """

    custom_operator_name = "@task.docker"

    template_fields: Sequence[str] = (*DockerOperator.template_fields, "op_args", "op_kwargs")

    def __init__(
        self,
        use_dill=False,
        python_command="python3",
        expect_airflow: bool = True,
        serializer: Serializer | None = None,
        **kwargs,
    ) -> None:
        if use_dill:
            warnings.warn(
                "`use_dill` is deprecated and will be removed in a future version. "
                "Please provide serializer='dill' instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=3,
            )
            if serializer:
                raise AirflowException(
                    "Both 'use_dill' and 'serializer' parameters are set. Please set only one of them"
                )
            serializer = "dill"
        serializer = serializer or "pickle"
        if serializer not in _SERIALIZERS:
            msg = (
                f"Unsupported serializer {serializer!r}. "
                f"Expected one of {', '.join(map(repr, _SERIALIZERS))}"
            )
            raise AirflowException(msg)

        command = "placeholder command"
        self.python_command = python_command
        self.expect_airflow = expect_airflow
        self.use_dill = serializer == "dill"
        self.serializer: Serializer = serializer

        super().__init__(
            command=command, retrieve_output=True, retrieve_output_path="/tmp/script.out", **kwargs
        )

    def generate_command(self):
        return (
            f"""bash -cx  '{_generate_decode_command("__PYTHON_SCRIPT", "/tmp/script.py",
                                                     self.python_command)} &&"""
            f'{_generate_decode_command("__PYTHON_INPUT", "/tmp/script.in", self.python_command)} &&'
            f"{self.python_command} /tmp/script.py /tmp/script.in /tmp/script.out none /tmp/script.out'"
        )

    def execute(self, context: Context):
        with TemporaryDirectory(prefix="venv") as tmp_dir:
            input_filename = os.path.join(tmp_dir, "script.in")
            script_filename = os.path.join(tmp_dir, "script.py")

            with open(input_filename, "wb") as file:
                if self.op_args or self.op_kwargs:
                    self.pickling_library.dump({"args": self.op_args, "kwargs": self.op_kwargs}, file)
            py_source = self.get_python_source()
            write_python_script(
                jinja_context={
                    "op_args": self.op_args,
                    "op_kwargs": self.op_kwargs,
                    "pickling_library": self.pickling_library.__name__,
                    "python_callable": self.python_callable.__name__,
                    "python_callable_source": py_source,
                    "expect_airflow": self.expect_airflow,
                    "string_args_global": False,
                },
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

            self.command = self.generate_command()
            return super().execute(context)

    @property
    def pickling_library(self):
        return _SERIALIZERS[self.serializer]


def docker_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Python operator decorator; wraps a function into an Airflow operator.

    Also accepts any argument that DockerOperator will via ``kwargs``. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: If set, function return value will be unrolled to multiple XCom values.
        Dict will unroll to XCom values with keys as XCom keys. Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_DockerDecoratedOperator,
        **kwargs,
    )
