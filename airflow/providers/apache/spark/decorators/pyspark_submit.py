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

import inspect
import os
import pickle
import warnings
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import TYPE_CHECKING, Callable, Sequence

import dill

from airflow.configuration import conf
from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.exceptions import AirflowException
from airflow.providers.apache.spark.decorators.pyspark import SPARK_CONTEXT_KEYS
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.python_virtualenv import write_python_script

if TYPE_CHECKING:
    from airflow.utils.context import Context


INPUT_FILENAME = "SCRIPT__GENERATED__AIRFLOW.IN"


class _PysparkSubmitDecoratedOperator(DecoratedOperator, SparkSubmitOperator):
    custom_operator_name = "@task.pyspark_submit"

    template_fields: Sequence[str] = (
        "conf",
        "files",
        "py_files",
        "jars",
        "driver_class_path",
        "packages",
        "exclude_packages",
        "keytab",
        "principal",
        "proxy_user",
        "name",
        "env_vars",
        "properties_file",
        "op_args",
        "op_kwargs",
    )

    def __init__(
        self,
        python_callable: Callable,
        op_args: Sequence | None = None,
        op_kwargs: dict | None = None,
        use_dill: bool = False,
        expect_airflow: bool = False,
        **kwargs,
    ):
        self.use_dill = use_dill
        self.expect_airflow = expect_airflow

        signature = inspect.signature(python_callable)
        parameters = [
            param.replace(default=None) if param.name in SPARK_CONTEXT_KEYS else param
            for param in signature.parameters.values()
        ]
        # mypy does not understand __signature__ attribute
        # see https://github.com/python/mypy/issues/12472
        python_callable.__signature__ = signature.replace(parameters=parameters)  # type: ignore[attr-defined]

        if kwargs.get("application"):
            if not conf.getboolean("operators", "ALLOW_ILLEGAL_ARGUMENTS"):
                raise AirflowException(
                    "Invalid argument 'application' were passed to `@task.pyspark_submit`."
                )
            warnings.warn(
                "Invalid argument 'application' were passed to @task.pyspark_submit.",
                UserWarning,
                stacklevel=2,
            )
        if kwargs.get("application_args"):
            if not conf.getboolean("operators", "ALLOW_ILLEGAL_ARGUMENTS"):
                raise AirflowException(
                    "Invalid argument 'application_args' were passed to `@task.pyspark_submit`."
                )
            warnings.warn(
                "Invalid argument 'application_args' were passed to `@task.pyspark_submit`.",
                UserWarning,
                stacklevel=2,
            )
        if op_kwargs:
            for key in SPARK_CONTEXT_KEYS:
                if key in op_kwargs:
                    if not conf.getboolean("operators", "ALLOW_ILLEGAL_ARGUMENTS"):
                        raise AirflowException(
                            f"Invalid key '{key}' in op_kwargs. You don't need to set it because it's a "
                            "variable that will be automatically set within the Python process of the Spark "
                            "job submitted via spark-submit."
                        )
                    warnings.warn(
                        f"Invalid key '{key}' in op_kwargs. You don't need to set it because it's a "
                        "variable that will be automatically set within the Python process of the Spark "
                        "job submitted via spark-submit.",
                        UserWarning,
                        stacklevel=2,
                    )

        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )

    def execute(self, context: Context):
        with TemporaryDirectory() as tmp_dir:
            script_filename = os.path.join(tmp_dir, "script.py")
            input_filename = os.path.join(tmp_dir, INPUT_FILENAME)

            if self.op_args or self.op_kwargs:
                with open(input_filename, "wb") as file:
                    self.pickling_library.dump({"args": self.op_args, "kwargs": self.op_kwargs}, file)
                files = self.files.split(",") if self.files else []
                self.files = ",".join(files + [input_filename])

            py_source = self.get_pyspark_source()

            write_python_script(
                jinja_context={
                    "op_args": None,
                    "op_kwargs": None,
                    "pickling_library": self.pickling_library.__name__,
                    "python_callable": self.python_callable.__name__,
                    "python_callable_source": py_source,
                    "expect_airflow": self.expect_airflow,
                },
                filename=script_filename,
            )
            self.application = script_filename
            return super().execute(context)

    def get_pyspark_source(self):
        py_source = self.get_python_source()
        parameters = inspect.signature(self.python_callable).parameters
        use_spark_context = use_spark_session = False
        if "sc" in parameters:
            use_spark_context = True
        if "spark" in parameters:
            use_spark_session = True

        py_source = dedent(
            f"""\
            from pyspark import SparkFiles
            from pyspark.sql import SparkSession

            # Script
            {{python_callable_source}}

            # args
            if {bool(self.op_args or self.op_kwargs)}:
                SparkSession.builder.getOrCreate()
                with open(SparkFiles.get("{INPUT_FILENAME}"), "rb") as file:
                    arg_dict = {self.pickling_library.__name__}.load(file)
            else:
                arg_dict = {{default_arg_dict}}

            if {use_spark_session}:
                arg_dict["kwargs"]["spark"] = SparkSession.builder.getOrCreate()
            if {use_spark_context}:
                spark = arg_dict.get("spark") or SparkSession.builder.getOrCreate()
                arg_dict["kwargs"]["sc"] = spark.sparkContext

            # Call
            {self.python_callable.__name__}(*arg_dict["args"], **arg_dict["kwargs"])

            # Exit
            exit(0)
            """
        ).format(python_callable_source=py_source, default_arg_dict='{"args": [], "kwargs": {}}')
        return py_source

    @property
    def pickling_library(self):
        if self.use_dill:
            return dill
        return pickle


def pyspark_submit_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """Wrap a Python function into an Airflow task that will be run using `pyspark-submit`."""
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=False,
        decorated_operator_class=_PysparkSubmitDecoratedOperator,
        **kwargs,
    )
