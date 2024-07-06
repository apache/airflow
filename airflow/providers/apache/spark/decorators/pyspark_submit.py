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
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.python_virtualenv import write_python_script

if TYPE_CHECKING:
    from airflow.utils.context import Context


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
        expect_airflow: bool = True,
        **kwargs,
    ):
        self.use_dill = use_dill
        self.expect_airflow = expect_airflow

        if kwargs.get("application"):
            if not conf.getboolean("operators", "ALLOW_ILLEGAL_ARGUMENTS"):
                raise AirflowException("Invalid argument 'application' were passed to @task.pyspark_submit.")
            warnings.warn(
                "Invalid argument 'application' were passed to @task.pyspark_submit.",
                UserWarning,
                stacklevel=2,
            )
        if kwargs.get("application_args"):
            if not conf.getboolean("operators", "ALLOW_ILLEGAL_ARGUMENTS"):
                raise AirflowException(
                    "Invalid argument 'application_args' were passed to @task.pyspark_submit."
                )
            warnings.warn(
                "Invalid argument 'application_args' were passed to `@task.pyspark_submit`.",
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
            input_filename = os.path.join(tmp_dir, "script.in")
            output_filename = os.path.join(tmp_dir, "script.out")
            error_filename = os.path.join(tmp_dir, "script.err")

            with open(input_filename, "w", encoding="utf-8") as file:
                if self.op_args or self.op_kwargs:
                    self.pickling_library.dump({"args": self.op_args, "kwargs": self.op_kwargs}, file)

            py_source = dedent(
                """\
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                sc = spark.sparkContext
                """
            )
            py_source += self.get_python_source()

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
            self.application = script_filename
            self.application_args = [
                input_filename,
                output_filename,
                "--no-string-args",
                error_filename,
            ]
            return super().execute(context)

    @property
    def pickling_library(self):
        if self.use_dill:
            return dill
        return pickle


def pyspark_submit_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_PysparkSubmitDecoratedOperator,
        **kwargs,
    )
