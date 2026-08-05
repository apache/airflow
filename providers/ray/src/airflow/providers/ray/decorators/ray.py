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
import re
import tempfile
import textwrap
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.providers.ray.exceptions import RayAirflowException
from airflow.providers.ray.operators.ray import SubmitRayJob
from airflow.sdk.bases.decorator import DecoratedOperator, TaskDecorator, task_decorator_factory

if TYPE_CHECKING:
    from airflow.sdk import Context


class _RayDecoratedOperator(DecoratedOperator, SubmitRayJob):  # type: ignore[misc]
    """
    A custom Airflow operator for Ray tasks.

    This operator combines the functionality of Airflow's DecoratedOperator
    with the Ray SubmitRayJob operator, allowing users to define tasks that
    submit jobs to a Ray cluster.

    :param config: Configuration dictionary for the Ray job.
    :param kwargs: Additional keyword arguments.
    """

    custom_operator_name = "@task.ray"
    _config: dict[str, Any] | Callable[..., dict[str, Any]] = {}

    template_fields: Any = (*SubmitRayJob.template_fields, "op_args", "op_kwargs")

    def __init__(self, config: dict[str, Any] | Callable[..., dict[str, Any]], **kwargs: Any) -> None:
        self._config = config
        self.kwargs = kwargs
        super().__init__(conn_id="", entrypoint="python script.py", runtime_env={}, **kwargs)

    def _build_config(self, context: Context) -> dict[str, Any]:
        if callable(self._config):
            config_params = inspect.signature(self._config).parameters
            config_kwargs = {k: v for k, v in self.kwargs.items() if k in config_params and k != "context"}
            if "context" in config_params:
                config_kwargs["context"] = context
            config = self._config(**config_kwargs)
            if not isinstance(config, dict):
                raise TypeError("Ray task config callable must return a dictionary")
            return config
        return self._config

    def _load_config(self, config: dict[str, Any]) -> None:
        self.conn_id: str = config.get("conn_id", "")
        self.is_decorated_function = False if "entrypoint" in config else True
        self.entrypoint: str = config.get("entrypoint", "python script.py")
        self.runtime_env: dict[str, Any] = config.get("runtime_env", {})

        self.num_cpus: int | float = config.get("num_cpus", 1)
        self.num_gpus: int | float = config.get("num_gpus", 0)
        self.memory: int | float = config.get("memory", 1)
        self.ray_resources: dict[str, Any] | None = config.get("resources")
        self.ray_cluster_yaml: str | None = config.get("ray_cluster_yaml")
        self.update_if_exists: bool = config.get("update_if_exists", False)
        self.kuberay_version: str = config.get("kuberay_version", "1.0.0")
        self.gpu_device_plugin_yaml: str = config.get("gpu_device_plugin_yaml", "")
        self.fetch_logs: bool = config.get("fetch_logs", True)
        self.wait_for_completion: bool = config.get("wait_for_completion", True)
        self.job_timeout_seconds: int = config.get("job_timeout_seconds", 600)
        self.poll_interval: int = config.get("poll_interval", 60)
        self.xcom_task_key: str | None = config.get("xcom_task_key")

        self.config = config

        if not isinstance(self.num_cpus, (int, float)):
            raise RayAirflowException("num_cpus should be an integer or float value")
        if not isinstance(self.num_gpus, (int, float)):
            raise RayAirflowException("num_gpus should be an integer or float value")

    def execute(self, context: Context) -> Any:
        """
        Execute the Ray task.

        :param context: The context in which the task is being executed.
        :return: The result of the Ray job execution.
        :raises RayAirflowException: If job submission fails.
        """
        config = self._build_config(context)
        self.log.info("Using the following config %s", config)
        self._load_config(config)

        with tempfile.TemporaryDirectory(prefix="ray_") as tmpdirname:
            temp_dir = Path(tmpdirname)

            if self.is_decorated_function:
                self.log.info(
                    "Entrypoint is not provided, is_decorated_function is set to %s",
                    self.is_decorated_function,
                )

                # Get the Python source code and extract just the function body
                full_source = inspect.getsource(self.python_callable)
                function_body = self._extract_function_body(full_source)

                # Prepare the function call
                args_str = ", ".join(repr(arg) for arg in self.op_args)
                kwargs_str = ", ".join(f"{k}={repr(v)}" for k, v in self.op_kwargs.items())
                call_str = f"{self.python_callable.__name__}({args_str}, {kwargs_str})"

                # Write the script with function definition and call
                script_filename = os.path.join(temp_dir, "script.py")
                with open(script_filename, "w") as file:
                    file.write(function_body)
                    file.write(f"\n\n# Execute the function\n{call_str}\n")

                # Set up Ray job
                self.entrypoint = f"python {os.path.basename(script_filename)}"
                self.runtime_env["working_dir"] = temp_dir

            self.log.info("Running ray job...")
            result = super().execute(context)

            return result

    def _extract_function_body(self, source: str) -> str:
        """Extract the function, excluding only the ray.task decorator."""
        self.log.info(r"Ray pipeline intended to be executed: \n %s", source)
        if "@ray.task" not in source:
            raise RayAirflowException("Unable to parse this body. Expects the `@ray.task` decorator.")
        lines = source.split("\n")
        # TODO: Review the current approach, that is quite hacky.
        # It feels a mistake to have a user-facing module named the same as the official ray SDK.
        # In particular, the decorator is working in a very artificial way, where ray means two different things
        # at the scope of the task definition (Astro Ray Provider decorator) and inside the decorated method (Ray SDK)
        # Find the line where the ray.task decorator is
        # Additionally, if users imported the ray decorator as "from airflow.providers.ray.decorators.ray import ray as ray_decorator
        # The following will stop working.
        ray_task_line = next(
            (i for i, line in enumerate(lines) if re.match(r"^\s*@ray\.task", line.strip())), -1
        )

        # Include everything except the ray.task decorator line
        body = "\n".join(lines[:ray_task_line] + lines[ray_task_line + 1 :])

        if not body:
            raise RayAirflowException("Failed to extract Ray pipeline code decorated with @ray.task")
        # Dedent the body
        return textwrap.dedent(body)


class ray:
    """Namespace for the Ray task decorator."""

    @staticmethod
    def task(
        python_callable: Callable[..., Any] | None = None,
        multiple_outputs: bool | None = None,
        config: dict[str, Any] | Callable[[], dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> TaskDecorator:
        """
        Define a task that submits a Ray job.

        :param python_callable: The callable function to decorate.
        :param multiple_outputs: If True, will return multiple outputs.
        :param config: A dictionary of configuration or a callable that returns a dictionary.
        :param kwargs: Additional keyword arguments.
        :return: The decorated task.
        """
        config = config or {}
        return task_decorator_factory(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            decorated_operator_class=_RayDecoratedOperator,
            config=config,
            **kwargs,
        )
