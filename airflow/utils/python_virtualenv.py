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
"""Utilities for creating a virtual environment."""

from __future__ import annotations

import json
import os
import sys
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any

import jinja2
from jinja2 import select_autoescape

from airflow.utils.decorators import remove_task_decorator as _remove_task_decorator
from airflow.utils.process_utils import execute_in_subprocess

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dagrun import DagRun
    from airflow.serialization.pydantic.dag_run import DagRunPydantic
    from airflow.utils.context import Context


def _generate_virtualenv_cmd(tmp_dir: str, python_bin: str, system_site_packages: bool) -> list[str]:
    cmd = [sys.executable, "-m", "virtualenv", tmp_dir]
    if system_site_packages:
        cmd.append("--system-site-packages")
    if python_bin is not None:
        cmd.append(f"--python={python_bin}")
    return cmd


def _generate_pip_install_cmd_from_file(
    tmp_dir: str, requirements_file_path: str, pip_install_options: list[str]
) -> list[str]:
    return [f"{tmp_dir}/bin/pip", "install", *pip_install_options, "-r", requirements_file_path]


def _generate_pip_install_cmd_from_list(
    tmp_dir: str, requirements: list[str], pip_install_options: list[str]
) -> list[str]:
    return [f"{tmp_dir}/bin/pip", "install", *pip_install_options, *requirements]


def _generate_pip_conf(conf_file: Path, index_urls: list[str]) -> None:
    if index_urls:
        pip_conf_options = f"index-url = {index_urls[0]}"
        if len(index_urls) > 1:
            pip_conf_options += f"\nextra-index-url = {' '.join(x for x in index_urls[1:])}"
    else:
        pip_conf_options = "no-index = true"
    conf_file.write_text(f"[global]\n{pip_conf_options}")


def remove_task_decorator(python_source: str, task_decorator_name: str) -> str:
    warnings.warn(
        "Import remove_task_decorator from airflow.utils.decorators instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return _remove_task_decorator(python_source, task_decorator_name)


def prepare_virtualenv(
    venv_directory: str,
    python_bin: str,
    system_site_packages: bool,
    requirements: list[str] | None = None,
    requirements_file_path: str | None = None,
    pip_install_options: list[str] | None = None,
    index_urls: list[str] | None = None,
) -> str:
    """
    Create a virtual environment and install the additional python packages.

    :param venv_directory: The path for directory where the environment will be created.
    :param python_bin: Path to the Python executable.
    :param system_site_packages: Whether to include system_site_packages in your virtualenv.
        See virtualenv documentation for more information.
    :param requirements: List of additional python packages.
    :param requirements_file_path: Path to the ``requirements.txt`` file.
    :param pip_install_options: a list of pip install options when installing requirements
        See 'pip install -h' for available options
    :param index_urls: an optional list of index urls to load Python packages from.
        If not provided the system pip conf will be used to source packages from.
    :return: Path to a binary file with Python in a virtual environment.
    """
    if pip_install_options is None:
        pip_install_options = []

    if index_urls is not None:
        _generate_pip_conf(Path(venv_directory) / "pip.conf", index_urls)

    virtualenv_cmd = _generate_virtualenv_cmd(venv_directory, python_bin, system_site_packages)
    execute_in_subprocess(virtualenv_cmd)

    if requirements is not None and requirements_file_path is not None:
        raise ValueError("Either requirements OR requirements_file_path has to be passed, but not both")

    pip_cmd = None
    if requirements is not None and len(requirements) != 0:
        pip_cmd = _generate_pip_install_cmd_from_list(venv_directory, requirements, pip_install_options)
    if requirements_file_path is not None and requirements_file_path:
        pip_cmd = _generate_pip_install_cmd_from_file(
            venv_directory, requirements_file_path, pip_install_options
        )

    if pip_cmd:
        execute_in_subprocess(pip_cmd)

    return f"{venv_directory}/bin/python"


def write_python_script(
    jinja_context: dict,
    filename: str,
    render_template_as_native_obj: bool = False,
):
    """
    Render the python script to a file to execute in the virtual environment.

    :param jinja_context: The jinja context variables to unpack and replace with its placeholders in the
        template file.
    :param filename: The name of the file to dump the rendered script to.
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
        template_env = jinja2.Environment(
            loader=template_loader,
            undefined=jinja2.StrictUndefined,
            autoescape=select_autoescape(["html", "xml"]),
        )
    template = template_env.get_template("python_virtualenv_script.jinja2")
    template.stream(**jinja_context).dump(filename)


def context_to_json(context: Context) -> str:
    from airflow.models.param import ParamsDict
    from airflow.models.taskinstance import SimpleTaskInstance
    from airflow.serialization.serialized_objects import SerializedBaseOperator, SerializedDAG

    context_copy: dict[str, Any] = dict(context)

    deprecated = {
        "execution_date",
        "next_ds",
        "next_ds_nodash",
        "next_execution_date",
        "prev_ds",
        "prev_ds_nodash",
        "prev_execution_date",
        "prev_execution_date_success",
        "tomorrow_ds",
        "tomorrow_ds_nodash",
        "yesterday_ds",
        "yesterday_ds_nodash",
    }
    exclude = {
        "conf",
        "conn",
        "inlets",
        "inlet_events",
        "macros",
        "outlets",
        "outlet_events",
        "triggering_dataset_events",
        "var",
    } | deprecated
    include = {
        "dag",
        "dag_run",
        "data_interval_end",
        "data_interval_start",
        "ds",
        "ds_nodash",
        "exception",
        "execution_date",
        "expanded_ti_count",
        "logical_date",
        "map_index_template",
        "next_ds",
        "next_ds_nodash",
        "next_execution_date",
        "params",
        "prev_data_interval_start_success",
        "prev_data_interval_end_success",
        "prev_ds",
        "prev_ds_nodash",
        "prev_execution_date",
        "prev_execution_date_success",
        "prev_start_date_success",
        "prev_end_date_success",
        "reason",
        "run_id",
        "task_instance",
        "task_instance_key_str",
        "test_mode",
        "templates_dict",
        "ti",
        "tomorrow_ds",
        "tomorrow_ds_nodash",
        "ts",
        "ts_nodash",
        "ts_nodash_with_tz",
        "try_number",
        "yesterday_ds",
        "yesterday_ds_nodash",
    }

    select = include - exclude
    context_copy = {key: context.get(key, None) for key in context.keys() if key in select}

    dag = context_copy.pop("dag", None)
    if dag is not None:
        context_copy["dag"] = SerializedDAG.serialize_dag(dag)

    task: BaseOperator | None = context_copy.pop("task", None)
    if task is not None:
        context_copy["task"] = SerializedBaseOperator.serialize_operator(task)

    dag_run: DagRun | DagRunPydantic | None = context_copy.pop("dag_run", None)
    if dag_run is not None:
        from airflow.utils.pydantic import BaseModel as BaseModelPydantic, is_pydantic_2_installed

        if isinstance(dag_run, BaseModelPydantic):
            if is_pydantic_2_installed():
                context_copy["dag_run"] = dag_run.model_dump()
            else:
                context_copy["dag_run"] = dag_run.dict()
        else:
            unset = object()
            columns: list[str] = dag_run.__table__.columns.keys()
            context_copy["dag_run"] = {
                key: v for key in columns if (v := getattr(dag_run, key, unset)) is not unset
            }

    context_copy.pop("ti", None)
    task_instance = context_copy.pop("task_instance", None)
    if task_instance is not None:
        simple_task_instance = SimpleTaskInstance.from_ti(task_instance)
        context_copy["ti"] = context_copy["task_instance"] = simple_task_instance.__dict__.copy()

    params: ParamsDict | dict[str, Any] | None = context_copy.pop("params", None)
    if params is not None:
        if isinstance(params, ParamsDict):
            context_copy["params"] = params.serialize()
        else:
            context_copy["params"] = dict(params)

    return json.dumps(context_copy, default=_datetime_to_iso_format)


def _datetime_to_iso_format(obj: Any) -> Any:
    from datetime import datetime as std_datetime

    if isinstance(obj, std_datetime):
        return obj.isoformat()
    return obj
