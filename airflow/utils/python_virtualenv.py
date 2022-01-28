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
#
"""Utilities for creating a virtual environment"""
import os
import sys
from collections import deque
from typing import List, Optional

import jinja2

from airflow.utils.process_utils import execute_in_subprocess


def _generate_virtualenv_cmd(tmp_dir: str, python_bin: str, system_site_packages: bool) -> List[str]:
    cmd = [sys.executable, '-m', 'virtualenv', tmp_dir]
    if system_site_packages:
        cmd.append('--system-site-packages')
    if python_bin is not None:
        cmd.append(f'--python={python_bin}')
    return cmd


def _generate_pip_install_cmd_from_file(tmp_dir: str, requirements_file_path: str) -> List[str]:
    cmd = [f'{tmp_dir}/bin/pip', 'install', '-r']
    return cmd + [requirements_file_path]


def _generate_pip_install_cmd_from_list(tmp_dir: str, requirements: List[str]) -> List[str]:
    cmd = [f'{tmp_dir}/bin/pip', 'install']
    return cmd + requirements


def _balance_parens(after_decorator):
    num_paren = 1
    after_decorator = deque(after_decorator)
    after_decorator.popleft()
    while num_paren:
        current = after_decorator.popleft()
        if current == "(":
            num_paren = num_paren + 1
        elif current == ")":
            num_paren = num_paren - 1
    return ''.join(after_decorator)


def remove_task_decorator(python_source: str, task_decorator_name: str) -> str:
    """
    Removed @task.virtualenv

    :param python_source:
    """
    if task_decorator_name not in python_source:
        return python_source
    split = python_source.split(task_decorator_name)
    before_decorator, after_decorator = split[0], split[1]
    if after_decorator[0] == "(":
        after_decorator = _balance_parens(after_decorator)
    if after_decorator[0] == "\n":
        after_decorator = after_decorator[1:]
    return before_decorator + after_decorator


def prepare_virtualenv(
    venv_directory: str,
    python_bin: str,
    system_site_packages: bool,
    requirements: Optional[List[str]] = None,
    requirements_file_path: Optional[str] = None,
) -> str:
    """Creates a virtual environment and installs the additional python packages.

    :param venv_directory: The path for directory where the environment will be created.
    :param python_bin: Path to the Python executable.
    :param system_site_packages: Whether to include system_site_packages in your virtualenv.
        See virtualenv documentation for more information.
    :param requirements: List of additional python packages.
    :param requirements_file_path: Path to the ``requirements.txt`` file.
    :return: Path to a binary file with Python in a virtual environment.
    :rtype: str
    """
    virtualenv_cmd = _generate_virtualenv_cmd(venv_directory, python_bin, system_site_packages)
    execute_in_subprocess(virtualenv_cmd)

    if requirements is not None and requirements_file_path is not None:
        raise Exception("Either requirements OR requirements_file_path has to be passed, but not both")

    pip_cmd = None
    if requirements is not None and len(requirements) != 0:
        pip_cmd = _generate_pip_install_cmd_from_list(venv_directory, requirements)
    if requirements_file_path is not None and requirements_file_path:
        pip_cmd = _generate_pip_install_cmd_from_file(venv_directory, requirements_file_path)

    if pip_cmd:
        execute_in_subprocess(pip_cmd)

    return f'{venv_directory}/bin/python'


def write_python_script(
    jinja_context: dict,
    filename: str,
    render_template_as_native_obj: bool = False,
):
    """
    Renders the python script to a file to execute in the virtual environment.

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
        template_env = jinja2.Environment(loader=template_loader, undefined=jinja2.StrictUndefined)
    template = template_env.get_template('python_virtualenv_script.jinja2')
    template.stream(**jinja_context).dump(filename)
