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
from collections import deque
from typing import List, Optional
from urllib.parse import SplitResult, urlunsplit

import jinja2

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Connection
from airflow.utils.process_utils import execute_in_subprocess


def _generate_virtualenv_cmd(tmp_dir: str, python_bin: str, system_site_packages: bool) -> List[str]:
    cmd = ['virtualenv', tmp_dir]
    if system_site_packages:
        cmd.append('--system-site-packages')
    if python_bin is not None:
        cmd.append(f'--python={python_bin}')
    return cmd


def _generate_pip_install_cmd(tmp_dir: str,
                              requirements: List[str],
                              connection_id: Optional[str] = None
                              ) -> Optional[List[str]]:
    if not requirements:
        return None

    if connection_id:
        con: Connection = BaseHook.get_connection(connection_id)
        user = con.login
        schema = con.schema or 'https'
        password = con.get_password()
        port = con.port or ''
        host = con.host
        path = con.extra_dejson.get('path', '')
        pypi_as_fallback = con.extra_dejson.get('pypi_as_fallback', False)

        if user:
            netloc = f'{user}:{password}@{host}'
        else:
            netloc = host

        netloc = netloc + f':{port}' if port else netloc

        index_url = SplitResult(scheme=schema,
                                netloc=netloc, path=path, query='', fragment='').geturl()

        private_cmd = [f'{tmp_dir}/bin/pip',
                       'install',
                       f'--index-url', index_url]

        if pypi_as_fallback:
            private_cmd.extend([f'--extra-index-url', 'https://pypi.org/simple'])
        return private_cmd + requirements

    public_cmd = [f'{tmp_dir}/bin/pip', 'install']

    return public_cmd + requirements


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
    venv_directory: str, python_bin: str, system_site_packages: bool, requirements: List[str],
    connection_id: Optional[str] = None
) -> str:
    """
    Creates a virtual environment and installs the additional python packages

    :param venv_directory: The path for directory where the environment will be created
    :type venv_directory: str
    :param python_bin: Path for python binary
    :type python_bin: str
    :param system_site_packages: Whether to include system_site_packages in your virtualenv.
        See virtualenv documentation for more information.
    :type system_site_packages: bool
    :param requirements: List of additional python packages
    :type requirements: List[str]
    :param connection_id: The connection ID of an http/https connection to use as the index
        url for installing packages from a private PyPi repository
    :type connection_id: str
    :rtype: str
    """
    virtualenv_cmd = _generate_virtualenv_cmd(venv_directory, python_bin, system_site_packages)
    execute_in_subprocess(virtualenv_cmd)
    pip_cmd = _generate_pip_install_cmd(venv_directory, requirements, connection_id)
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
    :type jinja_context: dict
    :param filename: The name of the file to dump the rendered script to.
    :type filename: str
    :param render_template_as_native_obj: If ``True``, rendered Jinja template would be converted
        to a native Python object
    """
    template_loader = jinja2.FileSystemLoader(searchpath=os.path.dirname(__file__))
    if render_template_as_native_obj:
        template_env = jinja2.nativetypes.NativeEnvironment(
            loader=template_loader, undefined=jinja2.StrictUndefined
        )
    else:
        template_env = jinja2.Environment(loader=template_loader, undefined=jinja2.StrictUndefined)
    template = template_env.get_template('python_virtualenv_script.jinja2')
    template.stream(**jinja_context).dump(filename)
