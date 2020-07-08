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
"""
Utilities for creating a virtual environment
"""
from typing import List, Optional

from airflow.utils.process_utils import execute_in_subprocess


def _generate_virtualenv_cmd(tmp_dir: str, python_bin: str, system_site_packages: bool) -> List[str]:
    cmd = ['virtualenv', tmp_dir]
    if system_site_packages:
        cmd.append('--system-site-packages')
    if python_bin is not None:
        cmd.append('--python={}'.format(python_bin))
    return cmd


def _generate_pip_install_cmd(tmp_dir: str, requirements: List[str]) -> Optional[List[str]]:
    if not requirements:
        return None
    # direct path alleviates need to activate
    cmd = ['{}/bin/pip'.format(tmp_dir), 'install']
    return cmd + requirements


def prepare_virtualenv(
    venv_directory: str,
    python_bin: str,
    system_site_packages: bool,
    requirements: List[str]
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
    :return: Path to a binary file with Python in a virtual environment.
    :rtype: str
    """
    virtualenv_cmd = _generate_virtualenv_cmd(venv_directory, python_bin, system_site_packages)
    execute_in_subprocess(virtualenv_cmd)
    pip_cmd = _generate_pip_install_cmd(venv_directory, requirements)
    if pip_cmd:
        execute_in_subprocess(pip_cmd)

    return '{}/bin/python'.format(venv_directory)
