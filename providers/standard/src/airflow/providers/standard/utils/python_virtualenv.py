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

import os
import shutil
import sys
from pathlib import Path

import jinja2
from jinja2 import select_autoescape

from airflow.configuration import conf
from airflow.utils.process_utils import execute_in_subprocess


def _is_uv_installed() -> bool:
    """
    Verify whether the uv tool is installed by checking if it's included in the system PATH or installed as a package.

    :return: True if it is. Whichever way of checking it works, is fine.
    """
    return bool(shutil.which("uv"))


def _use_uv() -> bool:
    """
    Check if the uv tool should be used.

    :return: True if uv should be used.
    """
    venv_install_method = conf.get("standard", "venv_install_method", fallback="auto").lower()
    if venv_install_method == "auto":
        return _is_uv_installed()
    if venv_install_method == "uv":
        return True
    return False


def _generate_uv_cmd(tmp_dir: str, python_bin: str, system_site_packages: bool) -> list[str]:
    """Build the command to install the venv via UV."""
    cmd = ["uv", "venv", "--allow-existing", "--seed"]
    if python_bin is not None:
        cmd += ["--python", python_bin]
    if system_site_packages:
        cmd.append("--system-site-packages")
    cmd.append(tmp_dir)
    return cmd


def _generate_venv_cmd(tmp_dir: str, python_bin: str, system_site_packages: bool) -> list[str]:
    """We are using venv command instead of venv module to allow creation of venv for different python versions."""
    if python_bin is None:
        python_bin = sys.executable
    cmd = [python_bin, "-m", "venv", tmp_dir]
    if system_site_packages:
        cmd.append("--system-site-packages")
    return cmd


def _generate_uv_install_cmd_from_file(
    tmp_dir: str, requirements_file_path: str, pip_install_options: list[str]
) -> list[str]:
    return [
        "uv",
        "pip",
        "install",
        "--python",
        f"{tmp_dir}/bin/python",
        *pip_install_options,
        "-r",
        requirements_file_path,
    ]


def _generate_pip_install_cmd_from_file(
    tmp_dir: str, requirements_file_path: str, pip_install_options: list[str]
) -> list[str]:
    return [f"{tmp_dir}/bin/pip", "install", *pip_install_options, "-r", requirements_file_path]


def _generate_uv_install_cmd_from_list(
    tmp_dir: str, requirements: list[str], pip_install_options: list[str]
) -> list[str]:
    return ["uv", "pip", "install", "--python", f"{tmp_dir}/bin/python", *pip_install_options, *requirements]


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

    if requirements is not None and requirements_file_path is not None:
        raise ValueError("Either requirements OR requirements_file_path has to be passed, but not both")

    if index_urls is not None:
        _generate_pip_conf(Path(venv_directory) / "pip.conf", index_urls)

    if _use_uv():
        venv_cmd = _generate_uv_cmd(venv_directory, python_bin, system_site_packages)
    else:
        venv_cmd = _generate_venv_cmd(venv_directory, python_bin, system_site_packages)
    execute_in_subprocess(venv_cmd)

    pip_cmd = None
    if requirements is not None and len(requirements) != 0:
        if _use_uv():
            pip_cmd = _generate_uv_install_cmd_from_list(venv_directory, requirements, pip_install_options)
        else:
            pip_cmd = _generate_pip_install_cmd_from_list(venv_directory, requirements, pip_install_options)
    if requirements_file_path is not None and requirements_file_path:
        if _use_uv():
            pip_cmd = _generate_uv_install_cmd_from_file(
                venv_directory, requirements_file_path, pip_install_options
            )
        else:
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
