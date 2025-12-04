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

from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest

from airflow.providers.standard.utils.python_virtualenv import _generate_pip_conf, _use_uv, prepare_virtualenv

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import remove_task_decorator


class TestPrepareVirtualenv:
    @mock.patch("shutil.which")
    def test_use_uv(self, mock_shutil_which):
        with conf_vars({("standard", "venv_install_method"): "auto"}):
            mock_shutil_which.side_effect = [True]
            assert _use_uv() is True

            mock_shutil_which.side_effect = [False]
            assert _use_uv() is False

        with conf_vars({("standard", "venv_install_method"): "uv"}):
            assert _use_uv() is True

        with conf_vars({("standard", "venv_install_method"): "pip"}):
            assert _use_uv() is False

    @pytest.mark.parametrize(
        ("index_urls", "expected_pip_conf_content", "unexpected_pip_conf_content"),
        [
            [[], ["[global]", "no-index ="], ["index-url", "extra", "http", "pypi"]],
            [["http://mysite"], ["[global]", "index-url", "http://mysite"], ["no-index", "extra", "pypi"]],
            [
                ["http://mysite", "https://othersite"],
                ["[global]", "index-url", "http://mysite", "extra", "https://othersite"],
                ["no-index", "pypi"],
            ],
            [
                ["http://mysite", "https://othersite", "http://site"],
                ["[global]", "index-url", "http://mysite", "extra", "https://othersite http://site"],
                ["no-index", "pypi"],
            ],
        ],
    )
    def test_generate_pip_conf(
        self,
        index_urls: list[str],
        expected_pip_conf_content: list[str],
        unexpected_pip_conf_content: list[str],
        tmp_path: Path,
    ):
        tmp_file = tmp_path / "pip.conf"
        _generate_pip_conf(tmp_file, index_urls)
        generated_conf = tmp_file.read_text()
        for term in expected_pip_conf_content:
            assert term in generated_conf
        for term in unexpected_pip_conf_content:
            assert term not in generated_conf

    @mock.patch("airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess")
    @conf_vars({("standard", "venv_install_method"): "pip"})
    def test_should_create_virtualenv_pip(self, mock_execute_in_subprocess):
        python_bin = prepare_virtualenv(
            venv_directory="/VENV", python_bin="pythonVER", system_site_packages=False, requirements=[]
        )
        assert python_bin == "/VENV/bin/python"
        mock_execute_in_subprocess.assert_called_once_with(["pythonVER", "-m", "venv", "/VENV"])

    @mock.patch("airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess")
    @conf_vars({("standard", "venv_install_method"): "uv"})
    def test_should_create_virtualenv_uv(self, mock_execute_in_subprocess):
        python_bin = prepare_virtualenv(
            venv_directory="/VENV", python_bin="pythonVER", system_site_packages=False, requirements=[]
        )
        assert python_bin == "/VENV/bin/python"
        mock_execute_in_subprocess.assert_called_once_with(
            ["uv", "venv", "--allow-existing", "--seed", "--python", "pythonVER", "/VENV"]
        )

    @mock.patch("airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess")
    @conf_vars({("standard", "venv_install_method"): "pip"})
    def test_should_create_virtualenv_with_system_packages_pip(self, mock_execute_in_subprocess):
        python_bin = prepare_virtualenv(
            venv_directory="/VENV", python_bin="pythonVER", system_site_packages=True, requirements=[]
        )
        assert python_bin == "/VENV/bin/python"
        mock_execute_in_subprocess.assert_called_once_with(
            ["pythonVER", "-m", "venv", "/VENV", "--system-site-packages"]
        )

    @mock.patch("airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess")
    @conf_vars({("standard", "venv_install_method"): "uv"})
    def test_should_create_virtualenv_with_system_packages_uv(self, mock_execute_in_subprocess):
        python_bin = prepare_virtualenv(
            venv_directory="/VENV", python_bin="pythonVER", system_site_packages=True, requirements=[]
        )
        assert python_bin == "/VENV/bin/python"
        mock_execute_in_subprocess.assert_called_once_with(
            [
                "uv",
                "venv",
                "--allow-existing",
                "--seed",
                "--python",
                "pythonVER",
                "--system-site-packages",
                "/VENV",
            ]
        )

    @mock.patch("airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess")
    @conf_vars({("standard", "venv_install_method"): "pip"})
    def test_pip_install_options_pip(self, mock_execute_in_subprocess):
        pip_install_options = ["--no-deps"]
        python_bin = prepare_virtualenv(
            venv_directory="/VENV",
            python_bin="pythonVER",
            system_site_packages=True,
            requirements=["apache-beam[gcp]"],
            pip_install_options=pip_install_options,
        )

        assert python_bin == "/VENV/bin/python"
        mock_execute_in_subprocess.assert_called_with(
            ["/VENV/bin/pip", "install", *pip_install_options, "apache-beam[gcp]"],
            env=mock.ANY,
        )

    @mock.patch("airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess")
    @conf_vars({("standard", "venv_install_method"): "uv"})
    def test_pip_install_options_uv(self, mock_execute_in_subprocess):
        pip_install_options = ["--no-deps"]
        python_bin = prepare_virtualenv(
            venv_directory="/VENV",
            python_bin="pythonVER",
            system_site_packages=True,
            requirements=["apache-beam[gcp]"],
            pip_install_options=pip_install_options,
        )

        assert python_bin == "/VENV/bin/python"
        mock_execute_in_subprocess.assert_called_with(
            [
                "uv",
                "pip",
                "install",
                "--python",
                "/VENV/bin/python",
                *pip_install_options,
                "apache-beam[gcp]",
            ],
            env=mock.ANY,
        )

    @mock.patch("airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess")
    @conf_vars({("standard", "venv_install_method"): "pip"})
    def test_should_create_virtualenv_with_extra_packages_pip(self, mock_execute_in_subprocess):
        python_bin = prepare_virtualenv(
            venv_directory="/VENV",
            python_bin="pythonVER",
            system_site_packages=False,
            requirements=["apache-beam[gcp]"],
        )
        assert python_bin == "/VENV/bin/python"

        mock_execute_in_subprocess.assert_any_call(["pythonVER", "-m", "venv", "/VENV"])

        mock_execute_in_subprocess.assert_called_with(
            ["/VENV/bin/pip", "install", "apache-beam[gcp]"], env=mock.ANY
        )

    @mock.patch("airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess")
    @conf_vars({("standard", "venv_install_method"): "uv"})
    def test_should_create_virtualenv_with_extra_packages_uv(self, mock_execute_in_subprocess):
        python_bin = prepare_virtualenv(
            venv_directory="/VENV",
            python_bin="pythonVER",
            system_site_packages=False,
            requirements=["apache-beam[gcp]"],
        )
        assert python_bin == "/VENV/bin/python"

        mock_execute_in_subprocess.assert_called_with(
            ["uv", "pip", "install", "--python", "/VENV/bin/python", "apache-beam[gcp]"],
            env=mock.ANY,
        )

    @pytest.mark.parametrize(
        ("decorators", "expected_decorators"),
        [
            (["@task.virtualenv"], []),
            (["@task.virtualenv()"], []),
            (['@task.virtualenv(serializer="dill")'], []),
            (["@foo", "@task.virtualenv", "@bar"], ["@foo", "@bar"]),
            (["@foo", "@task.virtualenv()", "@bar"], ["@foo", "@bar"]),
        ],
        ids=["without_parens", "parens", "with_args", "nested_without_parens", "nested_with_parens"],
    )
    def test_remove_task_decorator(self, decorators: list[str], expected_decorators: list[str]):
        concated_decorators = "\n".join(decorators)
        expected_decorator = "\n".join(expected_decorators)
        SCRIPT = dedent(
            """
        def f():
            # @task.virtualenv
            import funcsigs
        """
        )
        py_source = concated_decorators + SCRIPT
        expected_source = expected_decorator + SCRIPT if expected_decorator else SCRIPT.lstrip()

        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.virtualenv")
        assert res == expected_source
