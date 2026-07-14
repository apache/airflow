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

import json
import subprocess
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest

from airflow.providers.standard.exceptions import RequirementsResolutionError
from airflow.providers.standard.utils.python_virtualenv import (
    _generate_pip_conf,
    _use_uv,
    prepare_virtualenv,
    resolve_requirements_versions,
)

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
            ["uv", "venv", "--allow-existing", "--seed", "--python", "pythonVER", "/VENV"],
            env=mock.ANY,
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
            ],
            env=mock.ANY,
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

    @mock.patch("airflow.providers.standard.utils.python_virtualenv._generate_pip_conf")
    @mock.patch("airflow.providers.standard.utils.python_virtualenv._execute_in_subprocess")
    @conf_vars({("standard", "venv_install_method"): "uv"})
    def test_venv_creation_with_index_urls(
        self, mock_execute_in_subprocess, mock_generate_pip_conf, tmp_path: Path
    ):
        """
        Test that uv venv creation passes UV_DEFAULT_INDEX env var.

        When package-index connections are available, UV_DEFAULT_INDEX and UV_INDEX
        environment variables are passed to uv venv command to ensure packages can be
        downloaded from the specified index during venv creation (e.g., when installing
        seed packages like pip, setuptools, wheel).
        """
        venv_dir = str(tmp_path / "venv")
        python_bin = prepare_virtualenv(
            venv_directory=venv_dir,
            python_bin="pythonVER",
            system_site_packages=False,
            requirements=["somepackage"],
            index_urls=["https://private.package.index"],
        )
        assert python_bin == f"{venv_dir}/bin/python"

        # First call: venv creation should have UV_DEFAULT_INDEX in env
        venv_call_args = mock_execute_in_subprocess.call_args_list[0]
        venv_cmd = venv_call_args[0][0]
        venv_kwargs = venv_call_args[1]

        assert venv_cmd == ["uv", "venv", "--allow-existing", "--seed", "--python", "pythonVER", venv_dir]
        assert "env" in venv_kwargs
        assert venv_kwargs["env"]["UV_DEFAULT_INDEX"] == "https://private.package.index"

        # Second call: pip install should also have UV_DEFAULT_INDEX
        pip_call_args = mock_execute_in_subprocess.call_args_list[1]
        pip_cmd = pip_call_args[0][0]
        pip_kwargs = pip_call_args[1]

        assert pip_cmd == ["uv", "pip", "install", "--python", f"{venv_dir}/bin/python", "somepackage"]
        assert "env" in pip_kwargs
        assert pip_kwargs["env"]["UV_DEFAULT_INDEX"] == "https://private.package.index"

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


PIP_INSTALL_REPORT = json.dumps(
    {
        "version": "1",
        "install": [
            {"metadata": {"name": "numpy", "version": "2.0.1"}},
            {"metadata": {"name": "colormap", "version": "1.1.0"}},
        ],
    }
)


class TestResolveRequirementsVersions:
    @mock.patch("airflow.providers.standard.utils.python_virtualenv.subprocess.run", autospec=True)
    @conf_vars({("standard", "venv_install_method"): "uv"})
    def test_resolve_with_uv_builds_pip_compile_command(self, mock_run):
        captured = {}

        def capture_run(cmd, **kwargs):
            captured["requirements"] = Path(cmd[-1]).read_text()
            return subprocess.CompletedProcess(cmd, 0, stdout="pkg==1.0\n", stderr="")

        mock_run.side_effect = capture_run

        result = resolve_requirements_versions(requirements=["pkg"], python_bin="pythonVER")

        cmd = mock_run.call_args.args[0]
        assert cmd[:-1] == [
            "uv",
            "pip",
            "compile",
            "--python",
            "pythonVER",
            "--no-header",
            "--no-annotate",
            "--quiet",
        ]
        assert captured["requirements"] == "pkg"
        assert result == ["pkg==1.0"]

    @mock.patch("airflow.providers.standard.utils.python_virtualenv.subprocess.run", autospec=True)
    @conf_vars({("standard", "venv_install_method"): "uv"})
    def test_resolve_with_uv_passes_index_urls_as_env_vars(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess([], 0, stdout="pkg==1.0\n", stderr="")

        resolve_requirements_versions(
            requirements=["pkg"],
            python_bin="pythonVER",
            index_urls=["http://one", "http://two", "http://three"],
        )

        env = mock_run.call_args.kwargs["env"]
        assert env["UV_DEFAULT_INDEX"] == "http://one"
        assert env["UV_INDEX"] == "http://two http://three"

    @mock.patch("airflow.providers.standard.utils.python_virtualenv.subprocess.run", autospec=True)
    @conf_vars({("standard", "venv_install_method"): "uv"})
    def test_resolve_with_uv_ignores_comments_and_sorts_output(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(
            [], 0, stdout="numpy==2.0.1\n\n# via pandas\ncolormap==1.1.0\n", stderr=""
        )

        result = resolve_requirements_versions(requirements=["pkg"], python_bin="python")

        assert result == ["colormap==1.1.0", "numpy==2.0.1"]

    @pytest.mark.parametrize(
        ("index_urls", "expected_index_args"),
        [
            (None, []),
            ([], ["--no-index"]),
            (["http://one"], ["--index-url", "http://one"]),
            (
                ["http://one", "http://two", "http://three"],
                [
                    "--index-url",
                    "http://one",
                    "--extra-index-url",
                    "http://two",
                    "--extra-index-url",
                    "http://three",
                ],
            ),
        ],
    )
    @mock.patch("airflow.providers.standard.utils.python_virtualenv.subprocess.run", autospec=True)
    def test_resolve_with_pip_builds_dry_run_command(self, mock_run, index_urls, expected_index_args):
        captured = {}

        def capture_run(cmd, **kwargs):
            captured["requirements"] = Path(cmd[cmd.index("-r") + 1]).read_text()
            return subprocess.CompletedProcess(cmd, 0, stdout=PIP_INSTALL_REPORT, stderr="")

        mock_run.side_effect = capture_run

        with conf_vars({("standard", "venv_install_method"): "pip"}):
            result = resolve_requirements_versions(
                requirements=["colormap>=1.0", "numpy"], python_bin="pythonVER", index_urls=index_urls
            )

        cmd = mock_run.call_args.args[0]
        assert cmd[:9] == [
            "pythonVER",
            "-m",
            "pip",
            "install",
            "--dry-run",
            "--ignore-installed",
            "--quiet",
            "--report",
            "-",
        ]
        assert cmd[9] == "-r"
        assert cmd[11:] == expected_index_args
        assert captured["requirements"] == "colormap>=1.0\nnumpy"
        assert result == ["colormap==1.1.0", "numpy==2.0.1"]

    @pytest.mark.parametrize("install_method", ["uv", "pip"])
    @mock.patch("airflow.providers.standard.utils.python_virtualenv.subprocess.run", autospec=True)
    def test_resolve_failure_raises_with_stderr(self, mock_run, install_method):
        mock_run.return_value = subprocess.CompletedProcess([], 1, stdout="", stderr="resolution exploded")

        with conf_vars({("standard", "venv_install_method"): install_method}):
            with pytest.raises(RequirementsResolutionError, match="resolution exploded"):
                resolve_requirements_versions(requirements=["pkg"], python_bin="python")

    @mock.patch("airflow.providers.standard.utils.python_virtualenv.subprocess.run", autospec=True)
    @conf_vars({("standard", "venv_install_method"): "pip"})
    def test_resolve_with_pip_invalid_report_raises(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess([], 0, stdout="not json", stderr="")

        with pytest.raises(RequirementsResolutionError, match="installation report"):
            resolve_requirements_versions(requirements=["pkg"], python_bin="python")
