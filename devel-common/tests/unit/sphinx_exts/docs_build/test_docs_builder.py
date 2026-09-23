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

import io
import os
import subprocess
import sys
import types
from unittest import mock

from sphinx_exts.docs_build import docs_builder
from sphinx_exts.docs_build.code_utils import AIRFLOW_CONTENT_ROOT_PATH, DOCS_SOURCES_PATH
from sphinx_exts.docs_build.docs_builder import (
    AirflowDocsBuilder,
    _forget_sphinx_conf_modules,
    _RedirectableStream,
    get_available_packages,
)


def test_mypy_docs_package_is_available():
    assert "apache-airflow-mypy" in get_available_packages()


def test_mypy_docs_source_directory():
    builder = AirflowDocsBuilder(package_name="apache-airflow-mypy")

    assert builder._src_dir == AIRFLOW_CONTENT_ROOT_PATH / "dev" / "mypy" / "docs"


class TestRedirectableStream:
    def test_writes_go_to_target_while_set_and_to_fallback_otherwise(self):
        fallback = io.StringIO()
        target = io.StringIO()
        stream = _RedirectableStream(fallback)

        stream.write("before\n")
        stream.target = target
        stream.write("during\n")
        stream.target = None
        stream.write("after\n")

        assert fallback.getvalue() == "before\nafter\n"
        assert target.getvalue() == "during\n"

    def test_reference_kept_from_an_earlier_build_stays_usable_after_its_log_is_closed(self):
        """A library that cached the stream during build 1 must not hit a closed file in build 2."""
        fallback = io.StringIO()
        stream = _RedirectableStream(fallback)
        first_log = io.StringIO()
        stream.target = first_log
        cached_by_library = stream
        stream.target = None
        first_log.close()
        second_log = io.StringIO()
        stream.target = second_log

        cached_by_library.write("from build 2")

        assert second_log.getvalue() == "from build 2"
        assert stream.isatty() is False
        assert stream.encoding


class TestForgetSphinxConfModules:
    def test_forgets_conf_modules_but_keeps_build_script_and_unrelated_modules(self, monkeypatch):
        conf_module = types.ModuleType("docs.provider_conf")
        conf_module.__file__ = (DOCS_SOURCES_PATH / "provider_conf.py").as_posix()
        constants_module = types.ModuleType("docs.utils.conf_constants")
        constants_module.__file__ = (DOCS_SOURCES_PATH / "utils" / "conf_constants.py").as_posix()
        build_script = types.ModuleType("docs.build_docs")
        build_script.__file__ = (DOCS_SOURCES_PATH / "build_docs.py").as_posix()
        elsewhere = types.ModuleType("docs.elsewhere")
        elsewhere.__file__ = "/somewhere/else/docs/elsewhere.py"
        for module in (conf_module, constants_module, build_script, elsewhere):
            monkeypatch.setitem(sys.modules, module.__name__, module)

        _forget_sphinx_conf_modules()

        assert "docs.provider_conf" not in sys.modules
        assert "docs.utils.conf_constants" not in sys.modules
        assert sys.modules["docs.build_docs"] is build_script
        assert sys.modules["docs.elsewhere"] is elsewhere


class TestRunSphinxInProcess:
    def test_runs_build_main_with_output_in_log_file_and_restores_process_state(self, tmp_path, monkeypatch):
        builder = AirflowDocsBuilder(package_name="apache-airflow-providers-ftp")
        log_file = tmp_path / "build.log"
        previous_cwd = os.getcwd()
        previous_sys_path = list(sys.path)
        seen: dict[str, object] = {}

        def fake_build_main(argv):
            seen["argv"] = list(argv)
            seen["cwd"] = os.getcwd()
            seen["package"] = os.environ.get("AIRFLOW_PACKAGE_NAME")
            print("sphinx says hi")
            print("sphinx warns", file=sys.stderr)
            return 0

        monkeypatch.setattr(
            docs_builder,
            "build_main",
            mock.create_autospec(docs_builder.build_main, side_effect=fake_build_main),
        )
        monkeypatch.setattr(
            docs_builder,
            "_forget_sphinx_conf_modules",
            mock.create_autospec(docs_builder._forget_sphinx_conf_modules),
        )

        returncode = builder._run_sphinx(
            ["sphinx-build", "-T", "-b", "html", "src", "out"], log_file=log_file, verbose=False
        )

        assert returncode == 0
        assert seen["argv"] == ["-T", "-b", "html", "src", "out"]
        assert seen["cwd"] == AIRFLOW_CONTENT_ROOT_PATH.as_posix()
        assert seen["package"] == "apache-airflow-providers-ftp"
        assert log_file.read_text() == "sphinx says hi\nsphinx warns\n"
        assert os.getcwd() == previous_cwd
        assert sys.path == previous_sys_path
        docs_builder._forget_sphinx_conf_modules.assert_called_once_with()

    def test_autobuild_stays_a_subprocess(self, tmp_path, monkeypatch):
        builder = AirflowDocsBuilder(package_name="apache-airflow-providers-ftp")
        builder.is_autobuild = True
        run = mock.create_autospec(
            docs_builder.run, return_value=mock.Mock(spec=subprocess.CompletedProcess, returncode=3)
        )
        monkeypatch.setattr(docs_builder, "run", run)
        monkeypatch.setattr(docs_builder, "build_main", mock.create_autospec(docs_builder.build_main))

        returncode = builder._run_sphinx(
            ["sphinx-autobuild", "src", "out"], log_file=tmp_path / "log", verbose=False
        )

        assert returncode == 3
        run.assert_called_once()
        assert run.call_args.args[0] == ["sphinx-autobuild", "src", "out"]
        docs_builder.build_main.assert_not_called()
