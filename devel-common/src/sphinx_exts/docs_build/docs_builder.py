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

import contextlib
import os
import re
import shlex
import shutil
import signal
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from subprocess import run

from rich.console import Console
from sphinx.cmd.build import build_main

from sphinx_exts.docs_build.code_utils import (
    AIRFLOW_CONTENT_ROOT_PATH,
    ALL_PROVIDER_YAMLS,
    ALL_PROVIDER_YAMLS_WITH_SUSPENDED,
    CONSOLE_WIDTH,
    DOCS_SOURCES_PATH,
    GENERATED_PATH,
    PROCESS_TIMEOUT,
)
from sphinx_exts.docs_build.errors import DocBuildError, parse_sphinx_warnings
from sphinx_exts.docs_build.spelling_checks import SpellingError, parse_spelling_warnings

console = Console(force_terminal=True, color_system="standard", width=CONSOLE_WIDTH)

# Sphinx is run in the current process (one worker builds many packages in a row) so that the fixed
# start-up cost of a build is paid once per worker instead of once per package. For a small provider
# that fixed cost is ~20s out of ~22s: importing airflow, parsing and validating every provider.yaml,
# and - the largest part - autoapi's astroid parsing of the airflow modules every provider imports.
# The astroid parse cache in particular stays warm across the packages a worker builds.


def _forget_sphinx_conf_modules() -> None:
    """
    Drop the shared Sphinx configuration modules so the next conf.py re-executes them from scratch.

    The per-package ``conf.py`` files do ``from docs.provider_conf import *`` (or import
    ``docs.utils.conf_constants``) and then mutate the lists they get - ``extensions.append(...)``,
    ``autoapi_ignore.extend(...)``. Reusing the cached module would leak one package's additions into
    the next build, so everything under ``devel-common/src/docs`` except the build script is forgotten.
    """
    docs_sources_prefix = DOCS_SOURCES_PATH.as_posix()
    for name, module in list(sys.modules.items()):
        if name == "docs.build_docs" or not name.startswith("docs."):
            continue
        module_file = getattr(module, "__file__", None) or ""
        if module_file.startswith(docs_sources_prefix):
            del sys.modules[name]


class _RedirectableStream:
    """
    Stand-in for ``sys.stdout`` / ``sys.stderr`` that lives as long as the worker process.

    Libraries keep a reference to whatever stream is current when they first need one (docutils'
    ``Reporter`` in sphinx-argparse's nested parser is one example). Redirecting straight to a
    per-package log file would leave such references pointing at a closed file once that package is
    done and crash the next build with "I/O operation on closed file". This object never closes; it
    only changes where it writes: the current package's log file during a build, the worker's real
    stream otherwise.
    """

    def __init__(self, fallback) -> None:
        self._fallback = fallback
        self.target = None

    def _stream(self):
        return self.target if self.target is not None else self._fallback

    def write(self, data: str) -> int:
        return self._stream().write(data)

    def flush(self) -> None:
        self._stream().flush()

    def isatty(self) -> bool:
        return False

    @property
    def encoding(self) -> str:
        return getattr(self._stream(), "encoding", None) or "utf-8"


_STDOUT_PROXY = _RedirectableStream(sys.stdout)
_STDERR_PROXY = _RedirectableStream(sys.stderr)


@contextlib.contextmanager
def _output_to(log_file) -> Iterator[None]:
    """Send everything written to stdout/stderr during the block to ``log_file``."""
    _STDOUT_PROXY.target = log_file
    _STDERR_PROXY.target = log_file
    try:
        with contextlib.redirect_stdout(_STDOUT_PROXY), contextlib.redirect_stderr(_STDERR_PROXY):
            yield
    finally:
        _STDOUT_PROXY.target = None
        _STDERR_PROXY.target = None


@contextlib.contextmanager
def _working_directory(path: Path) -> Iterator[None]:
    previous = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@contextlib.contextmanager
def _sys_path_prepended(paths: list[Path]) -> Iterator[None]:
    entries = [path.as_posix() for path in paths]
    sys.path[:0] = entries
    try:
        yield
    finally:
        for entry in entries:
            with contextlib.suppress(ValueError):
                sys.path.remove(entry)


@contextlib.contextmanager
def _build_timeout(seconds: int) -> Iterator[None]:
    """Abort a runaway in-process build the way the previous subprocess timeout did."""
    if not hasattr(signal, "SIGALRM") or threading.current_thread() is not threading.main_thread():
        yield
        return

    def _raise_timeout(signum, frame):
        raise TimeoutError(f"Sphinx build did not finish within {seconds} seconds")

    previous_handler = signal.signal(signal.SIGALRM, _raise_timeout)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


class AirflowDocsBuilder:
    """Documentation builder for Airflow."""

    def __init__(self, package_name: str) -> None:
        self.package_name = package_name
        self.is_provider = False
        self.is_airflow = False
        self.is_chart = False
        self.is_docker_stack = False
        self.is_task_sdk = False
        self.is_providers_summary = False
        self.is_autobuild = False
        if self.package_name.startswith("apache-airflow-providers-"):
            self.package_id = self.package_name.split("apache-airflow-providers-", 1)[1].replace("-", ".")
            self.provider_path = (AIRFLOW_CONTENT_ROOT_PATH / "providers").joinpath(
                *self.package_id.split(".")
            )
            self.is_provider = True
        if self.package_name == "apache-airflow":
            self.is_airflow = True
        if self.package_name == "helm-chart":
            self.is_chart = True
        if self.package_name == "task-sdk":
            self.is_task_sdk = True
        if self.package_name == "docker-stack":
            self.is_docker_stack = True
        if self.package_name == "apache-airflow-providers":
            self.is_providers_summary = True
        if self.package_name == "apache-airflow-ctl":
            self.is_airflow_ctl = True

    @property
    def _doctree_dir(self) -> Path:
        return GENERATED_PATH / "_doctrees" / "docs" / self.package_name

    @property
    def is_versioned(self):
        """Is current documentation package versioned?"""
        # Disable versioning. This documentation does not apply to any released product and we can update
        # it as needed, i.e. with each new package of providers.
        return self.package_name not in ("apache-airflow-providers", "docker-stack")

    @property
    def _build_dir(self) -> Path:
        if self.is_versioned:
            version = "stable"
            return GENERATED_PATH / "_build" / "docs" / self.package_name / version
        return GENERATED_PATH / "_build" / "docs" / self.package_name

    @property
    def log_spelling_filename(self) -> Path:
        """Log from spelling job."""
        return self._build_dir / f"output-spelling-{self.package_name}.log"

    @property
    def log_spelling_output_dir(self) -> Path:
        """Results from spelling job."""
        return self._build_dir / f"output-spelling-results-{self.package_name}"

    @property
    def log_build_filename(self) -> Path:
        """Log from build job."""
        return self._build_dir / f"output-build-{self.package_name}.log"

    @property
    def log_build_warning_filename(self) -> Path:
        """Warnings from build job."""
        return self._build_dir / f"warning-build-{self.package_name}.log"

    @property
    def _src_dir(self) -> Path:
        if self.package_name == "helm-chart":
            return AIRFLOW_CONTENT_ROOT_PATH / "chart" / "docs"
        if self.package_name == "apache-airflow":
            return AIRFLOW_CONTENT_ROOT_PATH / "airflow-core" / "docs"
        if self.package_name == "docker-stack":
            return AIRFLOW_CONTENT_ROOT_PATH / "docker-stack-docs"
        if self.package_name == "apache-airflow-providers":
            return AIRFLOW_CONTENT_ROOT_PATH / "providers-summary-docs"
        if self.package_name.startswith("apache-airflow-providers-"):
            package_paths = self.package_name[len("apache-airflow-providers-") :].split("-")
            return (AIRFLOW_CONTENT_ROOT_PATH / "providers").joinpath(*package_paths) / "docs"
        if self.package_name == "apache-airflow-ctl":
            return AIRFLOW_CONTENT_ROOT_PATH / "airflow-ctl" / "docs"
        if self.package_name == "apache-airflow-mypy":
            return AIRFLOW_CONTENT_ROOT_PATH / "dev" / "mypy" / "docs"
        if self.package_name == "task-sdk":
            return AIRFLOW_CONTENT_ROOT_PATH / "task-sdk" / "docs"
        console.print(f"[red]Unknown package name: {self.package_name}")
        sys.exit(1)

    @property
    def pythonpath(self) -> list[Path]:
        path = []
        if (self._src_dir.parent / "tests").exists():
            path.append(self._src_dir.parent.joinpath("tests").resolve())
        return path

    @property
    def _generated_api_dir(self) -> Path:
        return self._build_dir.resolve() / "_api"

    @property
    def _api_dir(self) -> Path:
        return self._src_dir.resolve() / "_api"

    def clean_files(self) -> None:
        """Cleanup all artifacts generated by previous builds."""
        shutil.rmtree(self._api_dir, ignore_errors=True)
        shutil.rmtree(self._build_dir, ignore_errors=True)
        shutil.rmtree(self._doctree_dir, ignore_errors=True)
        self._api_dir.mkdir(parents=True, exist_ok=True)
        self._build_dir.mkdir(parents=True, exist_ok=True)

    def check_spelling(self, verbose: bool) -> tuple[list[SpellingError], list[DocBuildError]]:
        """
        Checks spelling

        :param verbose: whether to show output while running
        :return: list of errors
        """
        spelling_errors = []
        build_errors = []
        os.makedirs(self._build_dir, exist_ok=True)
        shutil.rmtree(self.log_spelling_output_dir, ignore_errors=True)
        self.log_spelling_output_dir.mkdir(parents=True, exist_ok=True)

        command = self.get_command()
        build_cmd = [
            command,
            "-W",  # turn warnings into errors
            "--color",  # do emit colored output
            "-T",  # show full traceback on exception
            "-b",  # builder to use
            "spelling",
            "-d",  # path for the cached environment and doctree files
            self._doctree_dir.as_posix(),
            # documentation source files
            self._src_dir.as_posix(),
            self.log_spelling_output_dir.as_posix(),
        ]
        if os.environ.get("CI", "") != "true" and verbose:
            console.print("[yellow]Command to run:[/] ", " ".join([shlex.quote(arg) for arg in build_cmd]))
        if verbose:
            console.print(
                f"[bright_blue]{self.package_name:60}:[/] The output is hidden until an error occurs."
            )
        returncode = self._run_sphinx(build_cmd, log_file=self.log_spelling_filename, verbose=verbose)
        if returncode != 0:
            spelling_errors.append(
                SpellingError(
                    file_path=None,
                    line_no=None,
                    spelling=None,
                    suggestion=None,
                    context_line=None,
                    message=f"Sphinx spellcheck returned non-zero exit status: {returncode}.",
                )
            )
            spelling_warning_text = ""
            for filepath in self.log_spelling_output_dir.rglob("*.spelling"):
                with open(filepath) as spelling_file:
                    spelling_warning_text += spelling_file.read()
            spelling_errors.extend(parse_spelling_warnings(spelling_warning_text, self._src_dir))
            if os.path.isfile(self.log_spelling_filename):
                with open(self.log_spelling_filename) as warning_file:
                    warning_text = warning_file.read()
                # Remove 7-bit C1 ANSI escape sequences
                warning_text = re.sub(r"\x1B[@-_][0-?]*[ -/]*[@-~]", "", warning_text)
                build_errors.extend(parse_sphinx_warnings(warning_text, self._src_dir))
            console.print(
                f"[bright_blue]{self.package_name:60}:[/] [red]Finished spell-checking with errors[/]"
            )
        else:
            if spelling_errors:
                console.print(
                    f"[bright_blue]{self.package_name:60}:[/] [yellow]Finished spell-checking with warnings[/]"
                )
            else:
                console.print(
                    f"[bright_blue]{self.package_name:60}:[/] [green]Finished spell-checking successfully[/]"
                )
        return spelling_errors, build_errors

    def build_sphinx_docs(self, verbose: bool) -> list[DocBuildError]:
        """
        Build Sphinx documentation.

        :param verbose: whether to show output while running
        :return: list of errors
        """
        build_errors = []
        os.makedirs(self._build_dir, exist_ok=True)
        command = self.get_command()
        build_cmd = [
            command,
            "-T",  # show full traceback on exception
            "--color",  # do emit colored output
            "-b",  # builder to use
            "html",
            "-d",  # path for the cached environment and doctree files
            self._doctree_dir.as_posix(),
            "-w",  # write warnings (and errors) to given file
            self.log_build_warning_filename.as_posix(),
            # documentation source files
            self._src_dir.as_posix(),
            self._build_dir.as_posix(),  # path to output directory
        ]
        if os.environ.get("CI", "") != "true" and verbose:
            console.print("[yellow]Command to run:[/] ", " ".join([shlex.quote(arg) for arg in build_cmd]))
        if verbose:
            console.print(
                f"[bright_blue]{self.package_name:60}:[/] Running sphinx. "
                f"The output is hidden until an error occurs."
            )
        returncode = self._run_sphinx(build_cmd, log_file=self.log_build_filename, verbose=verbose)
        if returncode != 0:
            build_errors.append(
                DocBuildError(
                    file_path=None,
                    line_no=None,
                    message=f"Sphinx returned non-zero exit status: {returncode}.",
                )
            )
        if self.log_build_warning_filename.is_file():
            warning_text = self.log_build_warning_filename.read_text()
            # Remove 7-bit C1 ANSI escape sequences
            warning_text = re.sub(r"\x1B[@-_][0-?]*[ -/]*[@-~]", "", warning_text)
            build_errors.extend(parse_sphinx_warnings(warning_text, self._src_dir))
        if build_errors:
            console.print(
                f"[bright_blue]{self.package_name:60}:[/] [red]Finished docs building with errors[/]"
            )
        else:
            console.print(
                f"[bright_blue]{self.package_name:60}:[/] [green]Finished docs building successfully[/]"
            )
        return build_errors

    def get_command(self) -> str:
        return "sphinx-autobuild" if self.is_autobuild else "sphinx-build"

    def _run_sphinx(self, build_cmd: list[str], *, log_file: Path, verbose: bool) -> int:
        """
        Run a ``sphinx-build`` / ``sphinx-autobuild`` command line and return its exit status.

        ``sphinx-build`` runs in the current process (see the module comment for why); its output goes
        to ``log_file`` unless ``verbose`` is set. ``sphinx-autobuild`` is a long-running server and
        keeps running as a subprocess.
        """
        if self.is_autobuild:
            env = os.environ.copy()
            env["AIRFLOW_PACKAGE_NAME"] = self.package_name
            if self.pythonpath:
                env["PYTHONPATH"] = ":".join([path.as_posix() for path in self.pythonpath])
            with open(log_file, "w") as output:
                completed_proc = run(
                    build_cmd,
                    check=False,
                    cwd=AIRFLOW_CONTENT_ROOT_PATH,
                    env=env,
                    stdout=output if not verbose else None,
                    stderr=output if not verbose else None,
                    timeout=PROCESS_TIMEOUT,
                )
            return completed_proc.returncode
        _forget_sphinx_conf_modules()
        os.environ["AIRFLOW_PACKAGE_NAME"] = self.package_name
        with (
            open(log_file, "w") as output,
            _output_to(output) if not verbose else contextlib.nullcontext(),
            _working_directory(AIRFLOW_CONTENT_ROOT_PATH),
            _sys_path_prepended(self.pythonpath),
            _build_timeout(PROCESS_TIMEOUT),
        ):
            # Sphinx reports a TimeoutError raised by the alarm like any other build failure.
            return build_main(build_cmd[1:])


def get_available_providers_distributions(include_suspended: bool = False):
    """Get list of all available providers packages to build."""
    return [
        provider["package-name"]
        for provider in (ALL_PROVIDER_YAMLS_WITH_SUSPENDED if include_suspended else ALL_PROVIDER_YAMLS)
    ]


def get_short_form(package_name: str) -> str | None:
    if package_name.startswith("apache-airflow-providers-"):
        return package_name.replace("apache-airflow-providers-", "").replace("-", ".")
    return None


def get_long_form(package_name: str) -> str | None:
    if package_name in get_available_packages():
        return package_name
    long_form = "apache-airflow-providers-" + package_name.replace(".", "-")
    if long_form not in get_available_packages():
        return None
    return long_form


def get_available_packages(include_suspended: bool = False, short_form: bool = False) -> list[str]:
    """Get list of all available packages to build."""
    provider_names = get_available_providers_distributions(include_suspended=include_suspended)
    if short_form:
        provider_names = [get_short_form(name) for name in provider_names]
    return [
        "apache-airflow",
        *provider_names,
        "apache-airflow-providers",
        "apache-airflow-ctl",
        "apache-airflow-mypy",
        "task-sdk",
        "helm-chart",
        "docker-stack",
    ]
