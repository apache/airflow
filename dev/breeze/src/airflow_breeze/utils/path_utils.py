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
"""
Useful tools for various Paths used inside Airflow Sources.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile
from functools import lru_cache
from pathlib import Path

from airflow_breeze import NAME
from airflow_breeze.utils.confirm import set_forced_answer
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.reinstall import reinstall_breeze, warn_dependencies_changed, warn_non_editable

AIRFLOW_CFG_FILE = "setup.cfg"


def search_upwards_for_airflow_sources_root(start_from: Path) -> Path | None:
    root = Path(start_from.root)
    d = start_from
    while d != root:
        attempt = d / AIRFLOW_CFG_FILE
        if attempt.exists() and "name = apache-airflow\n" in attempt.read_text():
            return attempt.parent
        d = d.parent
    return None


def in_autocomplete() -> bool:
    return os.environ.get(f"_{NAME.upper()}_COMPLETE") is not None


def in_self_upgrade() -> bool:
    return "self-upgrade" in sys.argv


def in_help() -> bool:
    return "--help" in sys.argv


def skip_upgrade_check():
    return in_self_upgrade() or in_autocomplete() or in_help() or hasattr(sys, '_called_from_test')


def skip_group_output():
    return in_autocomplete() or in_help() or os.environ.get('SKIP_GROUP_OUTPUT') is not None


def get_package_setup_metadata_hash() -> str:
    """
    Retrieves hash of setup files from the source of installation of Breeze.

    This is used in order to determine if we need to upgrade Breeze, because some
    setup files changed. Blake2b algorithm will not be flagged by security checkers
    as insecure algorithm (in Python 3.9 and above we can use `usedforsecurity=False`
    to disable it, but for now it's better to use more secure algorithms.
    """
    # local imported to make sure that autocomplete works
    try:
        from importlib.metadata import distribution  # type: ignore[attr-defined]
    except ImportError:
        from importlib_metadata import distribution  # type: ignore[no-redef]

    prefix = "Package config hash: "

    for line in distribution('apache-airflow-breeze').metadata.as_string().splitlines(keepends=False):
        if line.startswith(prefix):
            return line[len(prefix) :]
    return "NOT FOUND"


def get_sources_setup_metadata_hash(sources: Path) -> str:
    try:
        the_hash = hashlib.new("blake2b")
        the_hash.update((sources / "dev" / "breeze" / "setup.py").read_bytes())
        the_hash.update((sources / "dev" / "breeze" / "setup.cfg").read_bytes())
        the_hash.update((sources / "dev" / "breeze" / "pyproject.toml").read_bytes())
        return the_hash.hexdigest()
    except FileNotFoundError as e:
        return f"Missing file {e.filename}"


def get_installation_sources_config_metadata_hash() -> str:
    """
    Retrieves hash of setup.py and setup.cfg files from the source of installation of Breeze.

    This is used in order to determine if we need to upgrade Breeze, because some
    setup files changed. Blake2b algorithm will not be flagged by security checkers
    as insecure algorithm (in Python 3.9 and above we can use `usedforsecurity=False`
    to disable it, but for now it's better to use more secure algorithms.
    """
    installation_sources = get_installation_airflow_sources()
    if installation_sources is None:
        return "NOT FOUND"
    return get_sources_setup_metadata_hash(installation_sources)


def get_used_sources_setup_metadata_hash() -> str:
    """
    Retrieves hash of setup files from the currently used sources.
    """
    return get_sources_setup_metadata_hash(get_used_airflow_sources())


def set_forced_answer_for_upgrade_check():
    """When we run upgrade check --answer is not parsed yet, so we need to guess it."""
    if "--answer n" in " ".join(sys.argv).lower() or os.environ.get('ANSWER', '').lower().startswith("n"):
        set_forced_answer("no")
    if "--answer y" in " ".join(sys.argv).lower() or os.environ.get('ANSWER', '').lower().startswith("y"):
        set_forced_answer("yes")
    if "--answer q" in " ".join(sys.argv).lower() or os.environ.get('ANSWER', '').lower().startswith("q"):
        set_forced_answer("quit")


def reinstall_if_setup_changed() -> bool:
    """
    Prints warning if detected airflow sources are not the ones that Breeze was installed with.
    :return: True if warning was printed.
    """
    try:
        package_hash = get_package_setup_metadata_hash()
    except ModuleNotFoundError as e:
        if "importlib_metadata" in e.msg:
            return False
    sources_hash = get_installation_sources_config_metadata_hash()
    if sources_hash != package_hash:
        installation_sources = get_installation_airflow_sources()
        if installation_sources is not None:
            breeze_sources = installation_sources / "dev" / "breeze"
            warn_dependencies_changed()
            set_forced_answer_for_upgrade_check()
            reinstall_breeze(breeze_sources)
            set_forced_answer(None)
        return True
    return False


def reinstall_if_different_sources(airflow_sources: Path) -> bool:
    """
    Prints warning if detected airflow sources are not the ones that Breeze was installed with.
    :param airflow_sources: source for airflow code that we are operating on
    :return: True if warning was printed.
    """
    installation_airflow_sources = get_installation_airflow_sources()
    if installation_airflow_sources and airflow_sources != installation_airflow_sources:
        reinstall_breeze(airflow_sources / "dev" / "breeze")
        return True
    return False


def get_installation_airflow_sources() -> Path | None:
    """
    Retrieves the Root of the Airflow Sources where Breeze was installed from.
    :return: the Path for Airflow sources.
    """
    return search_upwards_for_airflow_sources_root(Path(__file__).resolve().parent)


def get_used_airflow_sources() -> Path:
    """
    Retrieves the Root of used Airflow Sources which we operate on. Those are either Airflow sources found
    upwards in directory tree or sources where Breeze was installed from.
    :return: the Path for Airflow sources we use.
    """
    current_sources = search_upwards_for_airflow_sources_root(Path.cwd())
    if current_sources is None:
        current_sources = get_installation_airflow_sources()
        if current_sources is None:
            warn_non_editable()
            sys.exit(1)
    return current_sources


@lru_cache(maxsize=None)
def find_airflow_sources_root_to_operate_on() -> Path:
    """
    Find the root of airflow sources we operate on. Handle the case when Breeze is installed via `pipx` from
    a different source tree, so it searches upwards of the current directory to find the right root of
    airflow directory we are actually in. This **might** be different than the sources of Airflow Breeze
    was installed from.

    If not found, we operate on Airflow sources that we were installed it. This handles the case when
    we run Breeze from a "random" directory.

    This method also handles the following errors and warnings:

       * It fails (and exits hard) if Breeze is installed in non-editable mode (in which case it will
         not find the Airflow sources when walking upwards the directory where it is installed)
       * It warns (with 2 seconds timeout) if you are using Breeze from a different airflow sources than
         the one you operate on.
       * If we are running in the same source tree as where Breeze was installed from (so no warning above),
         it warns (with 2 seconds timeout) if there is a change in setup.* files of Breeze since installation
         time. In such case usesr is encouraged to re-install Breeze to update dependencies.

    :return: Path for the found sources.

    """
    sources_root_from_env = os.getenv('AIRFLOW_SOURCES_ROOT', None)
    if sources_root_from_env:
        return Path(sources_root_from_env)
    installation_airflow_sources = get_installation_airflow_sources()
    if installation_airflow_sources is None and not skip_upgrade_check():
        get_console().print(
            "\n[error]Breeze should only be installed with -e flag[/]\n\n"
            "[warning]Please go to Airflow sources and run[/]\n\n"
            f"     {NAME} self-upgrade --force\n"
        )
        sys.exit(1)
    airflow_sources = get_used_airflow_sources()
    if not skip_upgrade_check():
        # only print warning and sleep if not producing complete results
        reinstall_if_different_sources(airflow_sources)
        reinstall_if_setup_changed()
    os.chdir(str(airflow_sources))
    return airflow_sources


AIRFLOW_SOURCES_ROOT = find_airflow_sources_root_to_operate_on().resolve()
BUILD_CACHE_DIR = AIRFLOW_SOURCES_ROOT / '.build'
DAGS_DIR = AIRFLOW_SOURCES_ROOT / 'dags'
FILES_DIR = AIRFLOW_SOURCES_ROOT / 'files'
HOOKS_DIR = AIRFLOW_SOURCES_ROOT / 'hooks'
MSSQL_DATA_VOLUME = AIRFLOW_SOURCES_ROOT / 'tmp_mssql_volume'
KUBE_DIR = AIRFLOW_SOURCES_ROOT / ".kube"
LOGS_DIR = AIRFLOW_SOURCES_ROOT / 'logs'
DIST_DIR = AIRFLOW_SOURCES_ROOT / 'dist'
SCRIPTS_CI_DIR = AIRFLOW_SOURCES_ROOT / 'scripts' / 'ci'
DOCKER_CONTEXT_DIR = AIRFLOW_SOURCES_ROOT / 'docker-context-files'
CACHE_TMP_FILE_DIR = tempfile.TemporaryDirectory()
OUTPUT_LOG = Path(CACHE_TMP_FILE_DIR.name, 'out.log')
BREEZE_SOURCES_ROOT = AIRFLOW_SOURCES_ROOT / "dev" / "breeze"


def create_volume_if_missing(volume_name: str):
    from airflow_breeze.utils.run_utils import run_command

    res_inspect = run_command(
        cmd=["docker", "volume", "inspect", volume_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if res_inspect.returncode != 0:
        result = run_command(
            cmd=["docker", "volume", "create", volume_name],
            check=False,
            capture_output=True,
        )
        if result.returncode != 0:
            get_console().print(
                "[warning]\nMypy Cache volume could not be created. Continuing, but you "
                "should make sure your docker works.\n\n"
                f"Error: {result.stdout}\n"
            )


def create_mypy_volume_if_needed():
    create_volume_if_missing("mypy-cache-volume")


def create_directories_and_files() -> None:
    """
    Creates all directories and files that are needed for Breeze to work via docker-compose.
    Checks if setup has been updates since last time and proposes to upgrade if so.
    """
    BUILD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DAGS_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    HOOKS_DIR.mkdir(parents=True, exist_ok=True)
    MSSQL_DATA_VOLUME.mkdir(parents=True, exist_ok=True)
    KUBE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_LOG.mkdir(parents=True, exist_ok=True)
    (AIRFLOW_SOURCES_ROOT / ".bash_aliases").touch()
    (AIRFLOW_SOURCES_ROOT / ".bash_history").touch()
    (AIRFLOW_SOURCES_ROOT / ".inputrc").touch()
