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
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from airflow_breeze import NAME
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.functools_cache import clearable_cache
from airflow_breeze.utils.reinstall import inform_about_self_upgrade, reinstall_breeze, warn_non_editable
from airflow_breeze.utils.shared_options import get_verbose, set_forced_answer

PYPROJECT_TOML_FILE = "pyproject.toml"


def search_upwards_for_airflow_root_path(start_from: Path) -> Path | None:
    root = Path(start_from.root)
    directory = start_from
    while directory != root:
        airflow_candidate_init_py = directory / "airflow-core" / "src" / "airflow" / "__init__.py"
        if airflow_candidate_init_py.exists() and "airflow" in airflow_candidate_init_py.read_text().lower():
            return directory
        airflow_2_candidate_init_py = directory / "airflow" / "__init__.py"
        if (
            airflow_2_candidate_init_py.exists()
            and "airflow" in airflow_2_candidate_init_py.read_text().lower()
            and directory.parent.name != "src"
        ):
            return directory
        directory = directory.parent
    return None


def in_autocomplete() -> bool:
    return os.environ.get(f"_{NAME.upper()}_COMPLETE") is not None


def in_self_upgrade() -> bool:
    return "self-upgrade" in sys.argv


def in_help() -> bool:
    return "--help" in sys.argv or "-h" in sys.argv


def skip_breeze_self_upgrade_check():
    return (
        in_self_upgrade()
        or in_autocomplete()
        or in_help()
        or hasattr(sys, "_called_from_test")
        or os.environ.get("SKIP_BREEZE_SELF_UPGRADE_CHECK")
    )


def skip_group_output():
    return in_autocomplete() or in_help() or os.environ.get("SKIP_GROUP_OUTPUT") is not None


def get_pyproject_toml_hash(sources: Path) -> str:
    try:
        the_hash = hashlib.new("blake2b")
        the_hash.update((sources / "dev" / "breeze" / PYPROJECT_TOML_FILE).read_bytes())
        return the_hash.hexdigest()
    except FileNotFoundError as e:
        return f"Missing file {e.filename}"


def get_installation_sources_config_metadata_hash() -> str:
    """
    Retrieves hash of pyproject.toml from the source of installation of Breeze.

    This is used in order to determine if we need to upgrade Breeze, because some
    setup files changed. Blake2b algorithm will not be flagged by security checkers
    as insecure algorithm (in Python 3.9 and above we can use `usedforsecurity=False`
    to disable it, but for now it's better to use more secure algorithms.
    """
    installation_sources = get_installation_airflow_sources()
    if installation_sources is None:
        return "NOT FOUND"
    return get_pyproject_toml_hash(installation_sources)


def get_used_sources_setup_metadata_hash() -> str:
    """
    Retrieves hash of setup files from the currently used sources.
    """
    return get_pyproject_toml_hash(get_used_airflow_sources())


def set_forced_answer_for_upgrade_check():
    """When we run upgrade check --answer is not parsed yet, so we need to guess it."""
    if "--answer n" in " ".join(sys.argv).lower() or os.environ.get("ANSWER", "").lower().startswith("n"):
        set_forced_answer("no")
    if "--answer y" in " ".join(sys.argv).lower() or os.environ.get("ANSWER", "").lower().startswith("y"):
        set_forced_answer("yes")
    if "--answer q" in " ".join(sys.argv).lower() or os.environ.get("ANSWER", "").lower().startswith("q"):
        set_forced_answer("quit")


MY_BREEZE_ROOT_PATH = Path(__file__).resolve().parents[1]


def reinstall_if_setup_changed() -> bool:
    """
    Prints warning if detected airflow sources are not the ones that Breeze was installed with.
    :return: True if warning was printed.
    """

    res = subprocess.run(
        ["uv", "tool", "upgrade", "apache-airflow-breeze"],
        cwd=MY_BREEZE_ROOT_PATH,
        check=True,
        text=True,
        capture_output=True,
    )
    if "Modified" in res.stderr:
        inform_about_self_upgrade()
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
    return search_upwards_for_airflow_root_path(Path(__file__).resolve().parent)


def get_used_airflow_sources() -> Path:
    """
    Retrieves the Root of used Airflow Sources which we operate on. Those are either Airflow sources found
    upwards in directory tree or sources where Breeze was installed from.
    :return: the Path for Airflow sources we use.
    """
    current_sources = search_upwards_for_airflow_root_path(Path.cwd())
    if current_sources is None:
        current_sources = get_installation_airflow_sources()
        if current_sources is None:
            warn_non_editable()
            sys.exit(1)
    return current_sources


@clearable_cache
def find_airflow_root_path_to_operate_on() -> Path:
    """
    Find the root of airflow sources we operate on. Handle the case when Breeze is installed via
    `pipx` or `uv tool` from a different source tree, so it searches upwards of the current directory
    to find the right root of airflow directory we are actually in. This **might** be different
    than the sources of Airflow Breeze was installed from.

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
    sources_root_from_env = os.getenv("AIRFLOW_ROOT_PATH", None)
    if sources_root_from_env:
        return Path(sources_root_from_env)
    installation_airflow_sources = get_installation_airflow_sources()
    if installation_airflow_sources is None and not skip_breeze_self_upgrade_check():
        get_console().print(
            "\n[error]Breeze should only be installed with --editable flag[/]\n\n"
            "[warning]Please go to Airflow sources and run[/]\n\n"
            f"     {NAME} setup self-upgrade --use-current-airflow-sources\n"
            '[warning]If during installation you see warning starting "Ignoring --editable install",[/]\n'
            '[warning]make sure you first downgrade "packaging" package to <23.2, for example by:[/]\n\n'
            f'     pip install "packaging<23.2"\n\n'
        )
        sys.exit(1)
    airflow_sources = get_used_airflow_sources()
    if not skip_breeze_self_upgrade_check():
        # only print warning and sleep if not producing complete results
        reinstall_if_different_sources(airflow_sources)
        reinstall_if_setup_changed()
    os.chdir(airflow_sources.as_posix())
    airflow_home_dir = Path(os.environ.get("AIRFLOW_HOME", (Path.home() / "airflow").resolve().as_posix()))
    if airflow_sources.resolve() == airflow_home_dir.resolve():
        get_console().print(
            f"\n[error]Your Airflow sources are checked out in {airflow_home_dir} which "
            f"is your also your AIRFLOW_HOME where airflow writes logs and database. \n"
            f"This is a bad idea because Airflow might override and cleanup your checked out "
            f"sources and .git repository.[/]\n"
        )
        get_console().print("\nPlease check out your Airflow code elsewhere.\n")
        sys.exit(1)
    return airflow_sources


AIRFLOW_ROOT_PATH = find_airflow_root_path_to_operate_on().resolve()
AIRFLOW_PYPROJECT_TOML_FILE_PATH = AIRFLOW_ROOT_PATH / "pyproject.toml"
AIRFLOW_CORE_ROOT_PATH = AIRFLOW_ROOT_PATH / "airflow-core"
AIRFLOW_CORE_SOURCES_PATH = AIRFLOW_CORE_ROOT_PATH / "src"
AIRFLOW_TASK_SDK_ROOT_PATH = AIRFLOW_ROOT_PATH / "task-sdk"
AIRFLOW_TASK_SDK_SOURCES_PATH = AIRFLOW_TASK_SDK_ROOT_PATH / "src"
AIRFLOW_WWW_DIR = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "www"
AIRFLOW_UI_DIR = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "ui"
# Do not delete it - it is used for old commit retrieval from providers
AIRFLOW_ORIGINAL_PROVIDERS_DIR = AIRFLOW_ROOT_PATH / "airflow" / "providers"
AIRFLOW_PROVIDERS_ROOT_PATH = AIRFLOW_ROOT_PATH / "providers"
AIRFLOW_DIST_PATH = AIRFLOW_ROOT_PATH / "dist"

TASK_SDK_ROOT_PATH = AIRFLOW_ROOT_PATH / "task-sdk"
TASK_SDK_DIST_PATH = TASK_SDK_ROOT_PATH / "dist"
TASK_SDK_SOURCES_PATH = TASK_SDK_ROOT_PATH / "src"

AIRFLOW_CTL_ROOT_PATH = AIRFLOW_ROOT_PATH / "airflow-ctl"
AIRFLOW_CTL_SOURCES_PATH = AIRFLOW_CTL_ROOT_PATH / "src"
AIRFLOW_CTL_DIST_PATH = AIRFLOW_CTL_ROOT_PATH / "dist"

# Same here - do not remove those this is used for past commit retrieval
PREVIOUS_AIRFLOW_PROVIDERS_SOURCES_PATH = AIRFLOW_PROVIDERS_ROOT_PATH / "src"
PREVIOUS_AIRFLOW_PROVIDERS_NS_PACKAGE_PATH = PREVIOUS_AIRFLOW_PROVIDERS_SOURCES_PATH / "airflow" / "providers"
PREVIOUS_TESTS_PROVIDERS_ROOT = AIRFLOW_PROVIDERS_ROOT_PATH / "tests"
PREVIOUS_SYSTEM_TESTS_PROVIDERS_PATH = AIRFLOW_PROVIDERS_ROOT_PATH / "tests" / "system"

AIRFLOW_DEVEL_COMMON_PATH = AIRFLOW_ROOT_PATH / "devel-common"
DOCS_ROOT = AIRFLOW_ROOT_PATH / "docs"
BUILD_CACHE_PATH = AIRFLOW_ROOT_PATH / ".build"
GENERATED_PATH = AIRFLOW_ROOT_PATH / "generated"
CONSTRAINTS_CACHE_PATH = BUILD_CACHE_PATH / "constraints"
PROVIDER_DEPENDENCIES_JSON_PATH = GENERATED_PATH / "provider_dependencies.json"
PROVIDER_METADATA_JSON_PATH = GENERATED_PATH / "provider_metadata.json"
UI_CACHE_PATH = BUILD_CACHE_PATH / "ui"
AIRFLOW_TMP_PATH = AIRFLOW_ROOT_PATH / "tmp"
UI_ASSET_COMPILE_LOCK = UI_CACHE_PATH / ".asset_compile.lock"
UI_ASSET_OUT_FILE = UI_CACHE_PATH / "asset_compile.out"
UI_ASSET_OUT_DEV_MODE_FILE = UI_CACHE_PATH / "asset_compile_dev_mode.out"
UI_ASSET_HASH_PATH = AIRFLOW_ROOT_PATH / ".build" / "ui" / "hash.txt"
UI_NODE_MODULES_PATH = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "ui" / "node_modules"
UI_DIST_PATH = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "ui" / "dist"

DAGS_PATH = AIRFLOW_ROOT_PATH / "dags"
FILES_PATH = AIRFLOW_ROOT_PATH / "files"
FILES_SBOM_PATH = FILES_PATH / "sbom"
KUBE_PATH = AIRFLOW_ROOT_PATH / ".kube"
LOGS_PATH = AIRFLOW_ROOT_PATH / "logs"
OUT_PATH = AIRFLOW_ROOT_PATH / "out"

REPRODUCIBLE_PATH = OUT_PATH / "reproducible"
DOCS_DIR = AIRFLOW_ROOT_PATH / "docs"
SCRIPTS_CI_PATH = AIRFLOW_ROOT_PATH / "scripts" / "ci"
SCRIPTS_DOCKER_PATH = AIRFLOW_ROOT_PATH / "scripts" / "docker"
SCRIPTS_CI_DOCKER_COMPOSE_PATH = SCRIPTS_CI_PATH / "docker-compose"
SCRIPTS_CI_DOCKER_COMPOSE_BASE_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "base.yml"
SCRIPTS_CI_DOCKER_COMPOSE_BASE_PORTS_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "base-ports.yml"
SCRIPTS_CI_DOCKER_COMPOSE_CI_TESTS_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "ci-tests.yml"
SCRIPTS_CI_DOCKER_COMPOSE_DEBUG_PORTS_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "debug-ports.yml"
SCRIPTS_CI_DOCKER_COMPOSE_DOCKER_SOCKET_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "docker-socket.yml"
SCRIPTS_CI_DOCKER_COMPOSE_ENABLE_TTY_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "enable-tty.yml"
SCRIPTS_CI_DOCKER_COMPOSE_FILES_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "files.yml"
SCRIPTS_CI_DOCKER_COMPOSE_FORWARD_CREDENTIALS_PATH = (
    SCRIPTS_CI_DOCKER_COMPOSE_PATH / "forward-credentials.yml"
)
SCRIPTS_CI_DOCKER_COMPOSE_INTEGRATION_CELERY_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "integration-celery.yml"
SCRIPTS_CI_DOCKER_COMPOSE_INTEGRATION_KERBEROS_PATH = (
    SCRIPTS_CI_DOCKER_COMPOSE_PATH / "integration-kerberos.yml"
)
SCRIPTS_CI_DOCKER_COMPOSE_LOCAL_ALL_SOURCES_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "local-all-sources.yml"
SCRIPTS_CI_DOCKER_COMPOSE_LOCAL_YAML_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "local.yml"
SCRIPTS_CI_DOCKER_COMPOSE_MYPY_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "local.yml"
SCRIPTS_CI_DOCKER_COMPOSE_PROVIDERS_AND_TESTS_SOURCES_PATH = (
    SCRIPTS_CI_DOCKER_COMPOSE_PATH / "providers-and-tests-sources.yml"
)
SCRIPTS_CI_DOCKER_COMPOSE_REMOVE_SOURCES_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "remove-sources.yml"
SCRIPTS_CI_DOCKER_COMPOSE_TESTS_SOURCES_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "tests-sources.yml"
GENERATED_DOCKER_COMPOSE_ENV_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "_generated_docker_compose.env"
GENERATED_DOCKER_ENV_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "_generated_docker.env"
GENERATED_DOCKER_LOCK_PATH = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "_generated.lock"
DOCKER_CONTEXT_PATH = AIRFLOW_ROOT_PATH / "docker-context-files"
CACHE_TEMP_PATH = tempfile.TemporaryDirectory()
OUTPUT_LOG_PATH = Path(CACHE_TEMP_PATH.name, "out.log")
BREEZE_ROOT_PATH = AIRFLOW_ROOT_PATH / "dev" / "breeze"
BREEZE_SOURCES_PATH = BREEZE_ROOT_PATH / "src"
BREEZE_DOC_PATH = BREEZE_ROOT_PATH / "doc"
BREEZE_IMAGES_PATH = BREEZE_DOC_PATH / "images"
AIRFLOW_HOME_PATH = Path(os.environ.get("AIRFLOW_HOME", Path.home() / "airflow"))


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
    BUILD_CACHE_PATH.mkdir(parents=True, exist_ok=True)
    DAGS_PATH.mkdir(parents=True, exist_ok=True)
    FILES_PATH.mkdir(parents=True, exist_ok=True)
    KUBE_PATH.mkdir(parents=True, exist_ok=True)
    LOGS_PATH.mkdir(parents=True, exist_ok=True)
    AIRFLOW_DIST_PATH.mkdir(parents=True, exist_ok=True)
    OUTPUT_LOG_PATH.mkdir(parents=True, exist_ok=True)
    (AIRFLOW_ROOT_PATH / ".bash_aliases").touch()
    (AIRFLOW_ROOT_PATH / ".bash_history").touch()
    (AIRFLOW_ROOT_PATH / ".inputrc").touch()


def cleanup_python_generated_files():
    if get_verbose():
        get_console().print("[info]Cleaning .pyc and __pycache__")
    permission_errors = []
    for path in AIRFLOW_ROOT_PATH.rglob("*.pyc"):
        try:
            path.unlink()
        except FileNotFoundError:
            # File has been removed in the meantime.
            pass
        except PermissionError:
            permission_errors.append(path)
    for path in AIRFLOW_ROOT_PATH.rglob("__pycache__"):
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            # File has been removed in the meantime.
            pass
        except PermissionError:
            permission_errors.append(path)
    if permission_errors:
        if platform.uname().system.lower() == "linux":
            get_console().print("[warning]There were files that you could not clean-up:\n")
            get_console().print(permission_errors)
            get_console().print(
                "Please run at earliest convenience:\n"
                "[warning]breeze ci fix-ownership[/]\n\n"
                "If you have sudo you can use:\n"
                "[warning]breeze ci fix-ownership --use-sudo[/]\n\n"
                "This will fix ownership of those.\n"
                "You can also remove those files manually using sudo."
            )
        else:
            get_console().print("[warnings]There were files that you could not clean-up:\n")
            get_console().print(permission_errors)
            get_console().print("You can also remove those files manually using sudo.")
    if get_verbose():
        get_console().print("[info]Cleaned")
