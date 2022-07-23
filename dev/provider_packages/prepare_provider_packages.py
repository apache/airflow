#!/usr/bin/env python3

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
"""Setup.py for the Provider packages of Airflow project."""
import collections
import difflib
import glob
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache
from os.path import dirname, relpath
from pathlib import Path
from random import choice
from shutil import copyfile
from typing import Any, Dict, Generator, Iterable, List, NamedTuple, Optional, Set, Tuple, Union

import jsonschema
import rich_click as click
import semver as semver
from github import Github, Issue, PullRequest, UnknownObjectException
from packaging.version import Version
from rich.console import Console
from rich.progress import Progress
from rich.syntax import Syntax
from yaml import safe_load

ALL_PYTHON_VERSIONS = ["3.7", "3.8", "3.9", "3.10"]

INITIAL_CHANGELOG_CONTENT = """


 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


Changelog
---------

1.0.0
.....

Initial version of the provider.
"""

HTTPS_REMOTE = "apache-https-for-providers"
HEAD_OF_HTTPS_REMOTE = f"{HTTPS_REMOTE}/main"

MY_DIR_PATH = Path(__file__).parent
AIRFLOW_SOURCES_ROOT_PATH = MY_DIR_PATH.parents[1]
AIRFLOW_PATH = AIRFLOW_SOURCES_ROOT_PATH / "airflow"
DIST_PATH = AIRFLOW_SOURCES_ROOT_PATH / "dist"
PROVIDERS_PATH = AIRFLOW_PATH / "providers"
DOCUMENTATION_PATH = AIRFLOW_SOURCES_ROOT_PATH / "docs"

DEPENDENCIES_JSON_FILE_PATH = AIRFLOW_SOURCES_ROOT_PATH / "generated" / "provider_dependencies.json"

TARGET_PROVIDER_PACKAGES_PATH = AIRFLOW_SOURCES_ROOT_PATH / "provider_packages"
GENERATED_AIRFLOW_PATH = TARGET_PROVIDER_PACKAGES_PATH / "airflow"
GENERATED_PROVIDERS_PATH = GENERATED_AIRFLOW_PATH / "providers"

PROVIDER_RUNTIME_DATA_SCHEMA_PATH = AIRFLOW_SOURCES_ROOT_PATH / "airflow" / "provider_info.schema.json"

CROSS_PROVIDERS_DEPS = "cross-providers-deps"
DEPS = "deps"

sys.path.insert(0, str(AIRFLOW_SOURCES_ROOT_PATH))


ALL_DEPENDENCIES = json.loads(DEPENDENCIES_JSON_FILE_PATH.read_text())

# those imports need to come after the above sys.path.insert to make sure that Airflow
# sources are importable without having to add the airflow sources to the PYTHONPATH before
# running the script
from setup import PREINSTALLED_PROVIDERS, ALL_PROVIDERS  # type: ignore[attr-defined] # isort:skip # noqa

# Note - we do not test protocols as they are not really part of the official API of
# Apache Airflow

logger = logging.getLogger(__name__)

PY3 = sys.version_info[0] == 3

console = Console(width=400, color_system="standard")


class ProviderPackageDetails(NamedTuple):
    provider_package_id: str
    full_package_name: str
    pypi_package_name: str
    source_provider_package_path: str
    documentation_provider_package_path: str
    provider_description: str
    versions: List[str]
    excluded_python_versions: List[str]


class EntityType(Enum):
    Operators = "Operators"
    Transfers = "Transfers"
    Sensors = "Sensors"
    Hooks = "Hooks"
    Secrets = "Secrets"


@click.group(context_settings={'help_option_names': ['-h', '--help'], 'max_content_width': 500})
def cli():
    ...


option_skip_tag_check = click.option(
    "--skip-tag-check/--no-skip-tag-check",
    default=False,
    is_flag=True,
    help="Skip checking if the tag already exists in the remote repository",
)

option_git_update = click.option(
    '--git-update/--no-git-update',
    default=True,
    is_flag=True,
    help=f"If the git remote {HTTPS_REMOTE} already exists, don't try to update it",
)

option_package_format = click.option(
    '--package-format',
    type=click.Choice(["wheel", "sdist", "both"]),
    help='Format of packages.',
    default="wheel",
    show_default=True,
    envvar='PACKAGE_FORMAT',
)

option_version_suffix = click.option(
    "--version-suffix",
    metavar="suffix",
    help=textwrap.dedent(
        """
        adds version suffix to version of the packages.
        only useful when generating rc candidates for pypi."""
    ),
)
option_verbose = click.option(
    "--verbose",
    is_flag=True,
    help="Print verbose information about performed steps",
)
option_force = click.option(
    "--force",
    is_flag=True,
    help="Forces regeneration of already generated documentation",
)
argument_package_id = click.argument('package_id')
argument_changelog_files = click.argument('changelog_files', nargs=-1)
argument_package_ids = click.argument('package_ids', nargs=-1)


@contextmanager
def with_group(title: str) -> Generator[None, None, None]:
    """
    If used in GitHub Action, creates an expandable group in the GitHub Action log.
    Otherwise, display simple text groups.

    For more information, see:
    https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-commands-for-github-actions#grouping-log-lines
    """
    if os.environ.get('GITHUB_ACTIONS', 'false') != "true":
        console.print("#" * 10 + ' [bright_blue]' + title + '[/] ' + "#" * 10)
        yield
        return
    console.print(f"::group::[bright_blue]{title}[/]")
    yield
    console.print("::endgroup::")


def get_source_airflow_folder() -> str:
    """
    Returns source directory for whole airflow (from the main airflow project).

    :return: the folder path
    """
    return os.path.abspath(AIRFLOW_SOURCES_ROOT_PATH)


def get_source_providers_folder() -> str:
    """
    Returns source directory for providers (from the main airflow project).

    :return: the folder path
    """
    return os.path.join(get_source_airflow_folder(), "airflow", "providers")


def get_target_folder() -> str:
    """
    Returns target directory for providers (in the provider_packages folder)

    :return: the folder path
    """
    return os.path.abspath(os.path.join(dirname(__file__), os.pardir, os.pardir, "provider_packages"))


def get_target_providers_folder() -> str:
    """
    Returns target directory for providers (in the provider_packages folder)

    :return: the folder path
    """
    return os.path.abspath(os.path.join(get_target_folder(), "airflow", "providers"))


def get_target_providers_package_folder(provider_package_id: str) -> str:
    """
    Returns target package folder based on package_id

    :return: the folder path
    """
    return os.path.join(get_target_providers_folder(), *provider_package_id.split("."))


def get_pip_package_name(provider_package_id: str) -> str:
    """
    Returns PIP package name for the package id.

    :param provider_package_id: id of the package
    :return: the name of pip package
    """
    return "apache-airflow-providers-" + provider_package_id.replace(".", "-")


def get_wheel_package_name(provider_package_id: str) -> str:
    """
    Returns PIP package name for the package id.

    :param provider_package_id: id of the package
    :return: the name of pip package
    """
    return "apache_airflow_providers_" + provider_package_id.replace(".", "_")


def get_long_description(provider_package_id: str) -> str:
    """
    Gets long description of the package.

    :param provider_package_id: package id
    :return: content of the description: README file
    """
    package_folder = get_target_providers_package_folder(provider_package_id)
    readme_file = os.path.join(package_folder, "README.md")
    if not os.path.exists(readme_file):
        return ""
    with open(readme_file, encoding='utf-8') as file:
        readme_contents = file.read()
    copying = True
    long_description = ""
    for line in readme_contents.splitlines(keepends=True):
        if line.startswith("**Table of contents**"):
            copying = False
            continue
        header_line = "## Provider package"
        if line.startswith(header_line):
            copying = True
        if copying:
            long_description += line
    return long_description


def get_install_requirements(provider_package_id: str, version_suffix: str) -> str:
    """
    Returns install requirements for the package.

    :param provider_package_id: id of the provider package
    :param version_suffix: optional version suffix for packages

    :return: install requirements of the package
    """
    install_requires = ALL_DEPENDENCIES[provider_package_id][DEPS]
    prefix = "\n    "
    return prefix + prefix.join(install_requires)


def get_setup_requirements() -> str:
    """
    Returns setup requirements (common for all package for now).
    :return: setup requirements
    """
    return """
    setuptools
    wheel
"""


def get_package_extras(provider_package_id: str) -> Dict[str, List[str]]:
    """
    Finds extras for the package specified.

    :param provider_package_id: id of the package
    """
    if provider_package_id == 'providers':
        return {}
    extras_dict: Dict[str, List[str]] = {
        module: [get_pip_package_name(module)]
        for module in ALL_DEPENDENCIES[provider_package_id][CROSS_PROVIDERS_DEPS]
    }
    provider_yaml_dict = get_provider_yaml(provider_package_id)
    additional_extras = provider_yaml_dict.get('additional-extras')
    if additional_extras:
        for entry in additional_extras:
            name = entry['name']
            dependencies = entry['dependencies']
            if name in extras_dict:
                # remove non-versioned dependencies if versioned ones are coming
                existing_dependencies = set(extras_dict[name])
                for new_dependency in dependencies:
                    for dependency in existing_dependencies:
                        # remove extra if exists as non-versioned one
                        if new_dependency.startswith(dependency):
                            extras_dict[name].remove(dependency)
                            break
                    extras_dict[name].append(new_dependency)
            else:
                extras_dict[name] = dependencies
    return extras_dict


def render_template(
    template_name: str,
    context: Dict[str, Any],
    extension: str,
    autoescape: bool = True,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template based on its name. Reads the template from <name>_TEMPLATE.md.jinja2 in current dir.
    :param template_name: name of the template to use
    :param context: Jinja2 context
    :param extension: Target file extension
    :param autoescape: Whether to autoescape HTML
    :param keep_trailing_newline: Whether to keep the newline in rendered output
    :return: rendered template
    """
    import jinja2

    template_loader = jinja2.FileSystemLoader(searchpath=MY_DIR_PATH)
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        keep_trailing_newline=keep_trailing_newline,
    )
    template = template_env.get_template(f"{template_name}_TEMPLATE{extension}.jinja2")
    content: str = template.render(context)
    return content


PR_PATTERN = re.compile(r".*\(#(\d+)\)")


class Change(NamedTuple):
    """Stores details about commits"""

    full_hash: str
    short_hash: str
    date: str
    version: str
    message: str
    message_without_backticks: str
    pr: Optional[str]


def get_change_from_line(line: str, version: str):
    split_line = line.split(" ", maxsplit=3)
    message = split_line[3]
    pr = None
    pr_match = PR_PATTERN.match(message)
    if pr_match:
        pr = pr_match.group(1)
    return Change(
        full_hash=split_line[0],
        short_hash=split_line[1],
        date=split_line[2],
        version=version,
        message=message,
        message_without_backticks=message.replace("`", "'").replace("&39;", "'"),
        pr=pr,
    )


def convert_git_changes_to_table(
    version: str, changes: str, base_url: str, markdown: bool = True
) -> Tuple[str, List[Change]]:
    """
    Converts list of changes from its string form to markdown/RST table and array of change information

    The changes are in the form of multiple lines where each line consists of:
    FULL_COMMIT_HASH SHORT_COMMIT_HASH COMMIT_DATE COMMIT_SUBJECT

    The subject can contain spaces but one of the preceding values can, so we can make split
    3 times on spaces to break it up.
    :param version: Version from which the changes are
    :param changes: list of changes in a form of multiple-line string
    :param base_url: base url for the commit URL
    :param markdown: if True, Markdown format is used else rst
    :return: formatted table + list of changes (starting from the latest)
    """
    from tabulate import tabulate

    lines = changes.split("\n")
    headers = ["Commit", "Committed", "Subject"]
    table_data = []
    changes_list: List[Change] = []
    for line in lines:
        if line == "":
            continue
        change = get_change_from_line(line, version)
        table_data.append(
            (
                f"[{change.short_hash}]({base_url}{change.full_hash})"
                if markdown
                else f"`{change.short_hash} <{base_url}{change.full_hash}>`_",
                change.date,
                f"`{change.message_without_backticks}`"
                if markdown
                else f"``{change.message_without_backticks}``",
            )
        )
        changes_list.append(change)
    header = ""
    if not table_data:
        return header, []
    table = tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")
    if not markdown:
        header += f"\n\n{version}\n" + "." * len(version) + "\n\n"
        release_date = table_data[0][1]
        header += f"Latest change: {release_date}\n\n"
    return header + table, changes_list


def convert_pip_requirements_to_table(requirements: Iterable[str], markdown: bool = True) -> str:
    """
    Converts PIP requirement list to a Markdown table.
    :param requirements: requirements list
    :param markdown: if True, Markdown format is used else rst
    :return: formatted table
    """
    from tabulate import tabulate

    headers = ["PIP package", "Version required"]
    table_data = []
    for dependency in requirements:
        found = re.match(r"(^[^<=>~]*)([^<=>~]?.*)$", dependency)
        if found:
            package = found.group(1)
            version_required = found.group(2)
            if version_required != "":
                version_required = f"`{version_required}`" if markdown else f'``{version_required}``'
            table_data.append((f"`{package}`" if markdown else f"``{package}``", version_required))
        else:
            table_data.append((dependency, ""))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


def convert_cross_package_dependencies_to_table(
    cross_package_dependencies: List[str],
    markdown: bool = True,
) -> str:
    """
    Converts cross-package dependencies to a Markdown table
    :param cross_package_dependencies: list of cross-package dependencies
    :param markdown: if True, Markdown format is used else rst
    :return: formatted table
    """
    from tabulate import tabulate

    headers = ["Dependent package", "Extra"]
    table_data = []
    prefix = "apache-airflow-providers-"
    base_url = "https://airflow.apache.org/docs/"
    for dependency in cross_package_dependencies:
        pip_package_name = f"{prefix}{dependency.replace('.','-')}"
        url_suffix = f"{dependency.replace('.','-')}"
        if markdown:
            url = f"[{pip_package_name}]({base_url}{url_suffix})"
        else:
            url = f"`{pip_package_name} <{base_url}{prefix}{url_suffix}>`_"
        table_data.append((url, f"`{dependency}`" if markdown else f"``{dependency}``"))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


LICENCE = """<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->
"""

LICENCE_RST = """
.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
"""

"""
Keeps information about historical releases.
"""
ReleaseInfo = collections.namedtuple(
    "ReleaseInfo", "release_version release_version_no_leading_zeros last_commit_hash content file_name"
)


def strip_leading_zeros(version: str) -> str:
    """
    Strips leading zeros from version number.

    This converts 1974.04.03 to 1974.4.3 as the format with leading month and day zeros is not accepted
    by PIP versioning.

    :param version: version number in CALVER format (potentially with leading 0s in date and month)
    :return: string with leading 0s after dot replaced.
    """
    return ".".join(str(int(i)) for i in version.split("."))


def get_previous_release_info(
    previous_release_version: Optional[str], past_releases: List[ReleaseInfo], current_release_version: str
) -> Optional[str]:
    """
    Find previous release. In case we are re-running current release we assume that last release was
    the previous one. This is needed so that we can generate list of changes since the previous release.
    :param previous_release_version: known last release version
    :param past_releases: list of past releases
    :param current_release_version: release that we are working on currently
    :return:
    """
    previous_release = None
    if previous_release_version == current_release_version:
        # Re-running for current release - use previous release as base for git log
        if len(past_releases) > 1:
            previous_release = past_releases[1].last_commit_hash
    else:
        previous_release = past_releases[0].last_commit_hash if past_releases else None
    return previous_release


def check_if_release_version_ok(
    past_releases: List[ReleaseInfo],
    current_release_version: str,
) -> Tuple[str, Optional[str]]:
    """
    Check if the release version passed is not later than the last release version
    :param past_releases: all past releases (if there are any)
    :param current_release_version: release version to check
    :return: Tuple of current/previous_release (previous might be None if there are no releases)
    """
    previous_release_version = past_releases[0].release_version if past_releases else None
    if current_release_version == '':
        if previous_release_version:
            current_release_version = previous_release_version
        else:
            current_release_version = (datetime.today() + timedelta(days=5)).strftime('%Y.%m.%d')
    if previous_release_version:
        if Version(current_release_version) < Version(previous_release_version):
            console.print(
                f"[red]The release {current_release_version} must be not less than "
                f"{previous_release_version} - last release for the package[/]"
            )
            raise Exception("Bad release version")
    return current_release_version, previous_release_version


def get_cross_provider_dependent_packages(provider_package_id: str) -> List[str]:
    """
    Returns cross-provider dependencies for the package.
    :param provider_package_id: package id
    :return: list of cross-provider dependencies
    """
    return ALL_DEPENDENCIES[provider_package_id][CROSS_PROVIDERS_DEPS]


def make_sure_remote_apache_exists_and_fetch(git_update: bool, verbose: bool):
    """
    Make sure that apache remote exist in git. We need to take a log from the apache
    repository - not locally.

    Also, the local repo might be shallow, so we need to un-shallow it.

    This will:
    * check if the remote exists and add if it does not
    * check if the local repo is shallow, mark it to un-shallow in this case
    * fetch from the remote including all tags and overriding local tags in case they are set differently

    :param git_update: If the git remote already exists, should we try to update it
    :param verbose: print verbose messages while fetching
    """
    try:
        check_remote_command = ["git", "remote", "get-url", HTTPS_REMOTE]
        if verbose:
            console.print(f"Running command: '{' '.join(check_remote_command)}'")
        subprocess.check_call(
            check_remote_command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Remote already exists, don't update it again!
        if not git_update:
            return
    except subprocess.CalledProcessError as ex:
        if ex.returncode == 128 or ex.returncode == 2:
            remote_add_command = [
                "git",
                "remote",
                "add",
                HTTPS_REMOTE,
                "https://github.com/apache/airflow.git",
            ]
            if verbose:
                console.print(f"Running command: '{' '.join(remote_add_command)}'")
            try:
                subprocess.check_output(
                    remote_add_command,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            except subprocess.CalledProcessError as ex:
                console.print("[red]Error: when adding remote:[/]", ex)
                sys.exit(128)
        else:
            raise
    if verbose:
        console.print("Fetching full history and tags from remote. ")
        console.print("This might override your local tags!")
    is_shallow_repo = (
        subprocess.check_output(["git", "rev-parse", "--is-shallow-repository"], stderr=subprocess.DEVNULL)
        == 'true'
    )
    fetch_command = ["git", "fetch", "--tags", "--force", HTTPS_REMOTE]
    if is_shallow_repo:
        if verbose:
            console.print(
                "This will also un-shallow the repository, "
                "making all history available and increasing storage!"
            )
        fetch_command.append("--unshallow")
    if verbose:
        console.print(f"Running command: '{' '.join(fetch_command)}'")
    try:
        subprocess.check_call(
            fetch_command,
        )
    except subprocess.CalledProcessError as e:
        console.print(
            '[yellow]Error when fetching tags from remote. Your tags might not be refreshed. '
            f'Please refresh the tags manually via {" ".join(fetch_command)}\n'
        )
        console.print(f'[yellow]The error was: {e}')


def get_git_log_command(
    verbose: bool, from_commit: Optional[str] = None, to_commit: Optional[str] = None
) -> List[str]:
    """
    Get git command to run for the current repo from the current folder (which is the package folder).
    :param verbose: whether to print verbose info while getting the command
    :param from_commit: if present - base commit from which to start the log from
    :param to_commit: if present - final commit which should be the start of the log
    :return: git command to run
    """
    git_cmd = [
        "git",
        "log",
        "--pretty=format:%H %h %cd %s",
        "--date=short",
    ]
    if from_commit and to_commit:
        git_cmd.append(f"{from_commit}...{to_commit}")
    elif from_commit:
        git_cmd.append(from_commit)
    git_cmd.extend(['--', '.'])
    if verbose:
        console.print(f"Command to run: '{' '.join(git_cmd)}'")
    return git_cmd


def get_git_tag_check_command(tag: str) -> List[str]:
    """
    Get git command to check if tag exits.
    :param tag: Tag to check
    :return: git command to run
    """
    return [
        "git",
        "rev-parse",
        tag,
    ]


def get_source_package_path(provider_package_id: str) -> str:
    """
    Retrieves source package path from package id.
    :param provider_package_id: id of the package
    :return: path of the providers folder
    """
    return os.path.join(PROVIDERS_PATH, *provider_package_id.split("."))


def get_documentation_package_path(provider_package_id: str) -> str:
    """
    Retrieves documentation package path from package id.
    :param provider_package_id: id of the package
    :return: path of the documentation folder
    """
    return os.path.join(
        DOCUMENTATION_PATH, f"apache-airflow-providers-{provider_package_id.replace('.','-')}"
    )


def get_generated_package_path(provider_package_id: str) -> str:
    """
    Retrieves generated package path from package id.
    :param provider_package_id: id of the package
    :return: path of the providers folder
    """
    provider_package_path = os.path.join(GENERATED_PROVIDERS_PATH, *provider_package_id.split("."))
    return provider_package_path


def get_additional_package_info(provider_package_path: str) -> str:
    """
    Returns additional info for the package.

    :param provider_package_path: path for the package
    :return: additional information for the path (empty string if missing)
    """
    additional_info_file_path = os.path.join(provider_package_path, "ADDITIONAL_INFO.md")
    if os.path.isfile(additional_info_file_path):
        with open(additional_info_file_path) as additional_info_file:
            additional_info = additional_info_file.read()

        additional_info_lines = additional_info.splitlines(keepends=True)
        result = ""
        skip_comment = True
        for line in additional_info_lines:
            if line.startswith(" -->"):
                skip_comment = False
                continue
            if not skip_comment:
                result += line
        return result
    return ""


def get_package_pip_name(provider_package_id: str):
    return f"apache-airflow-providers-{provider_package_id.replace('.', '-')}"


def validate_provider_info_with_runtime_schema(provider_info: Dict[str, Any]) -> None:
    """
    Validates provider info against the runtime schema. This way we check if the provider info in the
    packages is future-compatible. The Runtime Schema should only change when there is a major version
    change.

    :param provider_info: provider info to validate
    """

    with open(PROVIDER_RUNTIME_DATA_SCHEMA_PATH) as schema_file:
        schema = json.load(schema_file)
    try:
        jsonschema.validate(provider_info, schema=schema)
    except jsonschema.ValidationError as ex:
        console.print("[red]Provider info not validated against runtime schema[/]")
        raise Exception(
            "Error when validating schema. The schema must be compatible with "
            "airflow/provider_info.schema.json.",
            ex,
        )


def get_provider_yaml(provider_package_id: str) -> Dict[str, Any]:
    """
    Retrieves provider info from the provider yaml file. The provider yaml file contains more information
    than provider_info that is used at runtime. This method converts the full provider yaml file into
    stripped-down provider info and validates it against deprecated 2.0.0 schema and runtime schema.
    :param provider_package_id: package id to retrieve provider.yaml from
    :return: provider_info dictionary
    """
    provider_yaml_file_name = os.path.join(get_source_package_path(provider_package_id), "provider.yaml")
    if not os.path.exists(provider_yaml_file_name):
        raise Exception(f"The provider.yaml file is missing: {provider_yaml_file_name}")
    with open(provider_yaml_file_name) as provider_file:
        provider_yaml_dict = safe_load(provider_file)
    return provider_yaml_dict


def get_provider_info_from_provider_yaml(provider_package_id: str) -> Dict[str, Any]:
    """
    Retrieves provider info from the provider yaml file.
    :param provider_package_id: package id to retrieve provider.yaml from
    :return: provider_info dictionary
    """
    provider_yaml_dict = get_provider_yaml(provider_package_id=provider_package_id)
    validate_provider_info_with_runtime_schema(provider_yaml_dict)
    return provider_yaml_dict


def get_version_tag(version: str, provider_package_id: str, version_suffix: str = ''):
    if version_suffix is None:
        version_suffix = ''
    return f"providers-{provider_package_id.replace('.','-')}/{version}{version_suffix}"


def print_changes_table(changes_table):
    syntax = Syntax(changes_table, "rst", theme="ansi_dark")
    console.print(syntax)


def get_all_changes_for_package(
    provider_package_id: str,
    verbose: bool,
) -> Tuple[bool, Optional[Union[List[List[Change]], Change]], str]:
    """
    Retrieves all changes for the package.
    :param provider_package_id: provider package id
    :param verbose: whether to print verbose messages

    """
    provider_details = get_provider_details(provider_package_id)
    current_version = provider_details.versions[0]
    current_tag_no_suffix = get_version_tag(current_version, provider_package_id)
    if verbose:
        console.print(f"Checking if tag '{current_tag_no_suffix}' exist.")
    if not subprocess.call(
        get_git_tag_check_command(current_tag_no_suffix),
        cwd=provider_details.source_provider_package_path,
        stderr=subprocess.DEVNULL,
    ):
        if verbose:
            console.print(f"The tag {current_tag_no_suffix} exists.")
        # The tag already exists
        changes = subprocess.check_output(
            get_git_log_command(verbose, HEAD_OF_HTTPS_REMOTE, current_tag_no_suffix),
            cwd=provider_details.source_provider_package_path,
            text=True,
        )
        if changes:
            provider_details = get_provider_details(provider_package_id)
            doc_only_change_file = os.path.join(
                provider_details.source_provider_package_path, ".latest-doc-only-change.txt"
            )
            if os.path.exists(doc_only_change_file):
                with open(doc_only_change_file) as f:
                    last_doc_only_hash = f.read().strip()
                try:
                    changes_since_last_doc_only_check = subprocess.check_output(
                        get_git_log_command(verbose, HEAD_OF_HTTPS_REMOTE, last_doc_only_hash),
                        cwd=provider_details.source_provider_package_path,
                        text=True,
                    )
                    if not changes_since_last_doc_only_check:
                        console.print()
                        console.print(
                            "[yellow]The provider has doc-only changes since the last release. Skipping[/]"
                        )
                        # Returns 66 in case of doc-only changes
                        sys.exit(66)
                    if len(changes) > len(changes_since_last_doc_only_check):
                        # if doc-only was released after previous release - use it as starting point
                        # but if before - stay with the releases from last tag.
                        changes = changes_since_last_doc_only_check
                except subprocess.CalledProcessError:
                    # ignore when the commit mentioned as last doc-only change is obsolete
                    pass

            console.print(f"[yellow]The provider {provider_package_id} has changes since last release[/]")
            console.print()
            console.print(f"[bright_blue]Provider: {provider_package_id}[/]\n")
            changes_table, array_of_changes = convert_git_changes_to_table(
                f"NEXT VERSION AFTER + {provider_details.versions[0]}",
                changes,
                base_url="https://github.com/apache/airflow/commit/",
                markdown=False,
            )
            print_changes_table(changes_table)
            return False, array_of_changes[0], changes_table
        else:
            console.print(f"No changes for {provider_package_id}")
            return False, None, ""
    if verbose:
        console.print("The tag does not exist. ")
    if len(provider_details.versions) == 1:
        console.print(
            f"The provider '{provider_package_id}' has never been released but it is ready to release!\n"
        )
    else:
        console.print(f"New version of the '{provider_package_id}' package is ready to be released!\n")
    next_version_tag = HEAD_OF_HTTPS_REMOTE
    changes_table = ''
    current_version = provider_details.versions[0]
    list_of_list_of_changes: List[List[Change]] = []
    for version in provider_details.versions[1:]:
        version_tag = get_version_tag(version, provider_package_id)
        changes = subprocess.check_output(
            get_git_log_command(verbose, next_version_tag, version_tag),
            cwd=provider_details.source_provider_package_path,
            text=True,
        )
        changes_table_for_version, array_of_changes_for_version = convert_git_changes_to_table(
            current_version, changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
        )
        changes_table += changes_table_for_version
        list_of_list_of_changes.append(array_of_changes_for_version)
        next_version_tag = version_tag
        current_version = version
    changes = subprocess.check_output(
        get_git_log_command(verbose, next_version_tag),
        cwd=provider_details.source_provider_package_path,
        text=True,
    )
    changes_table_for_version, array_of_changes_for_version = convert_git_changes_to_table(
        current_version, changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
    )
    changes_table += changes_table_for_version
    if verbose:
        print_changes_table(changes_table)
    return True, list_of_list_of_changes if len(list_of_list_of_changes) > 0 else None, changes_table


def get_provider_details(provider_package_id: str) -> ProviderPackageDetails:
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    return ProviderPackageDetails(
        provider_package_id=provider_package_id,
        full_package_name=f"airflow.providers.{provider_package_id}",
        pypi_package_name=f"apache-airflow-providers-{provider_package_id.replace('.', '-')}",
        source_provider_package_path=get_source_package_path(provider_package_id),
        documentation_provider_package_path=get_documentation_package_path(provider_package_id),
        provider_description=provider_info['description'],
        versions=provider_info['versions'],
        excluded_python_versions=provider_info.get("excluded-python-versions") or [],
    )


def get_provider_requirements(provider_package_id: str) -> List[str]:
    provider_yaml = get_provider_yaml(provider_package_id)
    return provider_yaml['dependencies']


def get_provider_jinja_context(
    provider_info: Dict[str, Any],
    provider_details: ProviderPackageDetails,
    current_release_version: str,
    version_suffix: str,
):
    verify_provider_package(provider_details.provider_package_id)
    changelog_path = verify_changelog_exists(provider_details.provider_package_id)
    cross_providers_dependencies = get_cross_provider_dependent_packages(
        provider_package_id=provider_details.provider_package_id
    )
    release_version_no_leading_zeros = strip_leading_zeros(current_release_version)
    pip_requirements_table = convert_pip_requirements_to_table(
        get_provider_requirements(provider_details.provider_package_id)
    )
    pip_requirements_table_rst = convert_pip_requirements_to_table(
        get_provider_requirements(provider_details.provider_package_id), markdown=False
    )
    cross_providers_dependencies_table = convert_cross_package_dependencies_to_table(
        cross_providers_dependencies
    )
    cross_providers_dependencies_table_rst = convert_cross_package_dependencies_to_table(
        cross_providers_dependencies, markdown=False
    )
    with open(changelog_path) as changelog_file:
        changelog = changelog_file.read()
    supported_python_versions = [
        p for p in ALL_PYTHON_VERSIONS if p not in provider_details.excluded_python_versions
    ]
    python_requires = "~=3.7"
    for p in provider_details.excluded_python_versions:
        python_requires += f", !={p}"
    context: Dict[str, Any] = {
        "ENTITY_TYPES": list(EntityType),
        "README_FILE": "README.rst",
        "PROVIDER_PACKAGE_ID": provider_details.provider_package_id,
        "PACKAGE_PIP_NAME": get_pip_package_name(provider_details.provider_package_id),
        "PACKAGE_WHEEL_NAME": get_wheel_package_name(provider_details.provider_package_id),
        "FULL_PACKAGE_NAME": provider_details.full_package_name,
        "PROVIDER_PATH": provider_details.full_package_name.replace(".", "/"),
        "RELEASE": current_release_version,
        "RELEASE_NO_LEADING_ZEROS": release_version_no_leading_zeros,
        "VERSION_SUFFIX": version_suffix or '',
        "ADDITIONAL_INFO": get_additional_package_info(
            provider_package_path=provider_details.source_provider_package_path
        ),
        "CROSS_PROVIDERS_DEPENDENCIES": cross_providers_dependencies,
        "PIP_REQUIREMENTS": get_provider_requirements(provider_details.provider_package_id),
        "PROVIDER_TYPE": "Provider",
        "PROVIDERS_FOLDER": "providers",
        "PROVIDER_DESCRIPTION": provider_details.provider_description,
        "INSTALL_REQUIREMENTS": get_install_requirements(
            provider_package_id=provider_details.provider_package_id, version_suffix=version_suffix
        ),
        "SETUP_REQUIREMENTS": get_setup_requirements(),
        "EXTRAS_REQUIREMENTS": get_package_extras(provider_package_id=provider_details.provider_package_id),
        "CROSS_PROVIDERS_DEPENDENCIES_TABLE": cross_providers_dependencies_table,
        "CROSS_PROVIDERS_DEPENDENCIES_TABLE_RST": cross_providers_dependencies_table_rst,
        "PIP_REQUIREMENTS_TABLE": pip_requirements_table,
        "PIP_REQUIREMENTS_TABLE_RST": pip_requirements_table_rst,
        "PROVIDER_INFO": provider_info,
        "CHANGELOG_RELATIVE_PATH": relpath(
            provider_details.source_provider_package_path,
            provider_details.documentation_provider_package_path,
        ),
        "CHANGELOG": changelog,
        "SUPPORTED_PYTHON_VERSIONS": supported_python_versions,
        "PYTHON_REQUIRES": python_requires,
    }
    return context


def prepare_readme_file(context):
    readme_content = LICENCE_RST + render_template(
        template_name="PROVIDER_README", context=context, extension=".rst"
    )
    readme_file_path = os.path.join(TARGET_PROVIDER_PACKAGES_PATH, "README.rst")
    with open(readme_file_path, "wt") as readme_file:
        readme_file.write(readme_content)


def confirm(message: str, answer: Optional[str] = None) -> bool:
    """
    Ask user to confirm (case-insensitive).
    :param message: message to display
    :param answer: force answer if set
    :return: True if the answer is any form of y/yes. Exits with 65 exit code if any form of q/quit is chosen.
    """
    given_answer = answer.lower() if answer is not None else ""
    while given_answer not in ["y", "n", "q", "yes", "no", "quit"]:
        console.print(f"[yellow]{message}[y/n/q]?[/] ", end='')
        try:
            given_answer = input("").lower()
        except KeyboardInterrupt:
            given_answer = "q"
    if given_answer.lower() in ["q", "quit"]:
        # Returns 65 in case user decided to quit
        sys.exit(65)
    return given_answer in ["y", "yes"]


class TypeOfChange(Enum):
    DOCUMENTATION = "d"
    BUGFIX = "b"
    FEATURE = "f"
    BREAKING_CHANGE = "x"
    SKIP = "s"


def get_type_of_changes(answer: Optional[str]) -> TypeOfChange:
    """
    Ask user to specify type of changes (case-insensitive).
    :return: Type of change.
    """
    given_answer = ""
    if answer and answer.lower() in ["yes", "y"]:
        # Simulate all possible non-terminal answers
        return choice(
            [
                TypeOfChange.DOCUMENTATION,
                TypeOfChange.BUGFIX,
                TypeOfChange.FEATURE,
                TypeOfChange.BREAKING_CHANGE,
                TypeOfChange.SKIP,
            ]
        )
    while given_answer not in [*[t.value for t in TypeOfChange], "q"]:
        console.print(
            "[yellow]Type of change (d)ocumentation, (b)ugfix, (f)eature, (x)breaking "
            "change, (s)kip, (q)uit [d/b/f/x/s/q]?[/] ",
            end='',
        )
        try:
            given_answer = input("").lower()
        except KeyboardInterrupt:
            given_answer = 'q'
    if given_answer == "q":
        # Returns 65 in case user decided to quit
        sys.exit(65)
    return TypeOfChange(given_answer)


def mark_latest_changes_as_documentation_only(provider_package_id: str, latest_change: Change):
    provider_details = get_provider_details(provider_package_id=provider_package_id)
    console.print(
        f"Marking last change: {latest_change.short_hash} and all above changes since the last release "
        "as doc-only changes!"
    )
    with open(
        os.path.join(provider_details.source_provider_package_path, ".latest-doc-only-change.txt"), "tw"
    ) as f:
        f.write(latest_change.full_hash + "\n")
        # exit code 66 marks doc-only change marked
        sys.exit(66)


def add_new_version(type_of_change: TypeOfChange, provider_package_id: str):
    provider_details = get_provider_details(provider_package_id)
    version = provider_details.versions[0]
    v = semver.VersionInfo.parse(version)
    if type_of_change == TypeOfChange.BREAKING_CHANGE:
        v = v.bump_major()
    elif type_of_change == TypeOfChange.FEATURE:
        v = v.bump_minor()
    elif type_of_change == TypeOfChange.BUGFIX:
        v = v.bump_patch()
    provider_yaml_path = Path(get_source_package_path(provider_package_id)) / "provider.yaml"
    original_text = provider_yaml_path.read_text()
    new_text = re.sub(r'versions:', f'versions:\n  - {v}', original_text, 1)
    provider_yaml_path.write_text(new_text)
    console.print()
    console.print(f"[bright_blue]Bumped version to {v}")


def update_release_notes(
    provider_package_id: str,
    version_suffix: str,
    force: bool,
    verbose: bool,
    answer: Optional[str],
) -> bool:
    """
    Updates generated files (readme, changes and/or setup.cfg/setup.py/manifest.in/provider_info)

    :param provider_package_id: id of the package
    :param version_suffix: version suffix corresponding to the version in the code
    :param force: regenerate already released documentation
    :param verbose: whether to print verbose messages
    :param answer: force answer to question if set.
    :returns False if the package should be skipped, True if everything generated properly
    """
    verify_provider_package(provider_package_id)
    proceed, latest_change, changes = get_all_changes_for_package(provider_package_id, verbose)
    if not force:
        if proceed:
            if not confirm("Provider marked for release. Proceed", answer=answer):
                return False
        elif not latest_change:
            console.print()
            console.print(
                f"[yellow]Provider: {provider_package_id} - skipping documentation generation. No changes![/]"
            )
            console.print()
            return False
        else:
            type_of_change = get_type_of_changes(answer=answer)
            if type_of_change == TypeOfChange.DOCUMENTATION:
                if isinstance(latest_change, Change):
                    mark_latest_changes_as_documentation_only(provider_package_id, latest_change)
                else:
                    raise ValueError(
                        "Expected only one change to be present to mark changes "
                        f"in provider {provider_package_id} as docs-only. "
                        f"Received {len(latest_change)}."
                    )
            elif type_of_change == TypeOfChange.SKIP:
                return False
            elif type_of_change in [TypeOfChange.BUGFIX, TypeOfChange.FEATURE, TypeOfChange.BREAKING_CHANGE]:
                add_new_version(type_of_change, provider_package_id)
            proceed, latest_change, changes = get_all_changes_for_package(provider_package_id, verbose)
    provider_details = get_provider_details(provider_package_id)
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    jinja_context = get_provider_jinja_context(
        provider_info=provider_info,
        provider_details=provider_details,
        current_release_version=provider_details.versions[0],
        version_suffix=version_suffix,
    )
    jinja_context["DETAILED_CHANGES_RST"] = changes
    jinja_context["DETAILED_CHANGES_PRESENT"] = len(changes) > 0
    update_commits_rst(
        jinja_context, provider_package_id, provider_details.documentation_provider_package_path
    )
    return True


def update_setup_files(
    provider_package_id: str,
    version_suffix: str,
):
    """
    Updates generated setup.cfg/setup.py/manifest.in/provider_info for packages

    :param provider_package_id: id of the package
    :param version_suffix: version suffix corresponding to the version in the code
    :returns False if the package should be skipped, True if everything generated properly
    """
    verify_provider_package(provider_package_id)
    provider_details = get_provider_details(provider_package_id)
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    current_release_version = provider_details.versions[0]
    jinja_context = get_provider_jinja_context(
        provider_info=provider_info,
        provider_details=provider_details,
        current_release_version=current_release_version,
        version_suffix=version_suffix,
    )
    console.print()
    console.print(f"Generating setup files for {provider_package_id}")
    console.print()
    prepare_setup_py_file(jinja_context)
    prepare_setup_cfg_file(jinja_context)
    prepare_get_provider_info_py_file(jinja_context, provider_package_id)
    prepare_manifest_in_file(jinja_context)
    prepare_readme_file(jinja_context)
    return True


def replace_content(file_path, old_text, new_text, provider_package_id):
    if new_text != old_text:
        _, temp_file_path = tempfile.mkstemp()
        try:
            if os.path.isfile(file_path):
                copyfile(file_path, temp_file_path)
            with open(file_path, "wt") as readme_file:
                readme_file.write(new_text)
            console.print()
            console.print(f"Generated {file_path} file for the {provider_package_id} provider")
            console.print()
            if old_text != "":
                subprocess.call(["diff", "--color=always", temp_file_path, file_path])
        finally:
            os.remove(temp_file_path)


AUTOMATICALLY_GENERATED_MARKER = "AUTOMATICALLY GENERATED"
AUTOMATICALLY_GENERATED_CONTENT = (
    f".. THE REMAINDER OF THE FILE IS {AUTOMATICALLY_GENERATED_MARKER}. "
    f"IT WILL BE OVERWRITTEN AT RELEASE TIME!"
)


def update_index_rst(
    context,
    provider_package_id,
    target_path,
):
    index_update = render_template(
        template_name="PROVIDER_INDEX", context=context, extension='.rst', keep_trailing_newline=True
    )
    index_file_path = os.path.join(target_path, "index.rst")
    old_text = ""
    if os.path.isfile(index_file_path):
        with open(index_file_path) as readme_file_read:
            old_text = readme_file_read.read()
    new_text = deepcopy(old_text)
    lines = old_text.splitlines(keepends=False)
    for index, line in enumerate(lines):
        if AUTOMATICALLY_GENERATED_MARKER in line:
            new_text = "\n".join(lines[:index])
    new_text += "\n" + AUTOMATICALLY_GENERATED_CONTENT + "\n"
    new_text += index_update
    replace_content(index_file_path, old_text, new_text, provider_package_id)


def update_commits_rst(
    context,
    provider_package_id,
    target_path,
):
    new_text = render_template(
        template_name="PROVIDER_COMMITS", context=context, extension='.rst', keep_trailing_newline=True
    )
    index_file_path = os.path.join(target_path, "commits.rst")
    old_text = ""
    if os.path.isfile(index_file_path):
        with open(index_file_path) as readme_file_read:
            old_text = readme_file_read.read()
    replace_content(index_file_path, old_text, new_text, provider_package_id)


@lru_cache(maxsize=None)
def black_mode():
    from black import Mode, parse_pyproject_toml, target_version_option_callback

    config = parse_pyproject_toml(os.path.join(AIRFLOW_SOURCES_ROOT_PATH, "pyproject.toml"))

    target_versions = set(
        target_version_option_callback(None, None, tuple(config.get('target_version', ()))),
    )

    return Mode(
        target_versions=target_versions,
        line_length=config.get('line_length', Mode.line_length),
        is_pyi=bool(config.get('is_pyi', Mode.is_pyi)),
        string_normalization=not bool(config.get('skip_string_normalization', not Mode.string_normalization)),
        experimental_string_processing=bool(
            config.get('experimental_string_processing', Mode.experimental_string_processing)
        ),
    )


def black_format(content) -> str:
    from black import format_str

    return format_str(content, mode=black_mode())


def prepare_setup_py_file(context):
    setup_py_template_name = "SETUP"
    setup_py_file_path = os.path.abspath(os.path.join(get_target_folder(), "setup.py"))
    setup_py_content = render_template(
        template_name=setup_py_template_name, context=context, extension='.py', autoescape=False
    )
    with open(setup_py_file_path, "wt") as setup_py_file:
        setup_py_file.write(black_format(setup_py_content))


def prepare_setup_cfg_file(context):
    setup_cfg_template_name = "SETUP"
    setup_cfg_file_path = os.path.abspath(os.path.join(get_target_folder(), "setup.cfg"))
    setup_cfg_content = render_template(
        template_name=setup_cfg_template_name,
        context=context,
        extension='.cfg',
        autoescape=False,
        keep_trailing_newline=True,
    )
    with open(setup_cfg_file_path, "wt") as setup_cfg_file:
        setup_cfg_file.write(setup_cfg_content)


def prepare_get_provider_info_py_file(context, provider_package_id: str):
    get_provider_template_name = "get_provider_info"
    get_provider_file_path = os.path.abspath(
        os.path.join(
            get_target_providers_package_folder(provider_package_id),
            "get_provider_info.py",
        )
    )
    get_provider_content = render_template(
        template_name=get_provider_template_name,
        context=context,
        extension='.py',
        autoescape=False,
        keep_trailing_newline=True,
    )
    with open(get_provider_file_path, "wt") as get_provider_file:
        get_provider_file.write(black_format(get_provider_content))


def prepare_manifest_in_file(context):
    target = os.path.abspath(os.path.join(get_target_folder(), "MANIFEST.in"))
    content = render_template(
        template_name="MANIFEST",
        context=context,
        extension='.in',
        autoescape=False,
        keep_trailing_newline=True,
    )
    with open(target, "wt") as fh:
        fh.write(content)


def get_all_providers() -> List[str]:
    """
    Returns all providers for regular packages.
    :return: list of providers that are considered for provider packages
    """
    return list(ALL_PROVIDERS)


def verify_provider_package(provider_package_id: str) -> None:
    """
    Verifies if the provider package is good.
    :param provider_package_id: package id to verify
    :return: None
    """
    if provider_package_id not in get_all_providers():
        console.print(f"[red]Wrong package name: {provider_package_id}[/]")
        console.print("Use one of:")
        console.print(get_all_providers())
        raise Exception(f"The package {provider_package_id} is not a provider package.")


def verify_changelog_exists(package: str) -> str:
    provider_details = get_provider_details(package)
    changelog_path = os.path.join(provider_details.source_provider_package_path, "CHANGELOG.rst")
    if not os.path.isfile(changelog_path):
        console.print(f"[red]ERROR: Missing ${changelog_path}[/]")
        console.print("Please add the file with initial content:")
        console.print()
        syntax = Syntax(
            INITIAL_CHANGELOG_CONTENT,
            "rst",
            theme="ansi_dark",
        )
        console.print(syntax)
        console.print()
        raise Exception(f"Missing {changelog_path}")
    return changelog_path


@cli.command()
def list_providers_packages():
    """List all provider packages."""
    providers = get_all_providers()
    for provider in providers:
        console.print(provider)


@cli.command()
@option_version_suffix
@option_git_update
@argument_package_id
@option_force
@option_verbose
@click.option(
    "-a",
    "--answer",
    type=click.Choice(['y', 'n', 'q', 'yes', 'no', 'quit']),
    help="Force answer to questions.",
    envvar='ANSWER',
)
def update_package_documentation(
    version_suffix: str,
    git_update: bool,
    answer: Optional[str],
    package_id: str,
    force: bool,
    verbose: bool,
):
    """
    Updates package documentation.

    See `list-providers-packages` subcommand for the possible PACKAGE_ID values
    """
    provider_package_id = package_id
    verify_provider_package(provider_package_id)
    with with_group(f"Update release notes for package '{provider_package_id}' "):
        console.print("Updating documentation for the latest release version.")
        make_sure_remote_apache_exists_and_fetch(git_update, verbose)
        if not update_release_notes(
            provider_package_id, version_suffix, force=force, verbose=verbose, answer=answer
        ):
            # Returns 64 in case of skipped package
            sys.exit(64)


def tag_exists_for_version(provider_package_id: str, current_tag: str, verbose: bool):
    provider_details = get_provider_details(provider_package_id)
    if verbose:
        console.print(f"Checking if tag `{current_tag}` exists.")
    if not subprocess.call(
        get_git_tag_check_command(current_tag),
        cwd=provider_details.source_provider_package_path,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    ):
        if verbose:
            console.print(f"Tag `{current_tag}` exists.")
        return True
    if verbose:
        console.print(f"Tag `{current_tag}` does not exist.")
    return False


@cli.command()
@option_version_suffix
@option_git_update
@argument_package_id
@option_verbose
@option_skip_tag_check
def generate_setup_files(
    version_suffix: str, git_update: bool, package_id: str, verbose: bool, skip_tag_check: bool
):
    """
    Generates setup files for the package.

    See `list-providers-packages` subcommand for the possible PACKAGE_ID values
    """
    provider_package_id = package_id
    with with_group(f"Generate setup files for '{provider_package_id}'"):
        if not skip_tag_check:
            current_tag = get_current_tag(provider_package_id, version_suffix, git_update, verbose)
            if tag_exists_for_version(provider_package_id, current_tag, verbose):
                console.print(f"[yellow]The tag {current_tag} exists. Not preparing the package.[/]")
                sys.exit(64)
        if update_setup_files(provider_package_id, version_suffix):
            console.print(f"[green]Generated regular package setup files for {provider_package_id}[/]")
        else:
            sys.exit(64)


def get_current_tag(provider_package_id: str, suffix: str, git_update: bool, verbose: bool):
    verify_provider_package(provider_package_id)
    make_sure_remote_apache_exists_and_fetch(git_update, verbose)
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    versions: List[str] = provider_info['versions']
    current_version = versions[0]
    current_tag = get_version_tag(current_version, provider_package_id, suffix)
    return current_tag


def cleanup_remnants(verbose: bool):
    if verbose:
        console.print("Cleaning remnants")
    files = glob.glob("*.egg-info")
    for file in files:
        shutil.rmtree(file, ignore_errors=True)
    files = glob.glob("build")
    for file in files:
        shutil.rmtree(file, ignore_errors=True)


def verify_setup_cfg_prepared(provider_package):
    with open("setup.cfg") as f:
        setup_content = f.read()
    search_for = f"providers-{provider_package.replace('.','-')} for Apache Airflow"
    if search_for not in setup_content:
        console.print(
            f"[red]The setup.py is probably prepared for another package. "
            f"It does not contain [bold]{search_for}[/bold]![/]"
        )
        console.print(
            f"\nRun:\n\n[bold]./dev/provider_packages/prepare_provider_packages.py "
            f"generate-setup-files {provider_package}[/bold]\n"
        )
        raise Exception("Wrong setup!")


@cli.command()
@option_package_format
@option_git_update
@option_version_suffix
@argument_package_id
@option_verbose
@option_skip_tag_check
def build_provider_packages(
    package_format: str,
    git_update: bool,
    version_suffix: str,
    package_id: str,
    verbose: bool,
    skip_tag_check: bool,
):
    """
    Builds provider package.

    See `list-providers-packages` subcommand for the possible PACKAGE_ID values
    """

    import tempfile

    # we cannot use context managers because if the directory gets deleted (which bdist_wheel does),
    # the context manager will throw an exception when trying to delete it again
    tmp_build_dir = tempfile.TemporaryDirectory().name
    tmp_dist_dir = tempfile.TemporaryDirectory().name
    try:
        provider_package_id = package_id
        with with_group(f"Prepare provider package for '{provider_package_id}'"):
            if not skip_tag_check and (version_suffix.startswith("rc") or version_suffix == ""):
                # For RC and official releases we check if the "officially released" version exists
                # and skip the released if it was. This allows to skip packages that have not been
                # marked for release. For "dev" suffixes, we always build all packages
                released_tag = get_current_tag(provider_package_id, "", git_update, verbose)
                if tag_exists_for_version(provider_package_id, released_tag, verbose):
                    console.print(f"[yellow]The tag {released_tag} exists. Skipping the package.[/]")
                    return False
            console.print(f"Changing directory to {TARGET_PROVIDER_PACKAGES_PATH}")
            os.chdir(TARGET_PROVIDER_PACKAGES_PATH)
            cleanup_remnants(verbose)
            provider_package = package_id
            verify_setup_cfg_prepared(provider_package)

            console.print(f"Building provider package: {provider_package} in format {package_format}")
            command: List[str] = ["python3", "setup.py", "build", "--build-temp", tmp_build_dir]
            if version_suffix is not None:
                command.extend(['egg_info', '--tag-build', version_suffix])
            if package_format in ['sdist', 'both']:
                command.append("sdist")
            if package_format in ['wheel', 'both']:
                command.extend(["bdist_wheel", "--bdist-dir", tmp_dist_dir])
            console.print(f"Executing command: '{' '.join(command)}'")
            try:
                subprocess.check_call(args=command, stdout=subprocess.DEVNULL)
            except subprocess.CalledProcessError as ex:
                console.print("[red]The command returned an error %s", ex)
                sys.exit(ex.returncode)
            console.print(
                f"[green]Prepared provider package {provider_package} in format {package_format}[/]"
            )
    finally:
        shutil.rmtree(tmp_build_dir, ignore_errors=True)
        shutil.rmtree(tmp_dist_dir, ignore_errors=True)


def find_insertion_index_for_version(content: List[str], version: str) -> Tuple[int, bool]:
    """
    Finds insertion index for the specified version from the .rst changelog content.

    :param content: changelog split into separate lines
    :param version: version to look for

    :return: Tuple : insertion_index, append (whether to append or insert the changelog)
    """
    changelog_found = False
    skip_next_line = False
    index = 0
    for index, line in enumerate(content):
        if not changelog_found and line.strip() == version:
            changelog_found = True
            skip_next_line = True
        elif not skip_next_line and line and all(char == '.' for char in line):
            return index - 2, changelog_found
        else:
            skip_next_line = False
    return index, changelog_found


class ClassifiedChanges(NamedTuple):
    """Stores lists of changes classified automatically"""

    fixes: List[Change] = []
    features: List[Change] = []
    breaking_changes: List[Change] = []
    other: List[Change] = []


def get_changes_classified(changes: List[Change]) -> ClassifiedChanges:
    """
    Pre-classifies changes based on commit message, it's wildly guessing now,
    but if we switch to semantic commits, it could be automated. This list is supposed to be manually
    reviewed and re-classified by release manager anyway.

    :param changes: list of changes
    :return: list of changes classified semi-automatically to the fix/feature/breaking/other buckets
    """
    classified_changes = ClassifiedChanges()
    for change in changes:
        if "fix" in change.message.lower():
            classified_changes.fixes.append(change)
        elif "add" in change.message.lower():
            classified_changes.features.append(change)
        elif "breaking" in change.message.lower():
            classified_changes.breaking_changes.append(change)
        else:
            classified_changes.other.append(change)
    return classified_changes


@cli.command()
@argument_package_id
@option_verbose
def update_changelog(package_id: str, verbose: bool):
    """Updates changelog for the provider."""
    if _update_changelog(package_id, verbose):
        sys.exit(64)


def _update_changelog(package_id: str, verbose: bool) -> bool:
    """
    Internal update changelog method
    :param package_id: package id
    :param verbose: verbose flag
    :return: true if package is skipped
    """
    with with_group("Updates changelog for last release"):
        verify_provider_package(package_id)
        provider_details = get_provider_details(package_id)
        provider_info = get_provider_info_from_provider_yaml(package_id)
        current_release_version = provider_details.versions[0]
        jinja_context = get_provider_jinja_context(
            provider_info=provider_info,
            provider_details=provider_details,
            current_release_version=current_release_version,
            version_suffix='',
        )
        changelog_path = os.path.join(provider_details.source_provider_package_path, "CHANGELOG.rst")
        proceed, changes, _ = get_all_changes_for_package(
            package_id,
            verbose,
        )
        if not proceed:
            console.print(
                f"[yellow]The provider {package_id} is not being released. Skipping the package.[/]"
            )
            return True
        generate_new_changelog(package_id, provider_details, changelog_path, changes)
        console.print()
        console.print(f"Update index.rst for {package_id}")
        console.print()
        update_index_rst(jinja_context, package_id, provider_details.documentation_provider_package_path)
        return False


def generate_new_changelog(package_id, provider_details, changelog_path, changes):
    latest_version = provider_details.versions[0]
    with open(changelog_path) as changelog:
        current_changelog = changelog.read()
    current_changelog_lines = current_changelog.splitlines()
    insertion_index, append = find_insertion_index_for_version(current_changelog_lines, latest_version)
    if append:
        if not changes:
            console.print(
                f"[green]The provider {package_id} changelog for `{latest_version}` "
                "has first release. Not updating the changelog.[/]"
            )
            return
        new_changes = [
            change for change in changes[0] if change.pr and "(#" + change.pr + ")" not in current_changelog
        ]
        if not new_changes:
            console.print(
                f"[green]The provider {package_id} changelog for `{latest_version}` "
                "has no new changes. Not updating the changelog.[/]"
            )
            return
        context = {"new_changes": new_changes}
        generated_new_changelog = render_template(
            template_name='UPDATE_CHANGELOG', context=context, extension=".rst"
        )
    else:
        classified_changes = get_changes_classified(changes[0])
        context = {
            "version": latest_version,
            "version_header": "." * len(latest_version),
            "classified_changes": classified_changes,
        }
        generated_new_changelog = render_template(
            template_name='CHANGELOG', context=context, extension=".rst"
        )
    new_changelog_lines = current_changelog_lines[0:insertion_index]
    new_changelog_lines.extend(generated_new_changelog.splitlines())
    new_changelog_lines.extend(current_changelog_lines[insertion_index:])
    diff = "\n".join(difflib.context_diff(current_changelog_lines, new_changelog_lines, n=5))
    syntax = Syntax(diff, "diff")
    console.print(syntax)
    if not append:
        console.print(
            f"[green]The provider {package_id} changelog for `{latest_version}` "
            "version is missing. Generating fresh changelog.[/]"
        )
    else:
        console.print(
            f"[green]Appending the provider {package_id} changelog for `{latest_version}` version.[/]"
        )
    with open(changelog_path, "wt") as changelog:
        changelog.write("\n".join(new_changelog_lines))
        changelog.write("\n")


def get_package_from_changelog(changelog_path: str):
    folder = Path(changelog_path).parent
    package = ''
    separator = ''
    while not os.path.basename(folder) == 'providers':
        package = os.path.basename(folder) + separator + package
        separator = '.'
        folder = Path(folder).parent
    return package


@cli.command()
@argument_changelog_files
@option_git_update
@option_verbose
def update_changelogs(changelog_files: List[str], git_update: bool, verbose: bool):
    """Updates changelogs for multiple packages."""
    if git_update:
        make_sure_remote_apache_exists_and_fetch(git_update, verbose)
    for changelog_file in changelog_files:
        package_id = get_package_from_changelog(changelog_file)
        _update_changelog(package_id=package_id, verbose=verbose)


def get_prs_for_package(package_id: str) -> List[int]:
    pr_matcher = re.compile(r".*\(#([0-9]*)\)``$")
    verify_provider_package(package_id)
    changelog_path = verify_changelog_exists(package_id)
    provider_details = get_provider_details(package_id)
    current_release_version = provider_details.versions[0]
    prs = []
    with open(changelog_path) as changelog_file:
        changelog_lines = changelog_file.readlines()
        extract_prs = False
        skip_line = False
        for line in changelog_lines:
            if skip_line:
                # Skip first "....." header
                skip_line = False
                continue
            if line.strip() == current_release_version:
                extract_prs = True
                skip_line = True
                continue
            if extract_prs:
                if len(line) > 1 and all(c == '.' for c in line.strip()):
                    # Header for next version reached
                    break
                if line.startswith('.. Below changes are excluded from the changelog'):
                    # The reminder of PRs is not important skipping it
                    break
                match_result = pr_matcher.match(line.strip())
                if match_result:
                    prs.append(int(match_result.group(1)))
    return prs


PullRequestOrIssue = Union[PullRequest.PullRequest, Issue.Issue]


class ProviderPRInfo(NamedTuple):
    provider_details: ProviderPackageDetails
    pr_list: List[PullRequestOrIssue]


def is_package_in_dist(dist_files: List[str], package: str) -> bool:
    """Check if package has been prepared in dist folder."""
    for file in dist_files:
        if file.startswith(f'apache_airflow_providers_{package.replace(".","_")}') or file.startswith(
            f'apache-airflow-providers-{package.replace(".","-")}'
        ):
            return True
    return False


@cli.command()
@click.option(
    '--github-token',
    envvar='GITHUB_TOKEN',
    help=textwrap.dedent(
        """
      GitHub token used to authenticate.
      You can set omit it if you have GITHUB_TOKEN env variable set.
      Can be generated with:
      https://github.com/settings/tokens/new?description=Read%20sssues&scopes=repo:status"""
    ),
)
@click.option('--suffix', default='rc1')
@click.option(
    '--only-available-in-dist',
    is_flag=True,
    help='Only consider package ids with packages prepared in the dist folder',
)
@click.option('--excluded-pr-list', type=str, help="Coma-separated list of PRs to exclude from the issue.")
@argument_package_ids
def generate_issue_content(
    package_ids: List[str],
    github_token: str,
    suffix: str,
    only_available_in_dist: bool,
    excluded_pr_list: str,
):
    if not package_ids:
        package_ids = get_all_providers()
    """Generates content for issue to test the release."""
    with with_group("Generates GitHub issue content with people who can test it"):
        if excluded_pr_list:
            excluded_prs = [int(pr) for pr in excluded_pr_list.split(",")]
        else:
            excluded_prs = []
        all_prs: Set[int] = set()
        provider_prs: Dict[str, List[int]] = {}
        if only_available_in_dist:
            files_in_dist = os.listdir(str(DIST_PATH))
        prepared_package_ids = []
        for package_id in package_ids:
            if not only_available_in_dist or is_package_in_dist(files_in_dist, package_id):
                console.print(f"Extracting PRs for provider {package_id}")
                prepared_package_ids.append(package_id)
            else:
                console.print(f"Skipping extracting PRs for provider {package_id} as it is missing in dist")
                continue
            prs = get_prs_for_package(package_id)
            provider_prs[package_id] = list(filter(lambda pr: pr not in excluded_prs, prs))
            all_prs.update(provider_prs[package_id])
        g = Github(github_token)
        repo = g.get_repo("apache/airflow")
        pull_requests: Dict[int, PullRequestOrIssue] = {}
        with Progress(console=console) as progress:
            task = progress.add_task(f"Retrieving {len(all_prs)} PRs ", total=len(all_prs))
            pr_list = list(all_prs)
            for i in range(len(pr_list)):
                pr_number = pr_list[i]
                progress.console.print(
                    f"Retrieving PR#{pr_number}: https://github.com/apache/airflow/pull/{pr_number}"
                )
                try:
                    pull_requests[pr_number] = repo.get_pull(pr_number)
                except UnknownObjectException:
                    # Fallback to issue if PR not found
                    try:
                        pull_requests[pr_number] = repo.get_issue(pr_number)  # (same fields as PR)
                    except UnknownObjectException:
                        console.print(f"[red]The PR #{pr_number} could not be found[/]")
                progress.advance(task)
        interesting_providers: Dict[str, ProviderPRInfo] = {}
        non_interesting_providers: Dict[str, ProviderPRInfo] = {}
        for package_id in prepared_package_ids:
            pull_request_list = [pull_requests[pr] for pr in provider_prs[package_id] if pr in pull_requests]
            provider_details = get_provider_details(package_id)
            if pull_request_list:
                interesting_providers[package_id] = ProviderPRInfo(provider_details, pull_request_list)
            else:
                non_interesting_providers[package_id] = ProviderPRInfo(provider_details, pull_request_list)
        context = {
            'interesting_providers': interesting_providers,
            'date': datetime.now(),
            'suffix': suffix,
            'non_interesting_providers': non_interesting_providers,
        }
        issue_content = render_template(template_name="PROVIDER_ISSUE", context=context, extension=".md")
        console.print()
        console.print(
            "[green]Below you can find the issue content that you can use "
            "to ask contributor to test providers![/]"
        )
        console.print()
        console.print()
        console.print(
            "Issue title: [yellow]Status of testing Providers that were "
            f"prepared on { datetime.now().strftime('%B %d, %Y') }[/]"
        )
        console.print()
        syntax = Syntax(issue_content, "markdown", theme="ansi_dark")
        console.print(syntax)
        console.print()
        users: Set[str] = set()
        for provider_info in interesting_providers.values():
            for pr in provider_info.pr_list:
                users.add("@" + pr.user.login)
        console.print("All users involved in the PRs:")
        console.print(" ".join(users))


if __name__ == "__main__":
    # The cli exit code is:
    #   * 0 in case of success
    #   * 1 in case of error
    #   * 64 in case of skipped package
    #   * 65 in case user decided to quit
    #   * 66 in case package has doc-only changes
    try:
        cli()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(65)
        except SystemExit:
            os._exit(65)
