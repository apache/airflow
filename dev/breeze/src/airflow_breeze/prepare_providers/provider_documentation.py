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

import difflib
import os
import random
import re
import subprocess
import sys
import tempfile
from copy import deepcopy
from enum import Enum
from pathlib import Path
from shutil import copyfile
from time import time
from typing import Any, NamedTuple

import jinja2
import semver
from rich.syntax import Syntax

from airflow_breeze.utils.black_utils import black_format
from airflow_breeze.utils.confirm import Answer, confirm_action, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.packages import (
    HTTPS_REMOTE,
    ProviderPackageDetails,
    clear_cache_for_provider_metadata,
    get_provider_details,
    get_provider_jinja_context,
    get_source_package_path,
    refresh_provider_metadata_from_yaml_file,
    refresh_provider_metadata_with_provider_id,
    render_template,
)
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_verbose
from airflow_breeze.utils.versions import get_version_tag

PR_PATTERN = re.compile(r".*\(#(\d+)\)")

AUTOMATICALLY_GENERATED_MARKER = "AUTOMATICALLY GENERATED"
AUTOMATICALLY_GENERATED_CONTENT = (
    f".. THE REMAINDER OF THE FILE IS {AUTOMATICALLY_GENERATED_MARKER}. "
    f"IT WILL BE OVERWRITTEN AT RELEASE TIME!"
)

# Taken from pygrep hooks we are using in pre-commit
# https://github.com/pre-commit/pygrep-hooks/blob/main/.pre-commit-hooks.yaml
BACKTICKS_CHECK = re.compile(r"^(?! {4}).*(^| )`[^`]+`([^_]|$)", re.MULTILINE)

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

.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there
   are some breaking changes and you want to add an explanation to the users on how they are supposed
   to deal with them. The changelog is updated and maintained semi-automatically by release manager.

``{{ package_name }}``

Changelog
---------

1.0.0
.....

Initial version of the provider.
"""

SHORT_HASH_TO_TYPE_DICT = {}


class TypeOfChange(Enum):
    DOCUMENTATION = "d"
    BUGFIX = "b"
    FEATURE = "f"
    BREAKING_CHANGE = "x"
    SKIP = "s"
    MISC = "m"


# defines the precedence order for provider version bumps
# BREAKING_CHANGE > FEATURE > BUGFIX > MISC > DOCUMENTATION > SKIP
precedence_order = {
    TypeOfChange.SKIP: 0,
    TypeOfChange.DOCUMENTATION: 1,
    TypeOfChange.MISC: 2,
    TypeOfChange.BUGFIX: 3,
    TypeOfChange.FEATURE: 4,
    TypeOfChange.BREAKING_CHANGE: 5,
}


class Change(NamedTuple):
    """Stores details about commits"""

    full_hash: str
    short_hash: str
    date: str
    version: str
    message: str
    message_without_backticks: str
    pr: str | None


def get_most_impactful_change(changes: list[TypeOfChange]):
    return max(changes, key=lambda change: precedence_order[change])


def format_message_for_classification(message):
    num = re.search(r"#(\d+)", message).group(1)
    new_message = re.sub(r"#(\d+)", f"https://github.com/apache/airflow/pull/{num}", message)
    return new_message


class ClassifiedChanges:
    """Stores lists of changes classified automatically"""

    def __init__(self):
        self.fixes: list[Change] = []
        self.misc: list[Change] = []
        self.features: list[Change] = []
        self.breaking_changes: list[Change] = []
        self.other: list[Change] = []


class PrepareReleaseDocsChangesOnlyException(Exception):
    """Raised when package has only documentation changes."""


class PrepareReleaseDocsNoChangesException(Exception):
    """Raised when package has no changes."""


class PrepareReleaseDocsErrorOccurredException(Exception):
    """Raised when error occurred when preparing packages changes."""


class PrepareReleaseDocsUserSkippedException(Exception):
    """Raised when user skipped package."""


class PrepareReleaseDocsUserQuitException(Exception):
    """Raised when user decided to quit."""


TYPE_OF_CHANGE_DESCRIPTION = {
    TypeOfChange.DOCUMENTATION: "Documentation only changes - no version change needed, "
    "only documentation needs to be updated",
    TypeOfChange.BUGFIX: "Bugfix changes only - bump in PATCHLEVEL version needed",
    TypeOfChange.FEATURE: "Feature changes - bump in MINOR version needed",
    TypeOfChange.BREAKING_CHANGE: "Breaking changes - bump in MAJOR version needed",
    TypeOfChange.MISC: "Miscellaneous changes - bump in PATCHLEVEL version needed",
}


def _get_git_log_command(
    folder_paths: list[Path] | None = None, from_commit: str | None = None, to_commit: str | None = None
) -> list[str]:
    """Get git command to run for the current repo from the current folder.

    The current directory should always be the package folder.

    :param folder_paths: list of folder paths to check for changes
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
    elif to_commit:
        raise ValueError("It makes no sense to specify to_commit without from_commit.")
    folders = [folder_path.as_posix() for folder_path in folder_paths] if folder_paths else ["."]
    git_cmd.extend(["--", *folders])
    return git_cmd


def _get_change_from_line(line: str, version: str) -> Change:
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


def _convert_git_changes_to_table(
    version: str, changes: str, base_url: str, markdown: bool = True
) -> tuple[str, list[Change]]:
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

    lines = changes.splitlines()
    headers = ["Commit", "Committed", "Subject"]
    table_data = []
    changes_list: list[Change] = []
    for line in lines:
        if line == "":
            continue
        change = _get_change_from_line(line, version)
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


def _print_changes_table(changes_table):
    syntax = Syntax(changes_table, "rst", theme="ansi_dark")
    get_console().print(syntax)


def _get_all_changes_for_package(
    provider_package_id: str,
    base_branch: str,
    reapply_templates_only: bool,
    only_min_version_update: bool,
) -> tuple[bool, list[list[Change]], str]:
    """Retrieves all changes for the package.

    :param provider_package_id: provider package id
    :param base_branch: base branch to check changes in apache remote for changes
    :param reapply_templates_only: whether to only reapply templates without bumping the version
    :return tuple of:
        bool (whether to proceed with update)
        list of lists of changes for all past versions (might be empty)
        the same list converted to string RST table
    """
    provider_details = get_provider_details(provider_package_id)
    current_version = provider_details.versions[0]
    current_tag_no_suffix = get_version_tag(current_version, provider_package_id)
    if get_verbose():
        get_console().print(f"[info]Checking if tag '{current_tag_no_suffix}' exist.")
    result = run_command(
        ["git", "rev-parse", current_tag_no_suffix],
        cwd=AIRFLOW_SOURCES_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    providers_folder_paths = [
        provider_details.source_provider_package_path,
        provider_details.old_source_provider_package_path,
    ]
    if not reapply_templates_only and result.returncode == 0:
        if get_verbose():
            get_console().print(f"[info]The tag {current_tag_no_suffix} exists.")
        # The tag already exists
        result = run_command(
            _get_git_log_command(
                providers_folder_paths, f"{HTTPS_REMOTE}/{base_branch}", current_tag_no_suffix
            ),
            cwd=AIRFLOW_SOURCES_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        changes = result.stdout.strip()
        if changes:
            provider_details = get_provider_details(provider_package_id)
            doc_only_change_file = (
                provider_details.source_provider_package_path / ".latest-doc-only-change.txt"
            )
            if doc_only_change_file.exists():
                last_doc_only_hash = doc_only_change_file.read_text().strip()
                try:
                    result = run_command(
                        _get_git_log_command(
                            providers_folder_paths, f"{HTTPS_REMOTE}/{base_branch}", last_doc_only_hash
                        ),
                        cwd=AIRFLOW_SOURCES_ROOT,
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                    changes_since_last_doc_only_check = result.stdout.strip()
                    if not changes_since_last_doc_only_check:
                        get_console().print(
                            "\n[warning]The provider has doc-only changes since the last release. Skipping[/]"
                        )
                        raise PrepareReleaseDocsChangesOnlyException()
                    if len(changes.splitlines()) > len(changes_since_last_doc_only_check.splitlines()):
                        # if doc-only was released after previous release - use it as starting point
                        # but if before - stay with the releases from last tag.
                        changes = changes_since_last_doc_only_check
                except subprocess.CalledProcessError:
                    # ignore when the commit mentioned as last doc-only change is obsolete
                    pass
            if not only_min_version_update:
                get_console().print(
                    f"[warning]The provider {provider_package_id} has {len(changes.splitlines())} "
                    f"changes since last release[/]"
                )
                get_console().print(f"\n[info]Provider: {provider_package_id}[/]\n")
            changes_table, array_of_changes = _convert_git_changes_to_table(
                f"NEXT VERSION AFTER + {provider_details.versions[0]}",
                changes,
                base_url="https://github.com/apache/airflow/commit/",
                markdown=False,
            )
            if not only_min_version_update:
                _print_changes_table(changes_table)
            return False, [array_of_changes], changes_table
        else:
            if not only_min_version_update:
                get_console().print(f"[info]No changes for {provider_package_id}")
            return False, [], ""
    if len(provider_details.versions) == 1:
        get_console().print(
            f"[info]The provider '{provider_package_id}' has never "
            f"been released but it is ready to release!\n"
        )
    else:
        get_console().print(
            f"[info]New version of the '{provider_package_id}' package is ready to be released!\n"
        )
    next_version_tag = f"{HTTPS_REMOTE}/{base_branch}"
    changes_table = ""
    current_version = provider_details.versions[0]
    list_of_list_of_changes: list[list[Change]] = []
    for version in provider_details.versions[1:]:
        version_tag = get_version_tag(version, provider_package_id)
        result = run_command(
            _get_git_log_command(providers_folder_paths, next_version_tag, version_tag),
            cwd=AIRFLOW_SOURCES_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        changes = result.stdout.strip()
        changes_table_for_version, array_of_changes_for_version = _convert_git_changes_to_table(
            current_version, changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
        )
        changes_table += changes_table_for_version
        list_of_list_of_changes.append(array_of_changes_for_version)
        next_version_tag = version_tag
        current_version = version
    result = run_command(
        _get_git_log_command(providers_folder_paths, next_version_tag),
        cwd=provider_details.source_provider_package_path,
        capture_output=True,
        text=True,
        check=True,
    )
    changes = result.stdout.strip()
    changes_table_for_version, array_of_changes_for_version = _convert_git_changes_to_table(
        current_version, changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
    )
    changes_table += changes_table_for_version
    return True, list_of_list_of_changes, changes_table


def _ask_the_user_for_the_type_of_changes(non_interactive: bool) -> TypeOfChange:
    """Ask user to specify type of changes (case-insensitive).

    :return: Type of change.
    """
    # have to do that while waiting for Python 3.11+ StrEnum [*TypeOfChange] :(
    type_of_changes_array = [t.value for t in TypeOfChange]
    if non_interactive:
        # Simulate all possible non-terminal answers - this is useful for running on CI where we want to
        # Test all possibilities.
        return TypeOfChange(random.choice(type_of_changes_array))
    display_answers = "/".join(type_of_changes_array) + "/q"
    while True:
        get_console().print(
            "[warning]Type of change (d)ocumentation, (b)ugfix, (f)eature, (x)breaking "
            f"change, (m)misc, (s)kip, (q)uit [{display_answers}]?[/] ",
            end="",
        )
        try:
            given_answer = input("").lower()
        except KeyboardInterrupt:
            raise PrepareReleaseDocsUserQuitException()
        if given_answer == "q":
            raise PrepareReleaseDocsUserQuitException()
        if given_answer in type_of_changes_array:
            return TypeOfChange(given_answer)
        get_console().print(
            f"[warning] Wrong answer given: '{given_answer}'. Should be one of {display_answers}"
        )


def _mark_latest_changes_as_documentation_only(
    provider_package_id: str, list_of_list_of_latest_changes: list[list[Change]]
):
    latest_change = list_of_list_of_latest_changes[0][0]
    provider_details = get_provider_details(provider_id=provider_package_id)
    get_console().print(
        f"[special]Marking last change: {latest_change.short_hash} and all above "
        f"changes since the last release as doc-only changes!"
    )
    (provider_details.source_provider_package_path / ".latest-doc-only-change.txt").write_text(
        latest_change.full_hash + "\n"
    )
    raise PrepareReleaseDocsChangesOnlyException()


def _update_version_in_provider_yaml(
    provider_package_id: str,
    type_of_change: TypeOfChange,
) -> tuple[bool, bool, str]:
    """
    Updates provider version based on the type of change selected by the user
    :param type_of_change: type of change selected
    :param provider_package_id: provider package
    :return: tuple of two bools: (with_breaking_change, maybe_with_new_features, original_text)
    """
    provider_details = get_provider_details(provider_package_id)
    version = provider_details.versions[0]
    v = semver.VersionInfo.parse(version)
    with_breaking_changes = False
    maybe_with_new_features = False
    if type_of_change == TypeOfChange.BREAKING_CHANGE:
        v = v.bump_major()
        with_breaking_changes = True
        # we do not know, but breaking changes may also contain new features
        maybe_with_new_features = True
    elif type_of_change == TypeOfChange.FEATURE:
        v = v.bump_minor()
        maybe_with_new_features = True
    elif type_of_change == TypeOfChange.BUGFIX:
        v = v.bump_patch()
    elif type_of_change == TypeOfChange.MISC:
        v = v.bump_patch()
    provider_yaml_path = get_source_package_path(provider_package_id) / "provider.yaml"
    original_provider_yaml_content = provider_yaml_path.read_text()
    new_provider_yaml_content = re.sub(
        r"^versions:", f"versions:\n  - {v}", original_provider_yaml_content, 1, re.MULTILINE
    )
    provider_yaml_path.write_text(new_provider_yaml_content)
    get_console().print(f"[special]Bumped version to {v}\n")
    return with_breaking_changes, maybe_with_new_features, original_provider_yaml_content


def _update_source_date_epoch_in_provider_yaml(
    provider_package_id: str,
) -> None:
    """
    Updates source date epoch in provider yaml that then can be used to generate reproducible packages.

    :param provider_package_id: provider package
    """
    provider_yaml_path = get_source_package_path(provider_package_id) / "provider.yaml"
    original_text = provider_yaml_path.read_text()
    source_date_epoch = int(time())
    new_text = re.sub(
        r"source-date-epoch: [0-9]*", f"source-date-epoch: {source_date_epoch}", original_text, 1
    )
    provider_yaml_path.write_text(new_text)
    refresh_provider_metadata_with_provider_id(provider_package_id)
    get_console().print(f"[special]Updated source-date-epoch to {source_date_epoch}\n")


def _verify_changelog_exists(package: str) -> Path:
    provider_details = get_provider_details(package)
    changelog_path = Path(provider_details.source_provider_package_path) / "CHANGELOG.rst"
    if not os.path.isfile(changelog_path):
        get_console().print(f"\n[error]ERROR: Missing {changelog_path}[/]\n")
        get_console().print("[info]Please add the file with initial content:")
        get_console().print("----- START COPYING AFTER THIS LINE ------- ")
        processed_changelog = jinja2.Template(INITIAL_CHANGELOG_CONTENT, autoescape=True).render(
            package_name=provider_details.pypi_package_name,
        )
        syntax = Syntax(
            processed_changelog,
            "rst",
            theme="ansi_dark",
        )
        get_console().print(syntax)
        get_console().print("----- END COPYING BEFORE THIS LINE ------- ")
        sys.exit(1)
    return changelog_path


def _get_additional_package_info(provider_package_path: Path) -> str:
    """Returns additional info for the package.

    :param provider_package_path: path for the package
    :return: additional information for the path (empty string if missing)
    """
    additional_info_file_path = provider_package_path / "ADDITIONAL_INFO.md"
    if additional_info_file_path.is_file():
        additional_info = additional_info_file_path.read_text()
        additional_info_lines = additional_info.splitlines(keepends=True)
        result = ""
        skip_comment = True
        for line in additional_info_lines:
            if line.startswith(" -->"):
                skip_comment = False
            elif not skip_comment:
                result += line
        return result
    return ""


def replace_content(file_path: Path, old_text: str, new_text: str, provider_id: str):
    if new_text != old_text:
        _, temp_file_path = tempfile.mkstemp()
        try:
            if file_path.is_file():
                copyfile(file_path, temp_file_path)
            file_path.write_text(new_text)
            get_console().print(f"\n[info]Generated {file_path} file for the {provider_id} provider\n")
            if old_text != "":
                run_command(["diff", "--color=always", temp_file_path, file_path.as_posix()], check=False)
        finally:
            os.unlink(temp_file_path)


def _update_file(
    context: dict[str, Any],
    template_name: str,
    extension: str,
    file_name: str,
    provider_package_id: str,
    target_path: Path,
    regenerate_missing_docs: bool,
) -> None:
    target_file_path = target_path / file_name
    if regenerate_missing_docs and target_file_path.exists():
        if get_verbose():
            get_console().print(
                f"[warnings]The {target_file_path} exists - not regenerating it "
                f"for the provider {provider_package_id}[/]"
            )
        return
    new_text = render_template(
        template_name=template_name, context=context, extension=extension, keep_trailing_newline=True
    )
    target_file_path = target_path / file_name
    old_text = ""
    if target_file_path.is_file():
        old_text = target_file_path.read_text()
    replace_content(target_file_path, old_text, new_text, provider_package_id)
    index_path = target_path / "index.rst"
    if not index_path.exists():
        get_console().print(f"[error]ERROR! The index must exist for the provider docs: {index_path}")
        raise PrepareReleaseDocsErrorOccurredException()

    expected_link_in_index = f"<{file_name.split('.')[0]}>"
    if expected_link_in_index not in index_path.read_text():
        get_console().print(
            f"\n[error]ERROR! The {index_path} must contain "
            f"link to the generated documentation:[/]\n\n"
            f"[warning]{expected_link_in_index}[/]\n\n"
            f"[info]Please make sure to add it to {index_path}.\n"
        )

    get_console().print(f"[info]Checking for backticks correctly generated in: {target_file_path}")
    match = BACKTICKS_CHECK.search(target_file_path.read_text())
    if match:
        get_console().print(
            f"\n[error]ERROR: Single backticks (`) found in {target_file_path}:[/]\n\n"
            f"[warning]{match.group(0)}[/]\n\n"
            f"[info]Please fix them by replacing with double backticks (``).[/]\n"
        )
        raise PrepareReleaseDocsErrorOccurredException()

    # TODO: uncomment me. Linting revealed that our already generated provider docs have duplicate links
    #       in the generated files, we should fix those and uncomment linting as separate step - so that
    #       we do not hold current release for fixing the docs.
    # console.print(f"Linting: {file_path}")
    # errors = restructuredtext_lint.lint_file(file_path)
    # real_errors = False
    # if errors:
    #     for error in errors:
    #         # Skip known issue: linter with doc role similar to https://github.com/OCA/pylint-odoo/issues/38
    #         if (
    #             'No role entry for "doc"' in error.message
    #             or 'Unknown interpreted text role "doc"' in error.message
    #         ):
    #             continue
    #         real_errors = True
    #         console.print(f"* [red] {error.message}")
    #     if real_errors:
    #         console.print(f"\n[red] Errors found in {file_path}")
    #         raise PrepareReleaseDocsErrorOccurredException()

    get_console().print(f"[success]Generated {target_file_path} for {provider_package_id} is OK[/]")
    return


def _update_changelog_rst(
    context: dict[str, Any],
    provider_package_id: str,
    target_path: Path,
    regenerate_missing_docs: bool,
) -> None:
    _update_file(
        context=context,
        template_name="PROVIDER_CHANGELOG",
        extension=".rst",
        file_name="changelog.rst",
        provider_package_id=provider_package_id,
        target_path=target_path,
        regenerate_missing_docs=regenerate_missing_docs,
    )


def _update_commits_rst(
    context: dict[str, Any],
    provider_package_id: str,
    target_path: Path,
    regenerate_missing_docs: bool,
) -> None:
    _update_file(
        context=context,
        template_name="PROVIDER_COMMITS",
        extension=".rst",
        file_name="commits.rst",
        provider_package_id=provider_package_id,
        target_path=target_path,
        regenerate_missing_docs=regenerate_missing_docs,
    )


def update_release_notes(
    provider_package_id: str,
    reapply_templates_only: bool,
    base_branch: str,
    regenerate_missing_docs: bool,
    non_interactive: bool,
    only_min_version_update: bool,
) -> tuple[bool, bool]:
    """Updates generated files.

    This includes the readme, changes, and provider.yaml files.

    :param provider_package_id: id of the package
    :param reapply_templates_only: regenerate already released documentation only - without updating versions
    :param base_branch: base branch to check changes in apache remote for changes
    :param regenerate_missing_docs: whether to regenerate missing docs
    :param non_interactive: run in non-interactive mode (useful for CI)
    :return: tuple of two bools: (with_breaking_change, maybe_with_new_features)
    """
    proceed, list_of_list_of_changes, changes_as_table = _get_all_changes_for_package(
        provider_package_id=provider_package_id,
        base_branch=base_branch,
        reapply_templates_only=reapply_templates_only,
        only_min_version_update=only_min_version_update,
    )
    with_breaking_changes = False
    maybe_with_new_features = False
    original_provider_yaml_content: str | None = None
    marked_for_release = False
    if not reapply_templates_only:
        if proceed:
            if non_interactive:
                answer = Answer.YES
            else:
                provider_details = get_provider_details(provider_package_id)
                current_release_version = provider_details.versions[0]
                answer = user_confirm(
                    f"Provider {provider_package_id} with "
                    f"version: {current_release_version} marked for release. Proceed?"
                )
                marked_for_release = answer == Answer.YES
            if answer == Answer.NO:
                get_console().print(
                    f"\n[warning]Skipping provider: {provider_package_id} on user request![/]\n"
                )
                raise PrepareReleaseDocsUserSkippedException()
            elif answer == Answer.QUIT:
                raise PrepareReleaseDocsUserQuitException()
        elif not list_of_list_of_changes:
            get_console().print(
                f"\n[warning]Provider: {provider_package_id} - "
                f"skipping documentation generation. No changes![/]\n"
            )
            raise PrepareReleaseDocsNoChangesException()
        else:
            answer = confirm_action(
                f"Does the provider: {provider_package_id} have any changes apart from 'doc-only'?"
            )
            if answer == Answer.NO:
                _mark_latest_changes_as_documentation_only(provider_package_id, list_of_list_of_changes)
                return with_breaking_changes, maybe_with_new_features
            change_table_len = len(list_of_list_of_changes[0])
            table_iter = 0
            global SHORT_HASH_TO_TYPE_DICT
            type_of_current_package_changes: list[TypeOfChange] = []
            while table_iter < change_table_len:
                get_console().print()
                formatted_message = format_message_for_classification(
                    list_of_list_of_changes[0][table_iter].message_without_backticks
                )
                get_console().print(
                    f"[green]Define the type of change for "
                    f"`{formatted_message}`"
                    f" by referring to the above table[/]"
                )
                type_of_change = _ask_the_user_for_the_type_of_changes(non_interactive=non_interactive)
                change_hash = list_of_list_of_changes[0][table_iter].short_hash
                SHORT_HASH_TO_TYPE_DICT[change_hash] = type_of_change
                type_of_current_package_changes.append(type_of_change)
                table_iter += 1
                print()
            most_impactful = get_most_impactful_change(type_of_current_package_changes)
            get_console().print(
                f"[info]The version will be bumped because of {most_impactful} kind of change"
            )
            type_of_change = most_impactful
            if type_of_change == TypeOfChange.SKIP:
                raise PrepareReleaseDocsUserSkippedException()
            get_console().print(
                f"[info]Provider {provider_package_id} has been classified as:[/]\n\n"
                f"[special]{TYPE_OF_CHANGE_DESCRIPTION[type_of_change]}"
            )
            get_console().print()
            if type_of_change in [
                TypeOfChange.BUGFIX,
                TypeOfChange.FEATURE,
                TypeOfChange.BREAKING_CHANGE,
                TypeOfChange.MISC,
            ]:
                with_breaking_changes, maybe_with_new_features, original_provider_yaml_content = (
                    _update_version_in_provider_yaml(
                        provider_package_id=provider_package_id, type_of_change=type_of_change
                    )
                )
                _update_source_date_epoch_in_provider_yaml(provider_package_id)
            proceed, list_of_list_of_changes, changes_as_table = _get_all_changes_for_package(
                provider_package_id=provider_package_id,
                base_branch=base_branch,
                reapply_templates_only=reapply_templates_only,
                only_min_version_update=only_min_version_update,
            )
    else:
        _update_source_date_epoch_in_provider_yaml(provider_package_id)

    provider_details = get_provider_details(provider_package_id)
    current_release_version = provider_details.versions[0]
    if (not non_interactive) and (not marked_for_release):
        answer = user_confirm(
            f"Do you want to leave the version for {provider_package_id} with version: "
            f"{current_release_version} as is for the release?"
        )
    else:
        answer = Answer.YES

    if answer == Answer.NO:
        if original_provider_yaml_content is not None:
            # Restore original content of the provider.yaml
            (get_source_package_path(provider_package_id) / "provider.yaml").write_text(
                original_provider_yaml_content
            )
            clear_cache_for_provider_metadata(provider_package_id)

        type_of_change = _ask_the_user_for_the_type_of_changes(non_interactive=False)
        if type_of_change == TypeOfChange.SKIP:
            raise PrepareReleaseDocsUserSkippedException()
        get_console().print(
            f"[info]Provider {provider_package_id} has been classified as:[/]\n\n"
            f"[special]{TYPE_OF_CHANGE_DESCRIPTION[type_of_change]}"
        )
        get_console().print()
        if type_of_change == TypeOfChange.DOCUMENTATION:
            _mark_latest_changes_as_documentation_only(provider_package_id, list_of_list_of_changes)
        elif type_of_change in [
            TypeOfChange.BUGFIX,
            TypeOfChange.FEATURE,
            TypeOfChange.BREAKING_CHANGE,
            TypeOfChange.MISC,
        ]:
            with_breaking_changes, maybe_with_new_features, _ = _update_version_in_provider_yaml(
                provider_package_id=provider_package_id,
                type_of_change=type_of_change,
            )
            _update_source_date_epoch_in_provider_yaml(provider_package_id)
            proceed, list_of_list_of_changes, changes_as_table = _get_all_changes_for_package(
                provider_package_id=provider_package_id,
                base_branch=base_branch,
                reapply_templates_only=reapply_templates_only,
                only_min_version_update=only_min_version_update,
            )
    else:
        get_console().print(
            f"[info] Proceeding with provider: {provider_package_id} version as {current_release_version}"
        )
    provider_details = get_provider_details(provider_package_id)
    _verify_changelog_exists(provider_details.provider_id)
    jinja_context = get_provider_documentation_jinja_context(
        provider_id=provider_package_id,
        with_breaking_changes=with_breaking_changes,
        maybe_with_new_features=maybe_with_new_features,
    )
    jinja_context["DETAILED_CHANGES_RST"] = changes_as_table
    jinja_context["DETAILED_CHANGES_PRESENT"] = bool(changes_as_table)
    _update_changelog_rst(
        jinja_context,
        provider_package_id,
        provider_details.documentation_provider_package_path,
        regenerate_missing_docs,
    )
    _update_commits_rst(
        jinja_context,
        provider_package_id,
        provider_details.documentation_provider_package_path,
        regenerate_missing_docs,
    )
    return with_breaking_changes, maybe_with_new_features


def _find_insertion_index_for_version(content: list[str], version: str) -> tuple[int, bool]:
    """Finds insertion index for the specified version from the .rst changelog content.

    :param content: changelog split into separate lines
    :param version: version to look for

    :return: A 2-tuple. The first item indicates the insertion index, while the
        second is a boolean indicating whether to append (False) or insert (True)
        to the changelog.
    """
    changelog_found = False
    skip_next_line = False
    index = 0
    for index, line in enumerate(content):
        if not changelog_found and line.strip() == version:
            changelog_found = True
            skip_next_line = True
        elif not skip_next_line and line and all(char == "." for char in line):
            return index - 2, changelog_found
        else:
            skip_next_line = False
    return index, changelog_found


def _get_changes_classified(
    changes: list[Change], with_breaking_changes: bool, maybe_with_new_features: bool
) -> ClassifiedChanges:
    """
    Pre-classifies changes based on their type_of_change attribute derived based on release manager's call.

    The classification is based on the decision made by the release manager when classifying the release.
    If we switch to semantic commits, this process could be automated. This list is still supposed to be
    manually reviewed and re-classified by the release manager if needed.

    :param changes: list of changes to be classified
    :param with_breaking_changes: whether to include breaking changes in the classification
    :param maybe_with_new_features: whether to include new features in the classification
    :return: ClassifiedChanges object containing changes classified into fixes, features, breaking changes,
    misc.
    """
    classified_changes = ClassifiedChanges()
    for change in changes:
        type_of_change = None
        if change.short_hash in SHORT_HASH_TO_TYPE_DICT:
            type_of_change = SHORT_HASH_TO_TYPE_DICT[change.short_hash]

        if type_of_change == TypeOfChange.BUGFIX:
            classified_changes.fixes.append(change)
        elif type_of_change == TypeOfChange.MISC:
            classified_changes.misc.append(change)
        elif type_of_change == TypeOfChange.FEATURE and maybe_with_new_features:
            classified_changes.features.append(change)
        elif type_of_change == TypeOfChange.BREAKING_CHANGE and with_breaking_changes:
            classified_changes.breaking_changes.append(change)
        else:
            classified_changes.other.append(change)
    return classified_changes


def _generate_new_changelog(
    package_id: str,
    provider_details: ProviderPackageDetails,
    changes: list[list[Change]],
    context: dict[str, Any],
    with_breaking_changes: bool,
    maybe_with_new_features: bool,
):
    latest_version = provider_details.versions[0]
    current_changelog = provider_details.changelog_path.read_text()
    current_changelog_lines = current_changelog.splitlines()
    insertion_index, append = _find_insertion_index_for_version(current_changelog_lines, latest_version)
    new_context = deepcopy(context)
    if append:
        if not changes:
            get_console().print(
                f"[success]The provider {package_id} changelog for `{latest_version}` "
                "has first release. Not updating the changelog.[/]"
            )
            return
        new_changes = [
            change for change in changes[0] if change.pr and "(#" + change.pr + ")" not in current_changelog
        ]
        if not new_changes:
            get_console().print(
                f"[success]The provider {package_id} changelog for `{latest_version}` "
                "has no new changes. Not updating the changelog.[/]"
            )
            return
        new_context["new_changes"] = new_changes
        generated_new_changelog = render_template(
            template_name="UPDATE_CHANGELOG", context=new_context, extension=".rst"
        )
    else:
        if changes:
            classified_changes = _get_changes_classified(
                changes[0],
                with_breaking_changes=with_breaking_changes,
                maybe_with_new_features=maybe_with_new_features,
            )
        else:
            # change log exist but without version 1.0.0 entry
            classified_changes = None

        new_context.update(
            {
                "version": latest_version,
                "version_header": "." * len(latest_version),
                "classified_changes": classified_changes,
            }
        )
        generated_new_changelog = render_template(
            template_name="CHANGELOG", context=new_context, extension=".rst"
        )
    new_changelog_lines = current_changelog_lines[0:insertion_index]
    new_changelog_lines.extend(generated_new_changelog.splitlines())
    new_changelog_lines.extend(current_changelog_lines[insertion_index:])
    diff = "\n".join(difflib.context_diff(current_changelog_lines, new_changelog_lines, n=5))
    syntax = Syntax(diff, "diff")
    get_console().print(syntax)
    if not append:
        get_console().print(
            f"[success]The provider {package_id} changelog for `{latest_version}` "
            "version is missing. Generating fresh changelog.[/]"
        )
    else:
        get_console().print(
            f"[success]Appending the provider {package_id} changelog for `{latest_version}` version.[/]"
        )
    provider_details.changelog_path.write_text("\n".join(new_changelog_lines) + "\n")


def _update_index_rst(
    context: dict[str, Any],
    provider_package_id: str,
    target_path: Path,
):
    index_update = render_template(
        template_name="PROVIDER_INDEX", context=context, extension=".rst", keep_trailing_newline=True
    )
    index_file_path = target_path / "index.rst"
    old_text = ""
    if index_file_path.is_file():
        old_text = index_file_path.read_text()
    new_text = deepcopy(old_text)
    lines = old_text.splitlines(keepends=False)
    for index, line in enumerate(lines):
        if AUTOMATICALLY_GENERATED_MARKER in line:
            new_text = "\n".join(lines[:index])
    new_text += "\n" + AUTOMATICALLY_GENERATED_CONTENT + "\n"
    new_text += index_update
    replace_content(index_file_path, old_text, new_text, provider_package_id)


def get_provider_documentation_jinja_context(
    provider_id: str, with_breaking_changes: bool, maybe_with_new_features: bool
) -> dict[str, Any]:
    provider_details = get_provider_details(provider_id)
    jinja_context = get_provider_jinja_context(
        provider_id=provider_id,
        current_release_version=provider_details.versions[0],
        version_suffix="",
    )
    jinja_context["WITH_BREAKING_CHANGES"] = with_breaking_changes
    jinja_context["MAYBE_WITH_NEW_FEATURES"] = maybe_with_new_features

    jinja_context["ADDITIONAL_INFO"] = (
        _get_additional_package_info(provider_package_path=provider_details.source_provider_package_path),
    )
    return jinja_context


def update_changelog(
    package_id: str,
    base_branch: str,
    reapply_templates_only: bool,
    with_breaking_changes: bool,
    maybe_with_new_features: bool,
    only_min_version_update: bool,
):
    """Internal update changelog method.

    :param package_id: package id
    :param base_branch: base branch to check changes in apache remote for changes
    :param reapply_templates_only: only reapply templates, no changelog generation
    :param with_breaking_changes: whether there are any breaking changes
    :param maybe_with_new_features: whether there are any new features
    """
    provider_details = get_provider_details(package_id)
    jinja_context = get_provider_documentation_jinja_context(
        provider_id=package_id,
        with_breaking_changes=with_breaking_changes,
        maybe_with_new_features=maybe_with_new_features,
    )
    proceed, changes, _ = _get_all_changes_for_package(
        provider_package_id=package_id,
        base_branch=base_branch,
        reapply_templates_only=reapply_templates_only,
        only_min_version_update=only_min_version_update,
    )
    if not proceed:
        if not only_min_version_update:
            get_console().print(
                f"[warning]The provider {package_id} is not being released. Skipping the package.[/]"
            )
        raise PrepareReleaseDocsNoChangesException()
    if reapply_templates_only:
        get_console().print("[info]Only reapply templates, no changelog update[/]")
    else:
        _generate_new_changelog(
            package_id=package_id,
            provider_details=provider_details,
            changes=changes,
            context=jinja_context,
            with_breaking_changes=with_breaking_changes,
            maybe_with_new_features=maybe_with_new_features,
        )
    get_console().print(f"\n[info]Update index.rst for {package_id}\n")
    _update_index_rst(jinja_context, package_id, provider_details.documentation_provider_package_path)


def _generate_init_py_file_for_provider(
    context: dict[str, Any],
    target_path: Path,
):
    init_py_content = black_format(
        render_template(
            template_name="PROVIDER__INIT__PY",
            context=context,
            extension=".py",
            keep_trailing_newline=True,
        )
    )
    init_py_path = target_path / "__init__.py"
    init_py_path.write_text(init_py_content)


def _replace_min_airflow_version_in_provider_yaml(
    context: dict[str, Any],
    target_path: Path,
):
    provider_yaml_path = target_path / "provider.yaml"
    provider_yaml_txt = provider_yaml_path.read_text()
    provider_yaml_txt = re.sub(
        r" {2}- apache-airflow>=.*",
        f"  - apache-airflow>={context['MIN_AIRFLOW_VERSION']}",
        provider_yaml_txt,
    )
    provider_yaml_path.write_text(provider_yaml_txt)
    refresh_provider_metadata_from_yaml_file(provider_yaml_path)


def update_min_airflow_version(
    provider_package_id: str, with_breaking_changes: bool, maybe_with_new_features: bool
):
    """Updates min airflow version in provider yaml and __init__.py

    :param provider_package_id: provider package id
    :param with_breaking_changes: whether there are any breaking changes
    :param maybe_with_new_features: whether there are any new features
    :return:
    """
    provider_details = get_provider_details(provider_package_id)
    if provider_details.removed:
        return
    jinja_context = get_provider_documentation_jinja_context(
        provider_id=provider_package_id,
        with_breaking_changes=with_breaking_changes,
        maybe_with_new_features=maybe_with_new_features,
    )
    _generate_init_py_file_for_provider(
        context=jinja_context,
        target_path=provider_details.source_provider_package_path,
    )
    _replace_min_airflow_version_in_provider_yaml(
        context=jinja_context, target_path=provider_details.source_provider_package_path
    )
