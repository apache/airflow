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

import shutil
import subprocess
import sys
from pathlib import Path
from shutil import copytree, rmtree
from typing import Any, TextIO

from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.packages import (
    get_available_packages,
    get_latest_provider_tag,
    get_not_ready_provider_ids,
    get_provider_details,
    get_provider_jinja_context,
    get_removed_provider_ids,
    get_source_package_path,
    get_target_root_for_copied_provider_sources,
    render_template,
    tag_exists_for_provider,
)
from airflow_breeze.utils.path_utils import AIRFLOW_PROVIDERS_SRC, AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_utils import run_command

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


class PrepareReleasePackageTagExistException(Exception):
    """Tag already exist for the package."""


class PrepareReleasePackageWrongSetupException(Exception):
    """Wrong setup prepared for the package."""


class PrepareReleasePackageErrorBuildingPackageException(Exception):
    """Error when building the package."""


def copy_provider_sources_to_target(provider_id: str) -> Path:
    target_provider_root_path = get_target_root_for_copied_provider_sources(provider_id)

    if target_provider_root_path.exists() and not target_provider_root_path.is_dir():
        get_console().print(
            f"[error]Target folder for {provider_id} sources is not a directory "
            f"please delete {target_provider_root_path} and try again!"
        )
    rmtree(target_provider_root_path, ignore_errors=True)
    target_provider_root_path.mkdir(parents=True)
    source_provider_sources_path = get_source_package_path(provider_id)
    relative_provider_path = source_provider_sources_path.relative_to(
        AIRFLOW_PROVIDERS_SRC
    )
    target_providers_sub_folder = target_provider_root_path / relative_provider_path
    get_console().print(
        f"[info]Copying provider sources: {source_provider_sources_path} -> {target_providers_sub_folder}"
    )
    copytree(source_provider_sources_path, target_providers_sub_folder)
    shutil.copy(AIRFLOW_SOURCES_ROOT / "LICENSE", target_providers_sub_folder / "LICENSE")
    # We do not copy NOTICE from the top level source of Airflow because NOTICE only refers to
    # Airflow sources - not to providers. If any of the providers is going to have a code that
    # requires NOTICE, then it should be stored in the provider sources (airflow/providers/PROVIDER_ID)
    # And it will be copied from there.
    (target_providers_sub_folder / ".latest-doc-only-change.txt").unlink(missing_ok=True)
    (target_providers_sub_folder / "CHANGELOG.rst").unlink(missing_ok=True)
    (target_providers_sub_folder / "provider.yaml").unlink(missing_ok=True)
    return target_provider_root_path


def get_provider_package_jinja_context(
    provider_id: str, version_suffix: str
) -> dict[str, Any]:
    provider_details = get_provider_details(provider_id)
    jinja_context = get_provider_jinja_context(
        provider_id=provider_id,
        current_release_version=provider_details.versions[0],
        version_suffix=version_suffix,
    )
    return jinja_context


def _prepare_get_provider_info_py_file(
    context: dict[str, Any], provider_id: str, target_path: Path
):
    from airflow_breeze.utils.black_utils import black_format

    get_provider_template_name = "get_provider_info"
    get_provider_content = render_template(
        template_name=get_provider_template_name,
        context=context,
        extension=".py",
        autoescape=False,
        keep_trailing_newline=True,
    )
    target_provider_specific_path = (target_path / "airflow" / "providers").joinpath(
        *provider_id.split(".")
    )
    (target_provider_specific_path / "get_provider_info.py").write_text(
        black_format(get_provider_content)
    )
    get_console().print(
        f"[info]Generated get_provider_info.py in {target_provider_specific_path}[/]"
    )


def _prepare_pyproject_toml_file(context: dict[str, Any], target_path: Path):
    manifest_content = render_template(
        template_name="pyproject",
        context=context,
        extension=".toml",
        autoescape=False,
        keep_trailing_newline=True,
    )
    (target_path / "pyproject.toml").write_text(manifest_content)
    get_console().print(f"[info]Generated pyproject.toml in {target_path}[/]")


def _prepare_readme_file(context: dict[str, Any], target_path: Path):
    readme_content = LICENCE_RST + render_template(
        template_name="PROVIDER_README", context=context, extension=".rst"
    )
    (target_path / "README.rst").write_text(readme_content)
    get_console().print(f"[info]Generated README.rst in {target_path}[/]")


def generate_build_files(
    provider_id: str, version_suffix: str, target_provider_root_sources_path: Path
):
    get_console().print(f"\n[info]Generate build files for {provider_id}\n")
    jinja_context = get_provider_package_jinja_context(
        provider_id=provider_id, version_suffix=version_suffix
    )
    _prepare_get_provider_info_py_file(
        jinja_context, provider_id, target_provider_root_sources_path
    )
    _prepare_pyproject_toml_file(jinja_context, target_provider_root_sources_path)
    _prepare_readme_file(jinja_context, target_provider_root_sources_path)
    get_console().print(f"\n[info]Generated package build files for {provider_id}[/]\n")


def should_skip_the_package(provider_id: str, version_suffix: str) -> tuple[bool, str]:
    """Return True, version if the package should be skipped and False, good version suffix if not.

    For RC and official releases we check if the "officially released" version exists
    and skip the released if it was. This allows to skip packages that have not been
    marked for release in this wave. For "dev" suffixes, we always build all packages.
    """
    if version_suffix != "" and not version_suffix.startswith("rc"):
        return False, version_suffix
    if version_suffix == "":
        current_tag = get_latest_provider_tag(provider_id, "")
        if tag_exists_for_provider(provider_id, current_tag):
            get_console().print(
                f"[warning]The 'final' tag {current_tag} exists. Skipping the package.[/]"
            )
            return True, version_suffix
        return False, version_suffix
    # version_suffix starts with "rc"
    current_version = int(version_suffix[2:])
    release_tag = get_latest_provider_tag(provider_id, "")
    if tag_exists_for_provider(provider_id, release_tag):
        get_console().print(
            f"[warning]The tag {release_tag} exists. Provider is released. Skipping it.[/]"
        )
        return True, ""
    while True:
        current_tag = get_latest_provider_tag(provider_id, f"rc{current_version}")
        if tag_exists_for_provider(provider_id, current_tag):
            current_version += 1
            get_console().print(
                f"[warning]The tag {current_tag} exists. Checking rc{current_version}.[/]"
            )
        else:
            return False, f"rc{current_version}"


def cleanup_build_remnants(target_provider_root_sources_path: Path):
    get_console().print(
        f"\n[info]Cleaning remnants in {target_provider_root_sources_path}"
    )
    for file in target_provider_root_sources_path.glob("*.egg-info"):
        shutil.rmtree(file, ignore_errors=True)
    shutil.rmtree(target_provider_root_sources_path / "build", ignore_errors=True)
    shutil.rmtree(target_provider_root_sources_path / "dist", ignore_errors=True)
    get_console().print(
        f"[info]Cleaned remnants in {target_provider_root_sources_path}\n"
    )


def build_provider_package(
    provider_id: str, target_provider_root_sources_path: Path, package_format: str
):
    get_console().print(
        f"\n[info]Building provider package: {provider_id} in format {package_format} in "
        f"{target_provider_root_sources_path}\n"
    )
    command: list[str] = [
        sys.executable,
        "-m",
        "flit",
        "build",
        "--no-setup-py",
        "--no-use-vcs",
    ]
    if package_format != "both":
        command.extend(["--format", package_format])
    try:
        run_command(
            command,
            check=True,
            cwd=target_provider_root_sources_path,
            env={
                "SOURCE_DATE_EPOCH": str(
                    get_provider_details(provider_id).source_date_epoch
                ),
            },
        )
    except subprocess.CalledProcessError as ex:
        get_console().print("[error]The command returned an error %s", ex)
        raise PrepareReleasePackageErrorBuildingPackageException()
    get_console().print(
        f"\n[info]Prepared provider package {provider_id} in format {package_format}[/]\n"
    )


def move_built_packages_and_cleanup(
    target_provider_root_sources_path: Path, dist_folder: Path, skip_cleanup: bool
):
    for file in (target_provider_root_sources_path / "dist").glob("apache*"):
        get_console().print(f"[info]Moving {file} to {dist_folder}")
        # Shutil can move packages also between filesystems
        target_file = dist_folder / file.name
        target_file.unlink(missing_ok=True)
        shutil.move(file.as_posix(), dist_folder.as_posix())

    if skip_cleanup:
        get_console().print(
            f"[warning]NOT Cleaning up the {target_provider_root_sources_path} because "
            f"it was requested by the user[/]\n"
            f"\nYou can use the generated packages to work on the build"
            f"process and bring the changes back to the templates in Breeze "
            f"src/airflow_breeze/templates"
        )
    else:
        get_console().print(f"[info]Cleaning up {target_provider_root_sources_path}")
        shutil.rmtree(target_provider_root_sources_path, ignore_errors=True)
        get_console().print(f"[info]Cleaned up {target_provider_root_sources_path}")


def get_packages_list_to_act_on(
    package_list_file: TextIO | None,
    provider_packages: tuple[str, ...],
    include_not_ready: bool = False,
    include_removed: bool = False,
) -> list[str]:
    if package_list_file and provider_packages:
        get_console().print(
            "[error]You cannot specify individual provider packages when you specify package list file."
        )
        sys.exit(1)
    if package_list_file:
        removed_provider_ids = get_removed_provider_ids()
        not_ready_provider_ids = get_not_ready_provider_ids()
        return [
            package.strip()
            for package in package_list_file.readlines()
            if not package.strip().startswith("#")
            and (package.strip() not in removed_provider_ids or include_removed)
            and (package.strip() not in not_ready_provider_ids or include_not_ready)
        ]
    elif provider_packages:
        return list(provider_packages)
    return get_available_packages(
        include_removed=include_removed, include_not_ready=include_not_ready
    )
