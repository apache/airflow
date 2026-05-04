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
from typing import TextIO

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.packages import (
    get_available_distributions,
    get_latest_provider_tag,
    get_not_ready_provider_ids,
    get_provider_details,
    get_provider_distributions_metadata,
    get_removed_provider_ids,
    tag_exists_for_provider,
)
from airflow_breeze.utils.path_utils import AIRFLOW_DIST_PATH, AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.version_utils import is_local_package_version


def check_flit_worktree_compatibility(distribution_format: str) -> None:
    """Refuse to build provider sdists when VCS detection will silently break.

    Provider sdists are built with ``flit --use-vcs``. flit identifies the VCS
    via ``flit.vcs.identify_vcs``, which checks ``(p / ".git").is_dir()``.
    That check fails in two related scenarios, both of which produce a
    broken sdist with no warning from flit:

    1. **Plain git worktree on the host.** Inside a ``git worktree add ...``
       directory ``.git`` is a regular file containing a ``gitdir: <path>``
       pointer, not a directory. ``is_dir()`` returns False, ``identify_vcs``
       returns ``None``, and flit falls back to a non-VCS sdist that omits
       tracked files like ``docs/``, ``tests/`` and ``provider.yaml``. The
       resulting packages fail reproducibility checks against the released
       sdists on dist.apache.org.
    2. **Breeze Docker builds with a worktree mounted.** When the build runs
       inside Breeze's container, only the worktree directory is
       bind-mounted. The absolute ``gitdir:`` path baked into ``.git``
       points somewhere on the host (``<main_repo>/.git/worktrees/<name>``)
       that does not exist inside the container, so even if flit recognised
       worktrees, ``git ls-files`` would fail or misbehave.

    Wheels are built from ``pyproject.toml`` metadata and are unaffected, so
    this check is a no-op for ``--distribution-format wheel``. Providers
    that use ``hatchling`` with an explicit
    ``[tool.hatch.build.targets.sdist]`` ``include`` list are also
    unaffected.

    Upstream flit tracking:

    - Issue: https://github.com/pypa/flit/issues/798
    - Fix PR: https://github.com/pypa/flit/pull/799

    Airflow-side follow-up (remove this workaround when both conditions in
    the tracking issue are met):
    https://github.com/apache/airflow/issues/65772
    """
    if distribution_format == "wheel":
        return

    git_marker = AIRFLOW_ROOT_PATH / ".git"
    if not git_marker.is_file():
        # Regular .git directory (or missing entirely) — flit's VCS detection
        # works correctly, nothing to do.
        return

    # .git is a file — typical in a git worktree (or submodule). Parse the
    # pointer so we can give a precise error for each failure mode, and so
    # we detect the Breeze-in-Docker case where the target is unreachable.
    try:
        content = git_marker.read_text(encoding="utf-8").strip()
    except OSError as err:
        console_print(
            f"\n[error]Cannot read {git_marker}: {err}.[/]\n"
            "[info]Run this command from a plain `git checkout`, or pass "
            "`--distribution-format wheel` (wheels are unaffected).[/]\n"
        )
        sys.exit(1)

    if not content.startswith("gitdir:"):
        console_print(
            f"\n[error]Unexpected `.git` file format at {git_marker}.[/]\n\n"
            f"[warning]Expected a `gitdir: <path>` pointer (git worktree or "
            f"submodule), got:\n  {content!r}[/]\n\n"
            "[info]Run this command from a plain `git checkout`, or pass "
            "`--distribution-format wheel` (wheels are unaffected).[/]\n"
        )
        sys.exit(1)

    gitdir = Path(content[len("gitdir:") :].strip())
    if not gitdir.exists():
        console_print(
            "\n[error]Cannot build provider sdists: git worktree pointer "
            "targets a missing gitdir.[/]\n\n"
            f"[warning]The `.git` file at `{git_marker}` points to "
            f"`{gitdir}`, but that directory does not exist from here.[/]\n\n"
            "[info]This typically happens when Breeze runs the build inside "
            "Docker: only the worktree directory is bind-mounted into the "
            "container, so the main repo's `.git/worktrees/<name>` folder "
            "that the pointer references is not reachable. flit then either "
            "fails outright or silently produces an incomplete sdist that "
            "omits tracked files.[/]\n\n"
            "[info]Run this command from a plain `git checkout` (not a "
            "`git worktree add ...` directory), or pass "
            "`--distribution-format wheel` — wheels don't need VCS metadata "
            "and are unaffected.[/]\n"
        )
        sys.exit(1)

    # Healthy worktree on the host. flit's `.is_dir()` check still misses it,
    # so we refuse sdist builds here too.
    console_print(
        "\n[error]Cannot build provider sdists from a git worktree.[/]\n\n"
        "[warning]flit's `--use-vcs` does not recognise git worktrees "
        "(flit.vcs.identify_vcs uses `.git.is_dir()`, but in a worktree "
        "`.git` is a file). flit silently falls back to a minimal sdist "
        "that omits docs/, tests/, provider.yaml and other tracked files, "
        "so the resulting packages fail reproducibility checks against "
        "the released sdists on dist.apache.org.[/]\n\n"
        "[info]Run this command from a plain `git checkout` (not a "
        "`git worktree add ...` directory). If you only need wheels, pass "
        "`--distribution-format wheel` — wheels are built from "
        "`pyproject.toml` metadata and are not affected by this bug.[/]\n"
    )
    sys.exit(1)


class PrepareReleasePackageTagExistException(Exception):
    """Tag already exist for the package."""


class PrepareReleasePackageWrongSetupException(Exception):
    """Wrong setup prepared for the package."""


class PrepareReleasePackageErrorBuildingPackageException(Exception):
    """Error when building the package."""


def should_skip_the_package(provider_id: str, version_suffix: str) -> tuple[bool, str]:
    """Return True, version if the package should be skipped and False, good version suffix if not.

    For RC and official releases we check if the "officially released" version exists
    and skip the released if it was. This allows to skip packages that have not been
    marked for release in this wave. For "dev" suffixes, we always build all packages.
    A local version of an RC release will always be built.
    """
    if version_suffix != "" and (
        not version_suffix.startswith("rc") or is_local_package_version(version_suffix)
    ):
        return False, version_suffix
    if version_suffix == "":
        current_tag = get_latest_provider_tag(provider_id, "")
        if tag_exists_for_provider(provider_id, current_tag):
            console_print(f"[warning]The 'final' tag {current_tag} exists. Skipping the package.[/]")
            return True, version_suffix
        return False, version_suffix
    # version_suffix starts with "rc"
    current_version = int(version_suffix[2:])
    release_tag = get_latest_provider_tag(provider_id, "")
    if tag_exists_for_provider(provider_id, release_tag):
        console_print(f"[warning]The tag {release_tag} exists. Provider is released. Skipping it.[/]")
        return True, version_suffix
    while True:
        current_tag = get_latest_provider_tag(provider_id, f"rc{current_version}")
        if tag_exists_for_provider(provider_id, current_tag):
            current_version += 1
            console_print(f"[warning]The tag {current_tag} exists. Checking rc{current_version}.[/]")
        else:
            return False, f"rc{current_version}"


def cleanup_build_remnants(target_provider_root_sources_path: Path):
    console_print(f"\n[info]Cleaning remnants in {target_provider_root_sources_path}")
    for file in target_provider_root_sources_path.glob("*.egg-info"):
        shutil.rmtree(file, ignore_errors=True)
    shutil.rmtree(target_provider_root_sources_path / "build", ignore_errors=True)
    shutil.rmtree(target_provider_root_sources_path / "dist", ignore_errors=True)
    # Drop any untracked or .gitignored files that might leak into the
    # sdist/wheel via the explicit [tool.flit.sdist] include lists (which
    # scan directories like docs/, tests/ and src/ rather than git). The
    # only files that need this protection are ones that get *generated
    # in-tree* under the provider directory — e.g. docs/_api, sphinx
    # build caches, __pycache__, *.egg-info, leftover scratch files an RM
    # produced while iterating. Without `-x` those would survive into
    # `--no-use-vcs` sdists and break reproducibility against the
    # released artifacts on dist.apache.org.
    #
    # The top-level `.venv`, `.idea` and `.vscode` directories at the
    # repo root are NOT a concern: they live outside the provider
    # directory and are not in any flit include path, so flit would
    # never pick them up regardless of git-clean state. We still pass
    # `-e .venv -e .idea -e .vscode` purely as a safety net for the
    # rare case where someone has a per-provider venv or IDE config
    # *inside* the provider directory — those should be preserved.
    #
    # `-n` is run first so the RM sees what is about to be removed
    # before the destructive pass runs.
    console_print(
        f"[warning]Running git clean -fdx in {target_provider_root_sources_path} — "
        f"this will remove ALL untracked AND .gitignored (generated/local) files "
        f"under that path. .venv, .idea and .vscode are preserved.[/]"
    )
    git_clean_cmd = [
        "git",
        "clean",
        "-fdx",
        "-e",
        ".venv",
        "-e",
        ".idea",
        "-e",
        ".vscode",
        str(target_provider_root_sources_path),
    ]
    run_command([*git_clean_cmd[:2], "-ndx", *git_clean_cmd[3:]], cwd=AIRFLOW_ROOT_PATH, check=False)
    run_command(git_clean_cmd, cwd=AIRFLOW_ROOT_PATH, check=False)
    console_print(f"[info]Cleaned remnants in {target_provider_root_sources_path}\n")


def build_provider_distribution(
    provider_id: str, target_provider_root_sources_path: Path, distribution_format: str
):
    console_print(
        f"\n[info]Building provider package: {provider_id} "
        f"in format {distribution_format} in {target_provider_root_sources_path}\n"
    )
    provider_info = get_provider_distributions_metadata().get(provider_id)
    if not provider_info:
        raise RuntimeError(f"The provider {provider_id} has no provider.yaml defined.")
    build_backend = provider_info.get("build-system", "flit_core")
    build_env = {"SOURCE_DATE_EPOCH": str(get_provider_details(provider_id).source_date_epoch)}
    if build_backend == "flit_core":
        command: list[str] = [sys.executable, "-m", "flit", "build", "--no-setup-py", "--use-vcs"]
        console_print(
            "[warning]Workaround wheel-only package bug in flit by building both and removing sdist."
        )
        # Workaround https://github.com/pypa/flit/issues/743 bug in flit that causes .gitignored files
        # to be included in the package when --format wheel is used
        remove_sdist = False
        if distribution_format == "wheel":
            distribution_format = "both"
            remove_sdist = True
        if distribution_format != "both":
            command.extend(["--format", distribution_format])
        try:
            run_command(
                command,
                check=True,
                cwd=target_provider_root_sources_path,
                env=build_env,
            )
        except subprocess.CalledProcessError as ex:
            console_print(f"[error]The command returned an error {ex}")
            raise PrepareReleasePackageErrorBuildingPackageException()
        if remove_sdist:
            console_print("[warning]Removing sdist file to workaround flit bug on wheel-only packages")
            # Remove the sdist file if it was created
            package_prefix = "apache_airflow_providers_" + provider_id.replace(".", "_")
            for file in (target_provider_root_sources_path / "dist").glob(f"{package_prefix}*.tar.gz"):
                console_print(f"[info]Removing {file} to workaround flit bug on wheel-only packages")
                file.unlink(missing_ok=True)
    elif build_backend == "hatchling":
        command = [sys.executable, "-m", "hatch", "build", "-c", "-t", "custom"]
        if distribution_format == "sdist" or distribution_format == "both":
            command += ["-t", "sdist"]
        if distribution_format == "wheel" or distribution_format == "both":
            command += ["-t", "wheel"]
        try:
            run_command(
                cmd=command,
                cwd=target_provider_root_sources_path,
                env=build_env,
                check=True,
            )
        except subprocess.CalledProcessError as ex:
            console_print(f"[error]The command returned an error {ex}")
            raise PrepareReleasePackageErrorBuildingPackageException()
        shutil.copytree(target_provider_root_sources_path / "dist", AIRFLOW_DIST_PATH, dirs_exist_ok=True)
    else:
        console_print(f"[error]Unknown/unsupported build backend {build_backend}")
        raise PrepareReleasePackageErrorBuildingPackageException()
    console_print(f"\n[info]Prepared provider package {provider_id} in format {distribution_format}[/]\n")


def move_built_distributions_and_cleanup(
    target_provider_root_sources_path: Path,
    dist_folder: Path,
    skip_cleanup: bool,
    delete_only_build_and_dist_folders: bool = False,
):
    for file in (target_provider_root_sources_path / "dist").glob("apache*"):
        console_print(f"[info]Moving {file} to {dist_folder}")
        # Shutil can move packages also between filesystems
        target_file = dist_folder / file.name
        target_file.unlink(missing_ok=True)
        shutil.move(file.as_posix(), dist_folder.as_posix())

    if skip_cleanup:
        console_print(
            f"[warning]NOT Cleaning up the {target_provider_root_sources_path} because "
            f"it was requested by the user[/]\n"
            f"\nYou can use the generated packages to work on the build"
            f"process and bring the changes back to the templates in Breeze "
            f"src/airflow_breeze/templates"
        )
    else:
        console_print(
            f"[info]Cleaning up {target_provider_root_sources_path} with "
            f"delete_only_build_and_dist_folders={delete_only_build_and_dist_folders}"
        )
        if delete_only_build_and_dist_folders:
            shutil.rmtree(target_provider_root_sources_path / "build", ignore_errors=True)
            shutil.rmtree(target_provider_root_sources_path / "dist", ignore_errors=True)
            for file in target_provider_root_sources_path.glob("*.egg-info"):
                shutil.rmtree(file, ignore_errors=True)
        else:
            shutil.rmtree(target_provider_root_sources_path, ignore_errors=True)
        console_print(f"[info]Cleaned up {target_provider_root_sources_path}")


def get_packages_list_to_act_on(
    distributions_list_file: TextIO | None,
    provider_distributions: tuple[str, ...],
    include_not_ready: bool = False,
    include_removed: bool = False,
) -> list[str]:
    if distributions_list_file and provider_distributions:
        console_print(
            "[error]You cannot specify individual provider distributions when you specify package list file."
        )
        sys.exit(1)
    if distributions_list_file:
        removed_provider_ids = get_removed_provider_ids()
        not_ready_provider_ids = get_not_ready_provider_ids()
        return [
            package.strip()
            for package in distributions_list_file.readlines()
            if not package.strip().startswith("#")
            and (package.strip() not in removed_provider_ids or include_removed)
            and (package.strip() not in not_ready_provider_ids or include_not_ready)
        ]
    if provider_distributions:
        return list(provider_distributions)
    return get_available_distributions(include_removed=include_removed, include_not_ready=include_not_ready)
