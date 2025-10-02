#!/usr/bin/env python
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
"""Builds documentation and runs spell checking."""

from __future__ import annotations

import itertools
import multiprocessing
import os
import shutil
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any, NamedTuple, TypeVar

import rich_click as click
from click import Choice
from rich.console import Console
from tabulate import tabulate

from sphinx_exts.docs_build import dev_index_generator
from sphinx_exts.docs_build.code_utils import CONSOLE_WIDTH, GENERATED_PATH
from sphinx_exts.docs_build.docs_builder import (
    AirflowDocsBuilder,
    get_available_packages,
    get_long_form,
    get_short_form,
)
from sphinx_exts.docs_build.errors import DocBuildError, display_errors_summary
from sphinx_exts.docs_build.fetch_inventories import fetch_inventories
from sphinx_exts.docs_build.github_action_utils import with_group
from sphinx_exts.docs_build.package_filter import find_packages_to_build
from sphinx_exts.docs_build.spelling_checks import SpellingError, display_spelling_error_summary

TEXT_RED = "\033[31m"
TEXT_RESET = "\033[0m"

CHANNEL_INVITATION = """\
If you need help, write to #documentation channel on Airflow's Slack.
Channel link: https://apache-airflow.slack.com/archives/CJ1LVREHX
Invitation link: https://s.apache.org/airflow-slack\
"""

ERRORS_ELIGIBLE_TO_REBUILD = [
    "failed to reach any of the inventories with the following issues",
    "toctree contains reference to nonexisting document",
    "undefined label:",
    "unknown document:",
    "Error loading airflow.providers",
]

ON_GITHUB_ACTIONS = os.environ.get("GITHUB_ACTIONS", "false") == "true"

console = Console(force_terminal=True, color_system="standard", width=CONSOLE_WIDTH)

T = TypeVar("T")


class BetterChoice(Choice):
    """
    Nicer formatted choice class for click.

    We have a lot of parameters sometimes, and formatting
    them without spaces causes ugly artifacts as the words are broken. This one adds spaces so
    that when the long list of choices does not wrap on words.
    """

    name = "BetterChoice"

    def __init__(self, *args):
        super().__init__(*args)
        self.all_choices: Sequence[str] = self.choices

    def get_metavar(self, param) -> str:
        choices_str = " | ".join(self.all_choices)
        # Use curly braces to indicate a required argument.
        if param.required and param.param_type_name == "argument":
            return f"{{{choices_str}}}"

        if param.param_type_name == "argument" and param.nargs == -1:
            # avoid double [[ for multiple args
            return f"{choices_str}"

        # Use square braces to indicate an option or optional argument.
        return f"[{choices_str}]"


def partition(pred: Callable[[T], bool], iterable: Iterable[T]) -> tuple[Iterable[T], Iterable[T]]:
    """Use a predicate to partition entries into false entries and true entries"""
    iter_1, iter_2 = itertools.tee(iterable)
    return itertools.filterfalse(pred, iter_1), filter(pred, iter_2)


def _promote_new_flags():
    console.print()
    console.print("[yellow]Tired of waiting for documentation to be built?[/]")
    console.print()
    console.print("You can quickly build documentation locally with just one command.\n")
    console.print("    [bright_blue]cd DISTRIBUTION_FOLDER[/]")
    console.print("    [bright_blue]uv run --group docs build-docs[/]")
    console.print()
    console.print("If you want to interactively work on your documentation you can add --autobuild flag.\n")
    console.print("    [bright_blue]cd DISTRIBUTION_FOLDER[/]")
    console.print("    [bright_blue]uv run --group docs build-docs --autobuild[/]")
    console.print()
    console.print("You can build several documentation packages together if they are modified together:")
    console.print("    [bright_blue]uv run --group docs build-docs PACKAGE1 PACKAGE2[/]")
    console.print()
    console.print("In the root of Airflow repo, you can build all packages together:")
    console.print("    [bright_blue]uv run --group docs build-docs[/]")
    console.print()
    console.print("You can also use other extra flags to iterate faster:")
    console.print("   [bright_blue]--docs-only       - Only build documentation[/]")
    console.print("   [bright_blue]--spellcheck-only - Only perform spellchecking[/]")
    console.print()
    console.print("You can list all packages you can build:")
    console.print()
    console.print("   [bright_blue]--list-packages   - Shows the list of packages you can build[/]")
    console.print()
    console.print("You can run clean build - refreshing inter-sphinx inventories or refresh airflow ones.\n")
    console.print(
        "   [bright_blue]--clean-build                 - Refresh inventories and build files for all inter-sphinx references (including external ones)[/]"
    )
    console.print(
        "   [bright_blue]--refresh-airflow-inventories - Force refresh only airflow inventories (without cleaning build files or external inventories).[/]"
    )
    console.print()
    console.print("For more info:")
    console.print("   [bright_blue]uv run build-docs --help[/]")
    console.print()


class BuildSpecification(NamedTuple):
    """Specification of single build."""

    package_name: str
    is_autobuild: bool
    verbose: bool


class BuildDocsResult(NamedTuple):
    """Result of building documentation."""

    package_name: str
    log_file_name: Path
    errors: list[DocBuildError]


class SpellCheckResult(NamedTuple):
    """Result of spellcheck."""

    package_name: str
    log_file_name: Path
    spelling_errors: list[SpellingError]
    build_errors: list[DocBuildError]


def perform_docs_build_for_single_package(build_specification: BuildSpecification) -> BuildDocsResult:
    """Performs single package docs build."""
    builder = AirflowDocsBuilder(package_name=build_specification.package_name)
    builder.is_autobuild = build_specification.is_autobuild
    console.print(
        f"[bright_blue]{build_specification.package_name:60}:[/] Building documentation"
        + (" (autobuild)" if build_specification.is_autobuild else "")
    )
    result = BuildDocsResult(
        package_name=build_specification.package_name,
        errors=builder.build_sphinx_docs(
            verbose=build_specification.verbose,
        ),
        log_file_name=builder.log_build_filename,
    )
    return result


def perform_spell_check_for_single_package(build_specification: BuildSpecification) -> SpellCheckResult:
    """Performs single package spell check."""
    builder = AirflowDocsBuilder(package_name=build_specification.package_name)
    builder.is_autobuild = build_specification.is_autobuild
    console.print(f"[bright_blue]{build_specification.package_name:60}:[/] Checking spelling started")
    spelling_errors, build_errors = builder.check_spelling(
        verbose=build_specification.verbose,
    )
    result = SpellCheckResult(
        package_name=build_specification.package_name,
        spelling_errors=spelling_errors,
        build_errors=build_errors,
        log_file_name=builder.log_spelling_filename,
    )
    console.print(f"[bright_blue]{build_specification.package_name:60}:[/] Checking spelling completed")
    return result


def build_docs_for_packages(
    packages_to_build: list[str],
    is_autobuild: bool,
    docs_only: bool,
    spellcheck_only: bool,
    jobs: int,
    verbose: bool,
) -> tuple[dict[str, list[DocBuildError]], dict[str, list[SpellingError]]]:
    """Builds documentation for all packages and combines errors."""
    all_build_errors: dict[str, list[DocBuildError]] = defaultdict(list)
    all_spelling_errors: dict[str, list[SpellingError]] = defaultdict(list)
    with with_group("Cleaning documentation files"):
        for package_name in packages_to_build:
            console.print(f"[bright_blue]{package_name:60}:[/] Cleaning files")
            builder = AirflowDocsBuilder(package_name=package_name)
            builder.is_autobuild = is_autobuild
            builder.clean_files()
    if jobs > 1 and len(packages_to_build) > 1:
        run_in_parallel(
            all_build_errors=all_build_errors,
            all_spelling_errors=all_spelling_errors,
            packages_to_build=packages_to_build,
            docs_only=docs_only,
            jobs=jobs,
            spellcheck_only=spellcheck_only,
            verbose=verbose,
        )
    else:
        run_sequentially(
            all_build_errors=all_build_errors,
            all_spelling_errors=all_spelling_errors,
            packages_to_build=packages_to_build,
            is_autobuild=is_autobuild,
            docs_only=docs_only,
            spellcheck_only=spellcheck_only,
            verbose=verbose,
        )
    return all_build_errors, all_spelling_errors


def run_sequentially(
    all_build_errors,
    all_spelling_errors,
    is_autobuild,
    packages_to_build,
    docs_only,
    spellcheck_only,
    verbose,
):
    """Run both - spellcheck and docs build sequentially without multiprocessing"""
    if not spellcheck_only:
        for package_name in packages_to_build:
            build_result = perform_docs_build_for_single_package(
                build_specification=BuildSpecification(
                    package_name=package_name,
                    is_autobuild=is_autobuild,
                    verbose=verbose,
                )
            )
            if build_result.errors:
                all_build_errors[package_name].extend(build_result.errors)
                print_build_output(build_result)
    if not docs_only:
        for package_name in packages_to_build:
            spellcheck_result = perform_spell_check_for_single_package(
                build_specification=BuildSpecification(
                    package_name=package_name,
                    is_autobuild=is_autobuild,
                    verbose=verbose,
                )
            )
            if spellcheck_result.spelling_errors:
                all_spelling_errors[package_name].extend(spellcheck_result.spelling_errors)
                if spellcheck_only:
                    all_build_errors[package_name].extend(spellcheck_result.build_errors)
                print_spelling_output(spellcheck_result)


def run_in_parallel(
    all_build_errors: dict[str, list[DocBuildError]],
    all_spelling_errors: dict[str, list[SpellingError]],
    packages_to_build: list[str],
    docs_only: bool,
    jobs: int,
    spellcheck_only: bool,
    verbose: bool,
):
    """Run both - spellcheck and docs build sequentially without multiprocessing"""
    with multiprocessing.Pool(processes=jobs) as pool:
        if not spellcheck_only:
            run_docs_build_in_parallel(
                all_build_errors=all_build_errors,
                packages_to_build=packages_to_build,
                verbose=verbose,
                pool=pool,
            )
        if not docs_only:
            run_spell_check_in_parallel(
                all_spelling_errors=all_spelling_errors,
                all_build_errors=all_build_errors,
                packages_to_build=packages_to_build,
                verbose=verbose,
                pool=pool,
            )


def print_build_output(result: BuildDocsResult):
    """Prints output of docs build job."""
    with with_group(f"{TEXT_RED}Output for documentation build {result.package_name}{TEXT_RESET}"):
        console.print()
        console.print(f"[bright_blue]{result.package_name:60}: " + "#" * 80)
        with open(result.log_file_name) as output:
            for line in output.read().splitlines():
                console.print(f"{result.package_name:60} {line}")
        console.print(f"[bright_blue]{result.package_name:60}: " + "#" * 80)


def run_docs_build_in_parallel(
    all_build_errors: dict[str, list[DocBuildError]],
    packages_to_build: list[str],
    verbose: bool,
    pool: Any,  # Cannot use multiprocessing types here: https://github.com/python/typeshed/issues/4266
):
    """Runs documentation building in parallel."""
    doc_build_specifications: list[BuildSpecification] = []
    with with_group("Scheduling documentation to build"):
        for package_name in packages_to_build:
            console.print(f"[bright_blue]{package_name:60}:[/] Scheduling documentation to build")
            doc_build_specifications.append(
                BuildSpecification(
                    is_autobuild=False,
                    package_name=package_name,
                    verbose=verbose,
                )
            )
    with with_group("Running docs building"):
        console.print()
        result_list = pool.map(perform_docs_build_for_single_package, doc_build_specifications)
    for result in result_list:
        if result.errors:
            all_build_errors[result.package_name].extend(result.errors)
            print_build_output(result)


def print_spelling_output(result: SpellCheckResult):
    """Prints output of spell check job."""
    with with_group(f"{TEXT_RED}Output for spelling check: {result.package_name}{TEXT_RESET}"):
        console.print()
        console.print(f"[bright_blue]{result.package_name:60}: " + "#" * 80)
        with open(result.log_file_name) as output:
            for line in output.read().splitlines():
                console.print(f"{result.package_name:60} {line}")
        console.print(f"[bright_blue]{result.package_name:60}: " + "#" * 80)
        console.print()


def run_spell_check_in_parallel(
    all_spelling_errors: dict[str, list[SpellingError]],
    all_build_errors: dict[str, list[DocBuildError]],
    packages_to_build: list[str],
    verbose: bool,
    pool,
):
    """Runs spell check in parallel."""
    spell_check_specifications: list[BuildSpecification] = []
    with with_group("Scheduling spell checking of documentation"):
        for package_name in packages_to_build:
            console.print(f"[bright_blue]{package_name:60}:[/] Scheduling spellchecking")
            spell_check_specifications.append(
                BuildSpecification(package_name=package_name, is_autobuild=False, verbose=verbose)
            )
    with with_group("Running spell checking of documentation"):
        console.print()
        result_list = pool.map(perform_spell_check_for_single_package, spell_check_specifications)
    for result in result_list:
        if result.spelling_errors:
            all_spelling_errors[result.package_name].extend(result.spelling_errors)
            all_build_errors[result.package_name].extend(result.build_errors)
            print_spelling_output(result)


def display_packages_summary(
    build_errors: dict[str, list[DocBuildError]], spelling_errors: dict[str, list[SpellingError]]
):
    """Displays a summary that contains information on the number of errors in each packages"""
    packages_names = {*build_errors.keys(), *spelling_errors.keys()}
    tabular_data = [
        {
            "Package name": f"[bright_blue]{package_name}[/]",
            "Count of doc build errors": len(build_errors.get(package_name, [])),
            "Count of spelling errors": len(spelling_errors.get(package_name, [])),
        }
        for package_name in sorted(packages_names, key=lambda k: k or "")
    ]
    console.print("#" * 20, " Packages errors summary ", "#" * 20)
    console.print(tabulate(tabular_data=tabular_data, headers="keys"))
    console.print("#" * 50)


def print_build_errors_and_exit(
    build_errors: dict[str, list[DocBuildError]],
    spelling_errors: dict[str, list[SpellingError]],
    spellcheck_only: bool,
) -> None:
    """Prints build errors and exists."""
    if build_errors or spelling_errors:
        if build_errors:
            if spellcheck_only:
                console.print("f[warning]There were some build errors remaining.")
                console.print()
            else:
                display_errors_summary(build_errors)
                console.print()
        if spelling_errors:
            display_spelling_error_summary(spelling_errors)
            console.print()
        console.print("The documentation has errors.")
        display_packages_summary(build_errors, spelling_errors)
        console.print()
        console.print(CHANNEL_INVITATION)
        sys.exit(1)
    else:
        console.print("[green]Documentation build is successful[/]")


def do_list_packages():
    available_packages = get_available_packages()
    console.print(
        "\nAvailable packages (in parenthesis the short form of the"
        " package is shown if it has a short form):\n"
    )
    for package in available_packages:
        short_package_name = get_short_form(package)
        if short_package_name:
            console.print(f"[bright_blue]{package} ({short_package_name}) [/] ")
        else:
            console.print(f"[bright_blue]{package} [/] ")
    console.print()


def is_command_available(command):
    """Check if a command is available in the system's PATH."""
    return shutil.which(command) is not None


click.rich_click.OPTION_GROUPS = {
    "build-docs": [
        {
            "name": "Build scope (default is to build docs and spellcheck)",
            "options": ["--docs-only", "--spellcheck-only"],
        },
        {
            "name": "Type of build",
            "options": ["--autobuild", "--one-pass-only"],
        },
        {
            "name": "Cleaning inventories",
            "options": ["--clean-build", "--refresh-airflow-inventories"],
        },
        {
            "name": "Filtering options",
            "options": [
                "--package-filter",
                "--list-packages",
            ],
        },
        {
            "name": "Miscellaneous options",
            "options": [
                "--include-commits",
                "--jobs",
                "--verbose",
            ],
        },
    ],
}


@click.command(name="build-docs")
@click.option(
    "--autobuild",
    is_flag=True,
    help="Starts server, serving the build docs (sphinx-autobuild) rebuilding and "
    "refreshing the docs when they change on the disk. "
    "Implies --verbose, --docs-only and --one-pass-only",
)
@click.option("--one-pass-only", is_flag=True, help="Do not attempt multiple builds on error")
@click.option(
    "--package-filter",
    "package_filters",
    multiple=True,
    help="Filter(s) to use more than one can be specified. You can use glob pattern matching the full "
    "package name, for example `apache-airflow-providers-*`. "
    "Useful when you want to select several similarly named packages together. "
    "When no filtering is specified and the command is run inside one of the distribution packages with docs, "
    "only that package is selected to build. "
    "If the command is run in the root of the Airflow repo, all packages are selected to be built.",
)
@click.option("--docs-only", is_flag=True, help="Only build documentation")
@click.option("--spellcheck-only", is_flag=True, help="Only perform spellchecking")
@click.option(
    "--include-commits", help="Include commits in the documentation.", envvar="INCLUDE_COMMITS", is_flag=True
)
@click.option(
    "-j",
    "--jobs",
    default=os.cpu_count(),
    show_default=True,
    type=int,
    help="Number of parallel processes that will be spawned to build the docs. If not set, default - equal "
    "to the number of CPU cores - will be used. If set to 1, the build will be done sequentially.",
)
@click.option(
    "--list-packages",
    is_flag=True,
    help="Lists all available packages. You can use it to check the names of the packages you want to build.",
)
@click.option(
    "--clean-build",
    is_flag=True,
    help="Cleans the build directory before building the documentation and removes all inventory "
    "cache (including external inventories).",
)
@click.option(
    "--refresh-airflow-inventories",
    is_flag=True,
    help="When set, only airflow package inventories will be refreshed, regardless "
    "if they are already downloaded. With `--clean-build` - everything is cleaned..",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Increases the verbosity of the script i.e. always displays a full log of "
    "the build process, not just when it encounters errors",
)
@click.argument(
    "packages",
    nargs=-1,
    type=BetterChoice(get_available_packages(short_form=True)),
)
def build_docs(
    autobuild,
    one_pass_only,
    package_filters,
    clean_build,
    docs_only,
    spellcheck_only,
    include_commits,
    jobs,
    list_packages,
    refresh_airflow_inventories,
    verbose,
    packages,
):
    """Builds documentation and runs spell checking for all distribution packages of airflow.."""
    if list_packages:
        do_list_packages()
        sys.exit(0)
    command = "sphinx-autobuild" if autobuild else "sphinx-build"
    if not is_command_available(command):
        console.print(
            f"\n[red]Command '{command}' is not available. "
            "Please use `--group docs` after the `uv run` when running the command:[/]\n"
        )
        console.print("uv run --group docs build-docs ...\n")
        sys.exit(1)
    available_packages = get_available_packages()
    filters_to_add = []
    for package_name in packages:
        if package_name in available_packages:
            filters_to_add.append(package_name)
        else:
            long_form = get_long_form(package_name)
            if long_form:
                filters_to_add.append(long_form)
            else:
                console.print("[red]Bad package specified as argument[/]:", package_name)
                sys.exit(1)
    if filters_to_add:
        package_filters = tuple(set(package_filters).union(set(filters_to_add)))
    packages_to_build = find_packages_to_build(available_packages, package_filters)
    for package_name in packages_to_build:
        builder = AirflowDocsBuilder(package_name=package_name)
        api_dir = builder._api_dir
        if api_dir.exists():
            try:
                if api_dir.iterdir():
                    shutil.rmtree(api_dir)
            except StopIteration:
                pass
    with with_group("Fetching inventories"):
        # Inventories that could not be retrieved should be built first. This may mean this is a
        # new package.
        packages_without_inventories = fetch_inventories(
            clean_build=clean_build, refresh_airflow_inventories=refresh_airflow_inventories
        )
    normal_packages, priority_packages = partition(
        lambda d: d in packages_without_inventories, packages_to_build
    )
    normal_packages, priority_packages = list(normal_packages), list(priority_packages)
    if len(packages_to_build) > 1 and autobuild:
        console.print("[red]You cannot use more than 1 package with --autobuild. Quitting.[/]")
        sys.exit(1)
    if autobuild:
        console.print(
            "[yellow]Autobuild mode is enabled. Forcing --docs-only, --one-pass-only and --verbose[/]"
        )
        docs_only = True
        verbose = True
        one_pass_only = True

    if len(packages_to_build) == 1:
        console.print(
            "[yellow]Building one package. Forcing --one-pass-only and --jobs to 1 as only one pass is needed."
        )
        one_pass_only = True
        jobs = 1

    with with_group(
        f"Documentation will be built for {len(packages_to_build)} package(s)"
        + (f"with up to {jobs} parallel jobs," if jobs > 1 else "")
    ):
        for pkg_no, pkg in enumerate(packages_to_build, start=1):
            console.print(f"{pkg_no}. {pkg}")
    os.environ["INCLUDE_COMMITS"] = "true" if include_commits else "false"
    all_build_errors: dict[str | None, list[DocBuildError]] = {}
    all_spelling_errors: dict[str | None, list[SpellingError]] = {}
    if priority_packages:
        # Build priority packages
        package_build_errors, package_spelling_errors = build_docs_for_packages(
            packages_to_build=priority_packages,
            is_autobuild=autobuild,
            docs_only=docs_only,
            spellcheck_only=spellcheck_only,
            jobs=jobs,
            verbose=verbose,
        )
        if package_build_errors:
            all_build_errors.update(package_build_errors)
        if package_spelling_errors:
            all_spelling_errors.update(package_spelling_errors)

    # Build normal packages
    # If only one inventory is missing, the remaining packages are correct. If we are missing
    # two or more inventories, it is better to try to build for all packages as the previous packages
    # may have failed as well.
    package_build_errors, package_spelling_errors = build_docs_for_packages(
        packages_to_build=packages_to_build if len(priority_packages) > 1 else normal_packages,
        is_autobuild=autobuild,
        docs_only=docs_only,
        spellcheck_only=spellcheck_only,
        jobs=jobs,
        verbose=verbose,
    )
    if package_build_errors:
        all_build_errors.update(package_build_errors)
    if package_spelling_errors:
        all_spelling_errors.update(package_spelling_errors)

    if not one_pass_only:
        # Build documentation for some packages again if it can help them.
        package_build_errors = retry_building_docs_if_needed(
            all_build_errors=all_build_errors,
            all_spelling_errors=all_spelling_errors,
            autobuild=autobuild,
            docs_only=docs_only,
            jobs=jobs,
            verbose=verbose,
            package_build_errors=package_build_errors,
            originally_built_packages=packages_to_build,
            # If spellchecking fails, we need to rebuild all packages first in case some references
            # are broken between packages
            rebuild_all_packages=spellcheck_only,
        )

        # And try again in case one change spans across three-level dependencies.
        package_build_errors = retry_building_docs_if_needed(
            all_build_errors=all_build_errors,
            all_spelling_errors=all_spelling_errors,
            autobuild=autobuild,
            docs_only=docs_only,
            jobs=jobs,
            verbose=verbose,
            package_build_errors=package_build_errors,
            originally_built_packages=packages_to_build,
            # In the 3rd pass we only rebuild packages that failed in the 2nd pass
            # no matter if we do spellcheck-only build
            rebuild_all_packages=False,
        )

        if spellcheck_only:
            # And in case of spellcheck-only, we add a 4th pass to account for A->B-C case
            # For spellcheck-only build, the first pass does not solve any of the dependency
            # Issues, they only start getting solved and the 2nd pass so we might need to do one more pass
            package_build_errors = retry_building_docs_if_needed(
                all_build_errors=all_build_errors,
                all_spelling_errors=all_spelling_errors,
                autobuild=autobuild,
                docs_only=docs_only,
                jobs=jobs,
                verbose=verbose,
                package_build_errors=package_build_errors,
                originally_built_packages=packages_to_build,
                # In the 4th pass we only rebuild packages that failed in the 3rd pass
                # no matter if we do spellcheck-only build
                rebuild_all_packages=False,
            )

    dev_index_generator.generate_index(f"{GENERATED_PATH}/_build/index.html")

    if not package_filters:
        _promote_new_flags()

    print_build_errors_and_exit(
        all_build_errors,
        all_spelling_errors,
        spellcheck_only,
    )


def retry_building_docs_if_needed(
    all_build_errors: dict[str, list[DocBuildError]],
    all_spelling_errors: dict[str, list[SpellingError]],
    autobuild: bool,
    docs_only: bool,
    jobs: int,
    verbose: bool,
    package_build_errors: dict[str, list[DocBuildError]],
    originally_built_packages: list[str],
    rebuild_all_packages: bool,
) -> dict[str, list[DocBuildError]]:
    to_retry_packages = [
        package_name
        for package_name, errors in package_build_errors.items()
        if any(any((m in e.message) for m in ERRORS_ELIGIBLE_TO_REBUILD) for e in errors)
    ]
    if not to_retry_packages:
        console.print("[green]No packages to retry. No more passes are needed.[/]")
        return package_build_errors
    console.print("[warning] Some packages failed to build due to dependencies. We need another pass.[/]")
    # if we are rebuilding all packages, we need to retry all packages
    # even if there is one package to rebuild only
    if rebuild_all_packages:
        console.print("[warning]Rebuilding all originally built package as this is the first build pass:[/]")
        to_retry_packages = originally_built_packages
    console.print(f"[bright_blue]Packages to rebuild: {to_retry_packages}[/]")
    for package_name in to_retry_packages:
        if package_name in all_build_errors:
            del all_build_errors[package_name]
        if package_name in all_spelling_errors:
            del all_spelling_errors[package_name]
    package_build_errors, package_spelling_errors = build_docs_for_packages(
        packages_to_build=to_retry_packages,
        is_autobuild=autobuild,
        docs_only=docs_only,
        spellcheck_only=False,
        jobs=jobs,
        verbose=verbose,
    )
    if package_build_errors:
        all_build_errors.update(package_build_errors)
    if package_spelling_errors:
        all_spelling_errors.update(package_spelling_errors)
    return package_build_errors


if __name__ == "__main__":
    build_docs()
