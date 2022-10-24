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
from __future__ import annotations

import argparse
import multiprocessing
import os
import sys
from collections import defaultdict
from itertools import filterfalse, tee
from typing import Callable, Iterable, NamedTuple, TypeVar

from rich.console import Console
from tabulate import tabulate

from docs.exts.docs_build import dev_index_generator, lint_checks
from docs.exts.docs_build.code_utils import CONSOLE_WIDTH, PROVIDER_INIT_FILE
from docs.exts.docs_build.docs_builder import DOCS_DIR, AirflowDocsBuilder, get_available_packages
from docs.exts.docs_build.errors import DocBuildError, display_errors_summary
from docs.exts.docs_build.fetch_inventories import fetch_inventories
from docs.exts.docs_build.github_action_utils import with_group
from docs.exts.docs_build.package_filter import process_package_filters
from docs.exts.docs_build.spelling_checks import SpellingError, display_spelling_error_summary

TEXT_RED = "\033[31m"
TEXT_RESET = "\033[0m"

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        "To run this script, run the ./build_docs.py command"
    )

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


def partition(pred: Callable[[T], bool], iterable: Iterable[T]) -> tuple[Iterable[T], Iterable[T]]:
    """Use a predicate to partition entries into false entries and true entries"""
    iter_1, iter_2 = tee(iterable)
    return filterfalse(pred, iter_1), filter(pred, iter_2)


def _promote_new_flags():
    console.print()
    console.print("[yellow]Still tired of waiting for documentation to be built?[/]")
    console.print()
    if ON_GITHUB_ACTIONS:
        console.print("You can quickly build documentation locally with just one command.")
        console.print("    [info]breeze build-docs[/]")
        console.print()
        console.print("[yellow]Still too slow?[/]")
        console.print()
    console.print("You can only build one documentation package:")
    console.print("    [info]breeze build-docs --package-filter <PACKAGE-NAME>[/]")
    console.print()
    console.print("This usually takes from [yellow]20 seconds[/] to [yellow]2 minutes[/].")
    console.print()
    console.print("You can also use other extra flags to iterate faster:")
    console.print("   [info]--docs-only       - Only build documentation[/]")
    console.print("   [info]--spellcheck-only - Only perform spellchecking[/]")
    console.print()
    console.print("For more info:")
    console.print("   [info]breeze build-docs --help[/]")
    console.print()


def _get_parser():
    available_packages_list = " * " + "\n * ".join(get_available_packages())
    parser = argparse.ArgumentParser(
        description="Builds documentation and runs spell checking",
        epilog=f"List of supported documentation packages:\n{available_packages_list}",
    )
    parser.formatter_class = argparse.RawTextHelpFormatter
    parser.add_argument(
        "--disable-checks", dest="disable_checks", action="store_true", help="Disables extra checks"
    )
    parser.add_argument(
        "--disable-provider-checks",
        dest="disable_provider_checks",
        action="store_true",
        help="Disables extra checks for providers",
    )
    parser.add_argument(
        "--one-pass-only",
        dest="one_pass_only",
        action="store_true",
        help="Do not attempt multiple builds on error",
    )
    parser.add_argument(
        "--package-filter",
        action="append",
        help=(
            "Filter specifying for which packages the documentation is to be built. Wildcard are supported."
        ),
    )
    parser.add_argument("--docs-only", dest="docs_only", action="store_true", help="Only build documentation")
    parser.add_argument(
        "--spellcheck-only", dest="spellcheck_only", action="store_true", help="Only perform spellchecking"
    )
    parser.add_argument(
        "--for-production",
        dest="for_production",
        action="store_true",
        help="Builds documentation for official release i.e. all links point to stable version",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        dest="jobs",
        type=int,
        default=0,
        help=(
            """\
        Number of parallel processes that will be spawned to build the docs.

        If passed 0, the value will be determined based on the number of CPUs.
        """
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        help=(
            "Increases the verbosity of the script i.e. always displays a full log of "
            "the build process, not just when it encounters errors"
        ),
    )

    return parser


class BuildSpecification(NamedTuple):
    """Specification of single build."""

    package_name: str
    for_production: bool
    verbose: bool


class BuildDocsResult(NamedTuple):
    """Result of building documentation."""

    package_name: str
    log_file_name: str
    errors: list[DocBuildError]


class SpellCheckResult(NamedTuple):
    """Result of spellcheck."""

    package_name: str
    log_file_name: str
    errors: list[SpellingError]


def perform_docs_build_for_single_package(build_specification: BuildSpecification) -> BuildDocsResult:
    """Performs single package docs build."""
    builder = AirflowDocsBuilder(
        package_name=build_specification.package_name, for_production=build_specification.for_production
    )
    console.print(f"[info]{build_specification.package_name:60}:[/] Building documentation")
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
    builder = AirflowDocsBuilder(
        package_name=build_specification.package_name, for_production=build_specification.for_production
    )
    console.print(f"[info]{build_specification.package_name:60}:[/] Checking spelling started")
    result = SpellCheckResult(
        package_name=build_specification.package_name,
        errors=builder.check_spelling(
            verbose=build_specification.verbose,
        ),
        log_file_name=builder.log_spelling_filename,
    )
    console.print(f"[info]{build_specification.package_name:60}:[/] Checking spelling completed")
    return result


def build_docs_for_packages(
    current_packages: list[str],
    docs_only: bool,
    spellcheck_only: bool,
    for_production: bool,
    jobs: int,
    verbose: bool,
) -> tuple[dict[str, list[DocBuildError]], dict[str, list[SpellingError]]]:
    """Builds documentation for all packages and combines errors."""
    all_build_errors: dict[str, list[DocBuildError]] = defaultdict(list)
    all_spelling_errors: dict[str, list[SpellingError]] = defaultdict(list)
    with with_group("Cleaning documentation files"):
        for package_name in current_packages:
            console.print(f"[info]{package_name:60}:[/] Cleaning files")
            builder = AirflowDocsBuilder(package_name=package_name, for_production=for_production)
            builder.clean_files()
    if jobs > 1:
        run_in_parallel(
            all_build_errors,
            all_spelling_errors,
            current_packages,
            docs_only,
            for_production,
            jobs,
            spellcheck_only,
            verbose,
        )
    else:
        run_sequentially(
            all_build_errors,
            all_spelling_errors,
            current_packages,
            docs_only,
            for_production,
            spellcheck_only,
            verbose,
        )
    return all_build_errors, all_spelling_errors


def run_sequentially(
    all_build_errors,
    all_spelling_errors,
    current_packages,
    docs_only,
    for_production,
    spellcheck_only,
    verbose,
):
    """Run both - spellcheck and docs build sequentially without multiprocessing"""
    if not spellcheck_only:
        for package_name in current_packages:
            build_result = perform_docs_build_for_single_package(
                build_specification=BuildSpecification(
                    package_name=package_name,
                    for_production=for_production,
                    verbose=verbose,
                )
            )
            if build_result.errors:
                all_build_errors[package_name].extend(build_result.errors)
                print_build_output(build_result)
    if not docs_only:
        for package_name in current_packages:
            spellcheck_result = perform_spell_check_for_single_package(
                build_specification=BuildSpecification(
                    package_name=package_name,
                    for_production=for_production,
                    verbose=verbose,
                )
            )
            if spellcheck_result.errors:
                all_spelling_errors[package_name].extend(spellcheck_result.errors)
                print_spelling_output(spellcheck_result)


def run_in_parallel(
    all_build_errors,
    all_spelling_errors,
    current_packages,
    docs_only,
    for_production,
    jobs,
    spellcheck_only,
    verbose,
):
    """Run both - spellcheck and docs build sequentially without multiprocessing"""
    with multiprocessing.Pool(processes=jobs) as pool:
        if not spellcheck_only:
            run_docs_build_in_parallel(
                all_build_errors=all_build_errors,
                for_production=for_production,
                current_packages=current_packages,
                verbose=verbose,
                pool=pool,
            )
        if not docs_only:
            run_spell_check_in_parallel(
                all_spelling_errors=all_spelling_errors,
                for_production=for_production,
                current_packages=current_packages,
                verbose=verbose,
                pool=pool,
            )


def print_build_output(result: BuildDocsResult):
    """Prints output of docs build job."""
    with with_group(f"{TEXT_RED}Output for documentation build {result.package_name}{TEXT_RESET}"):
        console.print()
        console.print(f"[info]{result.package_name:60}: " + "#" * 80)
        with open(result.log_file_name) as output:
            for line in output.read().splitlines():
                console.print(f"{result.package_name:60} {line}")
        console.print(f"[info]{result.package_name:60}: " + "#" * 80)


def run_docs_build_in_parallel(
    all_build_errors: dict[str, list[DocBuildError]],
    for_production: bool,
    current_packages: list[str],
    verbose: bool,
    pool,
):
    """Runs documentation building in parallel."""
    doc_build_specifications: list[BuildSpecification] = []
    with with_group("Scheduling documentation to build"):
        for package_name in current_packages:
            console.print(f"[info]{package_name:60}:[/] Scheduling documentation to build")
            doc_build_specifications.append(
                BuildSpecification(
                    package_name=package_name,
                    for_production=for_production,
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
        console.print(f"[info]{result.package_name:60}: " + "#" * 80)
        with open(result.log_file_name) as output:
            for line in output.read().splitlines():
                console.print(f"{result.package_name:60} {line}")
        console.print(f"[info]{result.package_name:60}: " + "#" * 80)
        console.print()


def run_spell_check_in_parallel(
    all_spelling_errors: dict[str, list[SpellingError]],
    for_production: bool,
    current_packages: list[str],
    verbose: bool,
    pool,
):
    """Runs spell check in parallel."""
    spell_check_specifications: list[BuildSpecification] = []
    with with_group("Scheduling spell checking of documentation"):
        for package_name in current_packages:
            console.print(f"[info]{package_name:60}:[/] Scheduling spellchecking")
            spell_check_specifications.append(
                BuildSpecification(package_name=package_name, for_production=for_production, verbose=verbose)
            )
    with with_group("Running spell checking of documentation"):
        console.print()
        result_list = pool.map(perform_spell_check_for_single_package, spell_check_specifications)
    for result in result_list:
        if result.errors:
            all_spelling_errors[result.package_name].extend(result.errors)
            print_spelling_output(result)


def display_packages_summary(
    build_errors: dict[str, list[DocBuildError]], spelling_errors: dict[str, list[SpellingError]]
):
    """Displays a summary that contains information on the number of errors in each packages"""
    packages_names = {*build_errors.keys(), *spelling_errors.keys()}
    tabular_data = [
        {
            "Package name": f"[info]{package_name}[/]",
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
) -> None:
    """Prints build errors and exists."""
    if build_errors or spelling_errors:
        if build_errors:
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


def main():
    """Main code"""
    args = _get_parser().parse_args()
    available_packages = get_available_packages()
    docs_only = args.docs_only
    spellcheck_only = args.spellcheck_only
    disable_provider_checks = args.disable_provider_checks
    disable_checks = args.disable_checks
    package_filters = args.package_filter
    for_production = args.for_production

    with with_group("Available packages"):
        for pkg in sorted(available_packages):
            console.print(f" - {pkg}")

    if package_filters:
        console.print("Current package filters: ", package_filters)
    current_packages = process_package_filters(available_packages, package_filters)

    with with_group("Fetching inventories"):
        # Inventories that could not be retrieved should be built first. This may mean this is a
        # new package.
        packages_without_inventories = fetch_inventories()
    normal_packages, priority_packages = partition(
        lambda d: d in packages_without_inventories, current_packages
    )
    normal_packages, priority_packages = list(normal_packages), list(priority_packages)
    jobs = args.jobs if args.jobs != 0 else os.cpu_count()

    with with_group(
        f"Documentation will be built for {len(current_packages)} package(s) with {jobs} parallel jobs"
    ):
        for pkg_no, pkg in enumerate(current_packages, start=1):
            console.print(f"{pkg_no}. {pkg}")

    all_build_errors: dict[str | None, list[DocBuildError]] = {}
    all_spelling_errors: dict[str | None, list[SpellingError]] = {}
    if priority_packages:
        # Build priority packages
        package_build_errors, package_spelling_errors = build_docs_for_packages(
            current_packages=priority_packages,
            docs_only=docs_only,
            spellcheck_only=spellcheck_only,
            for_production=for_production,
            jobs=jobs,
            verbose=args.verbose,
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
        current_packages=current_packages if len(priority_packages) > 1 else normal_packages,
        docs_only=docs_only,
        spellcheck_only=spellcheck_only,
        for_production=for_production,
        jobs=jobs,
        verbose=args.verbose,
    )
    if package_build_errors:
        all_build_errors.update(package_build_errors)
    if package_spelling_errors:
        all_spelling_errors.update(package_spelling_errors)

    if not args.one_pass_only:
        # Build documentation for some packages again if it can help them.
        package_build_errors, package_spelling_errors = retry_building_docs_if_needed(
            all_build_errors,
            all_spelling_errors,
            args,
            docs_only,
            for_production,
            jobs,
            package_build_errors,
            package_spelling_errors,
            spellcheck_only,
        )

        # And try again in case one change spans across three-level dependencies
        retry_building_docs_if_needed(
            all_build_errors,
            all_spelling_errors,
            args,
            docs_only,
            for_production,
            jobs,
            package_build_errors,
            package_spelling_errors,
            spellcheck_only,
        )

    if not disable_checks:
        general_errors = lint_checks.run_all_check(disable_provider_checks=disable_provider_checks)
        if general_errors:
            all_build_errors[None] = general_errors

    dev_index_generator.generate_index(f"{DOCS_DIR}/_build/index.html")

    if not package_filters:
        _promote_new_flags()

    if os.path.exists(PROVIDER_INIT_FILE):
        os.remove(PROVIDER_INIT_FILE)

    print_build_errors_and_exit(
        all_build_errors,
        all_spelling_errors,
    )


def retry_building_docs_if_needed(
    all_build_errors,
    all_spelling_errors,
    args,
    docs_only,
    for_production,
    jobs,
    package_build_errors,
    package_spelling_errors,
    spellcheck_only,
):
    to_retry_packages = [
        package_name
        for package_name, errors in package_build_errors.items()
        if any(any((m in e.message) for m in ERRORS_ELIGIBLE_TO_REBUILD) for e in errors)
    ]
    if to_retry_packages:
        for package_name in to_retry_packages:
            if package_name in all_build_errors:
                del all_build_errors[package_name]
            if package_name in all_spelling_errors:
                del all_spelling_errors[package_name]

        package_build_errors, package_spelling_errors = build_docs_for_packages(
            current_packages=to_retry_packages,
            docs_only=docs_only,
            spellcheck_only=spellcheck_only,
            for_production=for_production,
            jobs=jobs,
            verbose=args.verbose,
        )
        if package_build_errors:
            all_build_errors.update(package_build_errors)
        if package_spelling_errors:
            all_spelling_errors.update(package_spelling_errors)
        return package_build_errors, package_spelling_errors
    return package_build_errors, package_spelling_errors


if __name__ == "__main__":
    main()
