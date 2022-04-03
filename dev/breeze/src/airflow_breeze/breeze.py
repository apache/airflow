#!/usr/bin/env python3
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
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

try:
    import rich_click as click
except ImportError:
    # We handle import errors so that click autocomplete works
    import click  # type: ignore[no-redef]

from click import ClickException

from airflow_breeze.cache import delete_cache, touch_cache_file, write_to_cache_file
from airflow_breeze.ci.build_image import build_image
from airflow_breeze.ci.build_params import BuildParams
from airflow_breeze.console import console
from airflow_breeze.docs_generator import build_documentation
from airflow_breeze.docs_generator.doc_builder import DocBuilder
from airflow_breeze.global_constants import (
    ALLOWED_BACKENDS,
    ALLOWED_DEBIAN_VERSIONS,
    ALLOWED_EXECUTORS,
    ALLOWED_INSTALL_AIRFLOW_VERSIONS,
    ALLOWED_INTEGRATIONS,
    ALLOWED_MSSQL_VERSIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    ALLOWED_USE_AIRFLOW_VERSIONS,
    get_available_packages,
)
from airflow_breeze.pre_commit_ids import PRE_COMMIT_LIST
from airflow_breeze.prod.build_prod_image import build_production_image
from airflow_breeze.shell.enter_shell import build_shell
from airflow_breeze.utils.docker_command_utils import check_docker_resources
from airflow_breeze.utils.path_utils import (
    __AIRFLOW_SOURCES_ROOT,
    create_directories,
    find_airflow_sources_root,
    get_airflow_sources_root,
)
from airflow_breeze.utils.run_utils import check_package_installed, run_command
from airflow_breeze.visuals import ASCIIART, ASCIIART_STYLE

AIRFLOW_SOURCES_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent.absolute()

NAME = "Breeze2"
VERSION = "0.0.1"


@click.group()
def main():
    find_airflow_sources_root()


option_verbose = click.option(
    "-v", "--verbose", is_flag=True, help="Print verbose information about performed steps", envvar='VERBOSE'
)

option_python_version = click.option(
    '-p',
    '--python',
    type=click.Choice(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    help='Choose your python version',
    envvar='PYTHON_MAJOR_MINOR_VERSION',
)

option_backend = click.option(
    '-b',
    '--backend',
    type=click.Choice(ALLOWED_BACKENDS),
    help='Choose your backend database',
)

option_github_repository = click.option(
    '-g',
    '--github-repository',
    help='GitHub repository used to pull, push images. Default: apache/airflow.',
    envvar='GITHUB_REPOSITORY',
)

option_github_image_id = click.option(
    '-s',
    '--github-image-id',
    help='Commit SHA of the image. \
    Breeze can automatically pull the commit SHA id specified Default: latest',
)

option_image_tag = click.option('--image-tag', help='Additional tag in the image.')

option_platform = click.option(
    '--platform', help='Builds image for the platform specified.', envvar='PLATFORM'
)

option_debian_version = click.option(
    '-d',
    '--debian-version',
    help='Debian version used for the image.',
    type=click.Choice(ALLOWED_DEBIAN_VERSIONS),
    envvar='DEBIAN_VERSION',
)
option_upgrade_to_newer_dependencies = click.option(
    '--upgrade-to-newer-dependencies',
    help='Upgrades PIP packages to latest versions available without looking at the constraints.',
    envvar='UPGRADE_TO_NEWER_DEPENDENCIES',
)
option_additional_extras = click.option(
    '--additional-extras',
    help='This installs additional extra package while installing airflow in the image.',
    envvar='ADDITIONAL_AIRFLOW_EXTRAS',
)
option_additional_dev_apt_deps = click.option(
    '--additional-dev-apt-deps',
    help='Additional apt dev dependencies to use when building the images.',
    envvar='ADDITIONAL_DEV_APT_DEPS',
)
option_additional_runtime_apt_deps = click.option(
    '--additional-runtime-apt-deps',
    help='Additional apt runtime dependencies to use when building the images.',
    envvar='ADDITIONAL_RUNTIME_APT_DEPS',
)
option_additional_python_deps = click.option(
    '--additional-python-deps',
    help='Additional python dependencies to use when building the images.',
    envvar='ADDITIONAL_PYTHON_DEPS',
)
option_additional_dev_apt_command = click.option(
    '--additional-dev-apt-command',
    help='Additional command executed before dev apt deps are installed.',
    envvar='ADDITIONAL_DEV_APT_COMMAND',
)
option_additional_runtime_apt_command = click.option(
    '--additional-runtime-apt-command',
    help='Additional command executed before runtime apt deps are installed.',
    envvar='ADDITIONAL_RUNTIME_APT_COMMAND',
)
option_additional_dev_apt_env = click.option(
    '--additional-dev-apt-env',
    help='Additional environment variables set when adding dev dependencies.',
    envvar='ADDITIONAL_DEV_APT_ENV',
)
option_additional_runtime_apt_env = click.option(
    '--additional-runtime-apt-env',
    help='Additional environment variables set when adding runtime dependencies.',
    envvar='ADDITIONAL_RUNTIME_APT_ENV',
)
option_dev_apt_command = click.option(
    '--dev-apt-command',
    help='The basic command executed before dev apt deps are installed.',
    envvar='DEV_APT_COMMAND',
)
option_dev_apt_deps = click.option(
    '--dev-apt-deps',
    help='The basic apt dev dependencies to use when building the images.',
    envvar='DEV_APT_DEPS',
)
option_runtime_apt_command = click.option(
    '--runtime-apt-command',
    help='The basic command executed before runtime apt deps are installed.',
    envvar='RUNTIME_APT_COMMAND',
)
option_runtime_apt_deps = click.option(
    '--runtime-apt-deps',
    help='The basic apt runtime dependencies to use when building the images.',
    envvar='RUNTIME_APT_DEPS',
)
option_ci_flag = click.option(
    '--ci',
    help='Enabling this option will off the pip progress bar',
    is_flag=True,
    envvar='CI',
)


@main.command()
def version():
    """Prints version of breeze.py."""
    console.print(ASCIIART, style=ASCIIART_STYLE)
    console.print(f"\n[green]{NAME} version: {VERSION}[/]\n")


@option_verbose
@main.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_python_version
@option_backend
@click.option('--integration', type=click.Choice(ALLOWED_INTEGRATIONS), multiple=True)
@click.option('-L', '--build-cache-local', is_flag=True)
@click.option('-U', '--build-cache-pulled', is_flag=True)
@click.option('-X', '--build-cache-disabled', is_flag=True)
@click.option('--postgres-version', type=click.Choice(ALLOWED_POSTGRES_VERSIONS))
@click.option('--mysql-version', type=click.Choice(ALLOWED_MYSQL_VERSIONS))
@click.option('--mssql-version', type=click.Choice(ALLOWED_MSSQL_VERSIONS))
@click.option(
    '--executor',
    type=click.Choice(ALLOWED_EXECUTORS),
    help='Executor to use in a kubernetes cluster. Default is KubernetesExecutor',
)
@click.option('-f', '--forward-credentials', is_flag=True)
@click.option('-l', '--skip-mounting-local-sources', is_flag=True)
@click.option('--use-airflow-version', type=click.Choice(ALLOWED_INSTALL_AIRFLOW_VERSIONS))
@click.option('--use-packages-from-dist', is_flag=True)
@click.option('--force-build', is_flag=True)
@click.argument('extra-args', nargs=-1, type=click.UNPROCESSED)
def shell(
    verbose: bool,
    python: str,
    backend: str,
    integration: Tuple[str],
    build_cache_local: bool,
    build_cache_pulled: bool,
    build_cache_disabled: bool,
    postgres_version: str,
    mysql_version: str,
    mssql_version: str,
    executor: str,
    forward_credentials: bool,
    skip_mounting_local_sources: bool,
    use_airflow_version: str,
    use_packages_from_dist: bool,
    force_build: bool,
    extra_args: Tuple,
):
    """Enters breeze.py environment. this is the default command use when no other is selected."""

    if verbose:
        console.print("\n[green]Welcome to breeze.py[/]\n")
        console.print(f"\n[green]Root of Airflow Sources = {__AIRFLOW_SOURCES_ROOT}[/]\n")
    build_shell(
        verbose,
        python_version=python,
        backend=backend,
        integration=integration,
        build_cache_local=build_cache_local,
        build_cache_disabled=build_cache_disabled,
        build_cache_pulled=build_cache_pulled,
        postgres_version=postgres_version,
        mysql_version=mysql_version,
        mssql_version=mssql_version,
        executor=executor,
        forward_credentials=str(forward_credentials),
        skip_mounting_local_sources=skip_mounting_local_sources,
        use_airflow_version=use_airflow_version,
        use_packages_from_dist=use_packages_from_dist,
        force_build=force_build,
        extra_args=extra_args,
    )


@option_verbose
@main.command(name='build-ci-image')
@option_additional_extras
@option_python_version
@option_additional_dev_apt_deps
@option_additional_runtime_apt_deps
@option_additional_python_deps
@option_additional_dev_apt_command
@option_runtime_apt_command
@option_additional_dev_apt_env
@option_additional_runtime_apt_env
@option_additional_runtime_apt_command
@option_dev_apt_command
@option_dev_apt_deps
@option_runtime_apt_command
@option_runtime_apt_deps
@option_github_repository
@click.option('--build-cache', help='Cache option')
@option_platform
@option_debian_version
@click.option('--prepare-buildx-cache', is_flag=True)
@option_ci_flag
@option_upgrade_to_newer_dependencies
def build_ci_image(
    verbose: bool,
    additional_extras: Optional[str],
    python: str,
    additional_dev_apt_deps: Optional[str],
    additional_runtime_apt_deps: Optional[str],
    additional_python_deps: Optional[str],
    additional_dev_apt_command: Optional[str],
    additional_runtime_apt_command: Optional[str],
    additional_dev_apt_env: Optional[str],
    additional_runtime_apt_env: Optional[str],
    dev_apt_command: Optional[str],
    dev_apt_deps: Optional[str],
    runtime_apt_command: Optional[str],
    runtime_apt_deps: Optional[str],
    github_repository: Optional[str],
    build_cache: Optional[str],
    platform: Optional[str],
    debian_version: Optional[str],
    prepare_buildx_cache: bool,
    ci: bool,
    upgrade_to_newer_dependencies: str = "false",
):
    """Builds docker CI image without entering the container."""

    if verbose:
        console.print(
            f"\n[blue]Building image of airflow from {__AIRFLOW_SOURCES_ROOT} "
            f"python version: {python}[/]\n"
        )
    create_directories()
    build_image(
        verbose,
        additional_extras=additional_extras,
        python_version=python,
        additional_dev_apt_deps=additional_dev_apt_deps,
        additional_runtime_apt_deps=additional_runtime_apt_deps,
        additional_python_deps=additional_python_deps,
        additional_runtime_apt_command=additional_runtime_apt_command,
        additional_dev_apt_command=additional_dev_apt_command,
        additional_dev_apt_env=additional_dev_apt_env,
        additional_runtime_apt_env=additional_runtime_apt_env,
        dev_apt_command=dev_apt_command,
        dev_apt_deps=dev_apt_deps,
        runtime_apt_command=runtime_apt_command,
        runtime_apt_deps=runtime_apt_deps,
        github_repository=github_repository,
        docker_cache=build_cache,
        platform=platform,
        debian_version=debian_version,
        prepare_buildx_cache=prepare_buildx_cache,
        ci=ci,
        upgrade_to_newer_dependencies=upgrade_to_newer_dependencies,
    )


@option_verbose
@main.command(name='build-prod-image')
@click.option(
    '--cleanup-docker-context-files', help='Preserves data volumes when stopping airflow.', is_flag=True
)
@click.option('--disable-mysql-client-installation', is_flag=True)
@click.option('--disable-mssql-client-installation', is_flag=True)
@click.option('--disable-postgres-client-installation', is_flag=True)
@click.option('--disable-pip-cache', is_flag=True)
@click.option('-t', '--install-airflow-reference')
@click.option('-a', '--install-airflow-version', type=click.Choice(ALLOWED_INSTALL_AIRFLOW_VERSIONS))
@click.option('-r', '--skip-rebuild-check', is_flag=True)
@click.option('-L', '--build-cache-local', is_flag=True)
@click.option('-U', '--build-cache-pulled', is_flag=True)
@click.option('-X', '--build-cache-disabled', is_flag=True)
@option_additional_extras
@option_python_version
@option_additional_dev_apt_deps
@option_additional_runtime_apt_deps
@option_additional_python_deps
@option_additional_dev_apt_command
@option_runtime_apt_command
@option_additional_dev_apt_env
@option_additional_runtime_apt_env
@option_additional_runtime_apt_command
@option_dev_apt_command
@option_dev_apt_deps
@option_runtime_apt_command
@option_runtime_apt_deps
@option_github_repository
@option_platform
@option_debian_version
@option_upgrade_to_newer_dependencies
@click.option('--prepare-buildx-cache', is_flag=True)
@click.option('--skip-installing-airflow-providers-from-sources', is_flag=True)
@click.option('--disable-pypi-when-building', is_flag=True)
@click.option('-E', '--extras')
@click.option('--installation-method', type=click.Choice(ALLOWED_USE_AIRFLOW_VERSIONS))
@click.option(
    '--install-from-docker-context-files',
    help='Install wheels from local docker-context-files when building image',
    is_flag=True,
)
@option_image_tag
@click.option('--github-token', envvar='GITHUB_TOKEN')
@option_ci_flag
def build_prod_image(
    verbose: bool,
    cleanup_docker_context_files: bool,
    disable_mysql_client_installation: bool,
    disable_mssql_client_installation: bool,
    disable_postgres_client_installation: bool,
    disable_pip_cache: bool,
    install_airflow_reference: Optional[str],
    install_airflow_version: Optional[str],
    skip_rebuild_check: bool,
    build_cache_local: bool,
    build_cache_pulled: bool,
    build_cache_disabled: bool,
    additional_extras: Optional[str],
    python: str,
    additional_dev_apt_deps: Optional[str],
    additional_runtime_apt_deps: Optional[str],
    additional_python_deps: Optional[str],
    additional_dev_apt_command: Optional[str],
    additional_runtime_apt_command: Optional[str],
    additional_dev_apt_env: Optional[str],
    additional_runtime_apt_env: Optional[str],
    dev_apt_command: Optional[str],
    dev_apt_deps: Optional[str],
    runtime_apt_command: Optional[str],
    runtime_apt_deps: Optional[str],
    github_repository: Optional[str],
    platform: Optional[str],
    debian_version: Optional[str],
    prepare_buildx_cache: bool,
    skip_installing_airflow_providers_from_sources: bool,
    disable_pypi_when_building: bool,
    extras: Optional[str],
    installation_method: Optional[str],
    install_from_docker_context_files: bool,
    image_tag: Optional[str],
    github_token: Optional[str],
    ci: bool,
    upgrade_to_newer_dependencies: str = "false",
):
    """Builds docker Production image without entering the container."""
    if verbose:
        console.print("\n[blue]Building image[/]\n")
    if prepare_buildx_cache:
        build_cache_pulled = True
        cleanup_docker_context_files = True
    build_production_image(
        verbose,
        cleanup_docker_context_files=cleanup_docker_context_files,
        disable_mysql_client_installation=disable_mysql_client_installation,
        disable_mssql_client_installation=disable_mssql_client_installation,
        disable_postgres_client_installation=disable_postgres_client_installation,
        disable_pip_cache=disable_pip_cache,
        install_airflow_reference=install_airflow_reference,
        install_airflow_version=install_airflow_version,
        skip_rebuild_check=skip_rebuild_check,
        build_cache_local=build_cache_local,
        build_cache_pulled=build_cache_pulled,
        build_cache_disabled=build_cache_disabled,
        additional_extras=additional_extras,
        python_version=python,
        additional_dev_apt_deps=additional_dev_apt_deps,
        additional_runtime_apt_deps=additional_runtime_apt_deps,
        additional_python_deps=additional_python_deps,
        additional_runtime_apt_command=additional_runtime_apt_command,
        additional_dev_apt_command=additional_dev_apt_command,
        additional_dev_apt_env=additional_dev_apt_env,
        additional_runtime_apt_env=additional_runtime_apt_env,
        dev_apt_command=dev_apt_command,
        dev_apt_deps=dev_apt_deps,
        runtime_apt_command=runtime_apt_command,
        runtime_apt_deps=runtime_apt_deps,
        github_repository=github_repository,
        platform=platform,
        debian_version=debian_version,
        upgrade_to_newer_dependencies=upgrade_to_newer_dependencies,
        prepare_buildx_cache=prepare_buildx_cache,
        skip_installing_airflow_providers_from_sources=skip_installing_airflow_providers_from_sources,
        disable_pypi_when_building=disable_pypi_when_building,
        extras=extras,
        installation_method=installation_method,
        install_docker_context_files=install_from_docker_context_files,
        image_tag=image_tag,
        github_token=github_token,
        ci=ci,
    )


@option_verbose
@main.command(name='start-airflow')
def start_airflow(verbose: bool):
    """Enters breeze.py environment and set up the tmux session"""
    if verbose:
        console.print("\n[green]Welcome to breeze.py[/]\n")
    console.print(ASCIIART, style=ASCIIART_STYLE)
    raise ClickException("\nPlease implement entering breeze.py\n")


BREEZE_COMMENT = "Added by Updated Airflow Breeze autocomplete setup"
START_LINE = f"# START: {BREEZE_COMMENT}\n"
END_LINE = f"# END: {BREEZE_COMMENT}\n"


def remove_autogenerated_code(script_path: str):
    lines = Path(script_path).read_text().splitlines(keepends=True)
    new_lines = []
    pass_through = True
    for line in lines:
        if line == START_LINE:
            pass_through = False
            continue
        if line.startswith(END_LINE):
            pass_through = True
            continue
        if pass_through:
            new_lines.append(line)
    Path(script_path).write_text("".join(new_lines))


def backup(script_path_file: Path):
    shutil.copy(str(script_path_file), str(script_path_file) + ".bak")


def write_to_shell(command_to_execute: str, script_path: str, force_setup: bool) -> bool:
    skip_check = False
    script_path_file = Path(script_path)
    if not script_path_file.exists():
        skip_check = True
    if not skip_check:
        if BREEZE_COMMENT in script_path_file.read_text():
            if not force_setup:
                console.print(
                    "\n[yellow]Autocompletion is already setup. Skipping. "
                    "You can force autocomplete installation by adding --force-setup[/]\n"
                )
                return False
            else:
                backup(script_path_file)
                remove_autogenerated_code(script_path)
    console.print(f"\nModifying the {script_path} file!\n")
    console.print(f"\nCopy of the file is held in {script_path}.bak !\n")
    backup(script_path_file)
    text = script_path_file.read_text()
    script_path_file.write_text(
        text + ("\n" if not text.endswith("\n") else "") + START_LINE + command_to_execute + "\n" + END_LINE
    )
    console.print(
        "\n[yellow]Please exit and re-enter your shell or run:[/]\n\n" f"   `source {script_path}`\n"
    )
    return True


@option_verbose
@click.option(
    '-f',
    '--force-setup',
    is_flag=True,
    help='Force autocomplete setup even if already setup before (overrides the setup).',
)
@main.command(name='setup-autocomplete')
def setup_autocomplete(verbose: bool, force_setup: bool):
    """
    Enables autocompletion of Breeze2 commands.
    """

    # Determine if the shell is bash/zsh/powershell. It helps to build the autocomplete path
    detected_shell = os.environ.get('SHELL')
    detected_shell = None if detected_shell is None else detected_shell.split(os.sep)[-1]
    if detected_shell not in ['bash', 'zsh', 'fish']:
        console.print(f"\n[red] The shell {detected_shell} is not supported for autocomplete![/]\n")
        sys.exit(1)
    console.print(f"Installing {detected_shell} completion for local user")
    autocomplete_path = (
        Path(AIRFLOW_SOURCES_DIR) / "dev" / "breeze" / "autocomplete" / f"{NAME}-complete-{detected_shell}.sh"
    )
    console.print(f"[bright_blue]Activation command script is available here: {autocomplete_path}[/]\n")
    console.print(
        f"[yellow]We need to add above script to your {detected_shell} profile and "
        "install 'click' package in your default python installation destination.[/]\n"
    )
    updated = False
    if click.confirm("Should we proceed ?"):
        if detected_shell == 'bash':
            script_path = str(Path('~').expanduser() / '.bash_completion')
            command_to_execute = f"source {autocomplete_path}"
            updated = write_to_shell(command_to_execute, script_path, force_setup)
        elif detected_shell == 'zsh':
            script_path = str(Path('~').expanduser() / '.zshrc')
            command_to_execute = f"source {autocomplete_path}"
            updated = write_to_shell(command_to_execute, script_path, force_setup)
        elif detected_shell == 'fish':
            # Include steps for fish shell
            script_path = str(Path('~').expanduser() / f'.config/fish/completions/{NAME}.fish')
            if os.path.exists(script_path) and not force_setup:
                console.print(
                    "\n[yellow]Autocompletion is already setup. Skipping. "
                    "You can force autocomplete installation by adding --force-setup[/]\n"
                )
            else:
                with open(autocomplete_path) as source_file, open(script_path, 'w') as destination_file:
                    for line in source_file:
                        destination_file.write(line)
                updated = True
        else:
            # Include steps for powershell
            subprocess.check_call(['powershell', 'Set-ExecutionPolicy Unrestricted -Scope CurrentUser'])
            script_path = (
                subprocess.check_output(['powershell', '-NoProfile', 'echo $profile']).decode("utf-8").strip()
            )
            command_to_execute = f". {autocomplete_path}"
            write_to_shell(command_to_execute, script_path, force_setup)
        if updated:
            run_command(['pip', 'install', '--upgrade', 'click'], verbose=True, check=False)
    else:
        console.print(
            "\nPlease follow the https://click.palletsprojects.com/en/8.1.x/shell-completion/ "
            "to setup autocompletion for breeze manually if you want to use it.\n"
        )


@main.command(name='config')
@option_python_version
@option_backend
@click.option('--cheatsheet/--no-cheatsheet', default=None)
@click.option('--asciiart/--no-asciiart', default=None)
def change_config(python, backend, cheatsheet, asciiart):
    """
    Toggles on/off cheatsheet, asciiart
    """
    if asciiart:
        console.print('[blue] ASCIIART enabled')
        delete_cache('suppress_asciiart')
    elif asciiart is not None:
        touch_cache_file('suppress_asciiart')
    else:
        pass
    if cheatsheet:
        console.print('[blue] Cheatsheet enabled')
        delete_cache('suppress_cheatsheet')
    elif cheatsheet is not None:
        touch_cache_file('suppress_cheatsheet')
    else:
        pass
    if python is not None:
        write_to_cache_file('PYTHON_MAJOR_MINOR_VERSION', python)
        console.print(f'[blue]Python cached_value {python}')
    if backend is not None:
        write_to_cache_file('BACKEND', backend)
        console.print(f'[blue]Backend cached_value {backend}')


@option_verbose
@main.command(name='build-docs')
@click.option('--docs-only', is_flag=True)
@click.option('--spellcheck-only', is_flag=True)
@click.option('--package-filter', type=click.Choice(get_available_packages()), multiple=True)
def build_docs(verbose: bool, docs_only: bool, spellcheck_only: bool, package_filter: Tuple[str]):
    """
    Builds documentation in the container
    """
    params = BuildParams()
    airflow_sources = str(get_airflow_sources_root())
    ci_image_name = params.airflow_ci_image_name
    check_docker_resources(verbose, airflow_sources, ci_image_name)
    doc_builder = DocBuilder(
        package_filter=package_filter, docs_only=docs_only, spellcheck_only=spellcheck_only
    )
    build_documentation.build(verbose, airflow_sources, ci_image_name, doc_builder)


@option_verbose
@main.command(
    name="static-check",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option('--all-files', is_flag=True)
@click.option('--show-diff-on-failure', is_flag=True)
@click.option('--last-commit', is_flag=True)
@click.option('-t', '--type', type=click.Choice(PRE_COMMIT_LIST), multiple=True)
@click.option('--files', is_flag=True)
@click.argument('precommit_args', nargs=-1, type=click.UNPROCESSED)
def static_check(
    verbose: bool,
    all_files: bool,
    show_diff_on_failure: bool,
    last_commit: bool,
    type: Tuple[str],
    files: bool,
    precommit_args: Tuple,
):
    if check_package_installed('pre_commit'):
        command_to_execute = ['pre-commit', 'run']
        for single_check in type:
            command_to_execute.append(single_check)
        if all_files:
            command_to_execute.append("--all-files")
        if show_diff_on_failure:
            command_to_execute.append("--show-diff-on-failure")
        if last_commit:
            command_to_execute.extend(["--from-ref", "HEAD^", "--to-ref", "HEAD"])
        if files:
            command_to_execute.append("--files")
        if precommit_args:
            command_to_execute.extend(precommit_args)
        run_command(command_to_execute, suppress_raise_exception=True, suppress_console_print=True, text=True)


if __name__ == '__main__':
    create_directories()
    main()
