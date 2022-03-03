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
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

import click_completion
import rich_click as click
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
    get_available_packages,
)
from airflow_breeze.pre_commit_ids import PRE_COMMIT_LIST
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

AIRFLOW_SOURCES_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent

NAME = "Breeze2"
VERSION = "0.0.1"


click_completion.init()


@click.group()
def main():
    find_airflow_sources_root()


option_verbose = click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print verbose information about performed steps",
)

option_python_version = click.option(
    '-p',
    '--python',
    type=click.Choice(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    help='Choose your python version',
)

option_backend = click.option(
    '-b',
    '--backend',
    type=click.Choice(ALLOWED_BACKENDS),
    help='Choose your backend database',
)

option_github_repository = click.option(
    '-g', '--github-repository', help='GitHub repository used to pull, push images. Default: apache/airflow.'
)

option_github_image_id = click.option(
    '-s',
    '--github-image-id',
    help='Commit SHA of the image. \
    Breeze can automatically pull the commit SHA id specified Default: latest',
)

option_image_tag = click.option('--image-tag', help='Additional tag in the image.')


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
@click.option(
    '--additional-extras',
    help='This installs additional extra package while installing airflow in the image.',
)
@option_python_version
@click.option(
    '--additional-dev-apt-deps', help='Additional apt dev dependencies to use when building the images.'
)
@click.option(
    '--additional-runtime-apt-deps',
    help='Additional apt runtime dependencies to use when building the images.',
)
@click.option(
    '--additional-python-deps', help='Additional python dependencies to use when building the images.'
)
@click.option(
    '--additional_dev_apt_command', help='Additional command executed before dev apt deps are installed.'
)
@click.option(
    '--additional_runtime_apt_command',
    help='Additional command executed before runtime apt deps are installed.',
)
@click.option(
    '--additional_dev_apt_env', help='Additional environment variables set when adding dev dependencies.'
)
@click.option(
    '--additional_runtime_apt_env',
    help='Additional environment variables set when adding runtime dependencies.',
)
@click.option('--dev-apt-command', help='The basic command executed before dev apt deps are installed.')
@click.option(
    '--dev-apt-deps',
    help='The basic apt dev dependencies to use when building the images.',
)
@click.option(
    '--runtime-apt-command', help='The basic command executed before runtime apt deps are installed.'
)
@click.option(
    '--runtime-apt-deps',
    help='The basic apt runtime dependencies to use when building the images.',
)
@click.option('--github-repository', help='Choose repository to push/pull image.')
@click.option('--build-cache', help='Cache option')
@click.option('--platform', help='Builds image for the platform specified.')
@click.option(
    '-d',
    '--debian-version',
    help='Debian version used for the image.',
    type=click.Choice(ALLOWED_DEBIAN_VERSIONS),
)
@click.option('--upgrade-to-newer-dependencies', is_flag=True)
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
    upgrade_to_newer_dependencies: bool,
):
    """Builds docker CI image without entering the container."""

    if verbose:
        console.print(
            f"\n[blue]Building image of airflow from {__AIRFLOW_SOURCES_ROOT} "
            f"python version: {python}[/]\n"
        )
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
        upgrade_to_newer_dependencies=str(upgrade_to_newer_dependencies).lower(),
    )


@option_verbose
@main.command(name='build-prod-image')
def build_prod_image(verbose: bool):
    """Builds docker Production image without entering the container."""
    if verbose:
        console.print("\n[blue]Building image[/]\n")
    raise ClickException("\nPlease implement building the Production image\n")


@option_verbose
@main.command(name='start-airflow')
def start_airflow(verbose: bool):
    """Enters breeze.py environment and set up the tmux session"""
    if verbose:
        console.print("\n[green]Welcome to breeze.py[/]\n")
    console.print(ASCIIART, style=ASCIIART_STYLE)
    raise ClickException("\nPlease implement entering breeze.py\n")


def write_to_shell(command_to_execute: str, script_path: str, breeze_comment: str):
    skip_check = False
    script_path_file = Path(script_path)
    if not script_path_file.exists():
        skip_check = True
    if not skip_check:
        with open(script_path) as script_file:
            if breeze_comment in script_file.read():
                click.echo("Autocompletion is already setup. Skipping")
                click.echo(f"Please exit and re-enter your shell or run: \'source {script_path}\'")
                sys.exit()
    click.echo(f"This will modify the {script_path} file")
    with open(script_path, 'a') as script_file:
        script_file.write(f"\n# START: {breeze_comment}\n")
        script_file.write(f"{command_to_execute}\n")
        script_file.write(f"# END: {breeze_comment}\n")
        click.echo(f"Please exit and re-enter your shell or run: \'source {script_path}\'")


@main.command(name='setup-autocomplete')
def setup_autocomplete():
    """
    Enables autocompletion of Breeze2 commands.
    Functionality: By default the generated shell scripts will be available in ./dev/breeze/autocomplete/ path
    Depending on the shell type in the machine we have to link it to the corresponding file
    """
    global NAME
    breeze_comment = "Added by Updated Airflow Breeze autocomplete setup"
    # Determine if the shell is bash/zsh/powershell. It helps to build the autocomplete path
    shell = click_completion.get_auto_shell()
    click.echo(f"Installing {shell} completion for local user")
    extra_env = {'_CLICK_COMPLETION_COMMAND_CASE_INSENSITIVE_COMPLETE': 'ON'}
    autocomplete_path = Path(AIRFLOW_SOURCES_DIR) / ".build/autocomplete" / f"{NAME}-complete.{shell}"
    shell, path = click_completion.core.install(
        shell=shell, prog_name=NAME, path=autocomplete_path, append=False, extra_env=extra_env
    )
    click.echo(f"Activation command scripts are created in this autocompletion path: {autocomplete_path}")
    if click.confirm(f"Do you want to add the above autocompletion scripts to your {shell} profile?"):
        if shell == 'bash':
            script_path = Path('~').expanduser() / '.bash_completion'
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, script_path, breeze_comment)
        elif shell == 'zsh':
            script_path = Path('~').expanduser() / '.zshrc'
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, script_path, breeze_comment)
        elif shell == 'fish':
            # Include steps for fish shell
            script_path = Path('~').expanduser() / f'.config/fish/completions/{NAME}.fish'
            with open(path) as source_file, open(script_path, 'w') as destination_file:
                for line in source_file:
                    destination_file.write(line)
        else:
            # Include steps for powershell
            subprocess.check_call(['powershell', 'Set-ExecutionPolicy Unrestricted -Scope CurrentUser'])
            script_path = subprocess.check_output(['powershell', '-NoProfile', 'echo $profile']).strip()
            command_to_execute = f". {autocomplete_path}"
            write_to_shell(command_to_execute, script_path.decode("utf-8"), breeze_comment)
    else:
        click.echo(f"Link for manually adding the autocompletion script to {shell} profile")


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
