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
"""Database sub-commands"""
import os
import textwrap
from tempfile import NamedTemporaryFile

import rich_click as click
import wrapt
from packaging.version import parse as parse_version
from rich.console import Console

from airflow import settings
from airflow.cli import airflow_cmd, click_dry_run, click_verbose, click_yes
from airflow.exceptions import AirflowException
from airflow.utils import cli as cli_utils, db as db_utils
from airflow.utils.db import REVISION_HEADS_MAP
from airflow.utils.db_cleanup import config_dict, run_cleanup
from airflow.utils.process_utils import execute_interactive

click_revision = click.option(
    '-r',
    '--revision',
    default=None,
    help="(Optional) If provided, only run migrations up to and including this revision.",
)
click_version = click.option(
    '-n',
    '--version',
    default=None,
    help=(
        "(Optional) The airflow version to upgrade to. Note: must provide either "
        "`--revision` or `--version`."
    ),
)
click_from_revision = click.option(
    '--from-revision', default=None, help="(Optional) If generating sql, may supply a *from* revision"
)
click_from_version = click.option('--from-version', default=None, help="From version help text")
click_show_sql_only = click.option(
    '-s',
    '--show-sql-only',
    is_flag=True,
    help=(
        "Don't actually run migrations; just print out sql scripts for offline migration. "
        "Required if using either `--from-version` or `--from-version`."
    ),
)


@airflow_cmd.group()
@click.pass_context
def db(ctx):
    """Commands for the metadata database"""


@db.command('init')
@click.pass_context
def db_init(ctx):
    """Initializes the metadata database"""
    console = Console()
    console.print(f"DB: {settings.engine.url}")
    db_utils.initdb()
    console.print("Initialization done")


@db.command('check-migrations')
@click.pass_context
@click.option(
    '-t',
    '--migration-wait-timeout',
    default=60,
    help="This command will wait for up to this time, specified in seconds",
)
def check_migrations(ctx, migration_wait_timeout):
    """Function to wait for all airflow migrations to complete. Used for launching airflow in k8s"""
    console = Console()
    console.print(f"Waiting for {migration_wait_timeout}s")
    db_utils.check_migrations(timeout=migration_wait_timeout)


@db.command('reset')
@click.pass_context
@click_yes
def db_reset(ctx, yes=False):
    """Burn down and rebuild the metadata database"""
    console = Console()
    console.print(f"DB: {settings.engine.url}")
    if yes or click.confirm("This will drop existing tables if they exist. Proceed? (y/n)"):
        db_utils.resetdb()
    else:
        console.print("Cancelled")


@wrapt.decorator
def check_revision_and_version_options(wrapped, instance, args, kwargs):
    """A decorator that defines upgrade/downgrade option checks in a single place"""

    def wrapper(ctx, revision, version, from_revision, from_version, *_args, show_sql_only=False, **_kwargs):
        if revision is not None and version is not None:
            raise SystemExit("Cannot supply both `--revision` and `--version`.")
        if from_revision is not None and from_version is not None:
            raise SystemExit("Cannot supply both `--from-revision` and `--from-version`")
        if (from_revision is not None or from_version is not None) and not show_sql_only:
            raise SystemExit(
                "Args `--from-revision` and `--from-version` may only be used with `--show-sql-only`"
            )
        if version is None and revision is None:
            raise SystemExit("Must provide either --revision or --version.")

        if from_version is not None:
            if parse_version(from_version) < parse_version('2.0.0'):
                raise SystemExit("--from-version must be greater than or equal to 2.0.0")
            from_revision = REVISION_HEADS_MAP.get(from_version)
            if not from_revision:
                raise SystemExit(f"Unknown version {from_version!r} supplied as `--from-version`.")

        if version is not None:
            revision = REVISION_HEADS_MAP.get(version)
            if not revision:
                raise SystemExit(f"Upgrading to version {version} is not supported.")

        return wrapped(
            ctx, revision, version, from_revision, from_version, *_args, show_sql_only=False, **_kwargs
        )

    return wrapper(*args, **kwargs)


@db.command('upgrade')
@click.pass_context
@click_revision
@click_version
@click_from_revision
@click_from_version
@click_show_sql_only
@click_yes
@check_revision_and_version_options
def upgrade(ctx, revision, version, from_revision, from_version, show_sql_only=False, yes=False):
    """
    Upgrade the metadata database to latest version

    Upgrade the schema of the metadata database.
    To print but not execute commands, use option ``--show-sql-only``.
    If using options ``--from-revision`` or ``--from-version``, you must also use
    ``--show-sql-only``, because if actually *running* migrations, we should only
    migrate from the *current* revision.
    """
    console = Console()
    console.print(f"Using DB (engine: {settings.engine.url})")

    if not show_sql_only:
        console.print(f"Performing upgrade with database {settings.engine.url}")
    else:
        console.print("Generating SQL for upgrade -- upgrade commands will *not* be submitted.")

    if show_sql_only or (
        yes
        or click.confirm(
            "\nWarning: About to run schema migrations for the airflow metastore. "
            "Please ensure you have backed up your database before any migration "
            "operation. Proceed? (y/n)\n"
        )
    ):
        db_utils.upgradedb(to_revision=revision, from_revision=from_revision, show_sql_only=show_sql_only)
        if not show_sql_only:
            console.print("Upgrades done")
    else:
        SystemExit("Cancelled")


@db.command('downgrade')
@click.pass_context
@click_revision
@click_version
@click_from_revision
@click_from_version
@click_show_sql_only
@click_yes
@check_revision_and_version_options
def downgrade(ctx, revision, version, from_revision, from_version, show_sql_only=False, yes=False):
    """
    Downgrade the schema of the metadata database

    Downgrade the schema of the metadata database.
    You must provide either `--revision` or `--version`.
    To print but not execute commands, use option `--show-sql-only`.
    If using options `--from-revision` or `--from-version`, you must also use `--show-sql-only`,
    because if actually *running* migrations, we should only migrate from the *current* revision.
    """
    console = Console()
    console.print(f"Using DB (engine: {settings.engine.url})")

    if not show_sql_only:
        console.print(f"Performing downgrade with database {settings.engine.url}")
    else:
        console.print("Generating SQL for downgrade -- downgrade commands will *not* be submitted.")

    if show_sql_only or (
        yes
        or click.confirm(
            "\nWarning: About to reverse schema migrations for the airflow metastore. "
            "Please ensure you have backed up your database before any migration "
            "operation. Proceed? (y/n)\n"
        )
    ):
        db_utils.downgrade(to_revision=revision, from_revision=from_revision, show_sql_only=show_sql_only)
        if not show_sql_only:
            console.print("Downgrades done")
    else:
        SystemExit("Cancelled")


@db.command('shell')
@click.pass_context
@cli_utils.action_cli(check_db=False)
def shell(ctx):
    """Runs a shell to access the database"""
    url = settings.engine.url
    console = Console()
    console.print(f"DB: {url}")

    if url.get_backend_name() == 'mysql':
        with NamedTemporaryFile(suffix="my.cnf") as f:
            content = textwrap.dedent(
                f"""
                [client]
                host     = {url.host}
                user     = {url.username}
                password = {url.password or ""}
                port     = {url.port or "3306"}
                database = {url.database}
                """
            ).strip()
            f.write(content.encode())
            f.flush()
            execute_interactive(["mysql", f"--defaults-extra-file={f.name}"])
    elif url.get_backend_name() == 'sqlite':
        execute_interactive(["sqlite3", url.database])
    elif url.get_backend_name() == 'postgresql':
        env = os.environ.copy()
        env['PGHOST'] = url.host or ""
        env['PGPORT'] = str(url.port or "5432")
        env['PGUSER'] = url.username or ""
        # PostgreSQL does not allow the use of PGPASSFILE if the current user is root.
        env["PGPASSWORD"] = url.password or ""
        env['PGDATABASE'] = url.database
        execute_interactive(["psql"], env=env)
    elif url.get_backend_name() == 'mssql':
        env = os.environ.copy()
        env['MSSQL_CLI_SERVER'] = url.host
        env['MSSQL_CLI_DATABASE'] = url.database
        env['MSSQL_CLI_USER'] = url.username
        env['MSSQL_CLI_PASSWORD'] = url.password
        execute_interactive(["mssql-cli"], env=env)
    else:
        raise AirflowException(f"Unknown driver: {url.drivername}")


@db.command('check')
@click.pass_context
@click.option(
    '-t',
    '--migration-wait-timeout',
    type=int,
    default=60,
    help="Tmeout to wait for the database to migrate",
)
@cli_utils.action_cli(check_db=False)
def check(ctx, migration_wait_timeout):
    """Runs a check command that checks if db is reachable"""
    console = Console()
    console.print(f"Waiting for {migration_wait_timeout}s")
    db_utils.check_migrations(timeout=migration_wait_timeout)


# lazily imported by CLI parser for `help` command
all_tables = sorted(config_dict)


@db.command('cleanup')
@click.pass_context
@click.option(
    '-t',
    '--tables',
    multiple=True,
    default=all_tables,
    show_default=True,
    help=(
        "Table names to perform maintenance on (use comma-separated list).\n"
        "Can be specified multiple times, all tables names will be used.\n"
    ),
)
@click.option(
    '--clean-before-timestamp',
    type=str,
    default=None,
    help="The date or timestamp before which data should be purged.\n"
    "If no timezone info is supplied then dates are assumed to be in airflow default timezone.\n"
    "Example: '2022-01-01 00:00:00+01:00'",
)
@click_dry_run
@click_verbose
@click_yes
@cli_utils.action_cli(check_db=False)
def cleanup_tables(ctx, tables, clean_before_timestamp, dry_run, verbose, yes):
    """Purge old records in metastore tables"""
    split_tables = []
    for table in tables:
        split_tables.extend(table.split(','))
    run_cleanup(
        table_names=split_tables,
        dry_run=dry_run,
        clean_before_timestamp=clean_before_timestamp,
        verbose=verbose,
        confirm=not yes,
    )
