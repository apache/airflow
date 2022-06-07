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
from airflow.utils import cli as cli_utils
from airflow.utils.process_utils import execute_interactive
from airflow.utils.timezone import parse as parsedate

click_to_revision = click.option(
    '-r',
    '--to-revision',
    default=None,
    help=(
        "(Optional) If provided, only run migrations up to and including this revision. Note: must "
        "provide either `--to-revision` or `--to-version`."
    ),
)
click_to_version = click.option(
    '-n',
    '--to-version',
    default=None,
    help=(
        "(Optional) The airflow version to upgrade to. Note: must provide either "
        "`--to-revision` or `--to-version`."
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
    default=False,
    help=(
        "Don't actually run migrations; just print out sql scripts for offline migration. "
        "Required if using either `--from-version` or `--from-version`."
    ),
)


@airflow_cmd.group()
@click.pass_context
def db(ctx):
    """Database operations"""


@db.command('init')
@click.pass_context
def db_init(ctx):
    """Initialize the metadata database"""
    from airflow.utils import db as db_utils

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
    """Wait for all airflow migrations to complete (used for launching airflow in k8s)"""
    from airflow.utils import db as db_utils

    console = Console()
    console.print(f"Waiting for {migration_wait_timeout}s")
    db_utils.check_migrations(timeout=migration_wait_timeout)


@db.command('reset')
@click.pass_context
@click.option(
    '-s',
    '--skip-init',
    help="Only remove tables; do not perform db init.",
    is_flag=True,
    default=False,
)
@click_yes
def db_reset(ctx, skip_init, yes):
    """Burn down and rebuild the metadata database"""

    console = Console()
    console.print(f"DB: {settings.engine.url}")
    if yes or click.confirm("This will drop existing tables if they exist. Proceed? (y/n)"):
        from airflow.utils import db as db_utils

        db_utils.resetdb(skip_init=skip_init)
    else:
        console.print("Cancelled")


@wrapt.decorator
def check_revision_and_version_options(wrapped, instance, args, kwargs):
    # Get the progressive aspect of the name of the wrapped function
    # upgrade -> upgrading
    # downgrade -> downgrading
    verb = f'{wrapped.__name__[:-1]}ing'

    def wrapper(ctx, to_revision, to_version, from_revision, from_version, show_sql_only, *_args, **_kwargs):
        if to_revision is not None and to_version is not None:
            raise SystemExit("Cannot supply both `--to-revision` and `--to-version`.")
        if from_revision is not None and from_version is not None:
            raise SystemExit("Cannot supply both `--from-revision` and `--from-version`")
        if (from_revision is not None or from_version is not None) and not show_sql_only:
            raise SystemExit(
                "Args `--from-revision` and `--from-version` may only be used with `--show-sql-only`"
            )

        if from_version is not None:
            if parse_version(from_version) < parse_version('2.0.0'):
                raise SystemExit("--from-version must be greater than or equal to 2.0.0")
            from airflow.utils.db import REVISION_HEADS_MAP

            from_revision = REVISION_HEADS_MAP.get(from_version)
            if not from_revision:
                raise SystemExit(f"Unknown version {from_version!r} supplied as `--from-version`.")

        if to_version is not None:
            from airflow.utils.db import REVISION_HEADS_MAP

            to_revision = REVISION_HEADS_MAP.get(to_version)
            if not to_revision:
                raise SystemExit(f"{verb.capitalize()} to version {to_version} is not supported.")

        return wrapped(
            ctx, to_revision, to_version, from_revision, from_version, show_sql_only, *_args, **_kwargs
        )

    return wrapper(*args, **kwargs)


@db.command('upgrade')
@click.pass_context
@click_to_revision
@click_to_version
@click_from_revision
@click_from_version
@click_show_sql_only
@click_yes
@check_revision_and_version_options
def upgrade(ctx, to_revision, to_version, from_revision, from_version, show_sql_only, yes):
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
            "operation. Proceed?\n"
        )
    ):
        from airflow.utils import db as db_utils

        db_utils.upgradedb(to_revision=to_revision, from_revision=from_revision, show_sql_only=show_sql_only)
        if not show_sql_only:
            console.print("Upgrades done")
    else:
        raise SystemExit("Cancelled")


@db.command('downgrade')
@click.pass_context
@click_to_revision
@click_to_version
@click_from_revision
@click_from_version
@click_show_sql_only
@click_yes
@check_revision_and_version_options
def downgrade(ctx, to_revision, to_version, from_revision, from_version, show_sql_only, yes):
    """
    Downgrade the schema of the metadata database

    You must provide either `--to-revision` or `--to-version`.
    To print but not execute commands, use option `--show-sql-only`.
    If using options `--from-revision` or `--from-version`, you must also use `--show-sql-only`,
    because if actually *running* migrations, we should only migrate from the *current* revision.
    """
    console = Console()
    console.print(f"Using DB (engine: {settings.engine.url})")

    if not (to_version or to_revision):
        raise SystemExit("Must provide either --to-revision or --to-version.")

    if not show_sql_only:
        console.print(f"Performing downgrade with database {settings.engine.url}")
    else:
        console.print("Generating SQL for downgrade -- downgrade commands will *not* be submitted.")

    if not (show_sql_only or yes):
        click.confirm(
            "\nWarning: About to reverse schema migrations for the airflow metastore. "
            "Please ensure you have backed up your database before any migration "
            "operation. Proceed?\n",
            abort=True,
        )

    from airflow.utils import db as db_utils

    db_utils.downgrade(to_revision=to_revision, from_revision=from_revision, show_sql_only=show_sql_only)
    if not show_sql_only:
        console.print("Downgrades done")


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
    help="Timeout to wait for the database to migrate",
)
@cli_utils.action_cli(check_db=False)
def check(ctx, migration_wait_timeout):
    """Check if the database can be reached"""
    from airflow.utils import db as db_utils

    console = Console()
    console.print(f"Waiting for {migration_wait_timeout}s")
    db_utils.check_migrations(timeout=migration_wait_timeout)


# lazily imported by CLI parser for `help` command
# Create a custom class that emulates a callable since click validates
# non-callable and make __str__ to return output for lazy processing.
class _CleanTableDefault:
    def __call__(self):
        pass

    def __str__(self):
        from airflow.utils.db_cleanup import config_dict

        return ','.join(sorted(config_dict))


@db.command('clean')
@click.pass_context
@click.option(
    '-t',
    '--tables',
    multiple=True,
    default=_CleanTableDefault(),
    show_default=True,
    help=(
        "Table names to perform maintenance on.\n"
        "Can be specified multiple times or use a comma-separated list.\n"
        "If not specified, all tables names will be cleaned.\n"
    ),
)
@click.option(
    '--clean-before-timestamp',
    required=True,
    metavar='TIMESTAMP',
    type=parsedate,
    help=(
        "The date or timestamp before which data should be purged.\n"
        "If no timezone info is supplied then dates are assumed to be in airflow default timezone.\n"
        "\n"
        "Example: '2022-01-01 00:00:00+01:00'\n"
    ),
)
@click_dry_run
@click_verbose
@click_yes
@cli_utils.action_cli(check_db=False)
def cleanup_tables(ctx, tables, clean_before_timestamp, dry_run, verbose, yes):
    """Purge old records in metastore tables"""
    from airflow.utils.db_cleanup import run_cleanup

    run_cleanup(
        table_names=[t.strip() for table in tables for t in table.split(',')] or None,
        dry_run=dry_run,
        clean_before_timestamp=clean_before_timestamp,
        verbose=verbose,
        confirm=not yes,
    )
