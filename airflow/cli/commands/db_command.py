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
"""Database sub-commands."""

from __future__ import annotations

import logging
import os
import textwrap
import warnings
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

from packaging.version import InvalidVersion, parse as parse_version
from tenacity import Retrying, stop_after_attempt, wait_fixed

from airflow import settings
from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.exceptions import AirflowException
from airflow.utils import cli as cli_utils, db
from airflow.utils.db import _REVISION_HEADS_MAP
from airflow.utils.db_cleanup import config_dict, drop_archived_tables, export_archived_records, run_cleanup
from airflow.utils.process_utils import execute_interactive
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

if TYPE_CHECKING:
    from tenacity import RetryCallState

log = logging.getLogger(__name__)


@providers_configuration_loaded
def initdb(args):
    """Initialize the metadata database."""
    warnings.warn(
        "`db init` is deprecated.  Use `db migrate` instead to migrate the db and/or "
        "airflow connections create-default-connections to create the default connections",
        DeprecationWarning,
        stacklevel=2,
    )
    print(f"DB: {settings.engine.url!r}")
    db.initdb()
    print("Initialization done")


@providers_configuration_loaded
def resetdb(args):
    """Reset the metadata database."""
    print(f"DB: {settings.engine.url!r}")
    if not (args.yes or input("This will drop existing tables if they exist. Proceed? (y/n)").upper() == "Y"):
        raise SystemExit("Cancelled")
    db.resetdb(skip_init=args.skip_init)


def upgradedb(args):
    """Upgrades the metadata database."""
    warnings.warn("`db upgrade` is deprecated. Use `db migrate` instead.", DeprecationWarning, stacklevel=2)
    migratedb(args)


def _get_version_revision(
    version: str, recursion_limit: int = 10, revision_heads_map: dict[str, str] | None = None
) -> str | None:
    """
    Recursively search for the revision of the given version in revision_heads_map.

    This searches given revision_heads_map for the revision of the given version, recursively
    searching for the previous version if the given version is not found.
    """
    if revision_heads_map is None:
        revision_heads_map = _REVISION_HEADS_MAP
    if version in revision_heads_map:
        return revision_heads_map[version]
    try:
        major, minor, patch = map(int, version.split("."))
    except ValueError:
        return None
    new_version = f"{major}.{minor}.{patch - 1}"
    recursion_limit -= 1
    if recursion_limit <= 0:
        # Prevent infinite recursion as I can't imagine 10 successive versions without migration
        return None
    return _get_version_revision(new_version, recursion_limit)


def run_db_migrate_command(args, command, revision_heads_map: dict[str, str], reserialize_dags: bool = True):
    """
    Run the db migrate command.

    param args: The parsed arguments.
    param command: The command to run.
    param airflow_db: Whether the command is for the airflow database.

    :meta private:
    """
    print(f"DB: {settings.engine.url!r}")
    if args.to_revision and args.to_version:
        raise SystemExit("Cannot supply both `--to-revision` and `--to-version`.")
    if args.from_version and args.from_revision:
        raise SystemExit("Cannot supply both `--from-revision` and `--from-version`")
    if (args.from_revision or args.from_version) and not args.show_sql_only:
        raise SystemExit(
            "Args `--from-revision` and `--from-version` may only be used with `--show-sql-only`"
        )
    to_revision = None
    from_revision = None
    if args.from_revision:
        from_revision = args.from_revision
    elif args.from_version:
        try:
            parse_version(args.from_version)
        except InvalidVersion:
            raise SystemExit(f"Invalid version {args.from_version!r} supplied as `--from-version`.")
        from_revision = _get_version_revision(args.from_version, revision_heads_map=revision_heads_map)
        if not from_revision:
            raise SystemExit(f"Unknown version {args.from_version!r} supplied as `--from-version`.")

    if args.to_version:
        try:
            parse_version(args.to_version)
        except InvalidVersion:
            raise SystemExit(f"Invalid version {args.to_version!r} supplied as `--to-version`.")
        to_revision = _get_version_revision(args.to_version, revision_heads_map=revision_heads_map)
        if not to_revision:
            raise SystemExit(f"Unknown version {args.to_version!r} supplied as `--to-version`.")
    elif args.to_revision:
        to_revision = args.to_revision

    if not args.show_sql_only:
        print(f"Performing upgrade to the metadata database {settings.engine.url!r}")
    else:
        print("Generating sql for upgrade -- upgrade commands will *not* be submitted.")
    if reserialize_dags:
        command(
            to_revision=to_revision,
            from_revision=from_revision,
            show_sql_only=args.show_sql_only,
            reserialize_dags=True,
        )
    else:
        command(
            to_revision=to_revision,
            from_revision=from_revision,
            show_sql_only=args.show_sql_only,
        )
    if not args.show_sql_only:
        print("Database migrating done!")


def run_db_downgrade_command(args, command, revision_heads_map: dict[str, str]):
    """
    Run the db downgrade command.

    param args: The parsed arguments.
    param command: The command to run.
    """
    if args.to_revision and args.to_version:
        raise SystemExit("Cannot supply both `--to-revision` and `--to-version`.")
    if args.from_version and args.from_revision:
        raise SystemExit("`--from-revision` may not be combined with `--from-version`")
    if (args.from_revision or args.from_version) and not args.show_sql_only:
        raise SystemExit(
            "Args `--from-revision` and `--from-version` may only be used with `--show-sql-only`"
        )
    if not (args.to_version or args.to_revision):
        raise SystemExit("Must provide either --to-revision or --to-version.")
    from_revision = None
    to_revision = None
    if args.from_revision:
        from_revision = args.from_revision
    elif args.from_version:
        from_revision = _get_version_revision(args.from_version, revision_heads_map=revision_heads_map)
        if not from_revision:
            raise SystemExit(f"Unknown version {args.from_version!r} supplied as `--from-version`.")
    if args.to_version:
        to_revision = _get_version_revision(args.to_version, revision_heads_map=revision_heads_map)
        if not to_revision:
            raise SystemExit(f"Downgrading to version {args.to_version} is not supported.")
    elif args.to_revision:
        to_revision = args.to_revision
    if not args.show_sql_only:
        print(f"Performing downgrade with database {settings.engine.url!r}")
    else:
        print("Generating sql for downgrade -- downgrade commands will *not* be submitted.")

    if args.show_sql_only or (
        args.yes
        or input(
            "\nWarning: About to reverse schema migrations for the airflow metastore. "
            "Please ensure you have backed up your database before any upgrade or "
            "downgrade operation. Proceed? (y/n)\n"
        ).upper()
        == "Y"
    ):
        command(to_revision=to_revision, from_revision=from_revision, show_sql_only=args.show_sql_only)
        if not args.show_sql_only:
            print("Downgrade complete")
    else:
        raise SystemExit("Cancelled")


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def migratedb(args):
    """Migrates the metadata database."""
    if args.from_version:
        try:
            parsed_version = parse_version(args.from_version)
        except InvalidVersion:
            raise SystemExit(f"Invalid version {args.from_version!r} supplied as `--from-version`.")
        if parsed_version < parse_version("2.0.0"):
            raise SystemExit("--from-version must be greater or equal to 2.0.0")
    run_db_migrate_command(args, db.upgradedb, _REVISION_HEADS_MAP, reserialize_dags=True)


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def downgrade(args):
    """Downgrades the metadata database."""
    run_db_downgrade_command(args, db.downgrade, _REVISION_HEADS_MAP)


@providers_configuration_loaded
def check_migrations(args):
    """Wait for all airflow migrations to complete. Used for launching airflow in k8s."""
    db.check_migrations(timeout=args.migration_wait_timeout)


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def shell(args):
    """Run a shell that allows to access metadata database."""
    url = settings.engine.url
    print(f"DB: {url!r}")

    if url.get_backend_name() == "mysql":
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
    elif url.get_backend_name() == "sqlite":
        execute_interactive(["sqlite3", url.database])
    elif url.get_backend_name() == "postgresql":
        env = os.environ.copy()
        env["PGHOST"] = url.host or ""
        env["PGPORT"] = str(url.port or "5432")
        env["PGUSER"] = url.username or ""
        # PostgreSQL does not allow the use of PGPASSFILE if the current user is root.
        env["PGPASSWORD"] = url.password or ""
        env["PGDATABASE"] = url.database
        execute_interactive(["psql"], env=env)
    else:
        raise AirflowException(f"Unknown driver: {url.drivername}")


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def check(args):
    """Run a check command that checks if db is available."""
    if InternalApiConfig.get_use_internal_api():
        return
    retries: int = args.retry
    retry_delay: int = args.retry_delay

    def _warn_remaining_retries(retrystate: RetryCallState):
        remain = retries - retrystate.attempt_number
        log.warning("%d retries remain. Will retry in %d seconds", remain, retry_delay)

    for attempt in Retrying(
        stop=stop_after_attempt(1 + retries),
        wait=wait_fixed(retry_delay),
        reraise=True,
        before_sleep=_warn_remaining_retries,
    ):
        with attempt:
            db.check()


# lazily imported by CLI parser for `help` command
all_tables = sorted(config_dict)


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def cleanup_tables(args):
    """Purges old records in metadata database."""
    run_cleanup(
        table_names=args.tables,
        dry_run=args.dry_run,
        clean_before_timestamp=args.clean_before_timestamp,
        verbose=args.verbose,
        confirm=not args.yes,
        skip_archive=args.skip_archive,
    )


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def export_archived(args):
    """Export archived records from metadata database."""
    export_archived_records(
        export_format=args.export_format,
        output_path=args.output_path,
        table_names=args.tables,
        drop_archives=args.drop_archives,
        needs_confirm=not args.yes,
    )


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def drop_archived(args):
    """Drop archived tables from metadata database."""
    drop_archived_tables(
        table_names=args.tables,
        needs_confirm=not args.yes,
    )
