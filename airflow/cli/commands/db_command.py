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
from __future__ import annotations

import os
import textwrap
from tempfile import NamedTemporaryFile

from packaging.version import parse as parse_version

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.utils import cli as cli_utils, db
from airflow.utils.db import REVISION_HEADS_MAP
from airflow.utils.db_cleanup import config_dict, run_cleanup
from airflow.utils.process_utils import execute_interactive


def initdb(args):
    """Initializes the metadata database"""
    print("DB: " + repr(settings.engine.url))
    db.initdb()
    print("Initialization done")


def resetdb(args):
    """Resets the metadata database"""
    print("DB: " + repr(settings.engine.url))
    if not (args.yes or input("This will drop existing tables if they exist. Proceed? (y/n)").upper() == "Y"):
        raise SystemExit("Cancelled")
    db.resetdb(skip_init=args.skip_init)


@cli_utils.action_cli(check_db=False)
def upgradedb(args):
    """Upgrades the metadata database"""
    print("DB: " + repr(settings.engine.url))
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
        if parse_version(args.from_version) < parse_version("2.0.0"):
            raise SystemExit("--from-version must be greater or equal to than 2.0.0")
        from_revision = REVISION_HEADS_MAP.get(args.from_version)
        if not from_revision:
            raise SystemExit(f"Unknown version {args.from_version!r} supplied as `--from-version`.")

    if args.to_version:
        to_revision = REVISION_HEADS_MAP.get(args.to_version)
        if not to_revision:
            raise SystemExit(f"Upgrading to version {args.to_version} is not supported.")
    elif args.to_revision:
        to_revision = args.to_revision

    if not args.show_sql_only:
        print("Performing upgrade with database " + repr(settings.engine.url))
    else:
        print("Generating sql for upgrade -- upgrade commands will *not* be submitted.")

    db.upgradedb(
        to_revision=to_revision,
        from_revision=from_revision,
        show_sql_only=args.show_sql_only,
        reserialize_dags=args.reserialize_dags,
    )
    if not args.show_sql_only:
        print("Upgrades done")


@cli_utils.action_cli(check_db=False)
def downgrade(args):
    """Downgrades the metadata database"""
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
    if args.from_revision:
        from_revision = args.from_revision
    elif args.from_version:
        from_revision = REVISION_HEADS_MAP.get(args.from_version)
        if not from_revision:
            raise SystemExit(f"Unknown version {args.from_version!r} supplied as `--from-version`.")
    if args.to_version:
        to_revision = REVISION_HEADS_MAP.get(args.to_version)
        if not to_revision:
            raise SystemExit(f"Downgrading to version {args.to_version} is not supported.")
    elif args.to_revision:
        to_revision = args.to_revision
    if not args.show_sql_only:
        print("Performing downgrade with database " + repr(settings.engine.url))
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
        db.downgrade(to_revision=to_revision, from_revision=from_revision, show_sql_only=args.show_sql_only)
        if not args.show_sql_only:
            print("Downgrade complete")
    else:
        raise SystemExit("Cancelled")


def check_migrations(args):
    """Function to wait for all airflow migrations to complete. Used for launching airflow in k8s"""
    db.check_migrations(timeout=args.migration_wait_timeout)


@cli_utils.action_cli(check_db=False)
def shell(args):
    """Run a shell that allows to access metadata database"""
    url = settings.engine.url
    print("DB: " + repr(url))

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
    elif url.get_backend_name() == "mssql":
        env = os.environ.copy()
        env["MSSQL_CLI_SERVER"] = url.host
        env["MSSQL_CLI_DATABASE"] = url.database
        env["MSSQL_CLI_USER"] = url.username
        env["MSSQL_CLI_PASSWORD"] = url.password
        execute_interactive(["mssql-cli"], env=env)
    else:
        raise AirflowException(f"Unknown driver: {url.drivername}")


@cli_utils.action_cli(check_db=False)
def check(_):
    """Runs a check command that checks if db is available."""
    db.check()


# lazily imported by CLI parser for `help` command
all_tables = sorted(config_dict)


@cli_utils.action_cli(check_db=False)
def cleanup_tables(args):
    """Purges old records in metadata database"""
    run_cleanup(
        table_names=args.tables,
        dry_run=args.dry_run,
        clean_before_timestamp=args.clean_before_timestamp,
        verbose=args.verbose,
        confirm=not args.yes,
        skip_archive=args.skip_archive,
    )
