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

from packaging.version import InvalidVersion, parse as parse_version

from airflow import settings
from airflow.cli.commands.db_command import get_version_revision
from airflow.providers.fab.auth_manager.models.db import _REVISION_HEADS_MAP, FABDBManager
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@providers_configuration_loaded
def resetdb(args):
    """Reset the metadata database."""
    print(f"DB: {settings.engine.url!r}")
    if not (args.yes or input("This will drop existing tables if they exist. Proceed? (y/n)").upper() == "Y"):
        raise SystemExit("Cancelled")
    FABDBManager(settings.Session()).resetdb(skip_init=args.skip_init)


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def migratedb(args):
    """Migrates the metadata database."""
    print(f"DB: {settings.engine.url!r}")
    session = settings.Session()
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
        from_revision = get_version_revision(args.from_version, revision_heads_map=_REVISION_HEADS_MAP)
        if not from_revision:
            raise SystemExit(f"Unknown version {args.from_version!r} supplied as `--from-version`.")

    if args.to_version:
        try:
            parse_version(args.to_version)
        except InvalidVersion:
            raise SystemExit(f"Invalid version {args.to_version!r} supplied as `--to-version`.")
        to_revision = get_version_revision(args.to_version, revision_heads_map=_REVISION_HEADS_MAP)
        if not to_revision:
            raise SystemExit(f"Unknown version {args.to_version!r} supplied as `--to-version`.")
    elif args.to_revision:
        to_revision = args.to_revision

    if not args.show_sql_only:
        print(f"Performing upgradedb to the FAB metadata database {settings.engine.url!r}")
    else:
        print("Generating sql for upgradedb -- FAB upgradedb commands will *not* be submitted.")

    FABDBManager(session).upgradedb(
        to_revision=to_revision,
        from_revision=from_revision,
        show_sql_only=args.show_sql_only,
    )
    if not args.show_sql_only:
        print("Database migrating done!")


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def downgrade(args):
    """Downgrades the metadata database."""
    session = settings.Session()
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
        from_revision = get_version_revision(args.from_version, revision_heads_map=_REVISION_HEADS_MAP)
        if not from_revision:
            raise SystemExit(f"Unknown version {args.from_version!r} supplied as `--from-version`.")
    if args.to_version:
        to_revision = get_version_revision(args.to_version, revision_heads_map=_REVISION_HEADS_MAP)
        if not to_revision:
            raise SystemExit(f"Downgrading to version {args.to_version} is not supported.")
    elif args.to_revision:
        to_revision = args.to_revision
    if not args.show_sql_only:
        print(f"Performing downgrade with database {settings.engine.url!r}")
    else:
        print("Generating sql for downgrade -- FAB downgrade commands will *not* be submitted.")

    if args.show_sql_only or (
        args.yes
        or input(
            "\nWarning: About to reverse schema migrations for the FAB metastore. "
            "Please ensure you have backed up your database before any upgradedb or "
            "downgrade operation. Proceed? (y/n)\n"
        ).upper()
        == "Y"
    ):
        FABDBManager(session).downgrade(
            to_revision=to_revision, from_revision=from_revision, show_sql_only=args.show_sql_only
        )
        if not args.show_sql_only:
            print("Downgrade complete")
    else:
        raise SystemExit("Cancelled")
