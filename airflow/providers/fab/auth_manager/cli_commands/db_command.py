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

from airflow import settings
from airflow.cli.commands.db_command import run_db_downgrade_command, run_db_migrate_command
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
    session = settings.Session()
    upgrade_command = FABDBManager(session).upgradedb
    run_db_migrate_command(args, upgrade_command, revision_heads_map=_REVISION_HEADS_MAP, airflow_db=False)


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def downgrade(args):
    """Downgrades the metadata database."""
    session = settings.Session()
    dwongrade_command = FABDBManager(session).downgrade
    run_db_downgrade_command(args, dwongrade_command, revision_heads_map=_REVISION_HEADS_MAP)
