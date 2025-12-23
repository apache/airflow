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
from airflow._shared.module_loading import import_string
from airflow.cli.commands.db_command import run_db_downgrade_command, run_db_migrate_command
from airflow.configuration import conf
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


def _get_db_manager(classpath: str):
    """Import the db manager class."""
    managers = conf.getlist("database", "external_db_managers")
    if classpath not in managers:
        raise SystemExit(f"DB manager {classpath} not found in configuration.")
    return import_string(classpath.strip())


@providers_configuration_loaded
def resetdb(args):
    """Reset the metadata database."""
    db_manager = _get_db_manager(args.import_path)
    if not (args.yes or input("This will drop existing tables if they exist. Proceed? (y/n)").upper() == "Y"):
        raise SystemExit("Cancelled")
    db_manager(settings.Session()).resetdb(skip_init=args.skip_init)


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def migratedb(args):
    """Migrates the metadata database."""
    db_manager = _get_db_manager(args.import_path)
    session = settings.Session()
    upgrade_command = db_manager(session).upgradedb
    run_db_migrate_command(args, upgrade_command, revision_heads_map=db_manager.revision_heads_map)


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def downgrade(args):
    """Downgrades the metadata database."""
    db_manager = _get_db_manager(args.import_path)
    session = settings.Session()
    downgrade_command = db_manager(session).downgrade
    run_db_downgrade_command(args, downgrade_command, revision_heads_map=db_manager.revision_heads_map)
