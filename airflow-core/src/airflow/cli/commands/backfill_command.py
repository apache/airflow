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

import json
import logging
import signal

from airflow import settings
from airflow.cli.simple_table import AirflowConsole
from airflow.exceptions import AirflowConfigException
from airflow.models.backfill import ReprocessBehavior, _create_backfill, _do_dry_run
from airflow.utils import cli as cli_utils
from airflow.utils.cli import sigint_handler
from airflow.utils.platform import getuser
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import create_session

log = logging.getLogger(__name__)


@cli_utils.action_cli
@providers_configuration_loaded
def create_backfill(args) -> None:
    """Create backfill job or dry run for a DAG or list of DAGs using regex."""
    logging.basicConfig(level=settings.LOGGING_LEVEL, format=settings.SIMPLE_LOG_FORMAT)
    signal.signal(signal.SIGTERM, sigint_handler)
    console = AirflowConsole()

    if args.reprocess_behavior is not None:
        reprocess_behavior = ReprocessBehavior(args.reprocess_behavior)
    else:
        reprocess_behavior = None

    if args.dry_run:
        console.print("Performing dry run of backfill.")
        console.print("Printing params:")
        params = dict(
            dag_id=args.dag_id,
            from_date=args.from_date,
            to_date=args.to_date,
            max_active_runs=args.max_active_runs,
            reverse=args.run_backwards,
            dag_run_conf=args.dag_run_conf,
            reprocess_behavior=reprocess_behavior,
            run_on_latest_version=args.run_on_latest_version,
        )
        for k, v in params.items():
            console.print(f"    - {k} = {v}")
        with create_session() as session:
            logical_dates = _do_dry_run(
                dag_id=args.dag_id,
                from_date=args.from_date,
                to_date=args.to_date,
                reverse=args.run_backwards,
                reprocess_behavior=args.reprocess_behavior,
                session=session,
            )
        console.print("Logical dates to be attempted:")
        for d in logical_dates:
            console.print(f"    - {d}")
        return

    try:
        user = getuser()
    except AirflowConfigException as e:
        log.warning("Failed to get user name from os: %s, not setting the triggering user", e)
        user = None

    # Parse dag_run_conf if provided
    dag_run_conf = None
    if args.dag_run_conf:
        try:
            dag_run_conf = json.loads(args.dag_run_conf)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in --dag-run-conf: {e}")

    _create_backfill(
        dag_id=args.dag_id,
        from_date=args.from_date,
        to_date=args.to_date,
        max_active_runs=args.max_active_runs,
        reverse=args.run_backwards,
        dag_run_conf=dag_run_conf,
        triggering_user_name=user,
        reprocess_behavior=reprocess_behavior,
        run_on_latest_version=args.run_on_latest_version,
    )
