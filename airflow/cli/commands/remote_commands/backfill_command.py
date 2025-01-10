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

import logging
import signal

from airflow import settings
from airflow.models.backfill import ReprocessBehavior, _create_backfill, _do_dry_run
from airflow.utils import cli as cli_utils
from airflow.utils.cli import sigint_handler
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import create_session


@cli_utils.action_cli
@providers_configuration_loaded
def create_backfill(args) -> None:
    """Create backfill job or dry run for a DAG or list of DAGs using regex."""
    logging.basicConfig(level=settings.LOGGING_LEVEL, format=settings.SIMPLE_LOG_FORMAT)
    signal.signal(signal.SIGTERM, sigint_handler)

    if args.reprocess_behavior is not None:
        reprocess_behavior = ReprocessBehavior(args.reprocess_behavior)
    else:
        reprocess_behavior = None

    if args.dry_run:
        print("Performing dry run of backfill.")
        print("Printing params:")
        params = dict(
            dag_id=args.dag_id,
            from_date=args.from_date,
            to_date=args.to_date,
            max_active_runs=args.max_active_runs,
            reverse=args.run_backwards,
            dag_run_conf=args.dag_run_conf,
            reprocess_behavior=reprocess_behavior,
        )
        for k, v in params.items():
            print(f"    - {k} = {v}")
        with create_session() as session:
            logical_dates = _do_dry_run(
                dag_id=args.dag_id,
                from_date=args.from_date,
                to_date=args.to_date,
                reverse=args.reverse,
                reprocess_behavior=args.reprocess_behavior,
                session=session,
            )
        print("Logical dates to be attempted:")
        for d in logical_dates:
            print(f"    - {d}")
        return

    _create_backfill(
        dag_id=args.dag_id,
        from_date=args.from_date,
        to_date=args.to_date,
        max_active_runs=args.max_active_runs,
        reverse=args.run_backwards,
        dag_run_conf=args.dag_run_conf,
        reprocess_behavior=reprocess_behavior,
    )
