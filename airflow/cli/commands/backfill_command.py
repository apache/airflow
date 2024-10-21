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
from airflow.models.backfill import _create_backfill
from airflow.utils import cli as cli_utils
from airflow.utils.cli import sigint_handler
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@cli_utils.action_cli
@providers_configuration_loaded
def create_backfill(args) -> None:
    """Create backfill job or dry run for a DAG or list of DAGs using regex."""
    logging.basicConfig(level=settings.LOGGING_LEVEL, format=settings.SIMPLE_LOG_FORMAT)
    signal.signal(signal.SIGTERM, sigint_handler)

    params = dict(
        dag_id=args.dag,
        from_date=args.from_date,
        to_date=args.to_date,
        max_active_runs=args.max_active_runs,
        reverse=args.run_backwards,
        dag_run_conf=args.dag_run_conf,
        dry_run=args.dry_run,
    )
    obj = _create_backfill(**params)
    print("Performing dry run of backfill.")
    print("Printing params:")
    for k, v in params.items():
        print(f"    - {k} = {v}")
    if isinstance(obj, list):
        print("Logical dates:")
        for info in obj:
            print(f"    - {info.logical_date}")
