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
"""Resolve the Dag run a command acts on from its ``run_id`` / ``--logical-date`` selectors."""

from __future__ import annotations

import datetime
import sys
from typing import TYPE_CHECKING

import rich

from airflowctl.api.client import ServerResponseError

if TYPE_CHECKING:
    from airflowctl.api.client import Client
    from airflowctl.api.datamodels.generated import DAGRunResponse


def _validate_selectors(args) -> None:
    """Exit unless exactly one of ``run_id`` and ``--logical-date`` was given."""
    if (args.run_id is None) == (args.logical_date is None):
        rich.print("[red]Provide either run_id or --logical-date, but not both[/red]")
        sys.exit(1)


def _get_dag_run_by_run_id(api_client: Client, dag_id: str, run_id: str) -> DAGRunResponse:
    """Get a Dag run by its run ID."""
    try:
        return api_client.dag_runs.get(dag_id=dag_id, dag_run_id=run_id, suppress_error_log=True)
    except ServerResponseError as e:
        if e.response.status_code != 404:
            raise
        rich.print(f"[red]Dag run {run_id!r} of Dag {dag_id!r} not found[/red]")
        sys.exit(1)


def _get_dag_run_by_logical_date(api_client: Client, dag_id: str, value: str) -> DAGRunResponse:
    """Get the Dag run with an exact logical date match."""
    try:
        logical_date = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        rich.print(f"[red]Invalid --logical-date: {value!r}[/red]")
        sys.exit(1)
    if logical_date.tzinfo is None:
        rich.print("[red]--logical-date must include a timezone offset[/red]")
        sys.exit(1)

    dag_runs = []
    try:
        dag_runs = api_client.dag_runs.list(
            dag_id=dag_id,
            logical_date_gte=logical_date,
            logical_date_lte=logical_date,
            order_by="-id",
            limit=1,
            suppress_error_log=True,
        ).dag_runs
    except ServerResponseError as e:
        if e.response.status_code != 404:
            raise
    if not dag_runs:
        rich.print(f"[red]Dag run for {dag_id} with logical date {value!r} not found[/red]")
        sys.exit(1)
    return dag_runs[0]


def resolve_dag_run(api_client: Client, args) -> DAGRunResponse:
    """Get the selected Dag run, fetching it when ``run_id`` was given."""
    _validate_selectors(args)
    if args.run_id:
        return _get_dag_run_by_run_id(api_client, args.dag_id, args.run_id)
    return _get_dag_run_by_logical_date(api_client, args.dag_id, args.logical_date)


def resolve_dag_run_id(api_client: Client, args) -> str:
    """
    Get the ID of the selected Dag run.

    A ``run_id`` is taken at face value rather than fetched, so that a caller acting on a nested
    resource reports the miss against that resource instead of against the Dag run.
    """
    _validate_selectors(args)
    if args.run_id:
        return args.run_id
    return _get_dag_run_by_logical_date(api_client, args.dag_id, args.logical_date).dag_run_id
