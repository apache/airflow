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

import datetime
import logging
from contextlib import closing
from typing import TYPE_CHECKING
from urllib.parse import quote, urlparse, urlunparse

from airflow.providers.common.compat.openlineage.check import require_openlineage_version
from airflow.providers.snowflake.version_compat import AIRFLOW_V_2_10_PLUS, AIRFLOW_V_3_0_PLUS
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from openlineage.client.event_v2 import RunEvent
    from openlineage.client.facet_v2 import JobFacet

    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


log = logging.getLogger(__name__)


def fix_account_name(name: str) -> str:
    """Fix account name to have the following format: <account_id>.<region>.<cloud>."""
    if not any(word in name for word in ["-", "_"]):
        # If there is neither '-' nor '_' in the name, we append `.us-west-1.aws`
        return f"{name}.us-west-1.aws"

    if "." in name:
        # Logic for account locator with dots remains unchanged
        spl = name.split(".")
        if len(spl) == 1:
            account = spl[0]
            region, cloud = "us-west-1", "aws"
        elif len(spl) == 2:
            account, region = spl
            cloud = "aws"
        else:
            account, region, cloud = spl
        return f"{account}.{region}.{cloud}"

    # Check for existing accounts with cloud names
    if cloud := next((c for c in ["aws", "gcp", "azure"] if c in name), ""):
        parts = name.split(cloud)
        account = parts[0].strip("-_.")

        if not (region := parts[1].strip("-_.").replace("_", "-")):
            return name
        return f"{account}.{region}.{cloud}"

    # Default case, return the original name
    return name


def fix_snowflake_sqlalchemy_uri(uri: str) -> str:
    """
    Fix snowflake sqlalchemy connection URI to OpenLineage structure.

    Snowflake sqlalchemy connection URI has following structure:
    'snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>'
    We want account identifier normalized. It can have two forms:
    - newer, in form of <organization>-<id>. In this case we want to do nothing.
    - older, composed of <id>-<region>-<cloud> where region and cloud can be
    optional in some cases. If <cloud> is omitted, it's AWS.
    If region and cloud are omitted, it's AWS us-west-1
    """
    try:
        parts = urlparse(uri)
    except ValueError:
        # snowflake.sqlalchemy.URL does not quote `[` and `]`
        # that's a rare case so we can run more debugging code here
        # to make sure we replace only password
        parts = urlparse(uri.replace("[", quote("[")).replace("]", quote("]")))

    hostname = parts.hostname
    if not hostname:
        return uri

    hostname = fix_account_name(hostname)
    # else - its new hostname, just return it
    return urlunparse((parts.scheme, hostname, parts.path, parts.params, parts.query, parts.fragment))


# todo: move this run_id logic into OpenLineage's listener to avoid differences
def _get_ol_run_id(task_instance) -> str:
    """
    Get OpenLineage run_id from TaskInstance.

    It's crucial that the task_instance's run_id creation logic matches OpenLineage's listener implementation.
    Only then can we ensure that the generated run_id aligns with the Airflow task,
    enabling a proper connection between events.
    """
    from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter

    def _get_logical_date():
        # todo: remove when min airflow version >= 3.0
        if AIRFLOW_V_3_0_PLUS:
            dagrun = task_instance.get_template_context()["dag_run"]
            return dagrun.logical_date or dagrun.run_after

        if hasattr(task_instance, "logical_date"):
            date = task_instance.logical_date
        else:
            date = task_instance.execution_date

        return date

    def _get_try_number_success():
        """We are running this in the _on_complete, so need to adjust for try_num changes."""
        # todo: remove when min airflow version >= 2.10.0
        if AIRFLOW_V_2_10_PLUS:
            return task_instance.try_number
        if task_instance.state == TaskInstanceState.SUCCESS:
            return task_instance.try_number - 1
        return task_instance.try_number

    # Generate same OL run id as is generated for current task instance
    return OpenLineageAdapter.build_task_instance_run_id(
        dag_id=task_instance.dag_id,
        task_id=task_instance.task_id,
        logical_date=_get_logical_date(),
        try_number=_get_try_number_success(),
        map_index=task_instance.map_index,
    )


def _get_parent_run_facet(task_instance):
    """
    Retrieve the ParentRunFacet associated with a specific Airflow task instance.

    This facet helps link OpenLineage events of child jobs - such as queries executed within
    external systems (e.g., Snowflake) by the Airflow task - to the original Airflow task execution.
    Establishing this connection enables better lineage tracking and observability.
    """
    from openlineage.client.facet_v2 import parent_run

    from airflow.providers.openlineage.conf import namespace

    parent_run_id = _get_ol_run_id(task_instance)

    return parent_run.ParentRunFacet(
        run=parent_run.Run(runId=parent_run_id),
        job=parent_run.Job(
            namespace=namespace(),
            name=f"{task_instance.dag_id}.{task_instance.task_id}",
        ),
    )


def _run_single_query_with_hook(hook: SnowflakeHook, sql: str) -> list[dict]:
    """Execute a query against Snowflake without adding extra logging or instrumentation."""
    with closing(hook.get_conn()) as conn:
        hook.set_autocommit(conn, False)
        with hook._get_cursor(conn, return_dictionaries=True) as cur:
            cur.execute(sql)
            result = cur.fetchall()
        conn.commit()
    return result


def _get_queries_details_from_snowflake(
    hook: SnowflakeHook, query_ids: list[str]
) -> dict[str, dict[str, str]]:
    """Retrieve execution details for specific queries from Snowflake's query history."""
    if not query_ids:
        return {}
    query_condition = f"IN {tuple(query_ids)}" if len(query_ids) > 1 else f"= '{query_ids[0]}'"
    query = (
        "SELECT "
        "QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        "FROM "
        "table(information_schema.query_history()) "
        f"WHERE "
        f"QUERY_ID {query_condition}"
        f";"
    )

    result = _run_single_query_with_hook(hook=hook, sql=query)

    return {row["QUERY_ID"]: row for row in result} if result else {}


def _create_snowflake_event_pair(
    job_namespace: str,
    job_name: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    is_successful: bool,
    run_facets: dict | None = None,
    job_facets: dict | None = None,
) -> tuple[RunEvent, RunEvent]:
    """Create a pair of OpenLineage RunEvents representing the start and end of a Snowflake job execution."""
    from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
    from openlineage.client.uuid import generate_new_uuid

    run = Run(runId=str(generate_new_uuid()), facets=run_facets or {})
    job = Job(namespace=job_namespace, name=job_name, facets=job_facets or {})

    start = RunEvent(
        eventType=RunState.START,
        eventTime=start_time.isoformat(),
        run=run,
        job=job,
    )
    end = RunEvent(
        eventType=RunState.COMPLETE if is_successful else RunState.FAIL,
        eventTime=end_time.isoformat(),
        run=run,
        job=job,
    )
    return start, end


@require_openlineage_version(provider_min_version="2.0.0")
def emit_openlineage_events_for_snowflake_queries(
    query_ids: list[str],
    query_source_namespace: str,
    task_instance,
    hook: SnowflakeHook | None = None,
    additional_run_facets: dict | None = None,
    additional_job_facets: dict | None = None,
) -> None:
    """
    Emit OpenLineage events for executed Snowflake queries.

    Metadata retrieval from Snowflake is attempted only if a `SnowflakeHook` is provided.
    If metadata is available, execution details such as start time, end time, execution status,
    error messages, and SQL text are included in the events. If no metadata is found, the function
    defaults to using the Airflow task instance's state and the current timestamp.

    Note that both START and COMPLETE event for each query will be emitted at the same time.
    If we are able to query Snowflake for query execution metadata, event times
    will correspond to actual query execution times.

    Args:
        query_ids: A list of Snowflake query IDs to emit events for.
        query_source_namespace: The namespace to be included in ExternalQueryRunFacet.
        task_instance: The Airflow task instance that run these queries.
        hook: A SnowflakeHook instance used to retrieve query metadata if available.
        additional_run_facets: Additional run facets to include in OpenLineage events.
        additional_job_facets: Additional job facets to include in OpenLineage events.
    """
    from openlineage.client.facet_v2 import job_type_job

    from airflow.providers.common.compat.openlineage.facet import (
        ErrorMessageRunFacet,
        ExternalQueryRunFacet,
        SQLJobFacet,
    )
    from airflow.providers.openlineage.conf import namespace
    from airflow.providers.openlineage.plugins.listener import get_openlineage_listener

    if not query_ids:
        log.debug("No Snowflake query IDs provided; skipping OpenLineage event emission.")
        return

    query_ids = [q for q in query_ids]  # Make a copy to make sure it does not change

    if hook:
        log.debug("Retrieving metadata for %s queries from Snowflake.", len(query_ids))
        snowflake_metadata = _get_queries_details_from_snowflake(hook, query_ids)
    else:
        log.debug("SnowflakeHook not provided. No extra metadata fill be fetched from Snowflake.")
        snowflake_metadata = {}

    # If real metadata is unavailable, we send events with eventTime=now
    default_event_time = timezone.utcnow()
    # If no query metadata is provided, we use task_instance's state when checking for success
    default_state = str(task_instance.state) if hasattr(task_instance, "state") else ""

    common_run_facets = {"parent": _get_parent_run_facet(task_instance)}
    common_job_facets: dict[str, JobFacet] = {
        "jobType": job_type_job.JobTypeJobFacet(
            jobType="QUERY",
            integration="SNOWFLAKE",
            processingType="BATCH",
        )
    }
    additional_run_facets = additional_run_facets or {}
    additional_job_facets = additional_job_facets or {}

    events: list[RunEvent] = []
    for counter, query_id in enumerate(query_ids, 1):
        query_metadata = snowflake_metadata.get(query_id, {})
        log.debug(
            "Metadata for query no. %s, (ID `%s`): `%s`",
            counter,
            query_id,
            query_metadata if query_metadata else "not found",
        )

        # TODO(potiuk): likely typing here needs to be fixed
        query_specific_run_facets = {  # type : ignore[assignment]
            "externalQuery": ExternalQueryRunFacet(externalQueryId=query_id, source=query_source_namespace)
        }
        if query_metadata.get("ERROR_MESSAGE"):
            query_specific_run_facets["error"] = ErrorMessageRunFacet(  # type: ignore[assignment]
                message=f"{query_metadata.get('ERROR_CODE')} : {query_metadata['ERROR_MESSAGE']}",
                programmingLanguage="SQL",
            )

        query_specific_job_facets = {}
        if query_metadata.get("QUERY_TEXT"):
            query_specific_job_facets["sql"] = SQLJobFacet(query=query_metadata["QUERY_TEXT"])

        log.debug("Creating OpenLineage event pair for query ID: %s", query_id)
        event_batch = _create_snowflake_event_pair(
            job_namespace=namespace(),
            job_name=f"{task_instance.dag_id}.{task_instance.task_id}.query.{counter}",
            start_time=query_metadata.get("START_TIME", default_event_time),  # type: ignore[arg-type]
            end_time=query_metadata.get("END_TIME", default_event_time),  # type: ignore[arg-type]
            # `EXECUTION_STATUS` can be `success`, `fail` or `incident` (Snowflake outage, so still failure)
            is_successful=query_metadata.get("EXECUTION_STATUS", default_state).lower() == "success",
            run_facets={**query_specific_run_facets, **common_run_facets, **additional_run_facets},
            job_facets={**query_specific_job_facets, **common_job_facets, **additional_job_facets},
        )
        events.extend(event_batch)

    log.debug("Generated %s OpenLineage events; emitting now.", len(events))
    client = get_openlineage_listener().adapter.get_or_create_openlineage_client()
    for event in events:
        client.emit(event)

    log.info("OpenLineage has successfully finished processing information about Snowflake queries.")
    return
