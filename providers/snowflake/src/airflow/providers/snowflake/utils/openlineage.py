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
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlparse, urlunparse

from airflow.providers.common.compat.openlineage.check import require_openlineage_version
from airflow.providers.snowflake.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils import timezone

if TYPE_CHECKING:
    from openlineage.client.event_v2 import RunEvent
    from openlineage.client.facet_v2 import JobFacet

    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    from airflow.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSqlApiHook


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
            # region can easily get duplicated without crashing snowflake, so we need to handle that as well
            # eg. account_locator.europe-west3.gcp.europe-west3.gcp will be ok for snowflake
            account, region, cloud, *rest = spl
            rest = [x for x in rest if x not in (region, cloud)]
            if rest:  # Not sure what could be left here, but leaving this just in case
                log.warning(
                    "Unexpected parts found in Snowflake uri hostname and will be ignored by OpenLineage: %s",
                    rest,
                )
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

    Snowflake sqlalchemy connection URI has the following structure:
    'snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>'
    We want account identifier normalized. It can have two forms:
    - newer, in form of <organization_id>-<account_id>. In this case we want to do nothing.
    - older, composed of <account_locator>.<region>.<cloud> where region and cloud can be
    optional in some cases. If <cloud> is omitted, it's AWS.
    If region and cloud are omitted, it's AWS us-west-1

    Current doc on Snowflake account identifiers:
    https://docs.snowflake.com/en/user-guide/admin-account-identifier
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


def _get_logical_date(task_instance):
    # todo: remove when min airflow version >= 3.0
    if AIRFLOW_V_3_0_PLUS:
        dagrun = task_instance.get_template_context()["dag_run"]
        return dagrun.logical_date or dagrun.run_after

    if hasattr(task_instance, "logical_date"):
        date = task_instance.logical_date
    else:
        date = task_instance.execution_date

    return date


def _get_dag_run_clear_number(task_instance):
    # todo: remove when min airflow version >= 3.0
    if AIRFLOW_V_3_0_PLUS:
        dagrun = task_instance.get_template_context()["dag_run"]
        return dagrun.clear_number
    return task_instance.dag_run.clear_number


# todo: move this run_id logic into OpenLineage's listener to avoid differences
def _get_ol_run_id(task_instance) -> str:
    """
    Get OpenLineage run_id from TaskInstance.

    It's crucial that the task_instance's run_id creation logic matches OpenLineage's listener implementation.
    Only then can we ensure that the generated run_id aligns with the Airflow task,
    enabling a proper connection between events.
    """
    from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter

    # Generate same OL run id as is generated for current task instance
    return OpenLineageAdapter.build_task_instance_run_id(
        dag_id=task_instance.dag_id,
        task_id=task_instance.task_id,
        logical_date=_get_logical_date(task_instance),
        try_number=task_instance.try_number,
        map_index=task_instance.map_index,
    )


# todo: move this run_id logic into OpenLineage's listener to avoid differences
def _get_ol_dag_run_id(task_instance) -> str:
    from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter

    return OpenLineageAdapter.build_dag_run_id(
        dag_id=task_instance.dag_id,
        logical_date=_get_logical_date(task_instance),
        clear_number=_get_dag_run_clear_number(task_instance),
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
    root_parent_run_id = _get_ol_dag_run_id(task_instance)

    return parent_run.ParentRunFacet(
        run=parent_run.Run(runId=parent_run_id),
        job=parent_run.Job(
            namespace=namespace(),
            name=f"{task_instance.dag_id}.{task_instance.task_id}",
        ),
        root=parent_run.Root(
            run=parent_run.RootRun(runId=root_parent_run_id),
            job=parent_run.RootJob(
                name=task_instance.dag_id,
                namespace=namespace(),
            ),
        ),
    )


def _run_single_query_with_hook(hook: SnowflakeHook, sql: str) -> list[dict]:
    """Execute a query against Snowflake without adding extra logging or instrumentation."""
    with closing(hook.get_conn()) as conn:
        hook.set_autocommit(conn, False)
        with hook._get_cursor(conn, return_dictionaries=True) as cur:
            cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3;")  # only for this session
            cur.execute(sql)
            result = cur.fetchall()
        conn.commit()
    return result


def _run_single_query_with_api_hook(hook: SnowflakeSqlApiHook, sql: str) -> list[dict[str, Any]]:
    """Execute a query against Snowflake API without adding extra logging or instrumentation."""
    # `hook.execute_query` resets the query_ids, so we need to save them and re-assign after we're done
    query_ids_before_execution = list(hook.query_ids)
    try:
        _query_ids = hook.execute_query(sql=sql, statement_count=0)
        hook.wait_for_query(query_id=_query_ids[0], raise_error=True, poll_interval=1, timeout=3)
        return hook.get_result_from_successful_sql_api_query(query_id=_query_ids[0])
    finally:
        hook.query_ids = query_ids_before_execution


def _process_data_from_api(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert 'START_TIME' and 'END_TIME' fields to UTC datetime objects."""
    for row in data:
        for key in ("START_TIME", "END_TIME"):
            row[key] = datetime.datetime.fromtimestamp(float(row[key]), timezone.utc)
    return data


def _get_queries_details_from_snowflake(
    hook: SnowflakeHook | SnowflakeSqlApiHook, query_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """Retrieve execution details for specific queries from Snowflake's query history."""
    if not query_ids:
        return {}
    query_condition = f"IN {tuple(query_ids)}" if len(query_ids) > 1 else f"= '{query_ids[0]}'"
    # https://docs.snowflake.com/en/sql-reference/account-usage#differences-between-account-usage-and-information-schema
    # INFORMATION_SCHEMA.QUERY_HISTORY has no latency, so it's better than ACCOUNT_USAGE.QUERY_HISTORY
    # https://docs.snowflake.com/en/sql-reference/functions/query_history
    # SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY() function seems the most suitable function for the job,
    # we get history of queries executed by the user, and we're using the same credentials.
    query = (
        "SELECT "
        "QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        "FROM "
        "table(snowflake.information_schema.query_history()) "
        f"WHERE "
        f"QUERY_ID {query_condition}"
        f";"
    )

    try:
        # Note: need to lazy import here to avoid circular imports
        from airflow.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSqlApiHook

        if isinstance(hook, SnowflakeSqlApiHook):
            result = _run_single_query_with_api_hook(hook=hook, sql=query)
            result = _process_data_from_api(data=result)
        else:
            result = _run_single_query_with_hook(hook=hook, sql=query)
    except Exception as e:
        log.info(
            "OpenLineage encountered an error while retrieving additional metadata about SQL queries"
            " from Snowflake. The process will continue with default values. Error details: %s",
            e,
        )
        result = []

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


@require_openlineage_version(provider_min_version="2.3.0")
def emit_openlineage_events_for_snowflake_queries(
    task_instance,
    hook: SnowflakeHook | SnowflakeSqlApiHook | None = None,
    query_ids: list[str] | None = None,
    query_source_namespace: str | None = None,
    query_for_extra_metadata: bool = False,
    additional_run_facets: dict | None = None,
    additional_job_facets: dict | None = None,
) -> None:
    """
    Emit OpenLineage events for executed Snowflake queries.

    Metadata retrieval from Snowflake is attempted only if `get_extra_metadata` is True and hook is provided.
    If metadata is available, execution details such as start time, end time, execution status,
    error messages, and SQL text are included in the events. If no metadata is found, the function
    defaults to using the Airflow task instance's state and the current timestamp.

    Note that both START and COMPLETE event for each query will be emitted at the same time.
    If we are able to query Snowflake for query execution metadata, event times
    will correspond to actual query execution times.

    Args:
        task_instance: The Airflow task instance that run these queries.
        hook: A supported Snowflake hook instance used to retrieve query metadata if available.
        If omitted, `query_ids` and `query_source_namespace` must be provided explicitly and
        `query_for_extra_metadata` must be `False`.
        query_ids: A list of Snowflake query IDs to emit events for, can only be None if `hook` is provided
        and `hook.query_ids` are present.
        query_source_namespace: The namespace to be included in ExternalQueryRunFacet,
        can be `None` only if hook is provided.
        query_for_extra_metadata: Whether to query Snowflake for additional metadata about queries.
        Must be `False` if `hook` is not provided.
        additional_run_facets: Additional run facets to include in OpenLineage events.
        additional_job_facets: Additional job facets to include in OpenLineage events.
    """
    from openlineage.client.facet_v2 import job_type_job

    from airflow.providers.common.compat.openlineage.facet import (
        ErrorMessageRunFacet,
        ExternalQueryRunFacet,
        RunFacet,
        SQLJobFacet,
    )
    from airflow.providers.openlineage.conf import namespace
    from airflow.providers.openlineage.plugins.listener import get_openlineage_listener

    log.info("OpenLineage will emit events for Snowflake queries.")

    if hook:
        if not query_ids:
            log.debug("No Snowflake query IDs provided; Checking `hook.query_ids` property.")
            query_ids = getattr(hook, "query_ids", [])
            if not query_ids:
                raise ValueError("No Snowflake query IDs provided and `hook.query_ids` are not present.")

        if not query_source_namespace:
            log.debug("No Snowflake query namespace provided; Creating one from scratch.")
            from airflow.providers.openlineage.sqlparser import SQLParser

            connection = hook.get_connection(hook.get_conn_id())
            query_source_namespace = SQLParser.create_namespace(
                hook.get_openlineage_database_info(connection)
            )
    else:
        if not query_ids:
            raise ValueError("If 'hook' is not provided, 'query_ids' must be set.")
        if not query_source_namespace:
            raise ValueError("If 'hook' is not provided, 'query_source_namespace' must be set.")
        if query_for_extra_metadata:
            raise ValueError("If 'hook' is not provided, 'query_for_extra_metadata' must be False.")

    query_ids = [q for q in query_ids]  # Make a copy to make sure we do not change hook's attribute

    if query_for_extra_metadata and hook:
        log.debug("Retrieving metadata for %s queries from Snowflake.", len(query_ids))
        snowflake_metadata = _get_queries_details_from_snowflake(hook, query_ids)
    else:
        log.debug("`query_for_extra_metadata` is False. No extra metadata fill be fetched from Snowflake.")
        snowflake_metadata = {}

    # If real metadata is unavailable, we send events with eventTime=now
    default_event_time = timezone.utcnow()
    # If no query metadata is provided, we use task_instance's state when checking for success
    # ti.state has no `value` attr (AF2) when task it's still running, in AF3 we get 'running', in that case
    # assuming it's user call and query succeeded, so we replace it with success.
    default_state = (
        getattr(task_instance.state, "value", "running") if hasattr(task_instance, "state") else ""
    )
    default_state = "success" if default_state == "running" else default_state

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

        query_specific_run_facets: dict[str, RunFacet] = {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=query_id, source=query_source_namespace)
        }
        if query_metadata.get("ERROR_MESSAGE"):
            query_specific_run_facets["error"] = ErrorMessageRunFacet(
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
            start_time=query_metadata.get("START_TIME", default_event_time),
            end_time=query_metadata.get("END_TIME", default_event_time),
            # `EXECUTION_STATUS` can be `success`, `fail` or `incident` (Snowflake outage, so still failure)
            is_successful=query_metadata.get("EXECUTION_STATUS", default_state).lower() == "success",
            run_facets={**query_specific_run_facets, **common_run_facets, **additional_run_facets},
            job_facets={**query_specific_job_facets, **common_job_facets, **additional_job_facets},
        )
        events.extend(event_batch)

    log.debug("Generated %s OpenLineage events; emitting now.", len(events))
    adapter = get_openlineage_listener().adapter
    for event in events:
        adapter.emit(event)

    log.info("OpenLineage has successfully finished processing information about Snowflake queries.")
    return
