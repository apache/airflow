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
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

from airflow.exceptions import TaskInstanceNotFound
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey, clear_task_instances
from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowOptionalProviderFeatureException,
    AirflowPlugin,
    BaseOperatorLink,
    TaskGroup,
    XCom,
)
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models import BaseOperator
    from airflow.providers.common.compat.sdk import Context
    from airflow.providers.databricks.operators.databricks import DatabricksTaskBaseOperator
    from airflow.sdk.types import Logger

log = logging.getLogger(__name__)


def get_databricks_task_ids(
    group_id: str, task_map: dict[str, DatabricksTaskBaseOperator], log: Logger
) -> list[str]:
    """
    Return a list of all Databricks task IDs for a dictionary of Airflow tasks.

    :param group_id: The task group ID.
    :param task_map: A dictionary mapping task IDs to BaseOperator instances.
    :param log: The logger to use for logging.
    :return: A list of Databricks task IDs for the given task group.
    """
    task_ids = []
    log.debug("Getting databricks task ids for group %s", group_id)
    for task_id, task in task_map.items():
        if task_id == f"{group_id}.launch":
            continue
        databricks_task_id = task.databricks_task_key
        log.debug("databricks task id for task %s is %s", task_id, databricks_task_id)
        task_ids.append(databricks_task_id)
    return task_ids


def _repair_task(
    databricks_conn_id: str,
    databricks_run_id: int,
    tasks_to_repair: list[str],
    logger: Logger | logging.Logger,
) -> int:
    """
    Repair a Databricks task using the Databricks API.

    This function allows the Airflow repair buttons to create a repair job for Databricks.
    It uses the Databricks API to get the latest repair ID before sending the repair query.

    :param databricks_conn_id: The Databricks connection ID.
    :param databricks_run_id: The Databricks run ID.
    :param tasks_to_repair: A list of Databricks task IDs to repair.
    :param logger: The logger to use for logging.
    :return: the repair id returned by the Databricks API.
    """
    hook = DatabricksHook(databricks_conn_id=databricks_conn_id)

    repair_history_id = hook.get_latest_repair_id(databricks_run_id)
    logger.debug("Latest repair ID is %s", repair_history_id)
    logger.debug(
        "Sending repair query for tasks %s on run %s",
        tasks_to_repair,
        databricks_run_id,
    )

    run_data = hook.get_run(databricks_run_id)
    repair_json = {
        "run_id": databricks_run_id,
        "latest_repair_id": repair_history_id,
        "rerun_tasks": tasks_to_repair,
        # Also rerun dependents so upstream-failed downstream tasks resume rather than
        # staying skipped after the repaired task succeeds.
        "rerun_dependent_tasks": True,
    }

    if "overriding_parameters" in run_data:
        repair_json["overriding_parameters"] = run_data["overriding_parameters"]

    return hook.repair_run(repair_json)


# TODO: Need to re-think on how to support the currently unavailable repair functionality in Airflow 3. Probably a
# good time to re-evaluate this would be once the plugin functionality is expanded in Airflow 3.1.
if not AIRFLOW_V_3_0_PLUS:
    from flask import flash, redirect, request, url_for
    from flask_appbuilder import BaseView
    from flask_appbuilder.api import expose

    try:
        from sqlalchemy import select
    except ImportError:
        select = None  # type: ignore[assignment,misc]
    from airflow.utils.session import NEW_SESSION, provide_session
    from airflow.www import auth

    def get_auth_decorator():
        from airflow.auth.managers.models.resource_details import DagAccessEntity

        return auth.has_access_dag("POST", DagAccessEntity.RUN)

    class RepairDatabricksTasks(BaseView, LoggingMixin):
        """Repair databricks tasks from Airflow."""

        default_view = "repair"

        @expose("/repair_databricks_job/<string:dag_id>/<string:run_id>", methods=("GET",))
        @get_auth_decorator()
        def repair(self, dag_id: str, run_id: str):
            return_url = self._get_return_url(dag_id, run_id)

            tasks_to_repair = request.values.get("tasks_to_repair")
            self.log.info("Tasks to repair: %s", tasks_to_repair)
            if not tasks_to_repair:
                flash("No tasks to repair. Not sending repair request.")
                return redirect(return_url)

            databricks_conn_id = request.values.get("databricks_conn_id")
            databricks_run_id = request.values.get("databricks_run_id")

            if not databricks_conn_id:
                flash("No Databricks connection ID provided. Cannot repair tasks.")
                return redirect(return_url)

            if not databricks_run_id:
                flash("No Databricks run ID provided. Cannot repair tasks.")
                return redirect(return_url)

            self.log.info("Repairing databricks job %s", databricks_run_id)
            res = _repair_task(
                databricks_conn_id=databricks_conn_id,
                databricks_run_id=int(databricks_run_id),
                tasks_to_repair=tasks_to_repair.split(","),
                logger=self.log,
            )
            self.log.info("Repairing databricks job query for run %s sent", databricks_run_id)

            self.log.info("Clearing tasks to rerun in airflow")

            run_id = unquote(run_id)
            _clear_task_instances(dag_id, run_id, tasks_to_repair.split(","), self.log)
            flash(f"Databricks repair job is starting!: {res}")
            return redirect(return_url)

        @staticmethod
        def _get_return_url(dag_id: str, run_id: str) -> str:
            return url_for("Airflow.grid", dag_id=dag_id, dag_run_id=run_id)

    def _get_dag(dag_id: str, session: Session):
        from airflow.models.serialized_dag import SerializedDagModel

        dag = SerializedDagModel.get_dag(dag_id, session=session)
        if not dag:
            raise AirflowException("Dag not found.")
        return dag

    def _get_dagrun(dag, run_id: str, session: Session) -> DagRun:
        """
        Retrieve the DagRun object associated with the specified DAG and run_id.

        :param dag: The DAG object associated with the DagRun to retrieve.
        :param run_id: The run_id associated with the DagRun to retrieve.
        :param session: The SQLAlchemy session to use for the query. If None, uses the default session.
        :return: The DagRun object associated with the specified DAG and run_id.
        """
        if select is None:
            raise AirflowOptionalProviderFeatureException(
                "sqlalchemy is required for workflow repair functionality. "
                "Install it with: pip install 'apache-airflow-providers-databricks[sqlalchemy]'"
            )
        if not session:
            raise AirflowException("Session not provided.")

        return session.scalars(
            select(DagRun).where(DagRun.dag_id == dag.dag_id, DagRun.run_id == run_id)
        ).one()

    @provide_session
    def _clear_task_instances(
        dag_id: str, run_id: str, task_ids: list[str], log: Logger, *, session: Session = NEW_SESSION
    ) -> None:
        dag = _get_dag(dag_id, session=session)
        log.debug("task_ids %s to clear", str(task_ids))
        dr: DagRun = _get_dagrun(dag, run_id, session=session)
        tis_to_clear = [ti for ti in dr.get_task_instances() if ti.databricks_task_key in task_ids]
        clear_task_instances(tis_to_clear, session)

    @provide_session
    def get_task_instance(operator: BaseOperator, dttm, *, session: Session = NEW_SESSION) -> TaskInstance:
        if select is None:
            raise AirflowOptionalProviderFeatureException(
                "sqlalchemy is required to get task instance. "
                "Install it with: pip install 'apache-airflow-providers-databricks[sqlalchemy]'"
            )
        dag_id = operator.dag.dag_id
        if hasattr(DagRun, "execution_date"):  # Airflow 2.x.
            dag_run = DagRun.find(dag_id, execution_date=dttm)[0]  # type: ignore[call-arg]
        else:
            dag_run = DagRun.find(dag_id, logical_date=dttm)[0]
        ti = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == dag_run.run_id,
                TaskInstance.task_id == operator.task_id,
            )
        ).one_or_none()
        if not ti:
            raise TaskInstanceNotFound("Task instance not found")
        return ti


def get_launch_task_id(task_group: TaskGroup) -> str:
    """
    Retrieve the launch task ID from the current task group or a parent task group, recursively.

    :param task_group: Task Group to be inspected
    :return: launch Task ID
    """
    try:
        launch_task_id = task_group.get_child_by_label("launch").task_id  # type: ignore[attr-defined]
    except KeyError as e:
        if not task_group.parent_group:
            raise AirflowException("No launch task can be found in the task group.") from e
        launch_task_id = get_launch_task_id(task_group.parent_group)

    return launch_task_id


def _get_launch_task_key(current_task_key: TaskInstanceKey, task_id: str) -> TaskInstanceKey:
    """
    Return the task key for the launch task.

    This allows us to gather databricks Metadata even if the current task has failed (since tasks only
    create xcom values if they succeed).

    :param current_task_key: The task key for the current task.
    :param task_id: The task ID for the current task.
    :return: The task key for the launch task.
    """
    if task_id:
        return TaskInstanceKey(
            dag_id=current_task_key.dag_id,
            task_id=task_id,
            run_id=current_task_key.run_id,
            try_number=current_task_key.try_number,
        )

    return current_task_key


def get_xcom_result(
    ti_key: TaskInstanceKey,
    key: str,
) -> Any:
    result = XCom.get_value(
        ti_key=ti_key,
        key=key,
    )
    from airflow.providers.databricks.operators.databricks_workflow import WorkflowRunMetadata

    return WorkflowRunMetadata(**result)


class WorkflowJobRunLink(BaseOperatorLink, LoggingMixin):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "See Databricks Job Run"

    @property
    def xcom_key(self) -> str:
        """XCom key where the link is stored during task execution."""
        return "databricks_job_run_link"

    def get_link(  # type: ignore[override]  # Signature intentionally kept this way for Airflow 2.x compatibility
        self,
        operator: BaseOperator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        if AIRFLOW_V_3_0_PLUS:
            # Use public XCom API to get the pre-computed link
            try:
                link = XCom.get_value(
                    ti_key=ti_key,
                    key=self.xcom_key,
                )
                return link if link else ""
            except Exception as e:
                self.log.warning("Failed to retrieve Databricks job run link from XCom: %s", e)
                return ""
        else:
            # Airflow 2.x - keep original implementation
            return self._get_link_legacy(operator, dttm, ti_key=ti_key)

    def _get_link_legacy(
        self,
        operator: BaseOperator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        """Legacy implementation for Airflow 2.x."""
        if not ti_key:
            ti = get_task_instance(operator, dttm)
            ti_key = ti.key
        task_group = operator.task_group
        if not task_group:
            raise AirflowException("Task group is required for generating Databricks Workflow Job Run Link.")
        self.log.info("Getting link for task %s", ti_key.task_id)
        if ".launch" not in ti_key.task_id:
            self.log.debug("Finding the launch task for job run metadata %s", ti_key.task_id)
            launch_task_id = get_launch_task_id(task_group)
            ti_key = _get_launch_task_key(ti_key, task_id=launch_task_id)
        metadata = get_xcom_result(ti_key, "return_value")

        hook = DatabricksHook(metadata.conn_id)
        return f"https://{hook.host}/#job/{metadata.job_id}/run/{metadata.run_id}"


def store_databricks_job_run_link(
    context: Context,
    metadata: Any,
    logger: Logger,
) -> None:
    """
    Store the Databricks job run link in XCom during task execution.

    This should be called by Databricks operators during their execution.
    """
    if not AIRFLOW_V_3_0_PLUS:
        return  # Only needed for Airflow 3

    try:
        hook = DatabricksHook(metadata.conn_id)
        link = f"https://{hook.host}/#job/{metadata.job_id}/run/{metadata.run_id}"

        # Store the link in XCom for the UI to retrieve as extra link
        context["ti"].xcom_push(key="databricks_job_run_link", value=link)
        logger.info("Stored Databricks job run link in XCom: %s", link)
    except Exception as e:
        logger.warning("Failed to store Databricks job run link: %s", e)


class WorkflowJobRepairAllFailedLink(BaseOperatorLink, LoggingMixin):
    """Constructs a link to send a request to repair all failed tasks in the Databricks workflow."""

    name = "Repair All Failed Tasks"

    def get_link(  # type: ignore[override]  # Signature intentionally kept this way for Airflow 2.x compatibility
        self,
        operator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        if AIRFLOW_V_3_0_PLUS:
            if ti_key is None:
                return ""
            metadata = _get_launch_metadata_v3(operator, ti_key)
            if not metadata:
                return ""
            # The set of failed tasks is resolved from the live Databricks run by the endpoint.
            return _build_repair_url(
                ti_key.dag_id, ti_key.run_id, metadata.conn_id, metadata.run_id, repair_all=True
            )

        if not ti_key:
            ti = get_task_instance(operator, dttm)
            ti_key = ti.key
        task_group = operator.task_group
        self.log.debug(
            "Creating link to repair all tasks for databricks job run %s",
            task_group.group_id,
        )

        metadata = get_xcom_result(ti_key, "return_value")

        tasks_str = self.get_tasks_to_run(ti_key, operator, self.log)
        self.log.debug("tasks to rerun: %s", tasks_str)

        query_params = {
            "dag_id": ti_key.dag_id,
            "databricks_conn_id": metadata.conn_id,
            "databricks_run_id": metadata.run_id,
            "run_id": ti_key.run_id,
            "tasks_to_repair": tasks_str,
        }

        return url_for("RepairDatabricksTasks.repair", **query_params)

    @classmethod
    def get_task_group_children(cls, task_group: TaskGroup) -> dict[str, BaseOperator]:
        """
        Given a TaskGroup, return children which are Tasks, inspecting recursively any TaskGroups within.

        :param task_group: An Airflow TaskGroup
        :return: Dictionary that contains Task IDs as keys and Tasks as values.
        """
        children: dict[str, Any] = {}
        for child_id, child in task_group.children.items():
            if isinstance(child, TaskGroup):
                child_children = cls.get_task_group_children(child)
                children = {**children, **child_children}
            else:
                children[child_id] = child
        return children

    def get_tasks_to_run(self, ti_key: TaskInstanceKey, operator: BaseOperator, log: Logger) -> str:
        task_group = operator.task_group
        if not task_group:
            raise AirflowException("Task group is required for generating repair link.")
        if not task_group.group_id:
            raise AirflowException("Task group ID is required for generating repair link.")

        from airflow.utils.session import create_session

        with create_session() as session:
            dag = _get_dag(ti_key.dag_id, session=session)
            dr = _get_dagrun(dag, ti_key.run_id, session=session)
        log.debug("Getting failed and skipped tasks for dag run %s", dr.run_id)
        task_group_sub_tasks = self.get_task_group_children(task_group).items()
        failed_and_skipped_tasks = self._get_failed_and_skipped_tasks(dr)
        log.debug("Failed and skipped tasks: %s", failed_and_skipped_tasks)

        tasks_to_run = {ti: t for ti, t in task_group_sub_tasks if ti in failed_and_skipped_tasks}

        return ",".join(get_databricks_task_ids(task_group.group_id, tasks_to_run, log))  # type: ignore[arg-type]

    @staticmethod
    def _get_failed_and_skipped_tasks(dr: DagRun) -> list[str]:
        """
        Return a list of task IDs for tasks that have failed or have been skipped in the given DagRun.

        :param dr: The DagRun object for which to retrieve failed and skipped tasks.

        :return: A list of task IDs for tasks that have failed or have been skipped.
        """
        return [
            t.task_id
            for t in dr.get_task_instances(
                state=[
                    TaskInstanceState.FAILED,
                    TaskInstanceState.SKIPPED,
                    TaskInstanceState.UP_FOR_RETRY,
                    TaskInstanceState.UPSTREAM_FAILED,
                    None,
                ],
            )
        ]


class WorkflowJobRepairSingleTaskLink(BaseOperatorLink, LoggingMixin):
    """Construct a link to send a repair request for a single databricks task."""

    name = "Repair a single task"

    def get_link(  # type: ignore[override]  # Signature intentionally kept this way for Airflow 2.x compatibility
        self,
        operator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        if AIRFLOW_V_3_0_PLUS:
            if ti_key is None:
                return ""
            metadata = _get_launch_metadata_v3(operator, ti_key)
            if not metadata:
                return ""
            return _build_repair_url(
                ti_key.dag_id,
                ti_key.run_id,
                metadata.conn_id,
                metadata.run_id,
                tasks_to_repair=[operator.databricks_task_key],
            )

        if not ti_key:
            ti = get_task_instance(operator, dttm)
            ti_key = ti.key

        task_group = operator.task_group
        if not task_group:
            raise AirflowException("Task group is required for generating repair link.")

        self.log.info(
            "Creating link to repair a single task for databricks job run %s task %s",
            task_group.group_id,
            ti_key.task_id,
        )

        from airflow.utils.session import create_session

        with create_session() as session:
            dag = _get_dag(ti_key.dag_id, session=session)
        task = dag.get_task(ti_key.task_id)
        if TYPE_CHECKING:
            assert isinstance(task, DatabricksTaskBaseOperator)

        if ".launch" not in ti_key.task_id:
            launch_task_id = get_launch_task_id(task_group)
            ti_key = _get_launch_task_key(ti_key, task_id=launch_task_id)
        metadata = get_xcom_result(ti_key, "return_value")

        query_params = {
            "dag_id": ti_key.dag_id,
            "databricks_conn_id": metadata.conn_id,
            "databricks_run_id": metadata.run_id,
            "run_id": ti_key.run_id,
            "tasks_to_repair": task.databricks_task_key,
        }
        return url_for("RepairDatabricksTasks.repair", **query_params)


# Airflow-3 repair backend. Flask-AppBuilder was dropped in Airflow 3, so the repair
# action is re-implemented as a FastAPI sub-application mounted on the API server, and the
# repair links (below) build URLs that point at it.
REPAIR_URL_PREFIX = "/databricks/workflow/repair"


def _build_repair_url(
    dag_id: str,
    run_id: str,
    databricks_conn_id: str,
    databricks_run_id: int,
    *,
    repair_all: bool = False,
    tasks_to_repair: list[str] | None = None,
) -> str:
    """Build the URL to the Airflow-3 FastAPI repair endpoint for a workflow run."""
    from urllib.parse import quote, urlencode

    from airflow.configuration import conf

    query: dict[str, Any] = {
        "databricks_conn_id": databricks_conn_id,
        "databricks_run_id": databricks_run_id,
    }
    if repair_all:
        query["repair_all"] = "true"
    if tasks_to_repair:
        query["tasks_to_repair"] = ",".join(tasks_to_repair)

    base_url = conf.get("api", "base_url", fallback="").rstrip("/")
    return f"{base_url}{REPAIR_URL_PREFIX}/{dag_id}/{quote(run_id, safe='')}?{urlencode(query)}"


def _get_launch_metadata_v3(operator: BaseOperator, ti_key: TaskInstanceKey) -> Any:
    """
    Read the launch task's ``WorkflowRunMetadata`` (conn_id, job_id, run_id) from XCom.

    Uses only the public XCom API keyed by ``ti_key`` — no metadata-DB access — which is the
    only mechanism available to extra links on Airflow 3.
    """
    task_group = operator.task_group
    if not task_group:
        return None
    if ".launch" not in ti_key.task_id:
        launch_task_id = get_launch_task_id(task_group)
        ti_key = _get_launch_task_key(ti_key, task_id=launch_task_id)
    result = XCom.get_value(ti_key=ti_key, key="return_value")
    if not result:
        return None
    from airflow.providers.databricks.operators.databricks_workflow import WorkflowRunMetadata

    return WorkflowRunMetadata(**result)


if AIRFLOW_V_3_0_PLUS:
    from fastapi import Depends, FastAPI, HTTPException, Request
    from fastapi.responses import RedirectResponse

    from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
    from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
    from airflow.api_fastapi.core_api.security import resolve_user_from_token

    repair_app = FastAPI(
        title="Databricks Workflow Repair",
        description="Repair failed tasks of a Databricks workflow run from Airflow.",
    )

    async def _resolve_request_user(request: Request):
        """Authenticate via the bearer header (UI XHR) or the ``_token`` cookie (link navigation)."""
        token = None
        auth_header = request.headers.get("Authorization", "")
        if auth_header.lower().startswith("bearer "):
            token = auth_header.split(" ", 1)[1]
        if not token:
            token = request.cookies.get(COOKIE_NAME_JWT_TOKEN)
        # resolve_user_from_token raises HTTP 401 for a missing/invalid token.
        return await resolve_user_from_token(token)

    async def _require_dag_run_edit(dag_id: str, request: Request):
        from airflow.api_fastapi.app import get_auth_manager

        user = await _resolve_request_user(request)
        authorized = get_auth_manager().is_authorized_dag(
            method="PUT",
            access_entity=DagAccessEntity.RUN,
            details=DagDetails(id=dag_id),
            user=user,
        )
        if not authorized:
            raise HTTPException(status_code=403, detail="Not authorized to repair runs of this Dag.")
        return user

    def _clear_repaired_and_downstream(
        dag_id: str, run_id: str, task_keys: list[str], logger: logging.Logger
    ) -> None:
        """
        Clear the repaired tasks' instances and their downstream instances for this run.

        Runs inside the API server (the DB-facing component), so clearing the repaired tasks plus
        their downstream lets the upstream-failed dependents resume deterministically when the
        repaired Databricks sub-runs succeed — without clearing the whole Dag.
        """
        import hashlib

        from sqlalchemy import select

        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.models.taskinstance import clear_task_instances
        from airflow.utils.session import create_session

        with create_session() as session:
            dag = SerializedDagModel.get_dag(dag_id, session=session)
            if dag is None:
                raise HTTPException(status_code=404, detail=f"Dag {dag_id} not found.")

            # Serialized tasks don't expose the operator's ``databricks_task_key`` property, so
            # reproduce its default: an explicit key if present, else md5(dag_id__task_id).
            key_to_task_id = {}
            for task in dag.tasks:
                task_key = (
                    getattr(task, "databricks_task_key", None)
                    or hashlib.md5(f"{dag_id}__{task.task_id}".encode()).hexdigest()
                )
                key_to_task_id[task_key] = task.task_id

            repaired_task_ids = [key_to_task_id[k] for k in task_keys if k in key_to_task_id]
            target_task_ids: set[str] = set(repaired_task_ids)
            for task_id in repaired_task_ids:
                target_task_ids.update(dag.get_task(task_id).get_flat_relative_ids(upstream=False))

            dr = session.scalars(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id)).one()
            tis_to_clear = [
                ti for ti in dr.get_task_instances(session=session) if ti.task_id in target_task_ids
            ]
            logger.info("Clearing %s task instances after Databricks repair", len(tis_to_clear))
            clear_task_instances(tis_to_clear, session)
            session.commit()

    @repair_app.get("/{dag_id}/{run_id}")
    def repair_databricks_workflow(
        dag_id: str,
        run_id: str,
        databricks_conn_id: str,
        databricks_run_id: int,
        tasks_to_repair: str | None = None,
        repair_all: bool = False,
        _user=Depends(_require_dag_run_edit),
    ):
        """Repair failed Databricks tasks for a workflow run and resume the Airflow run."""
        run_id = unquote(run_id)
        from airflow.configuration import conf

        base_url = conf.get("api", "base_url", fallback="").rstrip("/")
        return_url = f"{base_url}/dags/{dag_id}/runs/{run_id}"

        # Databricks API calls can fail (e.g. expired/invalid connection token); surface a clear
        # error to the UI instead of a bare 500.
        try:
            hook = DatabricksHook(databricks_conn_id=databricks_conn_id)
            if repair_all:
                task_keys = hook.get_run_failed_task_keys(databricks_run_id)
            elif tasks_to_repair:
                task_keys = tasks_to_repair.split(",")
            else:
                task_keys = []

            if not task_keys:
                log.info("No failed Databricks tasks to repair for run %s", databricks_run_id)
                return RedirectResponse(return_url, status_code=303)

            log.info("Repairing Databricks run %s tasks %s", databricks_run_id, task_keys)
            _repair_task(
                databricks_conn_id=databricks_conn_id,
                databricks_run_id=databricks_run_id,
                tasks_to_repair=task_keys,
                logger=log,
            )
        except HTTPException:
            raise
        except Exception as e:
            log.exception("Databricks repair failed for run %s", databricks_run_id)
            raise HTTPException(status_code=502, detail=f"Databricks repair request failed: {e}") from e

        _clear_repaired_and_downstream(dag_id, run_id, task_keys, log)
        return RedirectResponse(return_url, status_code=303)


class DatabricksWorkflowPlugin(AirflowPlugin):
    """
    Databricks Workflows plugin for Airflow.

    .. seealso::
        For more information on how to use this plugin, take a look at the guide:
        :ref:`howto/plugin:DatabricksWorkflowPlugin`
    """

    name = "databricks_workflow"

    operator_extra_links = [
        WorkflowJobRepairAllFailedLink(),
        WorkflowJobRepairSingleTaskLink(),
        WorkflowJobRunLink(),
    ]

    if AIRFLOW_V_3_0_PLUS:
        # Airflow 3: repair is served by a FastAPI sub-application on the API server.
        fastapi_apps = [
            {
                "app": repair_app,
                "name": "Databricks Workflow Repair",
                "url_prefix": REPAIR_URL_PREFIX,
            }
        ]
    else:
        # Airflow 2.x: repair is served by a Flask-AppBuilder view.
        repair_databricks_view = RepairDatabricksTasks()
        repair_databricks_package = {
            "view": repair_databricks_view,
        }
        appbuilder_views = [repair_databricks_package]
