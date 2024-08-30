#
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
"""This module contains Databricks operators."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from functools import cached_property
from logging import Logger
from typing import TYPE_CHECKING, Any, Sequence

from deprecated import deprecated

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.providers.databricks.hooks.databricks import DatabricksHook, RunLifeCycleState, RunState
from airflow.providers.databricks.operators.databricks_workflow import (
    DatabricksWorkflowTaskGroup,
    WorkflowRunMetadata,
)
from airflow.providers.databricks.plugins.databricks_workflow import (
    WorkflowJobRepairSingleTaskLink,
    WorkflowJobRunLink,
)
from airflow.providers.databricks.triggers.databricks import DatabricksExecutionTrigger
from airflow.providers.databricks.utils.databricks import normalise_json_content, validate_trigger_event

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context
    from airflow.utils.task_group import TaskGroup

DEFER_METHOD_NAME = "execute_complete"
XCOM_RUN_ID_KEY = "run_id"
XCOM_JOB_ID_KEY = "job_id"
XCOM_RUN_PAGE_URL_KEY = "run_page_url"


def _handle_databricks_operator_execution(operator, hook, log, context) -> None:
    """
    Handle the Airflow + Databricks lifecycle logic for a Databricks operator.

    :param operator: Databricks operator being handled
    :param context: Airflow context
    """
    if operator.do_xcom_push and context is not None:
        context["ti"].xcom_push(key=XCOM_RUN_ID_KEY, value=operator.run_id)

    log.info("Run submitted with run_id: %s", operator.run_id)
    run_page_url = hook.get_run_page_url(operator.run_id)
    if operator.do_xcom_push and context is not None:
        context["ti"].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=run_page_url)

    if operator.wait_for_termination:
        while True:
            run_info = hook.get_run(operator.run_id)
            run_state = RunState(**run_info["state"])
            if run_state.is_terminal:
                if run_state.is_successful:
                    log.info("%s completed successfully.", operator.task_id)
                    log.info("View run status, Spark UI, and logs at %s", run_page_url)
                    return
                if run_state.result_state == "FAILED":
                    failed_tasks = []
                    for task in run_info.get("tasks", []):
                        if task.get("state", {}).get("result_state", "") == "FAILED":
                            task_run_id = task["run_id"]
                            task_key = task["task_key"]
                            run_output = hook.get_run_output(task_run_id)
                            if "error" in run_output:
                                error = run_output["error"]
                            else:
                                error = run_state.state_message
                            failed_tasks.append({"task_key": task_key, "run_id": task_run_id, "error": error})

                    error_message = (
                        f"{operator.task_id} failed with terminal state: {run_state} "
                        f"and with the errors {failed_tasks}"
                    )
                else:
                    error_message = (
                        f"{operator.task_id} failed with terminal state: {run_state} "
                        f"and with the error {run_state.state_message}"
                    )

                should_repair = (
                    isinstance(operator, DatabricksRunNowOperator)
                    and operator.repair_run
                    and (
                        not operator.databricks_repair_reason_new_settings
                        or is_repair_reason_match_exist(operator, run_state)
                    )
                )

                if should_repair:
                    operator.repair_run = False
                    log.warning(
                        "%s but since repair run is set, repairing the run with all failed tasks",
                        error_message,
                    )
                    job_id = operator.json["job_id"]
                    update_job_for_repair(operator, hook, job_id, run_state)
                    latest_repair_id = hook.get_latest_repair_id(operator.run_id)
                    repair_json = {"run_id": operator.run_id, "rerun_all_failed_tasks": True}
                    if latest_repair_id is not None:
                        repair_json["latest_repair_id"] = latest_repair_id
                    operator.json["latest_repair_id"] = hook.repair_run(operator, repair_json)
                    _handle_databricks_operator_execution(operator, hook, log, context)
                raise AirflowException(error_message)

            log.info("%s in run state: %s", operator.task_id, run_state)
            log.info("View run status, Spark UI, and logs at %s", run_page_url)
            log.info("Sleeping for %s seconds.", operator.polling_period_seconds)
            time.sleep(operator.polling_period_seconds)

    log.info("View run status, Spark UI, and logs at %s", run_page_url)


def is_repair_reason_match_exist(operator: Any, run_state: RunState) -> bool:
    """
    Check if the repair reason matches the run state message.

    :param operator: Databricks operator being handled
    :param run_state: Run state of the Databricks job
    :return: True if repair reason matches the run state message, False otherwise
    """
    return any(reason in run_state.state_message for reason in operator.databricks_repair_reason_new_settings)


def update_job_for_repair(operator: Any, hook: Any, job_id: int, run_state: RunState) -> None:
    """
    Update job settings(partial) to repair the run with all failed tasks.

    :param operator: Databricks operator being handled
    :param hook: Databricks hook
    :param job_id: Job ID of Databricks
    :param run_state: Run state of the Databricks job
    """
    repair_reason = next(
        (
            reason
            for reason in operator.databricks_repair_reason_new_settings
            if reason in run_state.state_message
        ),
        None,
    )
    if repair_reason is not None:
        new_settings_json = normalise_json_content(
            operator.databricks_repair_reason_new_settings[repair_reason]
        )
        hook.update_job(job_id=job_id, json=new_settings_json)


def _handle_deferrable_databricks_operator_execution(operator, hook, log, context) -> None:
    """
    Handle the Airflow + Databricks lifecycle logic for deferrable Databricks operators.

    :param operator: Databricks async operator being handled
    :param context: Airflow context
    """
    job_id = hook.get_job_id(operator.run_id)
    if operator.do_xcom_push and context is not None:
        context["ti"].xcom_push(key=XCOM_JOB_ID_KEY, value=job_id)
    if operator.do_xcom_push and context is not None:
        context["ti"].xcom_push(key=XCOM_RUN_ID_KEY, value=operator.run_id)
    log.info("Run submitted with run_id: %s", operator.run_id)

    run_page_url = hook.get_run_page_url(operator.run_id)
    if operator.do_xcom_push and context is not None:
        context["ti"].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=run_page_url)
    log.info("View run status, Spark UI, and logs at %s", run_page_url)

    if operator.wait_for_termination:
        run_info = hook.get_run(operator.run_id)
        run_state = RunState(**run_info["state"])
        if not run_state.is_terminal:
            operator.defer(
                trigger=DatabricksExecutionTrigger(
                    run_id=operator.run_id,
                    databricks_conn_id=operator.databricks_conn_id,
                    polling_period_seconds=operator.polling_period_seconds,
                    retry_limit=operator.databricks_retry_limit,
                    retry_delay=operator.databricks_retry_delay,
                    retry_args=operator.databricks_retry_args,
                    run_page_url=run_page_url,
                    repair_run=getattr(operator, "repair_run", False),
                ),
                method_name=DEFER_METHOD_NAME,
            )
        else:
            if run_state.is_successful:
                log.info("%s completed successfully.", operator.task_id)


def _handle_deferrable_databricks_operator_completion(event: dict, log: Logger) -> None:
    validate_trigger_event(event)
    run_state = RunState.from_json(event["run_state"])
    run_page_url = event["run_page_url"]
    errors = event["errors"]
    log.info("View run status, Spark UI, and logs at %s", run_page_url)

    if run_state.is_successful:
        log.info("Job run completed successfully.")
        return

    error_message = f"Job run failed with terminal state: {run_state} and with the errors {errors}"

    if event.get("repair_run"):
        log.warning(
            "%s but since repair run is set, repairing the run with all failed tasks",
            error_message,
        )
        return
    raise AirflowException(error_message)


class DatabricksJobRunLink(BaseOperatorLink):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "See Databricks Job Run"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        return XCom.get_value(key=XCOM_RUN_PAGE_URL_KEY, ti_key=ti_key)


class DatabricksCreateJobsOperator(BaseOperator):
    """
    Creates (or resets) a Databricks job using the API endpoint.

    .. seealso::
        https://docs.databricks.com/api/workspace/jobs/create
        https://docs.databricks.com/api/workspace/jobs/reset

    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/create`` endpoint. The other named parameters
        (i.e. ``name``, ``tags``, ``tasks``, etc.) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        .. seealso::
            For more information about templating see :ref:`concepts:jinja-templating`.
    :param name: An optional name for the job.
    :param description: An optional description for the job.
    :param tags: A map of tags associated with the job.
    :param tasks: A list of task specifications to be executed by this job.
        Array of objects (JobTaskSettings).
    :param job_clusters: A list of job cluster specifications that can be shared and reused by
        tasks of this job. Array of objects (JobCluster).
    :param email_notifications: Object (JobEmailNotifications).
    :param webhook_notifications: Object (WebhookNotifications).
    :param notification_settings: Optional notification settings.
    :param timeout_seconds: An optional timeout applied to each run of this job.
    :param schedule: Object (CronSchedule).
    :param max_concurrent_runs: An optional maximum allowed number of concurrent runs of the job.
    :param git_source: An optional specification for a remote repository containing the notebooks
        used by this job's notebook tasks. Object (GitSource).
    :param access_control_list: List of permissions to set on the job. Array of object
        (AccessControlRequestForUser) or object (AccessControlRequestForGroup) or object
        (AccessControlRequestForServicePrincipal).

        .. seealso::
            This will only be used on create. In order to reset ACL consider using the Databricks
            UI.
    :param databricks_conn_id: Reference to the
        :ref:`Databricks connection <howto/connection:databricks>`. (templated)
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.

    """

    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ("json", "databricks_conn_id")
    # Databricks brand color (blue) under white text
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    def __init__(
        self,
        *,
        json: Any | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: dict[str, str] | None = None,
        tasks: list[dict] | None = None,
        job_clusters: list[dict] | None = None,
        email_notifications: dict | None = None,
        webhook_notifications: dict | None = None,
        notification_settings: dict | None = None,
        timeout_seconds: int | None = None,
        schedule: dict | None = None,
        max_concurrent_runs: int | None = None,
        git_source: dict | None = None,
        access_control_list: list[dict] | None = None,
        databricks_conn_id: str = "databricks_default",
        polling_period_seconds: int = 30,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        databricks_retry_args: dict[Any, Any] | None = None,
        **kwargs,
    ) -> None:
        """Create a new ``DatabricksCreateJobsOperator``."""
        super().__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        if name is not None:
            self.json["name"] = name
        if description is not None:
            self.json["description"] = description
        if tags is not None:
            self.json["tags"] = tags
        if tasks is not None:
            self.json["tasks"] = tasks
        if job_clusters is not None:
            self.json["job_clusters"] = job_clusters
        if email_notifications is not None:
            self.json["email_notifications"] = email_notifications
        if webhook_notifications is not None:
            self.json["webhook_notifications"] = webhook_notifications
        if notification_settings is not None:
            self.json["notification_settings"] = notification_settings
        if timeout_seconds is not None:
            self.json["timeout_seconds"] = timeout_seconds
        if schedule is not None:
            self.json["schedule"] = schedule
        if max_concurrent_runs is not None:
            self.json["max_concurrent_runs"] = max_concurrent_runs
        if git_source is not None:
            self.json["git_source"] = git_source
        if access_control_list is not None:
            self.json["access_control_list"] = access_control_list
        if self.json:
            self.json = normalise_json_content(self.json)

    @cached_property
    def _hook(self):
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller="DatabricksCreateJobsOperator",
        )

    def execute(self, context: Context) -> int:
        if "name" not in self.json:
            raise AirflowException("Missing required parameter: name")
        job_id = self._hook.find_job_id_by_name(self.json["name"])
        if job_id is None:
            return self._hook.create_job(self.json)
        self._hook.reset_job(str(job_id), self.json)
        if (access_control_list := self.json.get("access_control_list")) is not None:
            acl_json = {"access_control_list": access_control_list}
            self._hook.update_job_permission(job_id, normalise_json_content(acl_json))

        return job_id


class DatabricksSubmitRunOperator(BaseOperator):
    """
    Submits a Spark job run to Databricks using the api/2.1/jobs/runs/submit API endpoint.

    See: https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit

    There are three ways to instantiate this operator.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksSubmitRunOperator`

    :param tasks: Array of Objects(RunSubmitTaskSettings) <= 100 items.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit
    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/runs/submit`` endpoint. The other named parameters
        (i.e. ``spark_jar_task``, ``notebook_task``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        .. seealso::
            For more information about templating see :ref:`concepts:jinja-templating`.
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit
    :param spark_jar_task: The main class and parameters for the JAR task. Note that
        the actual JAR is specified in the ``libraries``.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` *OR* ``dbt_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparkjartask
    :param notebook_task: The notebook path and parameters for the notebook task.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` *OR* ``dbt_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobsnotebooktask
    :param spark_python_task: The python file path and parameters to run the python file with.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` *OR* ``dbt_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparkpythontask
    :param spark_submit_task: Parameters needed to run a spark-submit command.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` *OR* ``dbt_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparksubmittask
    :param pipeline_task: Parameters needed to execute a Delta Live Tables pipeline task.
        The provided dictionary must contain at least ``pipeline_id`` field!
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` *OR* ``dbt_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobspipelinetask
    :param dbt_task: Parameters needed to execute a dbt task.
        The provided dictionary must contain at least the ``commands`` field and the
        ``git_source`` parameter also needs to be set.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` *OR* ``dbt_task`` should be specified.
        This field will be templated.

    :param new_cluster: Specs for a new cluster on which this task will be run.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified
        (except when ``pipeline_task`` is used).
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobsclusterspecnewcluster
    :param existing_cluster_id: ID for existing cluster on which to run this task.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified
        (except when ``pipeline_task`` is used).
        This field will be templated.
    :param libraries: Libraries which this run will use.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#managedlibrarieslibrary
    :param run_name: The run name used for this task.
        By default this will be set to the Airflow ``task_id``. This ``task_id`` is a
        required parameter of the superclass ``BaseOperator``.
        This field will be templated.
    :param idempotency_token: an optional token that can be used to guarantee the idempotency of job run
        requests. If a run with the provided token already exists, the request does not create a new run but
        returns the ID of the existing run instead.  This token must have at most 64 characters.
    :param access_control_list: optional list of dictionaries representing Access Control List (ACL) for
        a given job run.  Each dictionary consists of following field - specific subject (``user_name`` for
        users, or ``group_name`` for groups), and ``permission_level`` for that subject.  See Jobs API
        documentation for more details.
    :param wait_for_termination: if we should wait for termination of the job run. ``True`` by default.
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty. (templated)
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    :param git_source: Optional specification of a remote git repository from which
        supported task types are retrieved.
    :param deferrable: Run operator in the deferrable mode.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit
    """

    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ("json", "databricks_conn_id")
    template_ext: Sequence[str] = (".json-tpl",)
    # Databricks brand color (blue) under white text
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"
    operator_extra_links = (DatabricksJobRunLink(),)

    def __init__(
        self,
        *,
        json: Any | None = None,
        tasks: list[object] | None = None,
        spark_jar_task: dict[str, str] | None = None,
        notebook_task: dict[str, str] | None = None,
        spark_python_task: dict[str, str | list[str]] | None = None,
        spark_submit_task: dict[str, list[str]] | None = None,
        pipeline_task: dict[str, str] | None = None,
        dbt_task: dict[str, str | list[str]] | None = None,
        new_cluster: dict[str, object] | None = None,
        existing_cluster_id: str | None = None,
        libraries: list[dict[str, Any]] | None = None,
        run_name: str | None = None,
        timeout_seconds: int | None = None,
        databricks_conn_id: str = "databricks_default",
        polling_period_seconds: int = 30,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        databricks_retry_args: dict[Any, Any] | None = None,
        do_xcom_push: bool = True,
        idempotency_token: str | None = None,
        access_control_list: list[dict[str, str]] | None = None,
        wait_for_termination: bool = True,
        git_source: dict[str, str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        """Create a new ``DatabricksSubmitRunOperator``."""
        super().__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        self.wait_for_termination = wait_for_termination
        self.deferrable = deferrable
        if tasks is not None:
            self.json["tasks"] = tasks
        if spark_jar_task is not None:
            self.json["spark_jar_task"] = spark_jar_task
        if notebook_task is not None:
            self.json["notebook_task"] = notebook_task
        if spark_python_task is not None:
            self.json["spark_python_task"] = spark_python_task
        if spark_submit_task is not None:
            self.json["spark_submit_task"] = spark_submit_task
        if pipeline_task is not None:
            self.json["pipeline_task"] = pipeline_task
        if dbt_task is not None:
            self.json["dbt_task"] = dbt_task
        if new_cluster is not None:
            self.json["new_cluster"] = new_cluster
        if existing_cluster_id is not None:
            self.json["existing_cluster_id"] = existing_cluster_id
        if libraries is not None:
            self.json["libraries"] = libraries
        if run_name is not None:
            self.json["run_name"] = run_name
        if timeout_seconds is not None:
            self.json["timeout_seconds"] = timeout_seconds
        if "run_name" not in self.json:
            self.json["run_name"] = run_name or kwargs["task_id"]
        if idempotency_token is not None:
            self.json["idempotency_token"] = idempotency_token
        if access_control_list is not None:
            self.json["access_control_list"] = access_control_list
        if git_source is not None:
            self.json["git_source"] = git_source

        if "dbt_task" in self.json and "git_source" not in self.json:
            raise AirflowException("git_source is required for dbt_task")
        if pipeline_task is not None and "pipeline_id" in pipeline_task and "pipeline_name" in pipeline_task:
            raise AirflowException("'pipeline_name' is not allowed in conjunction with 'pipeline_id'")

        # This variable will be used in case our task gets killed.
        self.run_id: int | None = None
        self.do_xcom_push = do_xcom_push

    @cached_property
    def _hook(self):
        return self._get_hook(caller="DatabricksSubmitRunOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def execute(self, context: Context):
        if (
            "pipeline_task" in self.json
            and self.json["pipeline_task"].get("pipeline_id") is None
            and self.json["pipeline_task"].get("pipeline_name")
        ):
            # If pipeline_id is not provided, we need to fetch it from the pipeline_name
            pipeline_name = self.json["pipeline_task"]["pipeline_name"]
            self.json["pipeline_task"]["pipeline_id"] = self._hook.find_pipeline_id_by_name(pipeline_name)
            del self.json["pipeline_task"]["pipeline_name"]
        json_normalised = normalise_json_content(self.json)
        self.run_id = self._hook.submit_run(json_normalised)
        if self.deferrable:
            _handle_deferrable_databricks_operator_execution(self, self._hook, self.log, context)
        else:
            _handle_databricks_operator_execution(self, self._hook, self.log, context)

    def on_kill(self):
        if self.run_id:
            self._hook.cancel_run(self.run_id)
            self.log.info(
                "Task: %s with run_id: %s was requested to be cancelled.", self.task_id, self.run_id
            )
        else:
            self.log.error("Error: Task: %s with invalid run_id was requested to be cancelled.", self.task_id)

    def execute_complete(self, context: dict | None, event: dict):
        _handle_deferrable_databricks_operator_completion(event, self.log)


@deprecated(
    reason=(
        "`DatabricksSubmitRunDeferrableOperator` has been deprecated. "
        "Please use `airflow.providers.databricks.operators.DatabricksSubmitRunOperator` "
        "with `deferrable=True` instead."
    ),
    category=AirflowProviderDeprecationWarning,
)
class DatabricksSubmitRunDeferrableOperator(DatabricksSubmitRunOperator):
    """Deferrable version of ``DatabricksSubmitRunOperator``."""

    def __init__(self, *args, **kwargs):
        super().__init__(deferrable=True, *args, **kwargs)

    def execute(self, context):
        hook = self._get_hook(caller="DatabricksSubmitRunDeferrableOperator")
        json_normalised = normalise_json_content(self.json)
        self.run_id = hook.submit_run(json_normalised)
        _handle_deferrable_databricks_operator_execution(self, hook, self.log, context)


class DatabricksRunNowOperator(BaseOperator):
    """
    Runs an existing Spark job run to Databricks using the api/2.1/jobs/run-now API endpoint.

    See: https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.1/jobs/run-now`` endpoint and pass it directly
    to our ``DatabricksRunNowOperator`` through the ``json`` parameter.
    For example ::

        json = {
            "job_id": 42,
            "notebook_params": {"dry-run": "true", "oldest-time-to-consider": "1457570074236"},
        }

        notebook_run = DatabricksRunNowOperator(task_id="notebook_run", json=json)

    Another way to accomplish the same thing is to use the named parameters
    of the ``DatabricksRunNowOperator`` directly. Note that there is exactly
    one named parameter for each top level parameter in the ``run-now``
    endpoint. In this method, your code would look like this: ::

        job_id = 42

        notebook_params = {"dry-run": "true", "oldest-time-to-consider": "1457570074236"}

        python_params = ["douglas adams", "42"]

        jar_params = ["douglas adams", "42"]

        spark_submit_params = ["--class", "org.apache.spark.examples.SparkPi"]

        notebook_run = DatabricksRunNowOperator(
            job_id=job_id,
            notebook_params=notebook_params,
            python_params=python_params,
            jar_params=jar_params,
            spark_submit_params=spark_submit_params,
        )

    In the case where both the json parameter **AND** the named parameters
    are provided, they will be merged together. If there are conflicts during the merge,
    the named parameters will take precedence and override the top level ``json`` keys.

    Currently the named parameters that ``DatabricksRunNowOperator`` supports are
        - ``job_id``
        - ``job_name``
        - ``json``
        - ``notebook_params``
        - ``python_params``
        - ``python_named_parameters``
        - ``jar_params``
        - ``spark_submit_params``
        - ``idempotency_token``
        - ``repair_run``
        - ``databricks_repair_reason_new_settings``
        - ``cancel_previous_runs``

    :param job_id: the job_id of the existing Databricks job.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param job_name: the name of the existing Databricks job.
        It must exist only one job with the specified name.
        ``job_id`` and ``job_name`` are mutually exclusive.
        This field will be templated.
    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/run-now`` endpoint. The other named parameters
        (i.e. ``notebook_params``, ``spark_submit_params``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        .. seealso::
            For more information about templating see :ref:`concepts:jinja-templating`.
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param notebook_params: A dict from keys to values for jobs with notebook task,
        e.g. "notebook_params": {"name": "john doe", "age":  "35"}.
        The map is passed to the notebook and will be accessible through the
        dbutils.widgets.get function. See Widgets for more information.
        If not specified upon run-now, the triggered run will use the
        job's base parameters. notebook_params cannot be
        specified in conjunction with jar_params. The json representation
        of this field (i.e. {"notebook_params":{"name":"john doe","age":"35"}})
        cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/user-guide/notebooks/widgets.html
    :param python_params: A list of parameters for jobs with python tasks,
        e.g. "python_params": ["john doe", "35"].
        The parameters will be passed to python file as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified in job setting.
        The json representation of this field (i.e. {"python_params":["john doe","35"]})
        cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param python_named_params: A list of named parameters for jobs with python wheel tasks,
        e.g. "python_named_params": {"name": "john doe", "age":  "35"}.
        If specified upon run-now, it would overwrite the parameters specified in job setting.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param jar_params: A list of parameters for jobs with JAR tasks,
        e.g. "jar_params": ["john doe", "35"].
        The parameters will be passed to JAR file as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified in
        job setting.
        The json representation of this field (i.e. {"jar_params":["john doe","35"]})
        cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param spark_submit_params: A list of parameters for jobs with spark submit task,
        e.g. "spark_submit_params": ["--class", "org.apache.spark.examples.SparkPi"].
        The parameters will be passed to spark-submit script as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified
        in job setting.
        The json representation of this field cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param idempotency_token: an optional token that can be used to guarantee the idempotency of job run
        requests. If a run with the provided token already exists, the request does not create a new run but
        returns the ID of the existing run instead.  This token must have at most 64 characters.
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty. (templated)
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default, the operator will poll every 30 seconds.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    :param wait_for_termination: if we should wait for termination of the job run. ``True`` by default.
    :param deferrable: Run operator in the deferrable mode.
    :param repair_run: Repair the databricks run in case of failure.
    :param databricks_repair_reason_new_settings: A dict of reason and new_settings JSON object for which
            to repair the run. `None` by default. `None` means to repair at all cases with existing job
            settings otherwise check whether `RunState` state_message contains reason and
            update job settings as per new_settings using databricks partial job update endpoint
            (https://docs.databricks.com/api/workspace/jobs/update). If nothing is matched, then repair
            will not get triggered.
    :param cancel_previous_runs: Cancel all existing running jobs before submitting new one.
    """

    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ("json", "databricks_conn_id")
    template_ext: Sequence[str] = (".json-tpl",)
    # Databricks brand color (blue) under white text
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"
    operator_extra_links = (DatabricksJobRunLink(),)

    def __init__(
        self,
        *,
        job_id: str | None = None,
        job_name: str | None = None,
        json: Any | None = None,
        notebook_params: dict[str, str] | None = None,
        python_params: list[str] | None = None,
        jar_params: list[str] | None = None,
        spark_submit_params: list[str] | None = None,
        python_named_params: dict[str, str] | None = None,
        idempotency_token: str | None = None,
        databricks_conn_id: str = "databricks_default",
        polling_period_seconds: int = 30,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        databricks_retry_args: dict[Any, Any] | None = None,
        do_xcom_push: bool = True,
        wait_for_termination: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        repair_run: bool = False,
        databricks_repair_reason_new_settings: dict[str, Any] | None = None,
        cancel_previous_runs: bool = False,
        **kwargs,
    ) -> None:
        """Create a new ``DatabricksRunNowOperator``."""
        super().__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        self.wait_for_termination = wait_for_termination
        self.deferrable = deferrable
        self.repair_run = repair_run
        self.databricks_repair_reason_new_settings = databricks_repair_reason_new_settings or {}
        self.cancel_previous_runs = cancel_previous_runs

        if job_id is not None:
            self.json["job_id"] = job_id
        if job_name is not None:
            self.json["job_name"] = job_name
        if "job_id" in self.json and "job_name" in self.json:
            raise AirflowException("Argument 'job_name' is not allowed with argument 'job_id'")
        if notebook_params is not None:
            self.json["notebook_params"] = notebook_params
        if python_params is not None:
            self.json["python_params"] = python_params
        if python_named_params is not None:
            self.json["python_named_params"] = python_named_params
        if jar_params is not None:
            self.json["jar_params"] = jar_params
        if spark_submit_params is not None:
            self.json["spark_submit_params"] = spark_submit_params
        if idempotency_token is not None:
            self.json["idempotency_token"] = idempotency_token
        if self.json:
            self.json = normalise_json_content(self.json)
        # This variable will be used in case our task gets killed.
        self.run_id: int | None = None
        self.do_xcom_push = do_xcom_push

    @cached_property
    def _hook(self):
        return self._get_hook(caller="DatabricksRunNowOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def execute(self, context: Context):
        hook = self._hook
        if "job_name" in self.json:
            job_id = hook.find_job_id_by_name(self.json["job_name"])
            if job_id is None:
                raise AirflowException(f"Job ID for job name {self.json['job_name']} can not be found")
            self.json["job_id"] = job_id
            del self.json["job_name"]

        if self.cancel_previous_runs and self.json["job_id"] is not None:
            hook.cancel_all_runs(self.json["job_id"])

        self.run_id = hook.run_now(self.json)
        if self.deferrable:
            _handle_deferrable_databricks_operator_execution(self, hook, self.log, context)
        else:
            _handle_databricks_operator_execution(self, hook, self.log, context)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        if event:
            _handle_deferrable_databricks_operator_completion(event, self.log)
            run_state = RunState.from_json(event["run_state"])
            should_repair = event["repair_run"] and (
                not self.databricks_repair_reason_new_settings
                or is_repair_reason_match_exist(self, run_state)
            )
            if should_repair:
                self.repair_run = False
                self.run_id = event["run_id"]
                job_id = self._hook.get_job_id(self.run_id)
                update_job_for_repair(self, self._hook, job_id, run_state)
                latest_repair_id = self._hook.get_latest_repair_id(self.run_id)
                repair_json = {"run_id": self.run_id, "rerun_all_failed_tasks": True}
                if latest_repair_id is not None:
                    repair_json["latest_repair_id"] = latest_repair_id
                self.json["latest_repair_id"] = self._hook.repair_run(repair_json)
                _handle_deferrable_databricks_operator_execution(self, self._hook, self.log, context)

    def on_kill(self) -> None:
        if self.run_id:
            self._hook.cancel_run(self.run_id)
            self.log.info(
                "Task: %s with run_id: %s was requested to be cancelled.", self.task_id, self.run_id
            )
        else:
            self.log.error("Error: Task: %s with invalid run_id was requested to be cancelled.", self.task_id)


@deprecated(
    reason=(
        "`DatabricksRunNowDeferrableOperator` has been deprecated. "
        "Please use `airflow.providers.databricks.operators.DatabricksRunNowOperator` "
        "with `deferrable=True` instead."
    ),
    category=AirflowProviderDeprecationWarning,
)
class DatabricksRunNowDeferrableOperator(DatabricksRunNowOperator):
    """Deferrable version of ``DatabricksRunNowOperator``."""

    def __init__(self, *args, **kwargs):
        super().__init__(deferrable=True, *args, **kwargs)


class DatabricksTaskBaseOperator(BaseOperator, ABC):
    """
    Base class for operators that are run as Databricks job tasks or tasks within a Databricks workflow.

    :param caller: The name of the caller operator to be used in the logs.
    :param databricks_conn_id: The name of the Airflow connection to use.
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param databricks_retry_delay: Number of seconds to wait between retries.
    :param databricks_retry_limit: Amount of times to retry if the Databricks backend is unreachable.
    :param deferrable: Whether to run the operator in the deferrable mode.
    :param existing_cluster_id: ID for existing cluster on which to run this task.
    :param job_cluster_key: The key for the job cluster.
    :param new_cluster: Specs for a new cluster on which this task will be run.
    :param notebook_packages: A list of the Python libraries to be installed on the cluster running the
        notebook.
    :param notebook_params: A dict of key-value pairs to be passed as optional params to the notebook task.
    :param polling_period_seconds: Controls the rate which we poll for the result of this notebook job run.
    :param wait_for_termination: if we should wait for termination of the job run. ``True`` by default.
    :param workflow_run_metadata: Metadata for the workflow run. This is used when the operator is used within
        a workflow. It is expected to be a dictionary containing the run_id and conn_id for the workflow.
    """

    def __init__(
        self,
        caller: str = "DatabricksTaskBaseOperator",
        databricks_conn_id: str = "databricks_default",
        databricks_retry_args: dict[Any, Any] | None = None,
        databricks_retry_delay: int = 1,
        databricks_retry_limit: int = 3,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        existing_cluster_id: str = "",
        job_cluster_key: str = "",
        new_cluster: dict[str, Any] | None = None,
        polling_period_seconds: int = 5,
        wait_for_termination: bool = True,
        workflow_run_metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        self.caller = caller
        self.databricks_conn_id = databricks_conn_id
        self.databricks_retry_args = databricks_retry_args
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_limit = databricks_retry_limit
        self.deferrable = deferrable
        self.existing_cluster_id = existing_cluster_id
        self.job_cluster_key = job_cluster_key
        self.new_cluster = new_cluster or {}
        self.polling_period_seconds = polling_period_seconds
        self.wait_for_termination = wait_for_termination
        self.workflow_run_metadata = workflow_run_metadata

        self.databricks_run_id: int | None = None

        super().__init__(**kwargs)

        if self._databricks_workflow_task_group is not None:
            self.operator_extra_links = (
                WorkflowJobRunLink(),
                WorkflowJobRepairSingleTaskLink(),
            )
        else:
            # Databricks does not support repair for non-workflow tasks, hence do not show the repair link.
            self.operator_extra_links = (DatabricksJobRunLink(),)

    @cached_property
    def _hook(self) -> DatabricksHook:
        return self._get_hook(caller=self.caller)

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def _get_databricks_task_id(self, task_id: str) -> str:
        """Get the databricks task ID using dag_id and task_id. Removes illegal characters."""
        return f"{self.dag_id}__{task_id.replace('.', '__')}"

    @property
    def _databricks_workflow_task_group(self) -> DatabricksWorkflowTaskGroup | None:
        """
        Traverse up parent TaskGroups until the `is_databricks` flag associated with the root DatabricksWorkflowTaskGroup is found.

        If found, returns the task group. Otherwise, return None.
        """
        parent_tg: TaskGroup | DatabricksWorkflowTaskGroup | None = self.task_group

        while parent_tg:
            if getattr(parent_tg, "is_databricks", False):
                return parent_tg  # type: ignore[return-value]

            if getattr(parent_tg, "task_group", None):
                parent_tg = parent_tg.task_group
            else:
                return None

        return None

    @abstractmethod
    def _get_task_base_json(self) -> dict[str, Any]:
        """Get the base json for the task."""
        raise NotImplementedError()

    def _get_run_json(self) -> dict[str, Any]:
        """Get run json to be used for task submissions."""
        run_json = {
            "run_name": self._get_databricks_task_id(self.task_id),
            **self._get_task_base_json(),
        }
        if self.new_cluster and self.existing_cluster_id:
            raise ValueError("Both new_cluster and existing_cluster_id are set. Only one should be set.")
        if self.new_cluster:
            run_json["new_cluster"] = self.new_cluster
        elif self.existing_cluster_id:
            run_json["existing_cluster_id"] = self.existing_cluster_id
        else:
            raise ValueError("Must specify either existing_cluster_id or new_cluster.")
        return run_json

    def _launch_job(self, context: Context | None = None) -> int:
        """Launch the job on Databricks."""
        run_json = self._get_run_json()
        self.databricks_run_id = self._hook.submit_run(run_json)
        url = self._hook.get_run_page_url(self.databricks_run_id)
        self.log.info("Check the job run in Databricks: %s", url)

        if self.do_xcom_push and context is not None:
            context["ti"].xcom_push(key=XCOM_RUN_ID_KEY, value=self.databricks_run_id)
            context["ti"].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=url)

        return self.databricks_run_id

    def _handle_terminal_run_state(self, run_state: RunState) -> None:
        """Handle the terminal state of the run."""
        if run_state.life_cycle_state != RunLifeCycleState.TERMINATED.value:
            raise AirflowException(
                f"Databricks job failed with state {run_state.life_cycle_state}. Message: {run_state.state_message}"
            )
        if not run_state.is_successful:
            raise AirflowException(
                f"Task failed. Final state {run_state.result_state}. Reason: {run_state.state_message}"
            )
        self.log.info("Task succeeded. Final state %s.", run_state.result_state)

    def _get_current_databricks_task(self) -> dict[str, Any]:
        """Retrieve the Databricks task corresponding to the current Airflow task."""
        if self.databricks_run_id is None:
            raise ValueError("Databricks job not yet launched. Please run launch_notebook_job first.")
        tasks = self._hook.get_run(self.databricks_run_id)["tasks"]

        # Because the task_key remains the same across multiple runs, and the Databricks API does not return
        # tasks sorted by their attempts/start time, we sort the tasks by start time. This ensures that we
        # map the latest attempt (whose status is to be monitored) of the task run to the task_key while
        # building the {task_key: task} map below.
        sorted_task_runs = sorted(tasks, key=lambda x: x["start_time"])

        return {task["task_key"]: task for task in sorted_task_runs}[
            self._get_databricks_task_id(self.task_id)
        ]

    def _convert_to_databricks_workflow_task(
        self, relevant_upstreams: list[BaseOperator], context: Context | None = None
    ) -> dict[str, object]:
        """Convert the operator to a Databricks workflow task that can be a task in a workflow."""
        base_task_json = self._get_task_base_json()
        result = {
            "task_key": self._get_databricks_task_id(self.task_id),
            "depends_on": [
                {"task_key": self._get_databricks_task_id(task_id)}
                for task_id in self.upstream_task_ids
                if task_id in relevant_upstreams
            ],
            **base_task_json,
        }

        if self.existing_cluster_id and self.job_cluster_key:
            raise ValueError(
                "Both existing_cluster_id and job_cluster_key are set. Only one can be set per task."
            )
        if self.existing_cluster_id:
            result["existing_cluster_id"] = self.existing_cluster_id
        elif self.job_cluster_key:
            result["job_cluster_key"] = self.job_cluster_key

        return result

    def monitor_databricks_job(self) -> None:
        """
        Monitor the Databricks job.

        Wait for the job to terminate. If deferrable, defer the task.
        """
        if self.databricks_run_id is None:
            raise ValueError("Databricks job not yet launched. Please run launch_notebook_job first.")
        current_task_run_id = self._get_current_databricks_task()["run_id"]
        run = self._hook.get_run(current_task_run_id)
        run_page_url = run["run_page_url"]
        self.log.info("Check the task run in Databricks: %s", run_page_url)
        run_state = RunState(**run["state"])
        self.log.info(
            "Current state of the the databricks task %s is %s",
            self._get_databricks_task_id(self.task_id),
            run_state.life_cycle_state,
        )
        if self.deferrable and not run_state.is_terminal:
            self.defer(
                trigger=DatabricksExecutionTrigger(
                    run_id=current_task_run_id,
                    databricks_conn_id=self.databricks_conn_id,
                    polling_period_seconds=self.polling_period_seconds,
                    retry_limit=self.databricks_retry_limit,
                    retry_delay=self.databricks_retry_delay,
                    retry_args=self.databricks_retry_args,
                    caller=self.caller,
                ),
                method_name=DEFER_METHOD_NAME,
            )
        while not run_state.is_terminal:
            time.sleep(self.polling_period_seconds)
            run = self._hook.get_run(current_task_run_id)
            run_state = RunState(**run["state"])
            self.log.info(
                "Current state of the databricks task %s is %s",
                self._get_databricks_task_id(self.task_id),
                run_state.life_cycle_state,
            )
        self._handle_terminal_run_state(run_state)

    def execute(self, context: Context) -> None:
        """Execute the operator. Launch the job and monitor it if wait_for_termination is set to True."""
        if self._databricks_workflow_task_group:
            # If we are in a DatabricksWorkflowTaskGroup, we should have an upstream task launched.
            if not self.workflow_run_metadata:
                launch_task_id = next(task for task in self.upstream_task_ids if task.endswith(".launch"))
                self.workflow_run_metadata = context["ti"].xcom_pull(task_ids=launch_task_id)
            workflow_run_metadata = WorkflowRunMetadata(  # type: ignore[arg-type]
                **self.workflow_run_metadata
            )
            self.databricks_run_id = workflow_run_metadata.run_id
            self.databricks_conn_id = workflow_run_metadata.conn_id
        else:
            self._launch_job(context=context)
        if self.wait_for_termination:
            self.monitor_databricks_job()

    def execute_complete(self, context: dict | None, event: dict) -> None:
        run_state = RunState.from_json(event["run_state"])
        self._handle_terminal_run_state(run_state)


class DatabricksNotebookOperator(DatabricksTaskBaseOperator):
    """
    Runs a notebook on Databricks using an Airflow operator.

    The DatabricksNotebookOperator allows users to launch and monitor notebook job runs on Databricks as
    Airflow tasks. It can be used as a part of a DatabricksWorkflowTaskGroup to take advantage of job
    clusters, which allows users to run their tasks on cheaper clusters that can be shared between tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksNotebookOperator`

    :param notebook_path: The path to the notebook in Databricks.
    :param source: Optional location type of the notebook. When set to WORKSPACE, the notebook will be retrieved
            from the local Databricks workspace. When set to GIT, the notebook will be retrieved from a Git repository
            defined in git_source. If the value is empty, the task will use GIT if git_source is defined
            and WORKSPACE otherwise. For more information please visit
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate
    :param databricks_conn_id: The name of the Airflow connection to use.
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param databricks_retry_delay: Number of seconds to wait between retries.
    :param databricks_retry_limit: Amount of times to retry if the Databricks backend is unreachable.
    :param deferrable: Whether to run the operator in the deferrable mode.
    :param existing_cluster_id: ID for existing cluster on which to run this task.
    :param job_cluster_key: The key for the job cluster.
    :param new_cluster: Specs for a new cluster on which this task will be run.
    :param notebook_packages: A list of the Python libraries to be installed on the cluster running the
        notebook.
    :param notebook_params: A dict of key-value pairs to be passed as optional params to the notebook task.
    :param polling_period_seconds: Controls the rate which we poll for the result of this notebook job run.
    :param wait_for_termination: if we should wait for termination of the job run. ``True`` by default.
    :param workflow_run_metadata: Metadata for the workflow run. This is used when the operator is used within
        a workflow. It is expected to be a dictionary containing the run_id and conn_id for the workflow.
    """

    template_fields = (
        "notebook_params",
        "workflow_run_metadata",
    )
    CALLER = "DatabricksNotebookOperator"

    def __init__(
        self,
        notebook_path: str,
        source: str,
        databricks_conn_id: str = "databricks_default",
        databricks_retry_args: dict[Any, Any] | None = None,
        databricks_retry_delay: int = 1,
        databricks_retry_limit: int = 3,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        existing_cluster_id: str = "",
        job_cluster_key: str = "",
        new_cluster: dict[str, Any] | None = None,
        notebook_packages: list[dict[str, Any]] | None = None,
        notebook_params: dict | None = None,
        polling_period_seconds: int = 5,
        wait_for_termination: bool = True,
        workflow_run_metadata: dict | None = None,
        **kwargs: Any,
    ):
        self.notebook_path = notebook_path
        self.source = source
        self.notebook_packages = notebook_packages or []
        self.notebook_params = notebook_params or {}

        super().__init__(
            caller=self.CALLER,
            databricks_conn_id=databricks_conn_id,
            databricks_retry_args=databricks_retry_args,
            databricks_retry_delay=databricks_retry_delay,
            databricks_retry_limit=databricks_retry_limit,
            deferrable=deferrable,
            existing_cluster_id=existing_cluster_id,
            job_cluster_key=job_cluster_key,
            new_cluster=new_cluster,
            polling_period_seconds=polling_period_seconds,
            wait_for_termination=wait_for_termination,
            workflow_run_metadata=workflow_run_metadata,
            **kwargs,
        )

    def _get_task_timeout_seconds(self) -> int:
        """
        Get the timeout seconds value for the Databricks job based on the execution timeout value provided for the Airflow task.

        By default, tasks in Airflow have an execution_timeout set to None. In Airflow, when
        execution_timeout is not defined, the task continues to run indefinitely. Therefore,
        to mirror this behavior in the Databricks Jobs API, we set the timeout to 0, indicating
        that the job should run indefinitely. This aligns with the default behavior of Databricks jobs,
        where a timeout seconds value of 0 signifies an indefinite run duration.
        More details can be found in the Databricks documentation:
        See https://docs.databricks.com/api/workspace/jobs/submit#timeout_seconds
        """
        if self.execution_timeout is None:
            return 0
        execution_timeout_seconds = int(self.execution_timeout.total_seconds())
        if execution_timeout_seconds == 0:
            raise ValueError(
                "If you've set an `execution_timeout` for the task, ensure it's not `0`. Set it instead to "
                "`None` if you desire the task to run indefinitely."
            )
        return execution_timeout_seconds

    def _get_task_base_json(self) -> dict[str, Any]:
        """Get task base json to be used for task submissions."""
        return {
            "timeout_seconds": self._get_task_timeout_seconds(),
            "email_notifications": {},
            "notebook_task": {
                "notebook_path": self.notebook_path,
                "source": self.source,
                "base_parameters": self.notebook_params,
            },
            "libraries": self.notebook_packages,
        }

    def _extend_workflow_notebook_packages(
        self, databricks_workflow_task_group: DatabricksWorkflowTaskGroup
    ) -> None:
        """Extend the task group packages into the notebook's packages, without adding any duplicates."""
        for task_group_package in databricks_workflow_task_group.notebook_packages:
            exists = any(
                task_group_package == existing_package for existing_package in self.notebook_packages
            )
            if not exists:
                self.notebook_packages.append(task_group_package)

    def _convert_to_databricks_workflow_task(
        self, relevant_upstreams: list[BaseOperator], context: Context | None = None
    ) -> dict[str, object]:
        """Convert the operator to a Databricks workflow task that can be a task in a workflow."""
        databricks_workflow_task_group = self._databricks_workflow_task_group
        if not databricks_workflow_task_group:
            raise AirflowException(
                "Calling `_convert_to_databricks_workflow_task` without a parent TaskGroup."
            )

        if hasattr(databricks_workflow_task_group, "notebook_packages"):
            self._extend_workflow_notebook_packages(databricks_workflow_task_group)

        if hasattr(databricks_workflow_task_group, "notebook_params"):
            self.notebook_params = {
                **self.notebook_params,
                **databricks_workflow_task_group.notebook_params,
            }

        return super()._convert_to_databricks_workflow_task(relevant_upstreams, context=context)


class DatabricksTaskOperator(DatabricksTaskBaseOperator):
    """
    Runs a task on Databricks using an Airflow operator.

    The DatabricksTaskOperator allows users to launch and monitor task job runs on Databricks as Airflow
    tasks. It can be used as a part of a DatabricksWorkflowTaskGroup to take advantage of job clusters, which
    allows users to run their tasks on cheaper clusters that can be shared between tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksTaskOperator`

    :param task_config: The configuration of the task to be run on Databricks.
    :param databricks_conn_id: The name of the Airflow connection to use.
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param databricks_retry_delay: Number of seconds to wait between retries.
    :param databricks_retry_limit: Amount of times to retry if the Databricks backend is unreachable.
    :param deferrable: Whether to run the operator in the deferrable mode.
    :param existing_cluster_id: ID for existing cluster on which to run this task.
    :param job_cluster_key: The key for the job cluster.
    :param new_cluster: Specs for a new cluster on which this task will be run.
    :param polling_period_seconds: Controls the rate which we poll for the result of this notebook job run.
    :param wait_for_termination: if we should wait for termination of the job run. ``True`` by default.
    """

    CALLER = "DatabricksTaskOperator"
    template_fields = ("workflow_run_metadata",)

    def __init__(
        self,
        task_config: dict,
        databricks_conn_id: str = "databricks_default",
        databricks_retry_args: dict[Any, Any] | None = None,
        databricks_retry_delay: int = 1,
        databricks_retry_limit: int = 3,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        existing_cluster_id: str = "",
        job_cluster_key: str = "",
        new_cluster: dict[str, Any] | None = None,
        polling_period_seconds: int = 5,
        wait_for_termination: bool = True,
        workflow_run_metadata: dict | None = None,
        **kwargs,
    ):
        self.task_config = task_config

        super().__init__(
            caller=self.CALLER,
            databricks_conn_id=databricks_conn_id,
            databricks_retry_args=databricks_retry_args,
            databricks_retry_delay=databricks_retry_delay,
            databricks_retry_limit=databricks_retry_limit,
            deferrable=deferrable,
            existing_cluster_id=existing_cluster_id,
            job_cluster_key=job_cluster_key,
            new_cluster=new_cluster,
            polling_period_seconds=polling_period_seconds,
            wait_for_termination=wait_for_termination,
            workflow_run_metadata=workflow_run_metadata,
            **kwargs,
        )

    def _get_task_base_json(self) -> dict[str, Any]:
        """Get task base json to be used for task submissions."""
        return self.task_config
