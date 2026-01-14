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
"""This module contains a Cloud Composer sensor."""

from __future__ import annotations

import json
from collections.abc import Collection, Iterable, Sequence
from datetime import datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING

from dateutil import parser
from google.api_core.exceptions import NotFound
from google.cloud.orchestration.airflow.service_v1.types import Environment, ExecuteAirflowCommandResponse

from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowSkipException,
    BaseSensorOperator,
    conf,
)
from airflow.providers.google.cloud.hooks.cloud_composer import CloudComposerHook
from airflow.providers.google.cloud.triggers.cloud_composer import (
    CloudComposerDAGRunTrigger,
    CloudComposerExternalTaskTrigger,
)
from airflow.providers.google.common.consts import GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME
from airflow.providers.standard.exceptions import (
    DuplicateStateError,
    ExternalDagFailedError,
    ExternalTaskFailedError,
    ExternalTaskGroupFailedError,
)
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class CloudComposerDAGRunSensor(BaseSensorOperator):
    """
    Check if a DAG run has completed.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: The name of the Composer environment.
    :param composer_dag_id: The ID of executable DAG.
    :param allowed_states: Iterable of allowed states, default is ``['success']``.
    :param execution_range: execution DAGs time range. Sensor checks DAGs states only for DAGs which were
        started in this time range. For yesterday, use [positive!] datetime.timedelta(days=1).
        For future, use [negative!] datetime.timedelta(days=-1). For specific time, use list of
        datetimes [datetime(2024,3,22,11,0,0), datetime(2024,3,22,12,0,0)].
        Or [datetime(2024,3,22,0,0,0)] in this case sensor will check for states from specific time in the
        past till current time execution.
        Default value datetime.timedelta(days=1).
    :param composer_dag_run_id: The Run ID of executable task. The 'execution_range' param is ignored, if both specified.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param poll_interval: Optional: Control the rate of the poll for the result of deferrable run.
    :param deferrable: Run sensor in deferrable mode.
    """

    template_fields = (
        "project_id",
        "region",
        "environment_id",
        "composer_dag_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        composer_dag_id: str,
        allowed_states: Iterable[str] | None = None,
        execution_range: timedelta | list[datetime] | None = None,
        composer_dag_run_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 10,
        use_rest_api: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.composer_dag_id = composer_dag_id
        self.allowed_states = list(allowed_states) if allowed_states else [TaskInstanceState.SUCCESS.value]
        self.execution_range = execution_range
        self.composer_dag_run_id = composer_dag_run_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.use_rest_api = use_rest_api

        if self.composer_dag_run_id and self.execution_range:
            self.log.warning(
                "The composer_dag_run_id parameter and execution_range parameter do not work together. This run will ignore execution_range parameter and count only specified composer_dag_run_id parameter."
            )

    def _get_logical_dates(self, context) -> tuple[datetime, datetime]:
        logical_date = context.get("logical_date", None)
        if logical_date is None:
            raise RuntimeError(
                "logical_date is None. Please make sure the sensor is not used in an asset-triggered Dag. "
                "CloudComposerDAGRunSensor was designed to be used in time-based scheduled Dags only, "
                "and asset-triggered Dags do not have logical_date. "
            )
        if isinstance(self.execution_range, timedelta):
            if self.execution_range < timedelta(0):
                return logical_date, logical_date - self.execution_range
            return logical_date - self.execution_range, logical_date
        if isinstance(self.execution_range, list) and len(self.execution_range) > 0:
            return self.execution_range[0], self.execution_range[1] if len(
                self.execution_range
            ) > 1 else logical_date
        return logical_date - timedelta(1), logical_date

    def poke(self, context: Context) -> bool:
        start_date, end_date = self._get_logical_dates(context)

        if datetime.now(end_date.tzinfo) < end_date:
            return False

        dag_runs = self._pull_dag_runs()

        if len(dag_runs) == 0:
            self.log.info("Dag runs are empty. Sensor waits for dag runs...")
            return False

        if self.composer_dag_run_id:
            self.log.info(
                "Sensor waits for allowed states %s for specified RunID: %s",
                self.allowed_states,
                self.composer_dag_run_id,
            )
            composer_dag_run_id_status = self._check_composer_dag_run_id_states(
                dag_runs=dag_runs,
            )
            return composer_dag_run_id_status
        self.log.info("Sensor waits for allowed states: %s", self.allowed_states)
        allowed_states_status = self._check_dag_runs_states(
            dag_runs=dag_runs,
            start_date=start_date,
            end_date=end_date,
        )

        return allowed_states_status

    def _pull_dag_runs(self) -> list[dict]:
        """Pull the list of dag runs."""
        if self.use_rest_api:
            try:
                environment = self.hook.get_environment(
                    project_id=self.project_id,
                    region=self.region,
                    environment_id=self.environment_id,
                    timeout=self.timeout,
                )
            except NotFound as not_found_err:
                self.log.info("The Composer environment %s does not exist.", self.environment_id)
                raise AirflowException(not_found_err)
            composer_airflow_uri = environment.config.airflow_uri

            self.log.info(
                "Pulling the DAG %s runs from the %s environment...",
                self.composer_dag_id,
                self.environment_id,
            )
            dag_runs_response = self.hook.get_dag_runs(
                composer_airflow_uri=composer_airflow_uri,
                composer_dag_id=self.composer_dag_id,
                timeout=self.timeout,
            )
            dag_runs = dag_runs_response["dag_runs"]
        else:
            cmd_parameters = (
                ["-d", self.composer_dag_id, "-o", "json"]
                if self._composer_airflow_version < 3
                else [self.composer_dag_id, "-o", "json"]
            )
            dag_runs_cmd = self.hook.execute_airflow_command(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                command="dags",
                subcommand="list-runs",
                parameters=cmd_parameters,
            )
            cmd_result = self.hook.wait_command_execution_result(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                execution_cmd_info=ExecuteAirflowCommandResponse.to_dict(dag_runs_cmd),
            )
            dag_runs = json.loads(cmd_result["output"][0]["content"])
        return dag_runs

    def _check_dag_runs_states(
        self,
        dag_runs: list[dict],
        start_date: datetime,
        end_date: datetime,
    ) -> bool:
        for dag_run in dag_runs:
            if (
                start_date.timestamp()
                < parser.parse(
                    dag_run["execution_date" if self._composer_airflow_version < 3 else "logical_date"]
                ).timestamp()
                < end_date.timestamp()
            ) and dag_run["state"] not in self.allowed_states:
                return False
        return True

    def _get_composer_airflow_version(self) -> int:
        """Return Composer Airflow version."""
        environment_obj = self.hook.get_environment(
            project_id=self.project_id,
            region=self.region,
            environment_id=self.environment_id,
        )
        environment_config = Environment.to_dict(environment_obj)
        image_version = environment_config["config"]["software_config"]["image_version"]
        return int(image_version.split("airflow-")[1].split(".")[0])

    def _check_composer_dag_run_id_states(self, dag_runs: list[dict]) -> bool:
        for dag_run in dag_runs:
            if (
                dag_run["dag_run_id" if self.use_rest_api else "run_id"] == self.composer_dag_run_id
                and dag_run["state"] in self.allowed_states
            ):
                return True
        return False

    def execute(self, context: Context) -> None:
        self._composer_airflow_version = self._get_composer_airflow_version()
        if self.deferrable:
            start_date, end_date = self._get_logical_dates(context)
            self.defer(
                timeout=timedelta(seconds=self.timeout) if self.timeout else None,
                trigger=CloudComposerDAGRunTrigger(
                    project_id=self.project_id,
                    region=self.region,
                    environment_id=self.environment_id,
                    composer_dag_id=self.composer_dag_id,
                    composer_dag_run_id=self.composer_dag_run_id,
                    start_date=start_date,
                    end_date=end_date,
                    allowed_states=self.allowed_states,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    poll_interval=self.poll_interval,
                    composer_airflow_version=self._composer_airflow_version,
                    use_rest_api=self.use_rest_api,
                ),
                method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
            )
        super().execute(context)

    def execute_complete(self, context: Context, event: dict):
        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info("DAG %s has executed successfully.", self.composer_dag_id)

    @cached_property
    def hook(self) -> CloudComposerHook:
        return CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class CloudComposerExternalTaskSensor(BaseSensorOperator):
    """
    Waits for a different DAG, task group, or task to complete for a specific composer environment.

    If both `composer_external_task_group_id` and `composer_external_task_id` are ``None`` (default), the sensor
    waits for the DAG.
    Values for `composer_external_task_group_id` and `composer_external_task_id` can't be set at the same time.

    By default, the CloudComposerExternalTaskSensor will wait for the external task to
    succeed, at which point it will also succeed. However, by default it will
    *not* fail if the external task fails, but will continue to check the status
    until the sensor times out (thus giving you time to retry the external task
    without also having to clear the sensor).

    By default, the CloudComposerExternalTaskSensor will not skip if the external task skips.
    To change this, simply set ``skipped_states=[TaskInstanceState.SKIPPED]``.
    Note that if you are monitoring multiple tasks, and one enters error state
    and the other enters a skipped state, then the external task will react to
    whichever one it sees first. If both happen together, then the failed state
    takes priority.

    It is possible to alter the default behavior by setting states which
    cause the sensor to fail, e.g. by setting ``allowed_states=[DagRunState.FAILED]``
    and ``failed_states=[DagRunState.SUCCESS]`` you will flip the behaviour to
    get a sensor which goes green when the external task *fails* and immediately
    goes red if the external task *succeeds*!

    Note that ``soft_fail`` is respected when examining the failed_states. Thus
    if the external task enters a failed state and ``soft_fail == True`` the
    sensor will _skip_ rather than fail. As a result, setting ``soft_fail=True``
    and ``failed_states=[DagRunState.SKIPPED]`` will result in the sensor
    skipping if the external task skips. However, this is a contrived
    example---consider using ``skipped_states`` if you would like this
    behaviour. Using ``skipped_states`` allows the sensor to skip if the target
    fails, but still enter failed state on timeout. Using ``soft_fail == True``
    as above will cause the sensor to skip if the target fails, but also if it
    times out.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: The name of the Composer environment.
    :param composer_external_dag_id: The dag_id that contains the task you want to
        wait for. (templated)
    :param composer_external_task_id: The task_id that contains the task you want to
        wait for. (templated)
    :param composer_external_task_ids: The list of task_ids that you want to wait for. (templated)
        If ``None`` (default value) the sensor waits for the DAG. Either
        composer_external_task_id or composer_external_task_ids can be passed to
        CloudComposerExternalTaskSensor, but not both.
    :param composer_external_task_group_id: The task_group_id that contains the task you want to
        wait for. (templated)
    :param allowed_states: Iterable of allowed states, default is ``['success']``
    :param skipped_states: Iterable of states to make this task mark as skipped, default is ``None``
    :param failed_states: Iterable of failed or dis-allowed states, default is ``None``
    :param execution_range: execution DAGs time range. Sensor checks DAGs states only for DAGs which were
        started in this time range. For yesterday, use [positive!] datetime.timedelta(days=1).
        For future, use [negative!] datetime.timedelta(days=-1). For specific time, use list of
        datetimes [datetime(2024,3,22,11,0,0), datetime(2024,3,22,12,0,0)].
        Or [datetime(2024,3,22,0,0,0)] in this case sensor will check for states from specific time in the
        past till current time execution.
        Default value datetime.timedelta(days=1).
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param poll_interval: Optional: Control the rate of the poll for the result of deferrable run.
    :param deferrable: Run sensor in deferrable mode.
    """

    template_fields = (
        "project_id",
        "region",
        "environment_id",
        "composer_external_dag_id",
        "composer_external_task_id",
        "composer_external_task_ids",
        "composer_external_task_group_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        composer_external_dag_id: str,
        composer_external_task_id: str | None = None,
        composer_external_task_ids: Collection[str] | None = None,
        composer_external_task_group_id: str | None = None,
        allowed_states: Iterable[str] | None = None,
        skipped_states: Iterable[str] | None = None,
        failed_states: Iterable[str] | None = None,
        execution_range: timedelta | list[datetime] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id

        self.allowed_states = list(allowed_states) if allowed_states else [TaskInstanceState.SUCCESS.value]
        self.skipped_states = list(skipped_states) if skipped_states else []
        self.failed_states = list(failed_states) if failed_states else []

        total_states = set(self.allowed_states + self.skipped_states + self.failed_states)

        if len(total_states) != len(self.allowed_states) + len(self.skipped_states) + len(self.failed_states):
            raise DuplicateStateError(
                "Duplicate values provided across allowed_states, skipped_states and failed_states."
            )

        # convert [] to None
        if not composer_external_task_ids:
            composer_external_task_ids = None

        # can't set both single task id and a list of task ids
        if composer_external_task_id is not None and composer_external_task_ids is not None:
            raise ValueError(
                "Only one of `composer_external_task_id` or `composer_external_task_ids` may "
                "be provided to CloudComposerExternalTaskSensor; "
                "use `composer_external_task_id` or `composer_external_task_ids` or `composer_external_task_group_id`."
            )

        # since both not set, convert the single id to a 1-elt list - from here on, we only consider the list
        if composer_external_task_id is not None:
            composer_external_task_ids = [composer_external_task_id]

        if composer_external_task_group_id is not None and composer_external_task_ids is not None:
            raise ValueError(
                "Only one of `composer_external_task_group_id` or `composer_external_task_ids` may "
                "be provided to CloudComposerExternalTaskSensor; "
                "use `composer_external_task_id` or `composer_external_task_ids` or `composer_external_task_group_id`."
            )

        # check the requested states are all valid states for the target type, be it dag or task
        if composer_external_task_ids or composer_external_task_group_id:
            if not total_states <= set(State.task_states):
                raise ValueError(
                    "Valid values for `allowed_states`, `skipped_states` and `failed_states` "
                    "when `composer_external_task_id` or `composer_external_task_ids` or `composer_external_task_group_id` "
                    f"is not `None`: {State.task_states}"
                )
        elif not total_states <= set(State.dag_states):
            raise ValueError(
                "Valid values for `allowed_states`, `skipped_states` and `failed_states` "
                f"when `composer_external_task_id` and `composer_external_task_group_id` is `None`: {State.dag_states}"
            )

        self.execution_range = execution_range
        self.composer_external_dag_id = composer_external_dag_id
        self.composer_external_task_id = composer_external_task_id
        self.composer_external_task_ids = composer_external_task_ids
        self.composer_external_task_group_id = composer_external_task_group_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def _get_logical_dates(self, context) -> tuple[datetime, datetime]:
        logical_date = context.get("logical_date", None)
        if logical_date is None:
            raise RuntimeError(
                "logical_date is None. Please make sure the sensor is not used in an asset-triggered Dag. "
                "CloudComposerDAGRunSensor was designed to be used in time-based scheduled Dags only, "
                "and asset-triggered Dags do not have logical_date. "
            )
        if isinstance(self.execution_range, timedelta):
            if self.execution_range < timedelta(0):
                return logical_date, logical_date - self.execution_range
            return logical_date - self.execution_range, logical_date
        if isinstance(self.execution_range, list) and len(self.execution_range) > 0:
            return self.execution_range[0], self.execution_range[1] if len(
                self.execution_range
            ) > 1 else logical_date
        return logical_date - timedelta(1), logical_date

    def poke(self, context: Context) -> bool:
        start_date, end_date = self._get_logical_dates(context)

        task_instances = self._get_task_instances(
            start_date=start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_date=end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        if len(task_instances) == 0:
            self.log.info("Task Instances are empty. Sensor waits for task instances...")
            return False

        if self.failed_states:
            external_task_status = self._check_task_instances_states(
                task_instances=task_instances,
                start_date=start_date,
                end_date=end_date,
                states=self.failed_states,
            )
            self._handle_failed_states(external_task_status)

        if self.skipped_states:
            external_task_status = self._check_task_instances_states(
                task_instances=task_instances,
                start_date=start_date,
                end_date=end_date,
                states=self.skipped_states,
            )
            self._handle_skipped_states(external_task_status)

        self.log.info("Sensor waits for allowed states: %s", self.allowed_states)
        external_task_status = self._check_task_instances_states(
            task_instances=task_instances,
            start_date=start_date,
            end_date=end_date,
            states=self.allowed_states,
        )
        return external_task_status

    def _get_task_instances(self, start_date: str, end_date: str) -> list[dict]:
        """Get the list of task instances."""
        try:
            environment = self.hook.get_environment(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                timeout=self.timeout,
            )
        except NotFound as not_found_err:
            self.log.info("The Composer environment %s does not exist.", self.environment_id)
            raise AirflowException(not_found_err)
        composer_airflow_uri = environment.config.airflow_uri

        self.log.info(
            "Pulling the DAG '%s' task instances from the '%s' environment...",
            self.composer_external_dag_id,
            self.environment_id,
        )
        task_instances_response = self.hook.get_task_instances(
            composer_airflow_uri=composer_airflow_uri,
            composer_dag_id=self.composer_external_dag_id,
            query_parameters={
                "execution_date_gte"
                if self._composer_airflow_version < 3
                else "logical_date_gte": start_date,
                "execution_date_lte" if self._composer_airflow_version < 3 else "logical_date_lte": end_date,
            },
            timeout=self.timeout,
        )
        task_instances = task_instances_response["task_instances"]

        if self.composer_external_task_ids:
            task_instances = [
                task_instance
                for task_instance in task_instances
                if task_instance["task_id"] in self.composer_external_task_ids
            ]
        elif self.composer_external_task_group_id:
            task_instances = [
                task_instance
                for task_instance in task_instances
                if self.composer_external_task_group_id in task_instance["task_id"].split(".")
            ]

        return task_instances

    def _check_task_instances_states(
        self,
        task_instances: list[dict],
        start_date: datetime,
        end_date: datetime,
        states: Iterable[str],
    ) -> bool:
        for task_instance in task_instances:
            if (
                start_date.timestamp()
                < parser.parse(
                    task_instance["execution_date" if self._composer_airflow_version < 3 else "logical_date"]
                ).timestamp()
                < end_date.timestamp()
            ) and task_instance["state"] not in states:
                return False
        return True

    def _get_composer_airflow_version(self) -> int:
        """Return Composer Airflow version."""
        environment_obj = self.hook.get_environment(
            project_id=self.project_id,
            region=self.region,
            environment_id=self.environment_id,
        )
        environment_config = Environment.to_dict(environment_obj)
        image_version = environment_config["config"]["software_config"]["image_version"]
        return int(image_version.split("airflow-")[1].split(".")[0])

    def _handle_failed_states(self, failed_status: bool) -> None:
        """Handle failed states and raise appropriate exceptions."""
        if failed_status:
            if self.composer_external_task_ids:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f"Some of the external tasks '{self.composer_external_task_ids}' "
                        f"in DAG '{self.composer_external_dag_id}' failed. Skipping due to soft_fail."
                    )
                raise ExternalTaskFailedError(
                    f"Some of the external tasks '{self.composer_external_task_ids}' "
                    f"in DAG '{self.composer_external_dag_id}' failed."
                )
            if self.composer_external_task_group_id:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f"The external task_group '{self.composer_external_task_group_id}' "
                        f"in DAG '{self.composer_external_dag_id}' failed. Skipping due to soft_fail."
                    )
                raise ExternalTaskGroupFailedError(
                    f"The external task_group '{self.composer_external_task_group_id}' "
                    f"in DAG '{self.composer_external_dag_id}' failed."
                )
            if self.soft_fail:
                raise AirflowSkipException(
                    f"The external DAG '{self.composer_external_dag_id}' failed. Skipping due to soft_fail."
                )
            raise ExternalDagFailedError(f"The external DAG '{self.composer_external_dag_id}' failed.")

    def _handle_skipped_states(self, skipped_status: bool) -> None:
        """Handle skipped states and raise appropriate exceptions."""
        if skipped_status:
            if self.composer_external_task_ids:
                raise AirflowSkipException(
                    f"Some of the external tasks '{self.composer_external_task_ids}' "
                    f"in DAG '{self.composer_external_dag_id}' reached a state in our states-to-skip-on list. Skipping."
                )
            if self.composer_external_task_group_id:
                raise AirflowSkipException(
                    f"The external task_group '{self.composer_external_task_group_id}' "
                    f"in DAG '{self.composer_external_dag_id}' reached a state in our states-to-skip-on list. Skipping."
                )
            raise AirflowSkipException(
                f"The external DAG '{self.composer_external_dag_id}' reached a state in our states-to-skip-on list. "
                "Skipping."
            )

    def execute(self, context: Context) -> None:
        self._composer_airflow_version = self._get_composer_airflow_version()

        if self.composer_external_task_ids and len(self.composer_external_task_ids) > len(
            set(self.composer_external_task_ids)
        ):
            raise ValueError("Duplicate task_ids passed in composer_external_task_ids parameter")

        if self.composer_external_task_ids:
            self.log.info(
                "Poking for tasks '%s' in dag '%s' on Composer environment '%s' ... ",
                self.composer_external_task_ids,
                self.composer_external_dag_id,
                self.environment_id,
            )

        if self.composer_external_task_group_id:
            self.log.info(
                "Poking for task_group '%s' in dag '%s' on Composer environment '%s' ... ",
                self.composer_external_task_group_id,
                self.composer_external_dag_id,
                self.environment_id,
            )

        if (
            self.composer_external_dag_id
            and not self.composer_external_task_group_id
            and not self.composer_external_task_ids
        ):
            self.log.info(
                "Poking for DAG '%s' on Composer environment '%s' ... ",
                self.composer_external_dag_id,
                self.environment_id,
            )

        if self.deferrable:
            start_date, end_date = self._get_logical_dates(context)
            self.defer(
                timeout=timedelta(seconds=self.timeout) if self.timeout else None,
                trigger=CloudComposerExternalTaskTrigger(
                    project_id=self.project_id,
                    region=self.region,
                    environment_id=self.environment_id,
                    composer_external_dag_id=self.composer_external_dag_id,
                    composer_external_task_ids=self.composer_external_task_ids,
                    composer_external_task_group_id=self.composer_external_task_group_id,
                    start_date=start_date,
                    end_date=end_date,
                    allowed_states=self.allowed_states,
                    skipped_states=self.skipped_states,
                    failed_states=self.failed_states,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    poll_interval=self.poll_interval,
                    composer_airflow_version=self._composer_airflow_version,
                ),
                method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
            )
        super().execute(context)

    def execute_complete(self, context: Context, event: dict):
        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        if event and event["status"] == "failed":
            self._handle_failed_states(True)
        elif event and event["status"] == "skipped":
            self._handle_skipped_states(True)

        self.log.info("External tasks for DAG '%s' has executed successfully.", self.composer_external_dag_id)

    @cached_property
    def hook(self) -> CloudComposerHook:
        return CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
