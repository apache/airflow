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
"""
Databricks hook.

This hook enable the submitting and running of jobs to the Databricks platform. Internally the
operators talk to the
``api/2.1/jobs/run-now``
`endpoint <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow>_`
or the ``api/2.1/jobs/runs/submit``
`endpoint <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit>`_.
"""

from __future__ import annotations

import json
from enum import Enum
from typing import Any

from requests import exceptions as requests_exceptions

from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

GET_CLUSTER_ENDPOINT = ("GET", "2.0/clusters/get")
RESTART_CLUSTER_ENDPOINT = ("POST", "2.0/clusters/restart")
START_CLUSTER_ENDPOINT = ("POST", "2.0/clusters/start")
TERMINATE_CLUSTER_ENDPOINT = ("POST", "2.0/clusters/delete")

CREATE_ENDPOINT = ("POST", "2.1/jobs/create")
RESET_ENDPOINT = ("POST", "2.1/jobs/reset")
UPDATE_ENDPOINT = ("POST", "2.1/jobs/update")
RUN_NOW_ENDPOINT = ("POST", "2.1/jobs/run-now")
SUBMIT_RUN_ENDPOINT = ("POST", "2.1/jobs/runs/submit")
GET_RUN_ENDPOINT = ("GET", "2.1/jobs/runs/get")
CANCEL_RUN_ENDPOINT = ("POST", "2.1/jobs/runs/cancel")
DELETE_RUN_ENDPOINT = ("POST", "2.1/jobs/runs/delete")
REPAIR_RUN_ENDPOINT = ("POST", "2.1/jobs/runs/repair")
OUTPUT_RUNS_JOB_ENDPOINT = ("GET", "2.1/jobs/runs/get-output")
CANCEL_ALL_RUNS_ENDPOINT = ("POST", "2.1/jobs/runs/cancel-all")

INSTALL_LIBS_ENDPOINT = ("POST", "2.0/libraries/install")
UNINSTALL_LIBS_ENDPOINT = ("POST", "2.0/libraries/uninstall")
UPDATE_REPO_ENDPOINT = ("PATCH", "2.0/repos/")
DELETE_REPO_ENDPOINT = ("DELETE", "2.0/repos/")
CREATE_REPO_ENDPOINT = ("POST", "2.0/repos")

LIST_JOBS_ENDPOINT = ("GET", "2.1/jobs/list")
LIST_PIPELINES_ENDPOINT = ("GET", "2.0/pipelines")
LIST_SQL_ENDPOINTS_ENDPOINT = ("GET", "2.0/sql/endpoints")

WORKSPACE_GET_STATUS_ENDPOINT = ("GET", "2.0/workspace/get-status")

SPARK_VERSIONS_ENDPOINT = ("GET", "2.0/clusters/spark-versions")
SQL_STATEMENTS_ENDPOINT = "2.0/sql/statements"


class RunLifeCycleState(Enum):
    """
    Enum for the run life cycle state concept of Databricks runs.

    See more information at: https://docs.databricks.com/api/azure/workspace/jobs/listruns#runs-state-life_cycle_state
    """

    BLOCKED = "BLOCKED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SKIPPED = "SKIPPED"
    TERMINATED = "TERMINATED"
    TERMINATING = "TERMINATING"
    WAITING_FOR_RETRY = "WAITING_FOR_RETRY"


class RunState:
    """Utility class for the run state concept of Databricks runs."""

    RUN_LIFE_CYCLE_STATES = [
        "PENDING",
        "RUNNING",
        "TERMINATING",
        "TERMINATED",
        "SKIPPED",
        "INTERNAL_ERROR",
        "QUEUED",
    ]

    def __init__(
        self, life_cycle_state: str, result_state: str = "", state_message: str = "", *args, **kwargs
    ) -> None:
        if life_cycle_state not in self.RUN_LIFE_CYCLE_STATES:
            raise AirflowException(
                f"Unexpected life cycle state: {life_cycle_state}: If the state has "
                "been introduced recently, please check the Databricks user "
                "guide for troubleshooting information"
            )

        self.life_cycle_state = life_cycle_state
        self.result_state = result_state
        self.state_message = state_message

    @property
    def is_terminal(self) -> bool:
        """True if the current state is a terminal state."""
        return self.life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")

    @property
    def is_successful(self) -> bool:
        """True if the result state is SUCCESS."""
        return self.result_state == "SUCCESS"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RunState):
            return NotImplemented
        return (
            self.life_cycle_state == other.life_cycle_state
            and self.result_state == other.result_state
            and self.state_message == other.state_message
        )

    def __hash__(self):
        return hash((self.life_cycle_state, self.result_state, self.state_message))

    def __repr__(self) -> str:
        return str(self.__dict__)

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data: str) -> RunState:
        return RunState(**json.loads(data))


class ClusterState:
    """Utility class for the cluster state concept of Databricks cluster."""

    CLUSTER_LIFE_CYCLE_STATES = [
        "PENDING",
        "RUNNING",
        "RESTARTING",
        "RESIZING",
        "TERMINATING",
        "TERMINATED",
        "ERROR",
        "UNKNOWN",
    ]

    def __init__(self, state: str = "", state_message: str = "", *args, **kwargs) -> None:
        if state not in self.CLUSTER_LIFE_CYCLE_STATES:
            raise AirflowException(
                f"Unexpected cluster life cycle state: {state}: If the state has "
                "been introduced recently, please check the Databricks user "
                "guide for troubleshooting information"
            )

        self.state = state
        self.state_message = state_message

    @property
    def is_terminal(self) -> bool:
        """True if the current state is a terminal state."""
        return self.state in ("TERMINATING", "TERMINATED", "ERROR", "UNKNOWN")

    @property
    def is_running(self) -> bool:
        """True if the current state is running."""
        return self.state in ("RUNNING", "RESIZING")

    def __eq__(self, other) -> bool:
        return self.state == other.state and self.state_message == other.state_message

    def __hash__(self):
        return hash((self.state, self.state_message))

    def __repr__(self) -> str:
        return str(self.__dict__)

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data: str) -> ClusterState:
        return ClusterState(**json.loads(data))


class SQLStatementState:
    """Utility class for the SQL statement state concept of Databricks statements."""

    SQL_STATEMENT_LIFE_CYCLE_STATES = [
        "PENDING",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
        "CANCELED",
        "CLOSED",
    ]

    def __init__(
        self, state: str = "", error_code: str = "", error_message: str = "", *args, **kwargs
    ) -> None:
        if state not in self.SQL_STATEMENT_LIFE_CYCLE_STATES:
            raise AirflowException(
                f"Unexpected SQL statement life cycle state: {state}: If the state has "
                "been introduced recently, please check the Databricks user "
                "guide for troubleshooting information"
            )

        self.state = state
        self.error_code = error_code
        self.error_message = error_message

    @property
    def is_terminal(self) -> bool:
        """True if the current state is a terminal state."""
        return self.state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED")

    @property
    def is_running(self) -> bool:
        """True if the current state is running."""
        return self.state in ("PENDING", "RUNNING")

    @property
    def is_successful(self) -> bool:
        """True if the state is SUCCEEDED."""
        return self.state == "SUCCEEDED"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SQLStatementState):
            return NotImplemented
        return (
            self.state == other.state
            and self.error_code == other.error_code
            and self.error_message == other.error_message
        )

    def __hash__(self):
        return hash((self.state, self.error_code, self.error_message))

    def __repr__(self) -> str:
        return str(self.__dict__)

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data: str) -> SQLStatementState:
        return SQLStatementState(**json.loads(data))


class DatabricksHook(BaseDatabricksHook):
    """
    Interact with Databricks.

    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    :param timeout_seconds: The amount of time in seconds the requests library
        will wait before timing-out.
    :param retry_limit: The number of times to retry the connection in case of
        service outages.
    :param retry_delay: The number of seconds to wait between retries (it
        might be a floating point number).
    :param retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    """

    hook_name = "Databricks"

    def __init__(
        self,
        databricks_conn_id: str = BaseDatabricksHook.default_conn_name,
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        retry_args: dict[Any, Any] | None = None,
        caller: str = "DatabricksHook",
    ) -> None:
        super().__init__(databricks_conn_id, timeout_seconds, retry_limit, retry_delay, retry_args, caller)

    def create_job(self, json: dict) -> int:
        """
        Call the ``api/2.1/jobs/create`` endpoint.

        :param json: The data used in the body of the request to the ``create`` endpoint.
        :return: the job_id as an int
        """
        response = self._do_api_call(CREATE_ENDPOINT, json)
        return response["job_id"]

    def reset_job(self, job_id: str, json: dict) -> None:
        """
        Call the ``api/2.1/jobs/reset`` endpoint.

        :param json: The data used in the new_settings of the request to the ``reset`` endpoint.
        """
        access_control_list = json.get("access_control_list", None)
        if access_control_list:
            self.log.info(
                "Updating job permission for Databricks workflow job id %s with access_control_list %s",
                job_id,
                access_control_list,
            )
            acl_json = {"access_control_list": access_control_list}
            self.update_job_permission(job_id=int(job_id), json=acl_json)

        self._do_api_call(RESET_ENDPOINT, {"job_id": job_id, "new_settings": json})

    def update_job(self, job_id: str, json: dict) -> None:
        """
        Call the ``api/2.1/jobs/update`` endpoint.

        :param job_id: The id of the job to update.
        :param json: The data used in the new_settings of the request to the ``update`` endpoint.
        """
        self._do_api_call(UPDATE_ENDPOINT, {"job_id": job_id, "new_settings": json})

    def run_now(self, json: dict) -> int:
        """
        Call the ``api/2.1/jobs/run-now`` endpoint.

        :param json: The data used in the body of the request to the ``run-now`` endpoint.
        :return: the run_id as an int
        """
        response = self._do_api_call(RUN_NOW_ENDPOINT, json)
        return response["run_id"]

    def submit_run(self, json: dict) -> int:
        """
        Call the ``api/2.1/jobs/runs/submit`` endpoint.

        :param json: The data used in the body of the request to the ``submit`` endpoint.
        :return: the run_id as an int
        """
        response = self._do_api_call(SUBMIT_RUN_ENDPOINT, json)
        return response["run_id"]

    def list_jobs(
        self,
        limit: int = 25,
        expand_tasks: bool = False,
        job_name: str | None = None,
        page_token: str | None = None,
        include_user_names: bool = False,
    ) -> list[dict[str, Any]]:
        """
        List the jobs in the Databricks Job Service.

        :param limit: The limit/batch size used to retrieve jobs.
        :param expand_tasks: Whether to include task and cluster details in the response.
        :param job_name: Optional name of a job to search.
        :param page_token: The optional page token pointing at the first first job to return.
        :return: A list of jobs.
        """
        has_more = True
        all_jobs = []
        if page_token is None:
            page_token = ""

        while has_more:
            payload: dict[str, Any] = {
                "limit": limit,
                "expand_tasks": expand_tasks,
                "include_user_names": include_user_names,
            }
            payload["page_token"] = page_token
            if job_name:
                payload["name"] = job_name
            response = self._do_api_call(LIST_JOBS_ENDPOINT, payload)
            jobs = response.get("jobs", [])
            if job_name:
                all_jobs += [j for j in jobs if j["settings"]["name"] == job_name]
            else:
                all_jobs += jobs
            has_more = response.get("has_more", False)
            if has_more:
                page_token = response.get("next_page_token", "")

        return all_jobs

    def find_job_id_by_name(self, job_name: str) -> int | None:
        """
        Find job id by its name; if there are multiple jobs with the same name, raise AirflowException.

        :param job_name: The name of the job to look up.
        :return: The job_id as an int or None if no job was found.
        """
        matching_jobs = self.list_jobs(job_name=job_name)

        if len(matching_jobs) > 1:
            raise AirflowException(
                f"There are more than one job with name {job_name}. Please delete duplicated jobs first"
            )

        if not matching_jobs:
            return None
        return matching_jobs[0]["job_id"]

    def list_pipelines(
        self, batch_size: int = 25, pipeline_name: str | None = None, notebook_path: str | None = None
    ) -> list[dict[str, Any]]:
        """
        List the pipelines in Databricks Delta Live Tables.

        :param batch_size: The limit/batch size used to retrieve pipelines.
        :param pipeline_name: Optional name of a pipeline to search. Cannot be combined with path.
        :param notebook_path: Optional notebook of a pipeline to search. Cannot be combined with name.
        :return: A list of pipelines.
        """
        has_more = True
        next_token = None
        all_pipelines = []
        filter = None
        if pipeline_name and notebook_path:
            raise AirflowException("Cannot combine pipeline_name and notebook_path in one request")

        if notebook_path:
            filter = f"notebook='{notebook_path}'"
        elif pipeline_name:
            filter = f"name LIKE '{pipeline_name}'"
        payload: dict[str, Any] = {
            "max_results": batch_size,
        }
        if filter:
            payload["filter"] = filter

        while has_more:
            if next_token is not None:
                payload = {**payload, "page_token": next_token}
            response = self._do_api_call(LIST_PIPELINES_ENDPOINT, payload)
            pipelines = response.get("statuses", [])
            all_pipelines += pipelines
            if "next_page_token" in response:
                next_token = response["next_page_token"]
            else:
                has_more = False

        return all_pipelines

    def find_pipeline_id_by_name(self, pipeline_name: str) -> str | None:
        """
        Find pipeline id by its name; if multiple pipelines with the same name, raise AirflowException.

        :param pipeline_name: The name of the pipeline to look up.
        :return: The pipeline_id as a GUID string or None if no pipeline was found.
        """
        matching_pipelines = self.list_pipelines(pipeline_name=pipeline_name)

        if len(matching_pipelines) > 1:
            raise AirflowException(
                f"There are more than one pipelines with name {pipeline_name}. "
                "Please delete duplicated pipelines first"
            )

        if not pipeline_name or len(matching_pipelines) == 0:
            return None
        return matching_pipelines[0]["pipeline_id"]

    def get_run_page_url(self, run_id: int) -> str:
        """
        Retrieve run_page_url.

        :param run_id: id of the run
        :return: URL of the run page
        """
        json = {"run_id": run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        return response["run_page_url"]

    async def a_get_run_page_url(self, run_id: int) -> str:
        """
        Async version of `get_run_page_url()`.

        :param run_id: id of the run
        :return: URL of the run page
        """
        json = {"run_id": run_id}
        response = await self._a_do_api_call(GET_RUN_ENDPOINT, json)
        return response["run_page_url"]

    def get_job_id(self, run_id: int) -> int:
        """
        Retrieve job_id from run_id.

        :param run_id: id of the run
        :return: Job id for given Databricks run
        """
        json = {"run_id": run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        return response["job_id"]

    def get_run_state(self, run_id: int) -> RunState:
        """
        Retrieve run state of the run.

        Please note that any Airflow tasks that call the ``get_run_state`` method will result in
        failure unless you have enabled xcom pickling.  This can be done using the following
        environment variable: ``AIRFLOW__CORE__ENABLE_XCOM_PICKLING``

        If you do not want to enable xcom pickling, use the ``get_run_state_str`` method to get
        a string describing state, or ``get_run_state_lifecycle``, ``get_run_state_result``, or
        ``get_run_state_message`` to get individual components of the run state.

        :param run_id: id of the run
        :return: state of the run
        """
        json = {"run_id": run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        state = response["state"]
        return RunState(**state)

    async def a_get_run_state(self, run_id: int) -> RunState:
        """
        Async version of `get_run_state()`.

        :param run_id: id of the run
        :return: state of the run
        """
        json = {"run_id": run_id}
        response = await self._a_do_api_call(GET_RUN_ENDPOINT, json)
        state = response["state"]
        return RunState(**state)

    def get_run(self, run_id: int) -> dict[str, Any]:
        """
        Retrieve run information.

        :param run_id: id of the run
        :return: state of the run
        """
        json = {"run_id": run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        return response

    async def a_get_run(self, run_id: int) -> dict[str, Any]:
        """
        Async version of `get_run`.

        :param run_id: id of the run
        :return: state of the run
        """
        json = {"run_id": run_id}
        response = await self._a_do_api_call(GET_RUN_ENDPOINT, json)
        return response

    def get_run_state_str(self, run_id: int) -> str:
        """
        Return the string representation of RunState.

        :param run_id: id of the run
        :return: string describing run state
        """
        state = self.get_run_state(run_id)
        run_state_str = (
            f"State: {state.life_cycle_state}. Result: {state.result_state}. {state.state_message}"
        )
        return run_state_str

    def get_run_state_lifecycle(self, run_id: int) -> str:
        """
        Return the lifecycle state of the run.

        :param run_id: id of the run
        :return: string with lifecycle state
        """
        return self.get_run_state(run_id).life_cycle_state

    def get_run_state_result(self, run_id: int) -> str:
        """
        Return the resulting state of the run.

        :param run_id: id of the run
        :return: string with resulting state
        """
        return self.get_run_state(run_id).result_state

    def get_run_state_message(self, run_id: int) -> str:
        """
        Return the state message for the run.

        :param run_id: id of the run
        :return: string with state message
        """
        return self.get_run_state(run_id).state_message

    def get_run_output(self, run_id: int) -> dict:
        """
        Retrieve run output of the run.

        :param run_id: id of the run
        :return: output of the run
        """
        json = {"run_id": run_id}
        run_output = self._do_api_call(OUTPUT_RUNS_JOB_ENDPOINT, json)
        return run_output

    async def a_get_run_output(self, run_id: int) -> dict:
        """
        Async version of `get_run_output()`.

        :param run_id: id of the run
        :return: output of the run
        """
        json = {"run_id": run_id}
        run_output = await self._a_do_api_call(OUTPUT_RUNS_JOB_ENDPOINT, json)
        return run_output

    def cancel_run(self, run_id: int) -> None:
        """
        Cancel the run.

        :param run_id: id of the run
        """
        json = {"run_id": run_id}
        self._do_api_call(CANCEL_RUN_ENDPOINT, json)

    def cancel_all_runs(self, job_id: int) -> None:
        """
        Cancel all active runs of a job asynchronously.

        :param job_id: The canonical identifier of the job to cancel all runs of
        """
        json = {"job_id": job_id}
        self._do_api_call(CANCEL_ALL_RUNS_ENDPOINT, json)

    def delete_run(self, run_id: int) -> None:
        """
        Delete a non-active run.

        :param run_id: id of the run
        """
        json = {"run_id": run_id}
        self._do_api_call(DELETE_RUN_ENDPOINT, json)

    def repair_run(self, json: dict) -> int:
        """
        Re-run one or more tasks.

        :param json: repair a job run.
        """
        response = self._do_api_call(REPAIR_RUN_ENDPOINT, json)
        return response["repair_id"]

    def get_latest_repair_id(self, run_id: int) -> int | None:
        """Get latest repair id if any exist for run_id else None."""
        json = {"run_id": run_id, "include_history": "true"}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        repair_history = response["repair_history"]
        if len(repair_history) == 1:
            return None
        return repair_history[-1]["id"]

    def get_cluster_state(self, cluster_id: str) -> ClusterState:
        """
        Retrieve run state of the cluster.

        :param cluster_id: id of the cluster
        :return: state of the cluster
        """
        json = {"cluster_id": cluster_id}
        response = self._do_api_call(GET_CLUSTER_ENDPOINT, json)
        state = response["state"]
        state_message = response["state_message"]
        return ClusterState(state, state_message)

    async def a_get_cluster_state(self, cluster_id: str) -> ClusterState:
        """
        Async version of `get_cluster_state`.

        :param cluster_id: id of the cluster
        :return: state of the cluster
        """
        json = {"cluster_id": cluster_id}
        response = await self._a_do_api_call(GET_CLUSTER_ENDPOINT, json)
        state = response["state"]
        state_message = response["state_message"]
        return ClusterState(state, state_message)

    def restart_cluster(self, json: dict) -> None:
        """
        Restarts the cluster.

        :param json: json dictionary containing cluster specification.
        """
        self._do_api_call(RESTART_CLUSTER_ENDPOINT, json)

    def start_cluster(self, json: dict) -> None:
        """
        Start the cluster.

        :param json: json dictionary containing cluster specification.
        """
        self._do_api_call(START_CLUSTER_ENDPOINT, json)

    def terminate_cluster(self, json: dict) -> None:
        """
        Terminate the cluster.

        :param json: json dictionary containing cluster specification.
        """
        self._do_api_call(TERMINATE_CLUSTER_ENDPOINT, json)

    def install(self, json: dict) -> None:
        """
        Install libraries on the cluster.

        Utility function to call the ``2.0/libraries/install`` endpoint.

        :param json: json dictionary containing cluster_id and an array of library
        """
        self._do_api_call(INSTALL_LIBS_ENDPOINT, json)

    def uninstall(self, json: dict) -> None:
        """
        Uninstall libraries on the cluster.

        Utility function to call the ``2.0/libraries/uninstall`` endpoint.

        :param json: json dictionary containing cluster_id and an array of library
        """
        self._do_api_call(UNINSTALL_LIBS_ENDPOINT, json)

    def update_repo(self, repo_id: str, json: dict[str, Any]) -> dict:
        """
        Update given Databricks Repos.

        :param repo_id: ID of Databricks Repos
        :param json: payload
        :return: metadata from update
        """
        method, base_path = UPDATE_REPO_ENDPOINT
        repos_endpoint = (method, f"{base_path}/{repo_id}")
        return self._do_api_call(repos_endpoint, json)

    def delete_repo(self, repo_id: str):
        """
        Delete given Databricks Repos.

        :param repo_id: ID of Databricks Repos
        :return:
        """
        method, base_path = DELETE_REPO_ENDPOINT
        repos_endpoint = (method, f"{base_path}/{repo_id}")
        self._do_api_call(repos_endpoint)

    def create_repo(self, json: dict[str, Any]) -> dict:
        """
        Create a Databricks Repos.

        :param json: payload
        :return:
        """
        return self._do_api_call(CREATE_REPO_ENDPOINT, json)

    def get_repo_by_path(self, path: str) -> str | None:
        """
        Obtain Repos ID by path.

        :param path: path to a repository
        :return: Repos ID if it exists, None if doesn't.
        """
        try:
            result = self._do_api_call(WORKSPACE_GET_STATUS_ENDPOINT, {"path": path}, wrap_http_errors=False)
            if result.get("object_type", "") == "REPO":
                return str(result["object_id"])
        except requests_exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise e

        return None

    def update_job_permission(self, job_id: int, json: dict[str, Any]) -> dict:
        """
        Update databricks job permission.

        :param job_id: job id
        :param json: payload
        :return: json containing permission specification
        """
        return self._do_api_call(("PATCH", f"2.0/permissions/jobs/{job_id}"), json)

    def post_sql_statement(self, json: dict[str, Any]) -> str:
        """
        Submit a SQL statement to the Databricks SQL Statements endpoint.

        :param json: The data used in the body of the request to the SQL Statements endpoint.
        :return: The statement_id as a string.
        """
        response = self._do_api_call(("POST", f"{SQL_STATEMENTS_ENDPOINT}"), json)
        return response["statement_id"]

    def get_sql_statement_state(self, statement_id: str) -> SQLStatementState:
        """
        Retrieve run state of the SQL statement.

        :param statement_id: ID of the SQL statement.
        :return: state of the SQL statement.
        """
        get_statement_endpoint = ("GET", f"{SQL_STATEMENTS_ENDPOINT}/{statement_id}")
        response = self._do_api_call(get_statement_endpoint)
        state = response["status"]["state"]
        error_code = response["status"].get("error", {}).get("error_code", "")
        error_message = response["status"].get("error", {}).get("message", "")
        return SQLStatementState(state, error_code, error_message)

    async def a_get_sql_statement_state(self, statement_id: str) -> SQLStatementState:
        """
        Async version of `get_sql_statement_state`.

        :param statement_id: ID of the SQL statement
        :return: state of the SQL statement
        """
        get_sql_statement_endpoint = ("GET", f"{SQL_STATEMENTS_ENDPOINT}/{statement_id}")
        response = await self._a_do_api_call(get_sql_statement_endpoint)
        state = response["status"]["state"]
        error_code = response["status"].get("error", {}).get("error_code", "")
        error_message = response["status"].get("error", {}).get("message", "")
        return SQLStatementState(state, error_code, error_message)

    def cancel_sql_statement(self, statement_id: str) -> None:
        """
        Cancel the SQL statement.

        :param statement_id: ID of the SQL statement
        """
        self.log.info("Canceling SQL statement with ID: %s", statement_id)
        cancel_sql_statement_endpoint = ("POST", f"{SQL_STATEMENTS_ENDPOINT}/{statement_id}/cancel")
        self._do_api_call(cancel_sql_statement_endpoint)

    def test_connection(self) -> tuple[bool, str]:
        """Test the Databricks connectivity from UI."""
        hook = DatabricksHook(databricks_conn_id=self.databricks_conn_id)
        try:
            hook._do_api_call(endpoint_info=SPARK_VERSIONS_ENDPOINT).get("versions")
            status = True
            message = "Connection successfully tested"
        except Exception as e:
            status = False
            message = str(e)

        return status, message
