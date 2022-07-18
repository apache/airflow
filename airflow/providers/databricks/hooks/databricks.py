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
import json
from typing import Any, Dict, List, Optional

from requests import exceptions as requests_exceptions

from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

RESTART_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/restart")
START_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/start")
TERMINATE_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/delete")

RUN_NOW_ENDPOINT = ('POST', 'api/2.1/jobs/run-now')
SUBMIT_RUN_ENDPOINT = ('POST', 'api/2.1/jobs/runs/submit')
GET_RUN_ENDPOINT = ('GET', 'api/2.1/jobs/runs/get')
CANCEL_RUN_ENDPOINT = ('POST', 'api/2.1/jobs/runs/cancel')
OUTPUT_RUNS_JOB_ENDPOINT = ('GET', 'api/2.1/jobs/runs/get-output')

INSTALL_LIBS_ENDPOINT = ('POST', 'api/2.0/libraries/install')
UNINSTALL_LIBS_ENDPOINT = ('POST', 'api/2.0/libraries/uninstall')

LIST_JOBS_ENDPOINT = ('GET', 'api/2.1/jobs/list')

WORKSPACE_GET_STATUS_ENDPOINT = ('GET', 'api/2.0/workspace/get-status')

RUN_LIFE_CYCLE_STATES = ['PENDING', 'RUNNING', 'TERMINATING', 'TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']

SPARK_VERSIONS_ENDPOINT = ('GET', 'api/2.0/clusters/spark-versions')


class RunState:
    """Utility class for the run state concept of Databricks runs."""

    def __init__(
        self, life_cycle_state: str, result_state: str = '', state_message: str = '', *args, **kwargs
    ) -> None:
        self.life_cycle_state = life_cycle_state
        self.result_state = result_state
        self.state_message = state_message

    @property
    def is_terminal(self) -> bool:
        """True if the current state is a terminal state."""
        if self.life_cycle_state not in RUN_LIFE_CYCLE_STATES:
            raise AirflowException(
                (
                    'Unexpected life cycle state: {}: If the state has '
                    'been introduced recently, please check the Databricks user '
                    'guide for troubleshooting information'
                ).format(self.life_cycle_state)
            )
        return self.life_cycle_state in ('TERMINATED', 'SKIPPED', 'INTERNAL_ERROR')

    @property
    def is_successful(self) -> bool:
        """True if the result state is SUCCESS"""
        return self.result_state == 'SUCCESS'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RunState):
            return NotImplemented
        return (
            self.life_cycle_state == other.life_cycle_state
            and self.result_state == other.result_state
            and self.state_message == other.state_message
        )

    def __repr__(self) -> str:
        return str(self.__dict__)

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data: str) -> 'RunState':
        return RunState(**json.loads(data))


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

    hook_name = 'Databricks'

    def __init__(
        self,
        databricks_conn_id: str = BaseDatabricksHook.default_conn_name,
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        retry_args: Optional[Dict[Any, Any]] = None,
    ) -> None:
        super().__init__(databricks_conn_id, timeout_seconds, retry_limit, retry_delay, retry_args)

    def run_now(self, json: dict) -> int:
        """
        Utility function to call the ``api/2.0/jobs/run-now`` endpoint.

        :param json: The data used in the body of the request to the ``run-now`` endpoint.
        :return: the run_id as an int
        :rtype: str
        """
        response = self._do_api_call(RUN_NOW_ENDPOINT, json)
        return response['run_id']

    def submit_run(self, json: dict) -> int:
        """
        Utility function to call the ``api/2.0/jobs/runs/submit`` endpoint.

        :param json: The data used in the body of the request to the ``submit`` endpoint.
        :return: the run_id as an int
        :rtype: str
        """
        response = self._do_api_call(SUBMIT_RUN_ENDPOINT, json)
        return response['run_id']

    def list_jobs(self, limit: int = 25, offset: int = 0, expand_tasks: bool = False) -> List[Dict[str, Any]]:
        """
        Lists the jobs in the Databricks Job Service.

        :param limit: The limit/batch size used to retrieve jobs.
        :param offset: The offset of the first job to return, relative to the most recently created job.
        :param expand_tasks: Whether to include task and cluster details in the response.
        :return: A list of jobs.
        """
        has_more = True
        jobs = []

        while has_more:
            json = {
                'limit': limit,
                'offset': offset,
                'expand_tasks': expand_tasks,
            }
            response = self._do_api_call(LIST_JOBS_ENDPOINT, json)
            jobs += response['jobs'] if 'jobs' in response else []
            has_more = response.get('has_more', False)
            if has_more:
                offset += len(response['jobs'])

        return jobs

    def find_job_id_by_name(self, job_name: str) -> Optional[int]:
        """
        Finds job id by its name. If there are multiple jobs with the same name, raises AirflowException.

        :param job_name: The name of the job to look up.
        :return: The job_id as an int or None if no job was found.
        """
        all_jobs = self.list_jobs()
        matching_jobs = [j for j in all_jobs if j['settings']['name'] == job_name]

        if len(matching_jobs) > 1:
            raise AirflowException(
                f"There are more than one job with name {job_name}. Please delete duplicated jobs first"
            )

        if not matching_jobs:
            return None
        else:
            return matching_jobs[0]['job_id']

    def get_run_page_url(self, run_id: int) -> str:
        """
        Retrieves run_page_url.

        :param run_id: id of the run
        :return: URL of the run page
        """
        json = {'run_id': run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        return response['run_page_url']

    async def a_get_run_page_url(self, run_id: int) -> str:
        """
        Async version of `get_run_page_url()`.
        :param run_id: id of the run
        :return: URL of the run page
        """
        json = {'run_id': run_id}
        response = await self._a_do_api_call(GET_RUN_ENDPOINT, json)
        return response['run_page_url']

    def get_job_id(self, run_id: int) -> int:
        """
        Retrieves job_id from run_id.

        :param run_id: id of the run
        :return: Job id for given Databricks run
        """
        json = {'run_id': run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        return response['job_id']

    def get_run_state(self, run_id: int) -> RunState:
        """
        Retrieves run state of the run.

        Please note that any Airflow tasks that call the ``get_run_state`` method will result in
        failure unless you have enabled xcom pickling.  This can be done using the following
        environment variable: ``AIRFLOW__CORE__ENABLE_XCOM_PICKLING``

        If you do not want to enable xcom pickling, use the ``get_run_state_str`` method to get
        a string describing state, or ``get_run_state_lifecycle``, ``get_run_state_result``, or
        ``get_run_state_message`` to get individual components of the run state.

        :param run_id: id of the run
        :return: state of the run
        """
        json = {'run_id': run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        state = response['state']
        return RunState(**state)

    async def a_get_run_state(self, run_id: int) -> RunState:
        """
        Async version of `get_run_state()`.
        :param run_id: id of the run
        :return: state of the run
        """
        json = {'run_id': run_id}
        response = await self._a_do_api_call(GET_RUN_ENDPOINT, json)
        state = response['state']
        return RunState(**state)

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
        Returns the lifecycle state of the run

        :param run_id: id of the run
        :return: string with lifecycle state
        """
        return self.get_run_state(run_id).life_cycle_state

    def get_run_state_result(self, run_id: int) -> str:
        """
        Returns the resulting state of the run

        :param run_id: id of the run
        :return: string with resulting state
        """
        return self.get_run_state(run_id).result_state

    def get_run_state_message(self, run_id: int) -> str:
        """
        Returns the state message for the run

        :param run_id: id of the run
        :return: string with state message
        """
        return self.get_run_state(run_id).state_message

    def get_run_output(self, run_id: int) -> dict:
        """
        Retrieves run output of the run.

        :param run_id: id of the run
        :return: output of the run
        """
        json = {'run_id': run_id}
        run_output = self._do_api_call(OUTPUT_RUNS_JOB_ENDPOINT, json)
        return run_output

    def cancel_run(self, run_id: int) -> None:
        """
        Cancels the run.

        :param run_id: id of the run
        """
        json = {'run_id': run_id}
        self._do_api_call(CANCEL_RUN_ENDPOINT, json)

    def restart_cluster(self, json: dict) -> None:
        """
        Restarts the cluster.

        :param json: json dictionary containing cluster specification.
        """
        self._do_api_call(RESTART_CLUSTER_ENDPOINT, json)

    def start_cluster(self, json: dict) -> None:
        """
        Starts the cluster.

        :param json: json dictionary containing cluster specification.
        """
        self._do_api_call(START_CLUSTER_ENDPOINT, json)

    def terminate_cluster(self, json: dict) -> None:
        """
        Terminates the cluster.

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

    def update_repo(self, repo_id: str, json: Dict[str, Any]) -> dict:
        """
        Updates given Databricks Repos

        :param repo_id: ID of Databricks Repos
        :param json: payload
        :return: metadata from update
        """
        repos_endpoint = ('PATCH', f'api/2.0/repos/{repo_id}')
        return self._do_api_call(repos_endpoint, json)

    def delete_repo(self, repo_id: str):
        """
        Deletes given Databricks Repos

        :param repo_id: ID of Databricks Repos
        :return:
        """
        repos_endpoint = ('DELETE', f'api/2.0/repos/{repo_id}')
        self._do_api_call(repos_endpoint)

    def create_repo(self, json: Dict[str, Any]) -> dict:
        """
        Creates a Databricks Repos

        :param json: payload
        :return:
        """
        repos_endpoint = ('POST', 'api/2.0/repos')
        return self._do_api_call(repos_endpoint, json)

    def get_repo_by_path(self, path: str) -> Optional[str]:
        """
        Obtains Repos ID by path
        :param path: path to a repository
        :return: Repos ID if it exists, None if doesn't.
        """
        try:
            result = self._do_api_call(WORKSPACE_GET_STATUS_ENDPOINT, {'path': path}, wrap_http_errors=False)
            if result.get('object_type', '') == 'REPO':
                return str(result['object_id'])
        except requests_exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise e

        return None

    def test_connection(self):
        """Test the Databricks connectivity from UI"""
        hook = DatabricksHook(databricks_conn_id=self.databricks_conn_id)
        try:
            hook._do_api_call(endpoint_info=SPARK_VERSIONS_ENDPOINT).get('versions')
            status = True
            message = 'Connection successfully tested'
        except Exception as e:
            status = False
            message = str(e)

        return status, message
