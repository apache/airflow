# -*- coding: utf-8 -*-
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
#
"""
This module contains a Google Cloud Dataproc hook.
"""

import time
import uuid
from typing import Dict, List, Optional, Any, Iterable

from googleapiclient.discovery import build
from zope.deprecation import deprecation

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.version import version

UUID_LENGTH = 9


class DataprocJobStatus:
    """
    Helper class with Dataproc jobs statuses.
    """
    ERROR = "ERROR"
    CANCELLED = "CANCALLED"
    DONE = "DONE"


class _DataProcJob(LoggingMixin):
    def __init__(
        self,
        dataproc_api: Any,
        project_id: str,
        job: Dict,
        region: str = 'global',
        job_error_states: Iterable[str] = None,
        num_retries: int = 5
    ) -> None:
        self.dataproc_api = dataproc_api
        self.project_id = project_id
        self.region = region
        self.num_retries = num_retries
        self.job_error_states = job_error_states

        # Check if the job to submit is already running on the cluster.
        # If so, don't resubmit the job.
        try:
            cluster_name = job['job']['placement']['clusterName']
        except KeyError:
            self.log.error('Job to submit is incorrectly configured.')
            raise

        jobs_on_cluster_response = dataproc_api.projects().regions().jobs().list(
            projectId=self.project_id,
            region=self.region,
            clusterName=cluster_name).execute()

        jobs_on_cluster = jobs_on_cluster_response.get('jobs', [])
        try:
            task_id_to_submit = job['job']['reference']['jobId'][:-UUID_LENGTH]
        except KeyError:
            self.log.error('Job to submit is incorrectly configured.')
            raise

        # There is a small set of states that we will accept as sufficient
        # for attaching the new task instance to the old Dataproc job.  We
        # generally err on the side of _not_ attaching, unless the prior
        # job is in a known-good state. For example, we don't attach to an
        # ERRORed job because we want Airflow to be able to retry the job.
        # The full set of possible states is here:
        # https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/projects.regions.jobs#State
        recoverable_states = frozenset([
            'PENDING',
            'SETUP_DONE',
            'RUNNING',
            'DONE',
        ])

        found_match = False
        for job_on_cluster in jobs_on_cluster:
            job_on_cluster_id = job_on_cluster['reference']['jobId']
            job_on_cluster_task_id = job_on_cluster_id[:-UUID_LENGTH]
            if task_id_to_submit == job_on_cluster_task_id:

                self.job = job_on_cluster
                self.job_id = self.job['reference']['jobId']
                found_match = True

                # We can stop looking once we find a matching job in a recoverable state.
                if self.job['status']['state'] in recoverable_states:
                    break

        if found_match and self.job['status']['state'] in recoverable_states:
            message = """
    Reattaching to previously-started DataProc job %s (in state %s).
    If this is not the desired behavior (ie if you would like to re-run this job),
    please delete the previous instance of the job by running:

    gcloud --project %s dataproc jobs delete %s --region %s
"""
            self.log.info(
                message,
                self.job_id,
                str(self.job['status']['state']),
                self.project_id,
                self.job_id,
                self.region,
            )

            return

        self.job = dataproc_api.projects().regions().jobs().submit(
            projectId=self.project_id,
            region=self.region,
            body=job).execute(num_retries=self.num_retries)
        self.job_id = self.job['reference']['jobId']

        self.log.info(
            'DataProc job %s is %s',
            self.job_id, str(self.job['status']['state'])
        )

    def wait_for_done(self) -> bool:
        """
        Awaits the Dataproc job to complete.

        :return: True if job was done
        :rtype: bool
        """
        while True:
            self.job = self.dataproc_api.projects().regions().jobs().get(
                projectId=self.project_id,
                region=self.region,
                jobId=self.job_id).execute(num_retries=self.num_retries)

            if self.job['status']['state'] == DataprocJobStatus.ERROR:
                self.log.error('DataProc job %s has errors', self.job_id)
                self.log.error(self.job['status']['details'])
                self.log.debug(str(self.job))
                self.log.info('Driver output location: %s',
                              self.job['driverOutputResourceUri'])
                return False

            if self.job['status']['state'] == DataprocJobStatus.CANCELLED:
                self.log.warning('DataProc job %s is cancelled', self.job_id)
                if 'details' in self.job['status']:
                    self.log.warning(self.job['status']['details'])
                self.log.debug(str(self.job))
                self.log.info('Driver output location: %s',
                              self.job['driverOutputResourceUri'])
                return False

            if self.job['status']['state'] == DataprocJobStatus.DONE:
                self.log.info('Driver output location: %s',
                              self.job['driverOutputResourceUri'])
                return True

            self.log.debug(
                'DataProc job %s is %s',
                self.job_id, str(self.job['status']['state'])
            )
            time.sleep(5)

    def raise_error(self, message=None):
        """
        Raises error when Dataproc job resulted in error.

        :param message: Custom message for the error.
        :raises: Exception
        """
        job_state = self.job['status']['state']
        # We always consider ERROR to be an error state.
        error = (self.job_error_states and job_state in self.job_error_states)
        error = error or (job_state == DataprocJobStatus.ERROR)
        if error:
            ex_message = message or ("Google DataProc job has state: %s" % job_state)
            ex_details = (str(self.job['status']['details'])
                          if 'details' in self.job['status']
                          else "No details available")
            raise Exception(ex_message + ": " + ex_details)

    def get(self) -> Dict:
        """
        Returns Dataproc job.
        """
        return self.job


class _DataProcJobBuilder:
    def __init__(
        self,
        project_id: str,
        task_id: str,
        cluster_name: str,
        job_type: str,
        properties: Dict[str, str]
    ) -> None:
        name = task_id + "_" + str(uuid.uuid4())[:8]
        self.job_type = job_type
        self.job = {
            "job": {
                "reference": {
                    "projectId": project_id,
                    "jobId": name,
                },
                "placement": {
                    "clusterName": cluster_name
                },
                "labels": {'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')},
                job_type: {
                }
            }
        }  # type: Dict[str, Any]
        if properties is not None:
            self.job["job"][job_type]["properties"] = properties

    def add_labels(self, labels):
        """
        Set labels for Dataproc job.

        :param labels: Labels for the job query.
        :type labels: dict
        """
        if labels:
            self.job["job"]["labels"].update(labels)

    def add_variables(self, variables: List[str]) -> None:
        """
        Set variables for Dataproc job.

        :param variables: Variables for the job query.
        :type variables: List[str]
        """
        if variables is not None:
            self.job["job"][self.job_type]["scriptVariables"] = variables

    def add_args(self, args: List[str]) -> None:
        """
        Set args for Dataproc job.

        :param args: Args for the job query.
        :type args: List[str]
        """
        if args is not None:
            self.job["job"][self.job_type]["args"] = args

    def add_query(self, query: List[str]) -> None:
        """
        Set query uris for Dataproc job.

        :param query: URIs for the job queries.
        :type query: List[str]
        """
        self.job["job"][self.job_type]["queryList"] = {'queries': [query]}

    def add_query_uri(self, query_uri: str) -> None:
        """
        Set query uri for Dataproc job.

        :param query_uri: URI for the job query.
        :type query_uri: str
        """
        self.job["job"][self.job_type]["queryFileUri"] = query_uri

    def add_jar_file_uris(self, jars: List[str]) -> None:
        """
        Set jars uris for Dataproc job.

        :param jars: List of jars URIs
        :type jars: List[str]
        """
        if jars is not None:
            self.job["job"][self.job_type]["jarFileUris"] = jars

    def add_archive_uris(self, archives: List[str]) -> None:
        """
        Set archives uris for Dataproc job.

        :param archives: List of archives URIs
        :type archives: List[str]
        """
        if archives is not None:
            self.job["job"][self.job_type]["archiveUris"] = archives

    def add_file_uris(self, files: List[str]) -> None:
        """
        Set file uris for Dataproc job.

        :param files: List of files URIs
        :type files: List[str]
        """
        if files is not None:
            self.job["job"][self.job_type]["fileUris"] = files

    def add_python_file_uris(self, pyfiles: List[str]) -> None:
        """
        Set python file uris for Dataproc job.

        :param pyfiles: List of python files URIs
        :type pyfiles: List[str]
        """
        if pyfiles is not None:
            self.job["job"][self.job_type]["pythonFileUris"] = pyfiles

    def set_main(self, main_jar: Optional[str], main_class: Optional[str]) -> None:
        """
        Set Dataproc main class.

        :param main_jar: URI for the main file.
        :type main_jar: str
        :param main_class: Name of the main class.
        :type main_class: str
        :raises: Exception
        """
        if main_class is not None and main_jar is not None:
            raise Exception("Set either main_jar or main_class")
        if main_jar:
            self.job["job"][self.job_type]["mainJarFileUri"] = main_jar
        else:
            self.job["job"][self.job_type]["mainClass"] = main_class

    def set_python_main(self, main: str) -> None:
        """
        Set Dataproc main python file uri.

        :param main: URI for the python main file.
        :type main: str
        """
        self.job["job"][self.job_type]["mainPythonFileUri"] = main

    def set_job_name(self, name: str) -> None:
        """
        Set Dataproc job name.

        :param name: Job name.
        :type name: str
        """
        self.job["job"]["reference"]["jobId"] = name + "_" + str(uuid.uuid4())[:8]

    def build(self) -> Dict:
        """
        Returns Dataproc job.

        :return: Dataproc job
        :rtype: dict
        """
        return self.job


class _DataProcOperation(LoggingMixin):
    """
    Continuously polls Dataproc Operation until it completes.
    """

    def __init__(self, dataproc_api: Any, operation: Dict, num_retries: int) -> None:
        self.dataproc_api = dataproc_api
        self.operation = operation
        self.operation_name = self.operation['name']
        self.num_retries = num_retries

    def wait_for_done(self) -> bool:
        """
        Awaits Dataproc operation to complete.

        :return: True if operation was done.
        :rtype: bool
        """
        if self._check_done():
            return True

        self.log.info('Waiting for Dataproc Operation %s to finish', self.operation_name)
        while True:
            time.sleep(10)
            self.operation = (
                self.dataproc_api.projects()
                .regions()
                .operations()
                .get(name=self.operation_name)
                .execute(num_retries=self.num_retries)
            )

            if self._check_done():
                return True

    def get(self) -> Dict:
        """
        Returns Dataproc operation.

        :return: Dataproc operation
        """
        return self.operation

    def _check_done(self) -> bool:
        if 'done' in self.operation:
            if 'error' in self.operation:
                self.log.warning(
                    'Dataproc Operation %s failed with error: %s',
                    self.operation_name, self.operation['error']['message'])
                self._raise_error()
            else:
                self.log.info(
                    'Dataproc Operation %s done', self.operation['name'])
                return True
        return False

    def _raise_error(self):
        raise Exception('Google Dataproc Operation %s failed: %s' %
                        (self.operation_name, self.operation['error']['message']))


class DataProcHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Dataproc APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param api_version: Version of Google Cloud API
    :type api_version: str
    """
    def __init__(
        self,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: str = None,
        api_version: str = 'v1beta2'
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self):
        """
        Returns a Google Cloud Dataproc service object.
        """
        http_authorized = self._authorize()
        return build(
            'dataproc', self.api_version, http=http_authorized,
            cache_discovery=False)

    def get_cluster(self, project_id: str, region: str, cluster_name: str):
        """
        Returns Google Cloud Dataproc cluster.

        :param project_id: The id of Google Cloud Dataproc project.
        :type project_id: str
        :param region: The region of Google Dataproc cluster.
        :type region: str
        :param cluster_name: The name of the Dataproc cluster.
        :type cluster_name: str
        :return: Dataproc cluster
        :rtype: dict
        """
        return self.get_conn().projects().regions().clusters().get(  # pylint: disable=no-member
            projectId=project_id,
            region=region,
            clusterName=cluster_name
        ).execute(num_retries=self.num_retries)

    def submit(
        self,
        project_id: str,
        job: Dict,
        region: str = 'global',
        job_error_states: Iterable[str] = None
    ) -> None:
        """
        Submits Google Cloud Dataproc job.

        :param project_id: The id of Google Cloud Dataproc project.
        :type project_id: str
        :param job: The job to be submitted
        :type job: dict
        :param region: The region of Google Dataproc cluster.
        :type region: str
        :param job_error_states: Job states that should be considered error states.
        :type job_error_states: List[str]
        :raises: Excepion
        """
        submitted = _DataProcJob(self.get_conn(), project_id, job, region,
                                 job_error_states=job_error_states,
                                 num_retries=self.num_retries)
        if not submitted.wait_for_done():
            submitted.raise_error()

    def create_job_template(
        self,
        task_id: str,
        cluster_name: str,
        job_type: str,
        properties: Dict[str, str],
    ) -> _DataProcJobBuilder:
        """
        Creates Google Cloud Dataproc job template.

        :param task_id: id of the task
        :type task_id: str
        :param cluster_name: Dataproc cluster name.
        :type cluster_name: str
        :param job_type: Type of Dataproc job.
        :type job_type: str
        :param properties: Additional properties of the job.
        :type properties: dict
        :return: Dataproc Job
        """
        if not self.project_id:
            raise AirflowException(
                "The project ID could not be determined. You should specify the project id "
                "in the connection configuration."
            )
        return _DataProcJobBuilder(
            self.project_id,
            task_id,
            cluster_name,
            job_type,
            properties
        )

    def wait(self, operation: Dict) -> None:
        """
        Awaits for Google Cloud Dataproc Operation to complete.
        """
        submitted = _DataProcOperation(self.get_conn(), operation,
                                       self.num_retries)
        submitted.wait_for_done()

    def cancel(self, project_id: str, job_id: str, region: str = 'global') -> Dict:
        """
        Cancel a Google Cloud DataProc job.

        :param project_id: Name of the project the job belongs to
        :type project_id: str
        :param job_id: Identifier of the job to cancel
        :type job_id: int
        :param region: Region used for the job
        :type region: str
        :return: A Job json dictionary representing the canceled job
        """
        return self.get_conn().projects().regions().jobs().cancel(  # pylint: disable=no-member
            projectId=project_id,
            region=region,
            jobId=job_id
        )

    def get_final_cluster_state(self, project_id, region, cluster_name, logger):
        """
        Poll for the state of a cluster until one is available

        :param project_id:
        :param region:
        :param cluster_name:
        :param logger:
        :return:
        """
        while True:
            state = DataProcHook.get_cluster_state(self.get_conn(), project_id, region, cluster_name)
            if state is None:
                logger.info("No state for cluster '%s'", cluster_name)
                time.sleep(15)
            else:
                logger.info("State for cluster '%s' is %s", cluster_name, state)
                return state

    @staticmethod
    def get_cluster_state(service, project_id, region, cluster_name):
        """
        Get the state of a cluster if it has one, otherwise None
        :param service:
        :param project_id:
        :param region:
        :param cluster_name:
        :return:
        """
        cluster = DataProcHook.find_cluster(service, project_id, region, cluster_name)
        if cluster and 'status' in cluster:
            return cluster['status']['state']
        else:
            return None

    @staticmethod
    def find_cluster(service, project_id, region, cluster_name):
        """
        Retrieve a cluster from the project/region if it exists, otherwise None
        :param service:
        :param project_id:
        :param region:
        :param cluster_name:
        :return:
        """
        cluster_list = DataProcHook.get_cluster_list_for_project(service, project_id, region)
        cluster = [c for c in cluster_list if c['clusterName'] == cluster_name]
        if cluster:
            return cluster[0]
        return None

    @staticmethod
    def get_cluster_list_for_project(service, project_id, region):
        """
        List all clusters for a given project/region, an empty list if none exist
        :param service:
        :param project_id:
        :param region:
        :return:
        """
        result = service.projects().regions().clusters().list(
            projectId=project_id,
            region=region
        ).execute()
        return result.get('clusters', [])

    @staticmethod
    def execute_dataproc_diagnose(service, project_id, region, cluster_name):
        """
        Execute the diagonse command against a given cluster, useful to get debugging
        information if something has gone wrong or cluster creation failed.
        :param service:
        :param project_id:
        :param region:
        :param cluster_name:
        :return:
        """
        response = service.projects().regions().clusters().diagnose(
            projectId=project_id,
            region=region,
            clusterName=cluster_name,
            body={}
        ).execute()
        operation_name = response['name']
        return operation_name

    @staticmethod
    def execute_delete(service, project_id, region, cluster_name):
        """
        Delete a specified cluster
        :param service:
        :param project_id:
        :param region:
        :param cluster_name:
        :return: The identifier of the operation being executed
        """
        response = service.projects().regions().clusters().delete(
            projectId=project_id,
            region=region,
            clusterName=cluster_name
        ).execute(num_retries=5)
        operation_name = response['name']
        return operation_name

    @staticmethod
    def wait_for_operation_done(service, operation_name):
        """
        Poll for the completion of a specific GCP operation
        :param service:
        :param operation_name:
        :return: The response code of the completed operation
        """
        while True:
            response = service.projects().regions().operations().get(
                name=operation_name
            ).execute(num_retries=5)

            if response.get('done'):
                return response
            time.sleep(15)

    @staticmethod
    def wait_for_operation_done_or_error(service, operation_name):
        """
        Block until the specified operation is done. Throws an AirflowException if
        the operation completed but had an error
        :param service:
        :param operation_name:
        :return:
        """
        response = DataProcHook.wait_for_operation_done(service, operation_name)
        if response.get('done'):
            if 'error' in response:
                raise AirflowException(str(response['error']))
            else:
                return


setattr(
    DataProcHook,
    "await",
    deprecation.deprecated(
        DataProcHook.wait, "renamed to 'wait' for Python3.7 compatibility"
    ),
)
