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
import time
import uuid

from googleapiclient.discovery import build
from zope.deprecation import deprecation

from airflow.version import version
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class _DataProcJob(LoggingMixin):
    def __init__(self, dataproc_api, project_id, job, region='global',
                 job_error_states=None, num_retries=5):
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

        UUID_LENGTH = 9
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

    def wait_for_done(self):
        while True:
            self.job = self.dataproc_api.projects().regions().jobs().get(
                projectId=self.project_id,
                region=self.region,
                jobId=self.job_id).execute(num_retries=self.num_retries)
            if 'ERROR' == self.job['status']['state']:
                self.log.error('DataProc job %s has errors', self.job_id)
                self.log.error(self.job['status']['details'])
                self.log.debug(str(self.job))
                return False
            if 'CANCELLED' == self.job['status']['state']:
                self.log.warning('DataProc job %s is cancelled', self.job_id)
                if 'details' in self.job['status']:
                    self.log.warning(self.job['status']['details'])
                self.log.debug(str(self.job))
                return False
            if 'DONE' == self.job['status']['state']:
                return True
            self.log.debug(
                'DataProc job %s is %s',
                self.job_id, str(self.job['status']['state'])
            )
            time.sleep(5)

    def raise_error(self, message=None):
        job_state = self.job['status']['state']
        # We always consider ERROR to be an error state.
        if (self.job_error_states and job_state in self.job_error_states) or 'ERROR' == job_state:
            ex_message = message or ("Google DataProc job has state: %s" % job_state)
            ex_details = (str(self.job['status']['details'])
                          if 'details' in self.job['status']
                          else "No details available")
            raise Exception(ex_message + ": " + ex_details)

    def get(self):
        return self.job


class _DataProcJobBuilder:
    def __init__(self, project_id, task_id, cluster_name, job_type, properties):
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
        }
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

    def add_variables(self, variables):
        if variables is not None:
            self.job["job"][self.job_type]["scriptVariables"] = variables

    def add_args(self, args):
        if args is not None:
            self.job["job"][self.job_type]["args"] = args

    def add_query(self, query):
        self.job["job"][self.job_type]["queryList"] = {'queries': [query]}

    def add_query_uri(self, query_uri):
        self.job["job"][self.job_type]["queryFileUri"] = query_uri

    def add_jar_file_uris(self, jars):
        if jars is not None:
            self.job["job"][self.job_type]["jarFileUris"] = jars

    def add_archive_uris(self, archives):
        if archives is not None:
            self.job["job"][self.job_type]["archiveUris"] = archives

    def add_file_uris(self, files):
        if files is not None:
            self.job["job"][self.job_type]["fileUris"] = files

    def add_python_file_uris(self, pyfiles):
        if pyfiles is not None:
            self.job["job"][self.job_type]["pythonFileUris"] = pyfiles

    def set_main(self, main_jar, main_class):
        if main_class is not None and main_jar is not None:
            raise Exception("Set either main_jar or main_class")
        if main_jar:
            self.job["job"][self.job_type]["mainJarFileUri"] = main_jar
        else:
            self.job["job"][self.job_type]["mainClass"] = main_class

    def set_python_main(self, main):
        self.job["job"][self.job_type]["mainPythonFileUri"] = main

    def set_job_name(self, name):
        self.job["job"]["reference"]["jobId"] = name + "_" + str(uuid.uuid4())[:8]

    def build(self):
        return self.job


class _DataProcOperation(LoggingMixin):
    """Continuously polls Dataproc Operation until it completes."""
    def __init__(self, dataproc_api, operation, num_retries):
        self.dataproc_api = dataproc_api
        self.operation = operation
        self.operation_name = self.operation['name']
        self.num_retries = num_retries

    def wait_for_done(self):
        if self._check_done():
            return True

        self.log.info(
            'Waiting for Dataproc Operation %s to finish', self.operation_name)
        while True:
            time.sleep(10)
            self.operation = (
                self.dataproc_api.projects()
                .regions()
                .operations()
                .get(name=self.operation_name)
                .execute(num_retries=self.num_retries))

            if self._check_done():
                return True

    def get(self):
        return self.operation

    def _check_done(self):
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
    """Hook for Google Cloud Dataproc APIs."""

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 api_version='v1beta2'):
        super(DataProcHook, self).__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self):
        """Returns a Google Cloud Dataproc service object."""
        http_authorized = self._authorize()
        return build(
            'dataproc', self.api_version, http=http_authorized,
            cache_discovery=False)

    def get_cluster(self, project_id, region, cluster_name):
        return self.get_conn().projects().regions().clusters().get(
            projectId=project_id,
            region=region,
            clusterName=cluster_name
        ).execute(num_retries=self.num_retries)

    def submit(self, project_id, job, region='global', job_error_states=None):
        submitted = _DataProcJob(self.get_conn(), project_id, job, region,
                                 job_error_states=job_error_states,
                                 num_retries=self.num_retries)
        if not submitted.wait_for_done():
            submitted.raise_error()

    def create_job_template(self, task_id, cluster_name, job_type, properties):
        return _DataProcJobBuilder(self.project_id, task_id, cluster_name,
                                   job_type, properties)

    def wait(self, operation):
        """Awaits for Google Cloud Dataproc Operation to complete."""
        submitted = _DataProcOperation(self.get_conn(), operation,
                                       self.num_retries)
        submitted.wait_for_done()

    def cancel(self, project_id, job_id, region='global'):
        """
        Cancel a Google Cloud DataProc job.
        :param project_id: Name of the project the job belongs to
        :type project_id: str
        :param job_id: Identifier of the job to cancel
        :type job_id: int
        :param region: Region used for the job
        :type region: str
        :returns A Job json dictionary representing the canceled job
        """
        return self.get_conn().projects().regions().jobs().cancel(
            projectId=project_id,
            region=region,
            jobId=job_id
        )


setattr(
    DataProcHook,
    "await",
    deprecation.deprecated(
        DataProcHook.wait, "renamed to 'wait' for Python3.7 compatibility"
    ),
)
