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

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class _DataProcJob(LoggingMixin):
    def __init__(self, dataproc_api, project_id, job, region='global',
                 job_error_states=None, num_retries=None):
        self.dataproc_api = dataproc_api
        self.project_id = project_id
        self.region = region
        self.num_retries = num_retries
        self.job = dataproc_api.projects().regions().jobs().submit(
            projectId=self.project_id,
            region=self.region,
            body=job).execute(num_retries=self.num_retries)
        self.job_id = self.job['reference']['jobId']
        self.job_error_states = job_error_states
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
                self.log.info('Driver output location: %s',
                              self.job['driverOutputResourceUri'])
                return False
            if 'CANCELLED' == self.job['status']['state']:
                self.log.warning('DataProc job %s is cancelled', self.job_id)
                if 'details' in self.job['status']:
                    self.log.warning(self.job['status']['details'])
                self.log.debug(str(self.job))
                self.log.info('Driver output location: %s',
                              self.job['driverOutputResourceUri'])
                return False
            if 'DONE' == self.job['status']['state']:
                self.log.info('Driver output location: %s',
                              self.job['driverOutputResourceUri'])
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
                job_type: {
                }
            }
        }
        if properties is not None:
            self.job["job"][job_type]["properties"] = properties

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
        super().__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version
        self.num_retries = self._get_field('num_retries', 5)

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
