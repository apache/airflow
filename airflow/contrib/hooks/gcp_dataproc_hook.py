# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging
import time
import uuid

from apiclient.discovery import build

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class _DataProcJob:
    def __init__(self, dataproc_api, project_id, job):
        self.dataproc_api = dataproc_api
        self.project_id = project_id
        self.job = dataproc_api.projects().regions().jobs().submit(
            projectId=self.project_id,
            region='global',
            body=job).execute()
        self.job_id = self.job['reference']['jobId']
        logging.info('DataProc job %s is %s', self.job_id,
                     str(self.job['status']['state']))

    def wait_for_done(self):
        while True:
            self.job = self.dataproc_api.projects().regions().jobs().get(
                projectId=self.project_id,
                region='global',
                jobId=self.job_id).execute()
            if 'ERROR' == self.job['status']['state']:
                print(str(self.job))
                logging.error('DataProc job %s has errors', self.job_id)
                logging.error(self.job['status']['details'])
                logging.debug(str(self.job))
                return False
            if 'CANCELLED' == self.job['status']['state']:
                print(str(self.job))
                logging.warning('DataProc job %s is cancelled', self.job_id)
                if 'details' in self.job['status']:
                    logging.warning(self.job['status']['details'])
                logging.debug(str(self.job))
                return False
            if 'DONE' == self.job['status']['state']:
                return True
            logging.debug('DataProc job %s is %s', self.job_id,
                          str(self.job['status']['state']))
            time.sleep(5)

    def raise_error(self, message=None):
        if 'ERROR' == self.job['status']['state']:
            if message is None:
                message = "Google DataProc job has error"
            raise Exception(message + ": " + str(self.job['status']['details']))

    def get(self):
        return self.job


class _DataProcJobBuilder:
    def __init__(self, project_id, task_id, dataproc_cluster, job_type, properties):
        name = task_id + "_" + str(uuid.uuid1())[:8]
        self.job_type = job_type
        self.job = {
            "job": {
                "reference": {
                    "projectId": project_id,
                    "jobId": name,
                },
                "placement": {
                    "clusterName": dataproc_cluster
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

    def add_jar_file_uris(self, jars):
        if jars is not None:
            self.job["job"][self.job_type]["jarFileUris"] = jars

    def add_archive_uris(self, archives):
        if archives is not None:
            self.job["job"][self.job_type]["archiveUris"] = archives

    def set_main(self, main_jar, main_class):
        if main_class is not None and main_jar is not None:
            raise Exception("Set either main_jar or main_class")
        if main_jar:
            self.job["job"][self.job_type]["mainJarFileUri"] = main_jar
        else:
            self.job["job"][self.job_type]["mainClass"] = main_class

    def set_python_main(self, main):
        self.job["job"][self.job_type]["mainPythonFileUri"] = main

    def build(self):
        return self.job


class DataProcHook(GoogleCloudBaseHook):
    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(DataProcHook, self).__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud DataProc service object.
        """
        http_authorized = self._authorize()
        return build('dataproc', 'v1', http=http_authorized)

    def submit(self, project_id, job):
        submitted = _DataProcJob(self.get_conn(), project_id, job)
        if not submitted.wait_for_done():
            submitted.raise_error("DataProcTask has errors")

    def create_job_template(self, task_id, dataproc_cluster, job_type, properties):
        return _DataProcJobBuilder(self.project_id, task_id, dataproc_cluster, job_type,
                                   properties)
