#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
import time
from apiclient.discovery import build
from apiclient import errors
from oauth2client.client import GoogleCredentials
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow import settings

logging.getLogger('GoogleCloudML').setLevel(settings.LOGGING_LEVEL)


class _CloudMLJob(object):
    """CloudML job operations helper class."""

    def __init__(self, cloudml, project_name, job_id, job_spec=None):
        assert project_name is not None and project_name is not ''
        self._cloudml = cloudml
        self._project_name = 'projects/{}'.format(project_name)
        self._job_id = job_id
        assert self._job_id is not None and self._job_id is not ''
        self._job_spec = job_spec
        if self._job_spec:
            assert self._job_id == self._job_spec['jobId']

    def get_job(self):
        """Gets a CloudML job based on the job name.

        :return: CloudML job object if succeed.
        :rtype: dict
        """
        name = '{}/jobs/{}'.format(self._project_name, self._job_id)
        request = self._cloudml.projects().jobs().get(name=name)
        while True:
            try:
                return request.execute()
            except errors.HttpError as e:
                if e.resp.status == 429:
                    time.sleep(10)  # polling after 10 seconds
                else:
                    logging.error('Failed to get CloudML job: {}'.format(e))
                    raise e

    def create_job(self):
        """Creates a Job on Cloud ML.

        :return: CloudML job creation request response.
        :rtype: dict
        """
        request = self._cloudml.projects().jobs().create(
            parent=self._project_name, body=self._job_spec)
        try:
            return request.execute()
        except errors.HttpError as e:
            logging.error('Failed to create CloudML job: {}'.format(e))
            raise

    def wait_for_done(self, interval):
        """Waits for the Job to reach a terminal state.

        This method will periodically check the job state until the job reach
        a terminal state.

        :param interval: Polling interval in seconds.
        :return: CloudML job object if succeed.
        :rtype: dict
        """
        assert interval > 0
        while True:
            job = self.get_job()
            state = job['state']
            if state in ['FAILED', 'SUCCEEDED', 'CANCELLED']:
                return job
            time.sleep(interval)


class CloudMLHook(GoogleCloudBaseHook):

    def __init__(self, gcp_conn_id='google_cloud_default', delegate_to=None):
        super(CloudMLHook, self).__init__(gcp_conn_id, delegate_to)
        self._cloudml = self.get_conn()

    def get_conn(self):
        """
        Returns a Google CloudML service object.
        """
        credentials = GoogleCredentials.get_application_default()
        return build('ml', 'v1', credentials=credentials)

    def create_job(self, project_name, job):
        """
        Creates a CloudML Job, and returns the Job object, which can be waited
        upon.

        project_name is the name of the project to use, such as
        'my-project'

        job is the complete Cloud ML Job object that should be provided to the
        Cloud ML API, such as

        {
          'jobId': 'my_job_id',
          'trainingInput': {
            'scaleTier': 'STANDARD_1',
            ...
          }
        }
        """
        cloudml_job = _CloudMLJob(
            self._cloudml, project_name, job['jobId'], job)
        cloudml_job.create_job()
        return cloudml_job.wait_for_done(10)  # Polling interval is 10 sec

    def get_job(self, project_name, job_id):
        """Gets a CloudML job based on the job name."""
        cloudml_job = _CloudMLJob(self._cloudml, project_name, job_id)
        return cloudml_job.get_job()

    def wait_for_job_done(self, project_name, job_id):
        """Waits for the Job to reach a terminal state."""
        cloudml_job = _CloudMLJob(self._cloudml, project_name, job_id)
        return cloudml_job.wait_for_done(10)  # Polling interval is 10 sec
