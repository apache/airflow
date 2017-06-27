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
import random
import time
from airflow import settings
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from apiclient.discovery import build
from apiclient import errors
from oauth2client.client import GoogleCredentials

logging.getLogger('GoogleCloudML').setLevel(settings.LOGGING_LEVEL)


def _poll_with_exponential_delay(request, max_n, is_done_func, is_error_func):

    for i in range(0, max_n):
        try:
            response = request.execute()
            if is_error_func(response):
                raise ValueError(
                    'The response contained an error: {}'.format(response))
            elif is_done_func(response):
                logging.info('Operation is done: {}'.format(response))
                return response
            else:
                time.sleep((2**i) + (random.randint(0, 1000) / 1000))
        except errors.HttpError as e:
            if e.resp.status != 429:
                logging.info(
                    'Something went wrong. Not retrying: {}'.format(e))
                raise
            else:
                time.sleep((2**i) + (random.randint(0, 1000) / 1000))


class _CloudMLJob(object):
    """
    CloudML job operations helper class.
    """

    def __init__(self, cloudml, project_name, job_id, job_spec=None):
        assert project_name
        self._cloudml = cloudml
        self._project_name = 'projects/{}'.format(project_name)
        self._job_id = job_id

        assert self._job_id
        self._job_spec = job_spec
        if self._job_spec:
            assert self._job_id == self._job_spec['jobId']

    def get_job(self):
        """
        Gets a CloudML job based on the job name.

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
                    raise

    def create_job(self):
        """
        Creates a Job on Cloud ML.

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
        """
        Waits for the Job to reach a terminal state.

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

    def create_version(self, project_name, model_name, version_spec):
        """
        Creates the Version on Cloud ML.

        Returns the operation if the version was created successfully and raise
        an error otherwise.
        """
        parent_name = 'projects/{}/models/{}'.format(project_name, model_name)
        create_request = self._cloudml.projects().models().versions().create(
            parent=parent_name, body=version_spec)
        response = create_request.execute()
        get_request = self._cloudml.projects().operations().get(
            name=response['name'])

        return _poll_with_exponential_delay(
            request=get_request,
            max_n=9,
            is_done_func=lambda resp: resp.get('done', False),
            is_error_func=lambda resp: resp.get('error', None) is not None)

    def set_default_version(self, project_name, model_name, version_name):
        """
        Sets a version to be the default. Blocks until finished.
        """
        full_version_name = 'projects/{}/models/{}/versions/{}'.format(
            project_name, model_name, version_name)
        request = self._cloudml.projects().models().versions().setDefault(
            name=full_version_name, body={})

        try:
            response = request.execute()
            logging.info(
                'Successfully set version: {} to default'.format(response))
            return response
        except errors.HttpError as e:
            logging.error('Something went wrong: {}'.format(e))
            raise

    def list_versions(self, project_name, model_name):
        """
        Lists all available versions of a model. Blocks until finished.
        """
        result = []
        full_parent_name = 'projects/{}/models/{}'.format(
            project_name, model_name)
        request = self._cloudml.projects().models().versions().list(
            parent=full_parent_name, pageSize=100)

        response = request.execute()
        next_page_token = response.get('nextPageToken', None)
        result.extend(response.get('versions', []))
        while next_page_token is not None:
            next_request = self._cloudml.projects().models().versions().list(
                parent=full_parent_name,
                pageToken=next_page_token,
                pageSize=100)
            response = next_request.execute()
            next_page_token = response.get('nextPageToken', None)
            result.extend(response.get('versions', []))
            time.sleep(5)
        return result

    def delete_version(self, project_name, model_name, version_name):
        """
        Deletes the given version of a model. Blocks until finished.
        """
        full_name = 'projects/{}/models/{}/versions/{}'.format(
            project_name, model_name, version_name)
        delete_request = self._cloudml.projects().models().versions().delete(
            name=full_name)
        response = delete_request.execute()
        get_request = self._cloudml.projects().operations().get(
            name=response['name'])

        return _poll_with_exponential_delay(
            request=get_request,
            max_n=9,
            is_done_func=lambda resp: resp.get('done', False),
            is_error_func=lambda resp: resp.get('error', None) is not None)

    def create_model(self, project_name, model):
        """
        Create a Model. Blocks until finished.
        """
        assert model['name'] is not None and model['name'] is not ''
        project = 'projects/{}'.format(project_name)

        request = self._cloudml.projects().models().create(
            parent=project, body=model)
        return request.execute()

    def get_model(self, project_name, model_name):
        """
        Gets a Model. Blocks until finished.
        """
        assert model_name is not None and model_name is not ''
        full_model_name = 'projects/{}/models/{}'.format(
            project_name, model_name)
        request = self._cloudml.projects().models().get(name=full_model_name)
        try:
            return request.execute()
        except errors.HttpError as e:
            if e.resp.status == 404:
                logging.error('Model was not found: {}'.format(e))
                return None
            raise

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
        """
        Gets a CloudML job based on the job name.
        """
        cloudml_job = _CloudMLJob(self._cloudml, project_name, job_id)
        return cloudml_job.get_job()

    def wait_for_job_done(self, project_name, job_id):
        """
        Waits for the Job to reach a terminal state.
        """
        cloudml_job = _CloudMLJob(self._cloudml, project_name, job_id)
        return cloudml_job.wait_for_done(10)  # Polling interval is 10 sec
