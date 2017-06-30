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

    def create_job(self, project_name, job, use_existing_job_fn=None):
        """
        Launches a CloudML job and wait for it to reach a terminal state.

        :param project_name: The Google Cloud project name within which CloudML
            job will be launched.
        :type project_name: string

        :param job: CloudML Job object that should be provided to the CloudML
            API, such as:
            {
              'jobId': 'my_job_id',
              'trainingInput': {
                'scaleTier': 'STANDARD_1',
                ...
              }
            }
        :type job: dict

        :param use_existing_job_fn: In case that a CloudML job with the same
            job_id already exist, this method (if provided) will decide whether
            we should use this existing job, continue waiting for it to finish
            and returning the job object. It should accepts a CloudML job
            object, and returns a boolean value indicating whether it is OK to
            reuse the existing job. If 'use_existing_job_fn' is not provided,
            we by default reuse the existing CloudML job.
        :type use_existing_job_fn: function

        :return: The CloudML job object if the job successfully reach a
            terminal state (which might be FAILED or CANCELLED state).
        :rtype: dict
        """
        request = self._cloudml.projects().jobs().create(
            parent='projects/{}'.format(project_name),
            body=job)
        job_id = job['jobId']

        try:
            request.execute()
        except errors.HttpError as e:
            # 409 means there is an existing job with the same job ID.
            if e.resp.status == 409:
                if use_existing_job_fn is not None:
                    existing_job = self._get_job(project_name, job_id)
                    if not use_existing_job_fn(existing_job):
                        logging.error(
                            'Job with job_id {} already exist, but it does '
                            'not match our expectation: {}'.format(
                                job_id, existing_job))
                        raise
                logging.info(
                    'Job with job_id {} already exist. Will waiting for it to '
                    'finish'.format(job_id))
            else:
                logging.error('Failed to create CloudML job: {}'.format(e))
                raise
        return self._wait_for_job_done(project_name, job_id)

    def _get_job(self, project_name, job_id):
        """
        Gets a CloudML job based on the job name.

        :return: CloudML job object if succeed.
        :rtype: dict

        Raises:
            apiclient.errors.HttpError: if HTTP error is returned from server
        """
        job_name = 'projects/{}/jobs/{}'.format(project_name, job_id)
        request = self._cloudml.projects().jobs().get(name=job_name)
        while True:
            try:
                return request.execute()
            except errors.HttpError as e:
                if e.resp.status == 429:
                    # polling after 30 seconds when quota failure occurs
                    time.sleep(30)
                else:
                    logging.error('Failed to get CloudML job: {}'.format(e))
                    raise

    def _wait_for_job_done(self, project_name, job_id, interval=30):
        """
        Waits for the Job to reach a terminal state.

        This method will periodically check the job state until the job reach
        a terminal state.

        Raises:
            apiclient.errors.HttpError: if HTTP error is returned when getting
            the job
        """
        assert interval > 0
        while True:
            job = self._get_job(project_name, job_id)
            if job['state'] in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return job
            time.sleep(interval)

    def create_version(self, project_name, model_name, version_spec):
        """
        Creates the Version on Cloud ML.

        Returns the operation if the version was created successfully and
        raises an error otherwise.
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
