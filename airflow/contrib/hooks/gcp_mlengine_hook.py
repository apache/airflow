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
"""
This module contains a Google ML Engine Hook.
"""
import time
from typing import Dict, List, Optional

from googleapiclient.discovery import build

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class MLEngineHook(GoogleCloudBaseHook):
    """
    Hook for Google ML Engine APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """
    def __init__(self, gcp_conn_id: str = 'google_cloud_default', delegate_to: str = None) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self.num_retries = self._get_field('num_retries', 9)  # type: int
        self._mlengine = self.get_conn()

    def get_conn(self):
        """
        Returns a Google MLEngine service object.
        """
        authed_http = self._authorize()
        return build('ml', 'v1', http=authed_http, cache_discovery=False)

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_job(self, job: Dict, project_id: str = None) -> Dict:
        """
        Launches a MLEngine job and wait for it to reach a terminal state.

        :param project_id: The Google Cloud project id within which MLEngine
            job will be launched.
        :type project_id: str
        :param job: MLEngine Job object that should be provided to the MLEngine
            API, such as: ::

                {
                  'jobId': 'my_job_id',
                  'trainingInput': {
                    'scaleTier': 'STANDARD_1',
                    ...
                  }
                }

        :type job: dict
        :return: The MLEngine job object if the job successfully reach a
            terminal state (which might be FAILED or CANCELLED state).
        :rtype: dict
        """
        request = self._mlengine.projects().jobs().create(  # pylint: disable=no-member
            parent='projects/{}'.format(project_id),
            body=job)
        job_id = job['jobId']

        request.execute(num_retries=self.num_retries)
        return self._wait_for_job_done(project_id, job_id)

    def _get_job(self, job_id: str, project_id: str) -> Dict:
        """
        Gets a MLEngine job based on the job name.

        :return: MLEngine job object if succeed.
        :rtype: dict
        :raises: googleapiclient.errors.HttpError
        """
        job_name = 'projects/{}/jobs/{}'.format(project_id, job_id)
        request = self._mlengine.projects().jobs().get(name=job_name)  # pylint: disable=no-member

        return request.execute(num_retries=self.num_retries)

    def _wait_for_job_done(self, job_id: str, project_id: str, interval: int = 30):
        """
        Waits for the Job to reach a terminal state.

        This method will periodically check the job state until the job reach
        a terminal state.
        :raises: googleapiclient.errors.HttpError
        """
        if interval <= 0:
            raise ValueError("Interval must be > 0")
        while True:
            job = self._get_job(project_id, job_id)
            if job['state'] in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return job
            time.sleep(interval)

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_version(self, model_name: str, version_spec: Dict, project_id: str = None,) -> Dict:
        """
        Creates the Version on Google Cloud ML Engine.

        Returns the operation if the version was created successfully and
        raises an error otherwise.
        """
        parent_name = 'projects/{}/models/{}'.format(project_id, model_name)
        create_request = self._mlengine.projects().models().versions().create(  # pylint: disable=no-member
            parent=parent_name, body=version_spec)
        response = create_request.execute()
        get_request = self._mlengine.projects().operations().get(  # pylint: disable=no-member
            name=response['name'])

        response = get_request.execute(num_retries=self.num_retries)
        if 'done' in response:
            self.log.info('Operation is done: %s', response)
            return response

        raise AirflowException('En error occurred: {}'.format(response))

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.fallback_to_default_project_id
    def set_default_version(self, model_name: str, version_name: str, project_id: str = None) -> Dict:
        """
        Sets a version to be the default. Blocks until finished.
        """
        full_version_name = 'projects/{}/models/{}/versions/{}'.format(
            project_id, model_name, version_name)
        request = self._mlengine.projects().models().versions().setDefault(  # pylint: disable=no-member
            name=full_version_name, body={})

        return request.execute(num_retries=self.num_retries)

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.fallback_to_default_project_id
    def list_versions(self, model_name: str, project_id: str = None) -> List[Dict]:
        """
        Lists all available versions of a model. Blocks until finished.
        """
        result = []  # type: List[Dict]
        full_parent_name = 'projects/{}/models/{}'.format(
            project_id, model_name)
        request = self._mlengine.projects().models().versions().list(  # pylint: disable=no-member
            parent=full_parent_name, pageSize=100)

        response = request.execute(num_retries=self.num_retries)
        next_page_token = response.get('nextPageToken', None)
        result.extend(response.get('versions', []))
        while next_page_token is not None:
            next_request = self._mlengine.projects().models().versions().list(  # pylint: disable=no-member
                parent=full_parent_name,
                pageToken=next_page_token,
                pageSize=100)
            response = next_request.execute()
            next_page_token = response.get('nextPageToken', None)
            result.extend(response.get('versions', []))
            time.sleep(5)
        return result

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.fallback_to_default_project_id
    def delete_version(self, model_name: str, version_name: str, project_id: str = None):
        """
        Deletes the given version of a model. Blocks until finished.
        """
        full_name = 'projects/{}/models/{}/versions/{}'.format(
            project_id, model_name, version_name)
        delete_request = self._mlengine.projects().models().versions().delete(  # pylint: disable=no-member
            name=full_name)
        response = delete_request.execute()
        get_request = self._mlengine.projects().operations().get(  # pylint: disable=no-member
            name=response['name'])

        response = get_request.execute(num_retries=self.num_retries)
        if 'done' in response:
            self.log.info('Operation is done: %s', response)
            return response

        raise AirflowException('En error occured: {}'.format(response))

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_model(self, model: Dict,  project_id: str = None) -> Dict:
        """
        Create a Model. Blocks until finished.
        """
        if not model['name']:
            raise ValueError("Model name must be provided and "
                             "could not be an empty string")
        project = 'projects/{}'.format(project_id)

        request = self._mlengine.projects().models().create(  # pylint: disable=no-member
            parent=project, body=model)
        return request.execute(num_retries=self.num_retries)

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.fallback_to_default_project_id
    def get_model(self, model_name: str,  project_id: str = None) -> Optional[Dict]:
        """
        Gets a Model. Blocks until finished.
        """
        if not model_name:
            raise ValueError("Model name must be provided and "
                             "it could not be an empty string")
        full_model_name = 'projects/{}/models/{}'.format(
            project_id, model_name)
        request = self._mlengine.projects().models().get(name=full_model_name)  # pylint: disable=no-member
        return request.execute(num_retries=self.num_retries)
