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
This module contains a Google ML Engine Hook.
"""

import random
import time
from typing import Dict, Callable, List, Optional

from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

from airflow.gcp.hooks.base import GoogleCloudBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


def _poll_with_exponential_delay(request, max_n, is_done_func, is_error_func):
    log = LoggingMixin().log

    for i in range(0, max_n):
        try:
            response = request.execute()
            if is_error_func(response):
                raise ValueError(
                    'The response contained an error: {}'.format(response)
                )
            if is_done_func(response):
                log.info('Operation is done: %s', response)
                return response

            time.sleep((2**i) + (random.randint(0, 1000) / 1000))
        except HttpError as e:
            if e.resp.status != 429:
                log.info('Something went wrong. Not retrying: %s', format(e))
                raise
            else:
                time.sleep((2**i) + (random.randint(0, 1000) / 1000))

    raise ValueError('Connection could not be established after {} retries.'.format(max_n))


class MLEngineHook(GoogleCloudBaseHook):
    """
    Hook for Google ML Engine APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """
    def __init__(self, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self._mlengine = self.get_conn()

    def get_conn(self):
        """
        Returns a Google MLEngine service object.
        """
        authed_http = self._authorize()
        return build('ml', 'v1', http=authed_http, cache_discovery=False)

    def create_job(self, project_id: str, job: Dict, use_existing_job_fn: Optional[Callable] = None) -> Dict:
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
        :param use_existing_job_fn: In case that a MLEngine job with the same
            job_id already exist, this method (if provided) will decide whether
            we should use this existing job, continue waiting for it to finish
            and returning the job object. It should accepts a MLEngine job
            object, and returns a boolean value indicating whether it is OK to
            reuse the existing job. If 'use_existing_job_fn' is not provided,
            we by default reuse the existing MLEngine job.
        :type use_existing_job_fn: function
        :return: The MLEngine job object if the job successfully reach a
            terminal state (which might be FAILED or CANCELLED state).
        :rtype: dict
        """
        request = self._mlengine.projects().jobs().create(  # pylint: disable=no-member
            parent='projects/{}'.format(project_id),
            body=job)
        job_id = job['jobId']

        try:
            request.execute()
        except HttpError as e:
            # 409 means there is an existing job with the same job ID.
            if e.resp.status == 409:
                if use_existing_job_fn is not None:
                    existing_job = self._get_job(project_id, job_id)
                    if not use_existing_job_fn(existing_job):
                        self.log.error(
                            'Job with job_id %s already exist, but it does '
                            'not match our expectation: %s',
                            job_id, existing_job
                        )
                        raise
                self.log.info(
                    'Job with job_id %s already exist. Will waiting for it to finish',
                    job_id
                )
            else:
                self.log.error('Failed to create MLEngine job: {}'.format(e))
                raise

        return self._wait_for_job_done(project_id, job_id)

    def _get_job(self, project_id: str, job_id: str) -> Dict:
        """
        Gets a MLEngine job based on the job name.

        :return: MLEngine job object if succeed.
        :rtype: dict
        :raises: googleapiclient.errors.HttpError
        """
        job_name = 'projects/{}/jobs/{}'.format(project_id, job_id)
        request = self._mlengine.projects().jobs().get(name=job_name)  # pylint: disable=no-member
        while True:
            try:
                return request.execute()
            except HttpError as e:
                if e.resp.status == 429:
                    # polling after 30 seconds when quota failure occurs
                    time.sleep(30)
                else:
                    self.log.error('Failed to get MLEngine job: {}'.format(e))
                    raise

    def _wait_for_job_done(self, project_id: str, job_id: str, interval: int = 30):
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

    def create_version(self, project_id: str, model_name: str, version_spec: Dict) -> Dict:
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

        return _poll_with_exponential_delay(
            request=get_request,
            max_n=9,
            is_done_func=lambda resp: resp.get('done', False),
            is_error_func=lambda resp: resp.get('error', None) is not None)

    def set_default_version(self, project_id: str, model_name: str, version_name: str) -> Dict:
        """
        Sets a version to be the default. Blocks until finished.
        """
        full_version_name = 'projects/{}/models/{}/versions/{}'.format(
            project_id, model_name, version_name)
        request = self._mlengine.projects().models().versions().setDefault(  # pylint: disable=no-member
            name=full_version_name, body={})

        try:
            response = request.execute()
            self.log.info('Successfully set version: %s to default', response)
            return response
        except HttpError as e:
            self.log.error('Something went wrong: %s', e)
            raise

    def list_versions(self, project_id: str, model_name: str) -> List[Dict]:
        """
        Lists all available versions of a model. Blocks until finished.
        """
        result = []  # type: List[Dict]
        full_parent_name = 'projects/{}/models/{}'.format(
            project_id, model_name)
        request = self._mlengine.projects().models().versions().list(  # pylint: disable=no-member
            parent=full_parent_name, pageSize=100)

        response = request.execute()
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

    def delete_version(self, project_id: str, model_name: str, version_name: str):
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

        return _poll_with_exponential_delay(
            request=get_request,
            max_n=9,
            is_done_func=lambda resp: resp.get('done', False),
            is_error_func=lambda resp: resp.get('error', None) is not None)

    def create_model(self, project_id: str, model: Dict) -> Dict:
        """
        Create a Model. Blocks until finished.
        """
        if not model['name']:
            raise ValueError("Model name must be provided and "
                             "could not be an empty string")
        project = 'projects/{}'.format(project_id)

        request = self._mlengine.projects().models().create(  # pylint: disable=no-member
            parent=project, body=model)
        return request.execute()

    def get_model(self, project_id: str, model_name: str) -> Optional[Dict]:
        """
        Gets a Model. Blocks until finished.
        """
        if not model_name:
            raise ValueError("Model name must be provided and "
                             "it could not be an empty string")
        full_model_name = 'projects/{}/models/{}'.format(
            project_id, model_name)
        request = self._mlengine.projects().models().get(name=full_model_name)  # pylint: disable=no-member
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status == 404:
                self.log.error('Model was not found: %s', e)
                return None
            raise
