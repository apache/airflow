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
from typing import Callable, Dict, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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
    def get_conn(self):
        """
        Retrieves the connection to MLEngine.

        :return: Google MLEngine services object.
        """
        authed_http = self._authorize()
        return build('ml', 'v1', http=authed_http, cache_discovery=False)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_job(
        self,
        job: Dict,
        project_id: Optional[str] = None,
        use_existing_job_fn: Optional[Callable] = None
    ) -> Dict:
        """
        Launches a MLEngine job and wait for it to reach a terminal state.

        :param project_id: The Google Cloud project id within which MLEngine
            job will be launched. If set to None or missing, the default project_id from the GCP
            connection is used.
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
        assert project_id is not None

        hook = self.get_conn()

        request = hook.projects().jobs().create(  # pylint: disable=no-member
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
        Gets a MLEngine job based on the job id.

        :param project_id: The project in which the Job is located.
            If set to None or missing, the default project_id from the GCP connection is used. (templated)
        :type project_id: str
        :param job_id: A unique id for the Google MLEngine job. (templated)
        :type job_id: str
        :return: MLEngine job object if succeed.
        :rtype: dict
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()
        job_name = 'projects/{}/jobs/{}'.format(project_id, job_id)
        request = hook.projects().jobs().get(name=job_name)  # pylint: disable=no-member
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

        :param project_id: The project in which the Job is located.
            If set to None or missing, the default project_id from the GCP connection is used. (templated)
        :type project_id: str
        :param job_id: A unique id for the Google MLEngine job. (templated)
        :type job_id: str
        :param interval: Time expressed in seconds after which the job status is checked again. (templated)
        :type interval: int
        :raises: googleapiclient.errors.HttpError
        """
        if interval <= 0:
            raise ValueError("Interval must be > 0")
        while True:
            job = self._get_job(project_id, job_id)
            if job['state'] in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return job
            time.sleep(interval)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_version(
        self,
        model_name: str,
        version_spec: Dict,
        project_id: Optional[str] = None,
    ) -> Dict:
        """
        Creates the Version on Google Cloud ML Engine.

        :param version_spec: A dictionary containing the information about the version. (templated)
        :type version_spec: dict
        :param model_name: The name of the Google Cloud ML Engine model that the version belongs to.
            (templated)
        :type model_name: str
        :param project_id: The Google Cloud project name to which MLEngine model belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
            (templated)
        :type project_id: str
        :return: If the version was created successfully, returns the operation.
            Otherwise raises an error .
        :rtype: dict
        """
        hook = self.get_conn()
        parent_name = 'projects/{}/models/{}'.format(project_id, model_name)
        create_request = hook.projects().models().versions().create(  # pylint: disable=no-member
            parent=parent_name, body=version_spec)
        response = create_request.execute()
        get_request = hook.projects().operations().get(  # pylint: disable=no-member
            name=response['name'])

        return _poll_with_exponential_delay(
            request=get_request,
            max_n=9,
            is_done_func=lambda resp: resp.get('done', False),
            is_error_func=lambda resp: resp.get('error', None) is not None)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def set_default_version(
        self,
        model_name: str,
        version_name: str,
        project_id: Optional[str] = None,
    ) -> Dict:
        """
        Sets a version to be the default. Blocks until finished.

        :param model_name: The name of the Google Cloud ML Engine model that the version belongs to.
            (templated)
        :type model_name: str
        :param version_name: A name to use for the version being operated upon. (templated)
        :type version_name: str
        :param project_id: The Google Cloud project name to which MLEngine model belongs.
            If set to None or missing, the default project_id from the GCP connection is used. (templated)
        :type project_id: str
        :return: If successful, return an instance of Version.
            Otherwise raises an error.
        :rtype: dict
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()
        full_version_name = 'projects/{}/models/{}/versions/{}'.format(
            project_id, model_name, version_name)
        request = hook.projects().models().versions().setDefault(  # pylint: disable=no-member
            name=full_version_name, body={})

        try:
            response = request.execute()
            self.log.info('Successfully set version: %s to default', response)
            return response
        except HttpError as e:
            self.log.error('Something went wrong: %s', e)
            raise

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def list_versions(
        self,
        model_name: str,
        project_id: Optional[str] = None,
    ) -> List[Dict]:
        """
        Lists all available versions of a model. Blocks until finished.

        :param model_name: The name of the Google Cloud ML Engine model that the version
            belongs to. (templated)
        :type model_name: str
        :param project_id: The Google Cloud project name to which MLEngine model belongs.
            If set to None or missing, the default project_id from the GCP connection is used. (templated)
        :type project_id: str
        :return: return an list of instance of Version.
        :rtype: List[Dict]
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()
        result = []  # type: List[Dict]
        full_parent_name = 'projects/{}/models/{}'.format(
            project_id, model_name)
        request = hook.projects().models().versions().list(  # pylint: disable=no-member
            parent=full_parent_name, pageSize=100)

        while request is not None:
            response = request.execute()
            result.extend(response.get('versions', []))

            request = hook.projects().models().versions().list_next(  # pylint: disable=no-member
                previous_request=request,
                previous_response=response)
            time.sleep(5)
        return result

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def delete_version(
        self,
        model_name: str,
        version_name: str,
        project_id: Optional[str] = None,
    ) -> Dict:
        """
        Deletes the given version of a model. Blocks until finished.

        :param model_name: The name of the Google Cloud ML Engine model that the version
            belongs to. (templated)
        :type model_name: str
        :param project_id: The Google Cloud project name to which MLEngine
            model belongs.
        :type project_id: str
        :return: If the version was deleted successfully, returns the operation.
            Otherwise raises an error.
        :rtype: Dict
        """
        assert project_id is not None

        hook = self.get_conn()
        full_name = 'projects/{}/models/{}/versions/{}'.format(
            project_id, model_name, version_name)
        delete_request = hook.projects().models().versions().delete(  # pylint: disable=no-member
            name=full_name)
        response = delete_request.execute()
        get_request = hook.projects().operations().get(  # pylint: disable=no-member
            name=response['name'])

        return _poll_with_exponential_delay(
            request=get_request,
            max_n=9,
            is_done_func=lambda resp: resp.get('done', False),
            is_error_func=lambda resp: resp.get('error', None) is not None)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_model(
        self,
        model: Dict,
        project_id: Optional[str] = None
    ) -> Dict:
        """
        Create a Model. Blocks until finished.

        :param model: A dictionary containing the information about the model.
        :type model: dict
        :param project_id: The Google Cloud project name to which MLEngine model belongs.
            If set to None or missing, the default project_id from the GCP connection is used. (templated)
        :type project_id: str
        :return: If the version was created successfully, returns the instance of Model.
            Otherwise raises an error.
        :rtype: Dict
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()
        if not model['name']:
            raise ValueError("Model name must be provided and "
                             "could not be an empty string")
        project = 'projects/{}'.format(project_id)

        request = hook.projects().models().create(  # pylint: disable=no-member
            parent=project, body=model)
        return request.execute()

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def get_model(
        self,
        model_name: str,
        project_id: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Gets a Model. Blocks until finished.

        :param model_name: The name of the model.
        :type model_name: str
        :param project_id: The Google Cloud project name to which MLEngine model belongs.
            If set to None or missing, the default project_id from the GCP connection is used. (templated)
        :type project_id: str
        :return: If the model exists, returns the instance of Model.
            Otherwise return None.
        :rtype: Dict
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()
        if not model_name:
            raise ValueError("Model name must be provided and "
                             "it could not be an empty string")
        full_model_name = 'projects/{}/models/{}'.format(
            project_id, model_name)
        request = hook.projects().models().get(name=full_model_name)  # pylint: disable=no-member
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status == 404:
                self.log.error('Model was not found: %s', e)
                return None
            raise

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def delete_model(
        self,
        model_name: str,
        delete_contents: bool = False,
        project_id: Optional[str] = None,
    ) -> None:
        """
        Delete a Model. Blocks until finished.

        :param model_name: The name of the model.
        :type model_name: str
        :param delete_contents: Whether to force the deletion even if the models is not empty.
            Will delete all version (if any) in the dataset if set to True.
            The default value is False.
        :type delete_contents: bool
        :param project_id: The Google Cloud project name to which MLEngine model belongs.
            If set to None or missing, the default project_id from the GCP connection is used. (templated)
        :type project_id: str
        :raises: googleapiclient.errors.HttpError
        """
        assert project_id is not None

        hook = self.get_conn()

        if not model_name:
            raise ValueError("Model name must be provided and it could not be an empty string")
        model_path = 'projects/{}/models/{}'.format(project_id, model_name)
        if delete_contents:
            self._delete_all_versions(model_name, project_id)
        request = hook.projects().models().delete(name=model_path)  # pylint: disable=no-member
        try:
            request.execute()
        except HttpError as e:
            if e.resp.status == 404:
                self.log.error('Model was not found: %s', e)
                return
            raise

    def _delete_all_versions(self, model_name: str, project_id: str):
        versions = self.list_versions(project_id=project_id, model_name=model_name)
        # The default version can only be deleted when it is the last one in the model
        non_default_versions = (version for version in versions if not version.get('isDefault', False))
        for version in non_default_versions:
            _, _, version_name = version['name'].rpartition('/')
            self.delete_version(project_id=project_id, model_name=model_name, version_name=version_name)
        default_versions = (version for version in versions if version.get('isDefault', False))
        for version in default_versions:
            _, _, version_name = version['name'].rpartition('/')
            self.delete_version(project_id=project_id, model_name=model_name, version_name=version_name)
