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
"""This module contains a Google ML Engine Hook."""

from __future__ import annotations

import contextlib
import logging
import random
import time
from typing import TYPE_CHECKING, Callable

from aiohttp import ClientSession
from gcloud.aio.auth import AioSession, Token
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)
from airflow.version import version as airflow_version

if TYPE_CHECKING:
    from httplib2 import Response
    from requests import Session

log = logging.getLogger(__name__)

_AIRFLOW_VERSION = "v" + airflow_version.replace(".", "-").replace("+", "-")


def _poll_with_exponential_delay(
    request, execute_num_retries, max_n, is_done_func, is_error_func
) -> Response:
    """
    Execute request with exponential delay.

    This method is intended to handle and retry in case of api-specific errors,
    such as 429 "Too Many Requests", unlike the `request.execute` which handles
    lower level errors like `ConnectionError`/`socket.timeout`/`ssl.SSLError`.

    :param request: request to be executed.
    :param execute_num_retries: num_retries for `request.execute` method.
    :param max_n: number of times to retry request in this method.
    :param is_done_func: callable to determine if operation is done.
    :param is_error_func: callable to determine if operation is failed.
    :return: response
    """
    for i in range(0, max_n):
        try:
            response = request.execute(num_retries=execute_num_retries)
            if is_error_func(response):
                raise ValueError(f"The response contained an error: {response}")
            if is_done_func(response):
                log.info("Operation is done: %s", response)
                return response

            time.sleep((2**i) + random.random())
        except HttpError as e:
            if e.resp.status != 429:
                log.info("Something went wrong. Not retrying: %s", format(e))
                raise
            else:
                time.sleep((2**i) + random.random())

    raise ValueError(f"Connection could not be established after {max_n} retries.")


class MLEngineHook(GoogleBaseHook):
    """
    Hook for Google ML Engine APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(self, **kwargs):
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(**kwargs)

    def get_conn(self) -> Resource:
        """
        Retrieve the connection to MLEngine.

        :return: Google MLEngine services object.
        """
        authed_http = self._authorize()
        return build("ml", "v1", http=authed_http, cache_discovery=False)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_job(self, job: dict, project_id: str, use_existing_job_fn: Callable | None = None) -> dict:
        """
        Launch a MLEngine job and wait for it to reach a terminal state.

        :param project_id: The Google Cloud project id within which MLEngine
            job will be launched. If set to None or missing, the default project_id from the Google Cloud
            connection is used.
        :param job: MLEngine Job object that should be provided to the MLEngine
            API, such as: ::

                {
                  'jobId': 'my_job_id',
                  'trainingInput': {
                    'scaleTier': 'STANDARD_1',
                    ...
                  }
                }

        :param use_existing_job_fn: In case that a MLEngine job with the same
            job_id already exist, this method (if provided) will decide whether
            we should use this existing job, continue waiting for it to finish
            and returning the job object. It should accepts a MLEngine job
            object, and returns a boolean value indicating whether it is OK to
            reuse the existing job. If 'use_existing_job_fn' is not provided,
            we by default reuse the existing MLEngine job.
        :return: The MLEngine job object if the job successfully reach a
            terminal state (which might be FAILED or CANCELLED state).
        """
        hook = self.get_conn()

        self._append_label(job)
        self.log.info("Creating job.")

        request = hook.projects().jobs().create(parent=f"projects/{project_id}", body=job)
        job_id = job["jobId"]

        try:
            request.execute(num_retries=self.num_retries)
        except HttpError as e:
            # 409 means there is an existing job with the same job ID.
            if e.resp.status == 409:
                if use_existing_job_fn is not None:
                    existing_job = self.get_job(project_id, job_id)
                    if not use_existing_job_fn(existing_job):
                        self.log.error(
                            "Job with job_id %s already exist, but it does not match our expectation: %s",
                            job_id,
                            existing_job,
                        )
                        raise
                self.log.info("Job with job_id %s already exist. Will waiting for it to finish", job_id)
            else:
                self.log.error("Failed to create MLEngine job: %s", e)
                raise

        return self._wait_for_job_done(project_id, job_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_job_without_waiting_result(
        self,
        body: dict,
        project_id: str,
    ):
        """
        Launch a MLEngine job and wait for it to reach a terminal state.

        :param project_id: The Google Cloud project id within which MLEngine
            job will be launched. If set to None or missing, the default project_id from the Google Cloud
            connection is used.
        :param body: MLEngine Job object that should be provided to the MLEngine
            API, such as: ::

                {
                  'jobId': 'my_job_id',
                  'trainingInput': {
                    'scaleTier': 'STANDARD_1',
                    ...
                  }
                }
        :return: The MLEngine job_id of the object if the job successfully reach a
            terminal state (which might be FAILED or CANCELLED state).
        """
        hook = self.get_conn()

        self._append_label(body)
        request = hook.projects().jobs().create(parent=f"projects/{project_id}", body=body)
        job_id = body["jobId"]
        request.execute(num_retries=self.num_retries)
        return job_id

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_job(
        self,
        job_id: str,
        project_id: str,
    ) -> dict:
        """
        Cancel a MLEngine job.

        :param project_id: The Google Cloud project id within which MLEngine
            job will be cancelled. If set to None or missing, the default project_id from the Google Cloud
            connection is used.
        :param job_id: A unique id for the want-to-be cancelled Google MLEngine training job.

        :return: Empty dict if cancelled successfully
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()

        request = hook.projects().jobs().cancel(name=f"projects/{project_id}/jobs/{job_id}")

        try:
            return request.execute(num_retries=self.num_retries)
        except HttpError as e:
            if e.resp.status == 404:
                self.log.error("Job with job_id %s does not exist. ", job_id)
                raise
            elif e.resp.status == 400:
                self.log.info("Job with job_id %s is already complete, cancellation aborted.", job_id)
                return {}
            else:
                self.log.error("Failed to cancel MLEngine job: %s", e)
                raise

    def get_job(self, project_id: str, job_id: str) -> dict:
        """
        Get a MLEngine job based on the job id.

        :param project_id: The project in which the Job is located. If set to None or missing, the default
            project_id from the Google Cloud connection is used. (templated)
        :param job_id: A unique id for the Google MLEngine job. (templated)
        :return: MLEngine job object if succeed.
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()
        job_name = f"projects/{project_id}/jobs/{job_id}"
        request = hook.projects().jobs().get(name=job_name)
        while True:
            try:
                return request.execute(num_retries=self.num_retries)
            except HttpError as e:
                if e.resp.status == 429:
                    # polling after 30 seconds when quota failure occurs
                    time.sleep(30)
                else:
                    self.log.error("Failed to get MLEngine job: %s", e)
                    raise

    def _wait_for_job_done(self, project_id: str, job_id: str, interval: int = 30):
        """
        Wait for the Job to reach a terminal state.

        This method will periodically check the job state until the job reach
        a terminal state.

        :param project_id: The project in which the Job is located. If set to None or missing, the default
            project_id from the Google Cloud connection is used. (templated)
        :param job_id: A unique id for the Google MLEngine job. (templated)
        :param interval: Time expressed in seconds after which the job status is checked again. (templated)
        :raises: googleapiclient.errors.HttpError
        """
        self.log.info("Waiting for job. job_id=%s", job_id)

        if interval <= 0:
            raise ValueError("Interval must be > 0")
        while True:
            job = self.get_job(project_id, job_id)
            if job["state"] in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                return job
            time.sleep(interval)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_version(
        self,
        model_name: str,
        version_spec: dict,
        project_id: str,
    ) -> dict:
        """
        Create the Version on Google Cloud ML Engine.

        :param version_spec: A dictionary containing the information about the version. (templated)
        :param model_name: The name of the Google Cloud ML Engine model that the version belongs to.
            (templated)
        :param project_id: The Google Cloud project name to which MLEngine model belongs.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
            (templated)
        :return: If the version was created successfully, returns the operation.
            Otherwise raises an error .
        """
        hook = self.get_conn()
        parent_name = f"projects/{project_id}/models/{model_name}"

        self._append_label(version_spec)

        create_request = hook.projects().models().versions().create(parent=parent_name, body=version_spec)
        response = create_request.execute(num_retries=self.num_retries)
        get_request = hook.projects().operations().get(name=response["name"])

        return _poll_with_exponential_delay(
            request=get_request,
            execute_num_retries=self.num_retries,
            max_n=9,
            is_done_func=lambda resp: resp.get("done", False),
            is_error_func=lambda resp: resp.get("error", None) is not None,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def set_default_version(
        self,
        model_name: str,
        version_name: str,
        project_id: str,
    ) -> dict:
        """
        Set a version to be the default. Blocks until finished.

        :param model_name: The name of the Google Cloud ML Engine model that the version belongs to.
            (templated)
        :param version_name: A name to use for the version being operated upon. (templated)
        :param project_id: The Google Cloud project name to which MLEngine model belongs. If set to None
            or missing, the default project_id from the Google Cloud connection is used. (templated)
        :return: If successful, return an instance of Version.
            Otherwise raises an error.
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()
        full_version_name = f"projects/{project_id}/models/{model_name}/versions/{version_name}"

        request = hook.projects().models().versions().setDefault(name=full_version_name, body={})

        try:
            response = request.execute(num_retries=self.num_retries)
            self.log.info("Successfully set version: %s to default", response)
            return response
        except HttpError as e:
            self.log.error("Something went wrong: %s", e)
            raise

    @GoogleBaseHook.fallback_to_default_project_id
    def list_versions(
        self,
        model_name: str,
        project_id: str,
    ) -> list[dict]:
        """
        List all available versions of a model. Blocks until finished.

        :param model_name: The name of the Google Cloud ML Engine model that the version
            belongs to. (templated)
        :param project_id: The Google Cloud project name to which MLEngine model belongs. If set to None or
            missing, the default project_id from the Google Cloud connection is used. (templated)
        :return: return an list of instance of Version.
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()
        result: list[dict] = []
        full_parent_name = f"projects/{project_id}/models/{model_name}"

        request = hook.projects().models().versions().list(parent=full_parent_name, pageSize=100)

        while request is not None:
            response = request.execute(num_retries=self.num_retries)
            result.extend(response.get("versions", []))

            request = (
                hook.projects()
                .models()
                .versions()
                .list_next(previous_request=request, previous_response=response)
            )
            time.sleep(5)
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_version(
        self,
        model_name: str,
        version_name: str,
        project_id: str,
    ) -> dict:
        """
        Delete the given version of a model. Blocks until finished.

        :param model_name: The name of the Google Cloud ML Engine model that the version
            belongs to. (templated)
        :param project_id: The Google Cloud project name to which MLEngine
            model belongs.
        :param version_name: A name to use for the version being operated upon. (templated)
        :return: If the version was deleted successfully, returns the operation.
            Otherwise raises an error.
        """
        hook = self.get_conn()
        full_name = f"projects/{project_id}/models/{model_name}/versions/{version_name}"
        delete_request = hook.projects().models().versions().delete(name=full_name)
        response = delete_request.execute(num_retries=self.num_retries)
        get_request = hook.projects().operations().get(name=response["name"])

        return _poll_with_exponential_delay(
            request=get_request,
            execute_num_retries=self.num_retries,
            max_n=9,
            is_done_func=lambda resp: resp.get("done", False),
            is_error_func=lambda resp: resp.get("error", None) is not None,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_model(
        self,
        model: dict,
        project_id: str,
    ) -> dict:
        """
        Create a Model. Blocks until finished.

        :param model: A dictionary containing the information about the model.
        :param project_id: The Google Cloud project name to which MLEngine model belongs. If set to None or
            missing, the default project_id from the Google Cloud connection is used. (templated)
        :return: If the version was created successfully, returns the instance of Model.
            Otherwise raises an error.
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()
        if "name" not in model or not model["name"]:
            raise ValueError("Model name must be provided and could not be an empty string")
        project = f"projects/{project_id}"

        self._append_label(model)
        try:
            request = hook.projects().models().create(parent=project, body=model)
            response = request.execute(num_retries=self.num_retries)
        except HttpError as e:
            if e.resp.status != 409:
                raise e
            str(e)  # Fills in the error_details field
            if not e.error_details or len(e.error_details) != 1:
                raise e

            error_detail = e.error_details[0]
            if error_detail["@type"] != "type.googleapis.com/google.rpc.BadRequest":
                raise e

            if "fieldViolations" not in error_detail or len(error_detail["fieldViolations"]) != 1:
                raise e

            field_violation = error_detail["fieldViolations"][0]
            if (
                field_violation["field"] != "model.name"
                or field_violation["description"] != "A model with the same name already exists."
            ):
                raise e
            response = self.get_model(model_name=model["name"], project_id=project_id)

        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def get_model(
        self,
        model_name: str,
        project_id: str,
    ) -> dict | None:
        """
        Get a Model. Blocks until finished.

        :param model_name: The name of the model.
        :param project_id: The Google Cloud project name to which MLEngine model belongs. If set to None
            or missing, the default project_id from the Google Cloud connection is used. (templated)
        :return: If the model exists, returns the instance of Model.
            Otherwise return None.
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()
        if not model_name:
            raise ValueError("Model name must be provided and it could not be an empty string")
        full_model_name = f"projects/{project_id}/models/{model_name}"
        request = hook.projects().models().get(name=full_model_name)
        try:
            return request.execute(num_retries=self.num_retries)
        except HttpError as e:
            if e.resp.status == 404:
                self.log.error("Model was not found: %s", e)
                return None
            raise

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_model(
        self,
        model_name: str,
        project_id: str,
        delete_contents: bool = False,
    ) -> None:
        """
        Delete a Model. Blocks until finished.

        :param model_name: The name of the model.
        :param delete_contents: Whether to force the deletion even if the models is not empty.
            Will delete all version (if any) in the dataset if set to True.
            The default value is False.
        :param project_id: The Google Cloud project name to which MLEngine model belongs. If set to None
            or missing, the default project_id from the Google Cloud connection is used. (templated)
        :raises: googleapiclient.errors.HttpError
        """
        hook = self.get_conn()

        if not model_name:
            raise ValueError("Model name must be provided and it could not be an empty string")
        model_path = f"projects/{project_id}/models/{model_name}"
        if delete_contents:
            self._delete_all_versions(model_name, project_id)
        request = hook.projects().models().delete(name=model_path)
        try:
            request.execute(num_retries=self.num_retries)
        except HttpError as e:
            if e.resp.status == 404:
                self.log.error("Model was not found: %s", e)
                return
            raise

    def _delete_all_versions(self, model_name: str, project_id: str):
        versions = self.list_versions(project_id=project_id, model_name=model_name)
        # The default version can only be deleted when it is the last one in the model
        non_default_versions = (version for version in versions if not version.get("isDefault", False))
        for version in non_default_versions:
            _, _, version_name = version["name"].rpartition("/")
            self.delete_version(project_id=project_id, model_name=model_name, version_name=version_name)
        default_versions = (version for version in versions if version.get("isDefault", False))
        for version in default_versions:
            _, _, version_name = version["name"].rpartition("/")
            self.delete_version(project_id=project_id, model_name=model_name, version_name=version_name)

    def _append_label(self, model: dict) -> None:
        model["labels"] = model.get("labels", {})
        model["labels"]["airflow-version"] = _AIRFLOW_VERSION


class MLEngineAsyncHook(GoogleBaseAsyncHook):
    """Class to get asynchronous hook for MLEngine."""

    sync_hook_class = MLEngineHook
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    def _check_fileds(
        self,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ):
        if not project_id:
            raise AirflowException("Google Cloud project id is required.")
        if not job_id:
            raise AirflowException("An unique job id is required for Google MLEngine training job.")

    async def _get_link(self, url: str, session: Session):
        async with Token(scopes=self.scopes) as token:
            session_aio = AioSession(session)
            headers = {
                "Authorization": f"Bearer {await token.get()}",
            }
            with contextlib.suppress(AirflowException):
                # suppress AirflowException because we don't want to raise exception
                job = await session_aio.get(url=url, headers=headers)

        return job

    async def get_job(self, job_id: str, session: Session, project_id: str = PROVIDE_PROJECT_ID):
        """Get the specified job resource by job ID and project ID."""
        self._check_fileds(project_id=project_id, job_id=job_id)

        url = f"https://ml.googleapis.com/v1/projects/{project_id}/jobs/{job_id}"
        return await self._get_link(url=url, session=session)

    async def get_job_status(
        self,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str | None:
        """
        Poll for job status asynchronously using gcloud-aio.

        Note that an OSError is raised when Job results are still pending.
        Exception means that Job finished with errors
        """
        self._check_fileds(project_id=project_id, job_id=job_id)
        async with ClientSession() as session:
            try:
                job = await self.get_job(
                    project_id=project_id,
                    job_id=job_id,
                    session=session,  #  type: ignore
                )
                job = await job.json(content_type=None)
                self.log.info("Retrieving json_response: %s", job)

                if job["state"] in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                    job_status = "success"
                elif job["state"] in ["PREPARING", "RUNNING"]:
                    job_status = "pending"
            except OSError:
                job_status = "pending"
            except Exception as e:
                self.log.info("Query execution finished with errors...")
                job_status = str(e)
            return job_status
