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
"""Hook for Google Cloud Build service."""
from __future__ import annotations

from typing import Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import AlreadyExists
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.devtools.cloudbuild_v1 import CloudBuildAsyncClient, CloudBuildClient, GetBuildRequest
from google.cloud.devtools.cloudbuild_v1.types import Build, BuildTrigger, RepoSource

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 5


class CloudBuildHook(GoogleBaseHook):
    """
    Hook for the Google Cloud Build Service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)
        self._client: dict[str, CloudBuildClient] = {}

    def _get_build_id_from_operation(self, operation: Operation) -> str:
        """
        Retrieve Cloud Build ID from Operation Object.

        :param operation: The proto to append resource_label airflow
            version to

        :return: Cloud Build ID
        """
        try:
            return operation.metadata.build.id
        except Exception:
            raise AirflowException("Could not retrieve Build ID from Operation.")

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def get_conn(self, location: str = "global") -> CloudBuildClient:
        """
        Retrieves the connection to Google Cloud Build.

        :param location: The location of the project.

        :return: Google Cloud Build client object.
        """
        if location not in self._client:
            client_options = None
            if location != "global":
                client_options = ClientOptions(api_endpoint=f"{location}-cloudbuild.googleapis.com:443")
            self._client[location] = CloudBuildClient(
                credentials=self.get_credentials(),
                client_info=CLIENT_INFO,
                client_options=client_options,
            )
        return self._client[location]

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_build(
        self,
        id_: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> Build:
        """
        Cancels a build in progress.

        :param id_: The ID of the build.
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start cancelling build: %s.", id_)

        build = client.cancel_build(
            request={"project_id": project_id, "id": id_},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Build has been cancelled: %s.", id_)

        return build

    @GoogleBaseHook.fallback_to_default_project_id
    def create_build_without_waiting_for_result(
        self,
        build: dict | Build,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> tuple[Operation, str]:
        """
        Starts a build with the specified configuration without waiting for it to finish.

        :param build: The build resource to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.Build`
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        parent = f"projects/{project_id}/locations/{location}"

        self.log.info("Start creating build...")

        operation = client.create_build(
            request={"parent": parent, "project_id": project_id, "build": build},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        id_ = self._get_build_id_from_operation(operation)
        return operation, id_

    @GoogleBaseHook.fallback_to_default_project_id
    def create_build(
        self,
        build: dict | Build,
        project_id: str = PROVIDE_PROJECT_ID,
        wait: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Build:
        """
        Starts a build with the specified configuration.

        :param build: The build resource to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.Build`
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param wait: Optional, wait for operation to finish.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.

        """
        client = self.get_conn()

        self.log.info("Start creating build...")

        operation = client.create_build(
            request={"project_id": project_id, "build": build},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        id_ = self._get_build_id_from_operation(operation)

        if not wait:
            return self.get_build(id_=id_, project_id=project_id)

        operation.result()

        self.log.info("Build has been created: %s.", id_)

        return self.get_build(id_=id_, project_id=project_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_build_trigger(
        self,
        trigger: dict | BuildTrigger,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> BuildTrigger:
        """
        Creates a new BuildTrigger.

        :param trigger: The BuildTrigger to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start creating build trigger...")

        try:
            trigger = client.create_build_trigger(
                request={"project_id": project_id, "trigger": trigger},
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            )
        except AlreadyExists:
            raise AirflowException("Cloud Build Trigger with such parameters already exists.")

        self.log.info("Build trigger has been created.")

        return trigger

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_build_trigger(
        self,
        trigger_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> None:
        """
        Deletes a BuildTrigger by its project ID and trigger ID.

        :param trigger_id: The ID of the BuildTrigger to delete.
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start deleting build trigger: %s.", trigger_id)

        client.delete_build_trigger(
            request={"project_id": project_id, "trigger_id": trigger_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Build trigger has been deleted: %s.", trigger_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_build(
        self,
        id_: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> Build:
        """
        Returns information about a previously requested build.

        :param id_: The ID of the build.
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start retrieving build: %s.", id_)

        build = client.get_build(
            request={"project_id": project_id, "id": id_},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Build has been retrieved: %s.", id_)

        return build

    @GoogleBaseHook.fallback_to_default_project_id
    def get_build_trigger(
        self,
        trigger_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> BuildTrigger:
        """
        Returns information about a BuildTrigger.

        :param trigger_id: The ID of the BuildTrigger to get.
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start retrieving build trigger: %s.", trigger_id)

        trigger = client.get_build_trigger(
            request={"project_id": project_id, "trigger_id": trigger_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Build trigger has been retrieved: %s.", trigger_id)

        return trigger

    @GoogleBaseHook.fallback_to_default_project_id
    def list_build_triggers(
        self,
        location: str = "global",
        project_id: str = PROVIDE_PROJECT_ID,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> list[BuildTrigger]:
        """
        Lists existing BuildTriggers.

        :param project_id: Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param location: The location of the project.
        :param page_size: Optional, number of results to return in the list.
        :param page_token: Optional, token to provide to skip to a particular spot in the list.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.

        """
        client = self.get_conn(location=location)

        parent = f"projects/{project_id}/locations/{location}"

        self.log.info("Start retrieving build triggers.")

        response = client.list_build_triggers(
            request={
                "parent": parent,
                "project_id": project_id,
                "page_size": page_size,
                "page_token": page_token,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Build triggers have been retrieved.")

        return list(response.triggers)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_builds(
        self,
        location: str = "global",
        project_id: str = PROVIDE_PROJECT_ID,
        page_size: int | None = None,
        page_token: int | None = None,
        filter_: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> list[Build]:
        """
        Lists previously requested builds.

        :param project_id: Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param location: The location of the project.
        :param page_size: Optional, number of results to return in the list.
        :param page_token: Optional, token to provide to skip to a particular spot in the list.
        :param filter_: Optional, the raw filter text to constrain the results.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.

        """
        client = self.get_conn(location=location)

        parent = f"projects/{project_id}/locations/{location}"

        self.log.info("Start retrieving builds.")

        response = client.list_builds(
            request={
                "parent": parent,
                "project_id": project_id,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter_,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Builds have been retrieved.")

        return list(response.builds)

    @GoogleBaseHook.fallback_to_default_project_id
    def retry_build(
        self,
        id_: str,
        project_id: str = PROVIDE_PROJECT_ID,
        wait: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> Build:
        """
        Creates a new build based on the specified build. This method creates a new build
        using the original build request, which may or may not result in an identical build.

        :param id_: Build ID of the original build.
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param wait: Optional, wait for operation to finish.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start retrying build: %s.", id_)

        operation = client.retry_build(
            request={"project_id": project_id, "id": id_},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        id_ = self._get_build_id_from_operation(operation)

        if not wait:
            return self.get_build(id_=id_, project_id=project_id, location=location)

        operation.result()

        self.log.info("Build has been retried: %s.", id_)

        return self.get_build(id_=id_, project_id=project_id, location=location)

    @GoogleBaseHook.fallback_to_default_project_id
    def run_build_trigger(
        self,
        trigger_id: str,
        source: dict | RepoSource,
        project_id: str = PROVIDE_PROJECT_ID,
        wait: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> Build:
        """
        Runs a BuildTrigger at a particular source revision.

        :param trigger_id: The ID of the trigger.
        :param source: Source to build against this trigger. If a dict is provided, it must be of the
            same form as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.RepoSource`
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param wait: Optional, wait for operation to finish.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start running build trigger: %s.", trigger_id)
        operation = client.run_build_trigger(
            request={"project_id": project_id, "trigger_id": trigger_id, "source": source},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        id_ = self._get_build_id_from_operation(operation)

        if not wait:
            return self.get_build(id_=id_, project_id=project_id, location=location)
        operation.result()

        self.log.info("Build trigger has been run: %s.", trigger_id)

        return self.get_build(id_=id_, project_id=project_id, location=location)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_build_trigger(
        self,
        trigger_id: str,
        trigger: dict | BuildTrigger,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> BuildTrigger:
        """
        Updates a BuildTrigger by its project ID and trigger ID.

        :param trigger_id: The ID of the trigger.
        :param trigger: The BuildTrigger to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start updating build trigger: %s.", trigger_id)

        trigger = client.update_build_trigger(
            request={"project_id": project_id, "trigger_id": trigger_id, "trigger": trigger},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Build trigger has been updated: %s.", trigger_id)

        return trigger


class CloudBuildAsyncHook(GoogleBaseHook):
    """Asynchronous Hook for the Google Cloud Build Service."""

    def __init__(self, **kwargs):
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(**kwargs)

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_cloud_build(
        self,
        id_: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> Build:
        """Retrieves a Cloud Build with a specified id."""
        if not id_:
            raise AirflowException("Google Cloud Build id is required.")

        client_options = None
        if location != "global":
            client_options = ClientOptions(api_endpoint=f"{location}-cloudbuild.googleapis.com:443")
        client = CloudBuildAsyncClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

        request = GetBuildRequest(
            project_id=project_id,
            id=id_,
        )
        build_instance = await client.get_build(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return build_instance
