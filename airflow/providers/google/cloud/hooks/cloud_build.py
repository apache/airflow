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

import time
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.cloud.devtools.cloudbuild_v1 import CloudBuildClient
from google.cloud.devtools.cloudbuild_v1.types import (
    Build, BuildTrigger, ListBuildTriggersResponse, RepoSource,
)
from google.protobuf.json_format import MessageToDict

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 5


class CloudBuildHook(GoogleBaseHook):
    """
    Hook for the Google Cloud Build Service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    def __init__(self, gcp_conn_id: str = "google_cloud_default", delegate_to: Optional[str] = None) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self._client: Optional[CloudBuildClient] = None

    def get_conn(self) -> CloudBuildClient:
        """
        Retrieves the connection to Google Cloud Build.

        :return: Google Cloud Build client object.
        :rtype: `google.cloud.devtools.cloudbuild_v1.CloudBuildClient`
        """
        if not self._client:
            self._client = CloudBuildClient(credentials=self._get_credentials(), client_info=self.client_info)
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_build(
        self,
        id_: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Build:
        """
        Cancels a build in progress.

        :param id_: The ID of the build.
        :type id_: str
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]

        :rtype: `google.cloud.devtools.cloudbuild_v1.types.Build`
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        return client.cancel_build(
            project_id=project_id, id_=id_, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_build(
        self,
        build: Union[Dict, Build],
        project_id: str,
        wait: bool = True,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Build:
        """
        Starts a build with the specified configuration.

        :param build: The build resource to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.Build`
        :type build: Union[dict, `google.cloud.devtools.cloudbuild_v1.types.Build`]
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param wait: Optional, wait for operation to finish.
        :type wait: Optional[bool]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]

        :rtype: `google.cloud.devtools.cloudbuild_v1.types.Build`
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        operation = client.create_build(
            project_id=project_id, build=build, retry=retry, timeout=timeout, metadata=metadata
        )

        operation_dict = MessageToDict(operation)
        try:
            id_ = operation_dict["metadata"]["build"]["id"]
        except Exception:
            raise AirflowException("Could not retrieve Build ID from Operation.")

        if wait:
            return self._wait_for_operation_to_complete(
                func=self.get_build,
                id_=id_,  # type: ignore
                project_id=project_id  # type: ignore
            )
        else:
            return self.get_build(id_=id_, project_id=project_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_build_trigger(
        self,
        trigger: Union[dict, BuildTrigger],
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> BuildTrigger:
        """
        Creates a new BuildTrigger.

        :param trigger: The BuildTrigger to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
        :type trigger: Union[dict, `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`]
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]

        :rtype: `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        return client.create_build_trigger(
            project_id=project_id, trigger=trigger, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_build_trigger(
        self,
        trigger_id: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes a BuildTrigger by its project ID and trigger ID.

        :param trigger_id: The ID of the BuildTrigger to delete.
        :type trigger_id: str
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        client.delete_build_trigger(
            project_id=project_id, trigger_id=trigger_id, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_build(
        self,
        id_: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Build:
        """
        Returns information about a previously requested build.

        :param id_: The ID of the build.
        :type id_: str
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]

        :rtype: `google.cloud.devtools.cloudbuild_v1.types.Build`
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        return client.get_build(
            project_id=project_id, id_=id_, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_build_trigger(
        self,
        trigger_id: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> BuildTrigger:
        """
        Returns information about a BuildTrigger.

        :param trigger_id: The ID of the BuildTrigger to get.
        :type trigger_id: str
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]

        :rtype: `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        return client.get_build_trigger(
            project_id=project_id, trigger_id=trigger_id, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_build_triggers(
        self,
        project_id: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> ListBuildTriggersResponse:
        """
        Lists existing BuildTriggers.

        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param page_size: Optional, number of results to return in the list.
        :type page_size: Optional[int]
        :param page_token: Optional, token to provide to skip to a particular spot in the list.
        :type page_token: Optional[str]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]

        :rtype: `google.cloud.devtools.cloudbuild_v1.types.ListBuildTriggersResponse`
        """

        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        return client.list_build_triggers(
            project_id=project_id,
            page_size=page_size,
            page_token=page_token,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_builds(
        self,
        project_id: str,
        page_size: Optional[int] = None,
        filter_: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[Build]:
        """
        Lists previously requested builds.

        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param page_size: Optional, number of results to return in the list.
        :type page_size: Optional[int]
        :param filter_: Optional, the raw filter text to constrain the results.
        :type filter_: Optional[str]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]

        :rtype: List[`google.cloud.devtools.cloudbuild_v1.types.Build`]
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        builds = client.list_builds(
            project_id=project_id,
            page_size=page_size,
            filter_=filter_,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(builds)

    @GoogleBaseHook.fallback_to_default_project_id
    def retry_build(
        self,
        id_: str,
        project_id: str,
        wait: bool = True,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Build:
        """
        Creates a new build based on the specified build. This method creates a new build
        using the original build request, which may or may not result in an identical build.

        :param id_: Build ID of the original build.
        :type id_: str
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param wait: Optional, wait for operation to finish.
        :type wait: Optional[bool]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]

        :rtype: `google.cloud.devtools.cloudbuild_v1.types.Build`
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        operation = client.retry_build(
            project_id=project_id, id_=id_, retry=retry, timeout=timeout, metadata=metadata
        )

        operation_dict = MessageToDict(operation)
        try:
            id_ = operation_dict["metadata"]["build"]["id"]
        except Exception:
            raise AirflowException("Could not retrieve Build ID from Operation.")

        if wait:
            return self._wait_for_operation_to_complete(
                func=self.get_build,
                id_=id_,  # type: ignore
                project_id=project_id  # type: ignore
            )
        else:
            return self.get_build(id_=id_, project_id=project_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def run_build_trigger(
        self,
        trigger_id: str,
        source: Union[dict, RepoSource],
        project_id: str,
        wait: bool = True,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Build:
        """
        Runs a BuildTrigger at a particular source revision.

        :param trigger_id: The ID of the trigger.
        :type trigger_id: str
        :param source: Source to build against this trigger. If a dict is provided, it must be of the
            same form as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.RepoSource`
        :type source: Union[dict, `google.cloud.devtools.cloudbuild_v1.types.RepoSource`]
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param wait: Optional, wait for operation to finish.
        :type wait: Optional[bool]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]

        :rtype: `google.cloud.devtools.cloudbuild_v1.types.Build`
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        operation = client.run_build_trigger(
            project_id=project_id,
            trigger_id=trigger_id,
            source=source,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        operation_dict = MessageToDict(operation)
        try:
            id_ = operation_dict["metadata"]["build"]["id"]
        except Exception:
            raise AirflowException("Could not retrieve Build ID from Operation.")

        if wait:
            return self._wait_for_operation_to_complete(
                func=self.get_build,
                id_=id_,  # type: ignore
                project_id=project_id  # type: ignore
            )
        else:
            return self.get_build(id_=id_, project_id=project_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_build_trigger(
        self,
        trigger_id: str,
        trigger: Union[dict, BuildTrigger],
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> BuildTrigger:
        """
        Updates a BuildTrigger by its project ID and trigger ID.

        :param trigger_id: The ID of the trigger.
        :type trigger_id: str
        :param trigger: The BuildTrigger to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
        :type trigger: Union[dict, `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`]
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :type retry: Optional[Retry]
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :type timeout: Optional[float]
        :param metadata: Optional, additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]

        :rtype: `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
        """
        if not project_id:
            raise ValueError("The project_id should be set")

        client = self.get_conn()
        return client.update_build_trigger(
            project_id=project_id,
            trigger_id=trigger_id,
            trigger=trigger,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def _wait_for_operation_to_complete(self, func: Callable, **kwargs: Optional[dict]) -> dict:
        """
        Waits for the named operation to complete - checks status of the
        asynchronous call.

        :param func: The function that needs to be called.
        :type func: Callable
        :param kwargs: dict of function keyword arguments
        :type kwargs: dict
        :return: The response returned by the operation.
        :rtype: dict
        :exception: AirflowException in case error is returned.
        """
        while True:
            operation = func(**kwargs)
            operation_dict = MessageToDict(operation)
            status = operation_dict["status"]
            if status:
                if status == "SUCCESS":
                    return operation_dict
                elif status in ["FAILURE", "INTERNAL_ERROR", "TIMEOUT", "CANCELLED", "EXPIRED"]:
                    raise AirflowException(str(operation_dict))
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)
