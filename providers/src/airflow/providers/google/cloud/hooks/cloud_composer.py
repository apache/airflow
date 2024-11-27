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
from __future__ import annotations

import asyncio
import time
from collections.abc import MutableSequence, Sequence
from typing import TYPE_CHECKING

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.orchestration.airflow.service_v1 import (
    EnvironmentsAsyncClient,
    EnvironmentsClient,
    ImageVersionsClient,
    PollAirflowCommandResponse,
)

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.operation_async import AsyncOperation
    from google.api_core.retry import Retry
    from google.api_core.retry_async import AsyncRetry
    from google.cloud.orchestration.airflow.service_v1.services.environments.pagers import (
        ListEnvironmentsPager,
    )
    from google.cloud.orchestration.airflow.service_v1.services.image_versions.pagers import (
        ListImageVersionsPager,
    )
    from google.cloud.orchestration.airflow.service_v1.types import (
        Environment,
        ExecuteAirflowCommandResponse,
    )
    from google.protobuf.field_mask_pb2 import FieldMask


class CloudComposerHook(GoogleBaseHook):
    """Hook for Google Cloud Composer APIs."""

    client_options = ClientOptions(api_endpoint="composer.googleapis.com:443")

    def get_environment_client(self) -> EnvironmentsClient:
        """Retrieve client library object that allow access Environments service."""
        return EnvironmentsClient(
            credentials=self.get_credentials(),
            client_info=CLIENT_INFO,
            client_options=self.client_options,
        )

    def get_image_versions_client(self) -> ImageVersionsClient:
        """Retrieve client library object that allow access Image Versions service."""
        return ImageVersionsClient(
            credentials=self.get_credentials(),
            client_info=CLIENT_INFO,
            client_options=self.client_options,
        )

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Wait for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def get_operation(self, operation_name):
        return self.get_environment_client().transport.operations_client.get_operation(name=operation_name)

    def get_environment_name(self, project_id, region, environment_id):
        return f"projects/{project_id}/locations/{region}/environments/{environment_id}"

    def get_parent(self, project_id, region):
        return f"projects/{project_id}/locations/{region}"

    @GoogleBaseHook.fallback_to_default_project_id
    def create_environment(
        self,
        project_id: str,
        region: str,
        environment: Environment | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create a new environment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param environment:  The environment to create. This corresponds to the ``environment`` field on the
            ``request`` instance; if ``request`` is provided, this should not be set.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        result = client.create_environment(
            request={"parent": self.get_parent(project_id, region), "environment": environment},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_environment(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete an environment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        name = self.get_environment_name(project_id, region, environment_id)
        result = client.delete_environment(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_environment(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Environment:
        """
        Get an existing environment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        result = client.get_environment(
            request={"name": self.get_environment_name(project_id, region, environment_id)},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_environments(
        self,
        project_id: str,
        region: str,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListEnvironmentsPager:
        """
        List environments.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param page_size: The maximum number of environments to return.
        :param page_token: The next_page_token value returned from a previous List
            request, if any.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        result = client.list_environments(
            request={
                "parent": self.get_parent(project_id, region),
                "page_size": page_size,
                "page_token": page_token,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_environment(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        environment: Environment | dict,
        update_mask: dict | FieldMask,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        r"""
        Update an environment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
        :param environment:  A patch environment. Fields specified by the ``updateMask`` will be copied from
            the patch environment into the environment under update.

            This corresponds to the ``environment`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param update_mask:  Required. A comma-separated list of paths, relative to ``Environment``, of fields
            to update. If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.protobuf.field_mask_pb2.FieldMask`
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        name = self.get_environment_name(project_id, region, environment_id)

        result = client.update_environment(
            request={"name": name, "environment": environment, "update_mask": update_mask},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_image_versions(
        self,
        project_id: str,
        region: str,
        page_size: int | None = None,
        page_token: str | None = None,
        include_past_releases: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListImageVersionsPager:
        """
        List ImageVersions for provided location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param page_size: The maximum number of environments to return.
        :param page_token: The next_page_token value returned from a previous List
            request, if any.
        :param include_past_releases: Flag to include past releases
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_image_versions_client()
        result = client.list_image_versions(
            request={
                "parent": self.get_parent(project_id, region),
                "page_size": page_size,
                "page_token": page_token,
                "include_past_releases": include_past_releases,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def execute_airflow_command(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        command: str,
        subcommand: str,
        parameters: MutableSequence[str],
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ExecuteAirflowCommandResponse:
        """
        Execute Airflow command for provided Composer environment.

        :param project_id: The ID of the Google Cloud project that the service belongs to.
        :param region: The ID of the Google Cloud region that the service belongs to.
        :param environment_id: The ID of the Google Cloud environment that the service belongs to.
        :param command: Airflow command.
        :param subcommand: Airflow subcommand.
        :param parameters: Parameters for the Airflow command/subcommand as an array of arguments. It may
            contain positional arguments like ``["my-dag-id"]``, key-value parameters like ``["--foo=bar"]``
            or ``["--foo","bar"]``, or other flags like ``["-f"]``.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        result = client.execute_airflow_command(
            request={
                "environment": self.get_environment_name(project_id, region, environment_id),
                "command": command,
                "subcommand": subcommand,
                "parameters": parameters,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def poll_airflow_command(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        execution_id: str,
        pod: str,
        pod_namespace: str,
        next_line_number: int,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> PollAirflowCommandResponse:
        """
        Poll Airflow command execution result for provided Composer environment.

        :param project_id: The ID of the Google Cloud project that the service belongs to.
        :param region: The ID of the Google Cloud region that the service belongs to.
        :param environment_id: The ID of the Google Cloud environment that the service belongs to.
        :param execution_id: The unique ID of the command execution.
        :param pod: The name of the pod where the command is executed.
        :param pod_namespace: The namespace of the pod where the command is executed.
        :param next_line_number: Line number from which new logs should be fetched.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        result = client.poll_airflow_command(
            request={
                "environment": self.get_environment_name(project_id, region, environment_id),
                "execution_id": execution_id,
                "pod": pod,
                "pod_namespace": pod_namespace,
                "next_line_number": next_line_number,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def wait_command_execution_result(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        execution_cmd_info: dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        poll_interval: int = 10,
    ) -> dict:
        while True:
            try:
                result = self.poll_airflow_command(
                    project_id=project_id,
                    region=region,
                    environment_id=environment_id,
                    execution_id=execution_cmd_info["execution_id"],
                    pod=execution_cmd_info["pod"],
                    pod_namespace=execution_cmd_info["pod_namespace"],
                    next_line_number=1,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )
            except Exception as ex:
                self.log.exception("Exception occurred while polling CMD result")
                raise AirflowException(ex)

            result_dict = PollAirflowCommandResponse.to_dict(result)
            if result_dict["output_end"]:
                return result_dict

            self.log.info("Waiting for result...")
            time.sleep(poll_interval)


class CloudComposerAsyncHook(GoogleBaseHook):
    """Hook for Google Cloud Composer async APIs."""

    client_options = ClientOptions(api_endpoint="composer.googleapis.com:443")

    def get_environment_client(self) -> EnvironmentsAsyncClient:
        """Retrieve client library object that allow access Environments service."""
        return EnvironmentsAsyncClient(
            credentials=self.get_credentials(),
            client_info=CLIENT_INFO,
            client_options=self.client_options,
        )

    def get_environment_name(self, project_id, region, environment_id):
        return f"projects/{project_id}/locations/{region}/environments/{environment_id}"

    def get_parent(self, project_id, region):
        return f"projects/{project_id}/locations/{region}"

    async def get_operation(self, operation_name):
        return await self.get_environment_client().transport.operations_client.get_operation(
            name=operation_name
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def create_environment(
        self,
        project_id: str,
        region: str,
        environment: Environment | dict,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Create a new environment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param environment:  The environment to create. This corresponds to the ``environment`` field on the
            ``request`` instance; if ``request`` is provided, this should not be set.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        return await client.create_environment(
            request={"parent": self.get_parent(project_id, region), "environment": environment},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def delete_environment(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Delete an environment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        name = self.get_environment_name(project_id, region, environment_id)
        return await client.delete_environment(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def update_environment(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        environment: Environment | dict,
        update_mask: dict | FieldMask,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        r"""
        Update an environment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
        :param environment:  A patch environment. Fields specified by the ``updateMask`` will be copied from
            the patch environment into the environment under update.

            This corresponds to the ``environment`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param update_mask:  Required. A comma-separated list of paths, relative to ``Environment``, of fields
            to update. If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.protobuf.field_mask_pb2.FieldMask`
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        name = self.get_environment_name(project_id, region, environment_id)

        return await client.update_environment(
            request={"name": name, "environment": environment, "update_mask": update_mask},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def execute_airflow_command(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        command: str,
        subcommand: str,
        parameters: MutableSequence[str],
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Execute Airflow command for provided Composer environment.

        :param project_id: The ID of the Google Cloud project that the service belongs to.
        :param region: The ID of the Google Cloud region that the service belongs to.
        :param environment_id: The ID of the Google Cloud environment that the service belongs to.
        :param command: Airflow command.
        :param subcommand: Airflow subcommand.
        :param parameters: Parameters for the Airflow command/subcommand as an array of arguments. It may
            contain positional arguments like ``["my-dag-id"]``, key-value parameters like ``["--foo=bar"]``
            or ``["--foo","bar"]``, or other flags like ``["-f"]``.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()

        return await client.execute_airflow_command(
            request={
                "environment": self.get_environment_name(project_id, region, environment_id),
                "command": command,
                "subcommand": subcommand,
                "parameters": parameters,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def poll_airflow_command(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        execution_id: str,
        pod: str,
        pod_namespace: str,
        next_line_number: int,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AsyncOperation:
        """
        Poll Airflow command execution result for provided Composer environment.

        :param project_id: The ID of the Google Cloud project that the service belongs to.
        :param region: The ID of the Google Cloud region that the service belongs to.
        :param environment_id: The ID of the Google Cloud environment that the service belongs to.
        :param execution_id: The unique ID of the command execution.
        :param pod: The name of the pod where the command is executed.
        :param pod_namespace: The namespace of the pod where the command is executed.
        :param next_line_number: Line number from which new logs should be fetched.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()

        return await client.poll_airflow_command(
            request={
                "environment": self.get_environment_name(project_id, region, environment_id),
                "execution_id": execution_id,
                "pod": pod,
                "pod_namespace": pod_namespace,
                "next_line_number": next_line_number,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    async def wait_command_execution_result(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        execution_cmd_info: dict,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        poll_interval: int = 10,
    ) -> dict:
        while True:
            try:
                result = await self.poll_airflow_command(
                    project_id=project_id,
                    region=region,
                    environment_id=environment_id,
                    execution_id=execution_cmd_info["execution_id"],
                    pod=execution_cmd_info["pod"],
                    pod_namespace=execution_cmd_info["pod_namespace"],
                    next_line_number=1,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )
            except Exception as ex:
                self.log.exception("Exception occurred while polling CMD result")
                raise AirflowException(ex)

            result_dict = PollAirflowCommandResponse.to_dict(result)
            if result_dict["output_end"]:
                return result_dict

            self.log.info("Sleeping for %s seconds.", poll_interval)
            await asyncio.sleep(poll_interval)
