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

from typing import TYPE_CHECKING, Sequence

from google.api_core.exceptions import AlreadyExists
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.orchestration.airflow.service_v1 import ImageVersion
from google.cloud.orchestration.airflow.service_v1.types import Environment
from google.protobuf.field_mask_pb2 import FieldMask

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.cloud_composer import CloudComposerHook
from airflow.providers.google.cloud.links.base import BaseGoogleLink
from airflow.providers.google.cloud.triggers.cloud_composer import CloudComposerExecutionTrigger
from airflow.providers.google.common.consts import GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

if TYPE_CHECKING:
    from airflow.utils.context import Context

CLOUD_COMPOSER_BASE_LINK = "https://console.cloud.google.com/composer/environments"
CLOUD_COMPOSER_DETAILS_LINK = (
    CLOUD_COMPOSER_BASE_LINK + "/detail/{region}/{environment_id}/monitoring?project={project_id}"
)
CLOUD_COMPOSER_ENVIRONMENTS_LINK = CLOUD_COMPOSER_BASE_LINK + "?project={project_id}"


class CloudComposerEnvironmentLink(BaseGoogleLink):
    """Helper class for constructing Cloud Composer Environment Link"""

    name = "Cloud Composer Environment"
    key = "composer_conf"
    format_str = CLOUD_COMPOSER_DETAILS_LINK

    @staticmethod
    def persist(
        operator_instance: (
            CloudComposerCreateEnvironmentOperator
            | CloudComposerUpdateEnvironmentOperator
            | CloudComposerGetEnvironmentOperator
        ),
        context: Context,
    ) -> None:
        operator_instance.xcom_push(
            context,
            key=CloudComposerEnvironmentLink.key,
            value={
                "project_id": operator_instance.project_id,
                "region": operator_instance.region,
                "environment_id": operator_instance.environment_id,
            },
        )


class CloudComposerEnvironmentsLink(BaseGoogleLink):
    """Helper class for constructing Cloud Composer Environment Link"""

    name = "Cloud Composer Environment List"
    key = "composer_conf"
    format_str = CLOUD_COMPOSER_ENVIRONMENTS_LINK

    @staticmethod
    def persist(operator_instance: CloudComposerListEnvironmentsOperator, context: Context) -> None:
        operator_instance.xcom_push(
            context,
            key=CloudComposerEnvironmentsLink.key,
            value={
                "project_id": operator_instance.project_id,
            },
        )


class CloudComposerCreateEnvironmentOperator(BaseOperator):
    """
    Create a new environment.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
    :param environment:  The environment to create.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param deferrable: Run operator in the deferrable mode
    :param pooling_period_seconds: Optional: Control the rate of the poll for the result of deferrable run.
        By default the trigger will poll every 30 seconds.
    """

    template_fields = (
        "project_id",
        "region",
        "environment_id",
        "environment",
        "impersonation_chain",
    )

    operator_extra_links = (CloudComposerEnvironmentLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        environment: Environment | dict,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        deferrable: bool = False,
        pooling_period_seconds: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.environment = environment
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to
        self.deferrable = deferrable
        self.pooling_period_seconds = pooling_period_seconds

    def execute(self, context: Context):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )

        name = hook.get_environment_name(self.project_id, self.region, self.environment_id)
        if isinstance(self.environment, Environment):
            self.environment.name = name
        else:
            self.environment["name"] = name

        CloudComposerEnvironmentLink.persist(operator_instance=self, context=context)
        try:
            result = hook.create_environment(
                project_id=self.project_id,
                region=self.region,
                environment=self.environment,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            if not self.deferrable:
                environment = hook.wait_for_operation(timeout=self.timeout, operation=result)
                return Environment.to_dict(environment)
            else:
                self.defer(
                    trigger=CloudComposerExecutionTrigger(
                        project_id=self.project_id,
                        region=self.region,
                        operation_name=result.operation.name,
                        gcp_conn_id=self.gcp_conn_id,
                        impersonation_chain=self.impersonation_chain,
                        delegate_to=self.delegate_to,
                        pooling_period_seconds=self.pooling_period_seconds,
                    ),
                    method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
                )
        except AlreadyExists:
            environment = hook.get_environment(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            return Environment.to_dict(environment)

    def execute_complete(self, context: Context, event: dict):
        if event["operation_done"]:
            hook = CloudComposerHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                delegate_to=self.delegate_to,
            )

            env = hook.get_environment(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            return Environment.to_dict(env)
        else:
            raise AirflowException(f"Unexpected error in the operation: {event['operation_name']}")


class CloudComposerDeleteEnvironmentOperator(BaseOperator):
    """
    Delete an environment.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param deferrable: Run operator in the deferrable mode
    :param pooling_period_seconds: Optional: Control the rate of the poll for the result of deferrable run.
        By default the trigger will poll every 30 seconds.
    """

    template_fields = (
        "project_id",
        "region",
        "environment_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        deferrable: bool = False,
        pooling_period_seconds: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to
        self.deferrable = deferrable
        self.pooling_period_seconds = pooling_period_seconds

    def execute(self, context: Context):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        result = hook.delete_environment(
            project_id=self.project_id,
            region=self.region,
            environment_id=self.environment_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        if not self.deferrable:
            hook.wait_for_operation(timeout=self.timeout, operation=result)
        else:
            self.defer(
                trigger=CloudComposerExecutionTrigger(
                    project_id=self.project_id,
                    region=self.region,
                    operation_name=result.operation.name,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    delegate_to=self.delegate_to,
                    pooling_period_seconds=self.pooling_period_seconds,
                ),
                method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
            )

    def execute_complete(self, context: Context, event: dict):
        pass


class CloudComposerGetEnvironmentOperator(BaseOperator):
    """
    Get an existing environment.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    """

    template_fields = (
        "project_id",
        "region",
        "environment_id",
        "impersonation_chain",
    )

    operator_extra_links = (CloudComposerEnvironmentLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to

    def execute(self, context: Context):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )

        result = hook.get_environment(
            project_id=self.project_id,
            region=self.region,
            environment_id=self.environment_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        CloudComposerEnvironmentLink.persist(operator_instance=self, context=context)
        return Environment.to_dict(result)


class CloudComposerListEnvironmentsOperator(BaseOperator):
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
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    """

    template_fields = (
        "project_id",
        "region",
        "impersonation_chain",
    )

    operator_extra_links = (CloudComposerEnvironmentsLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.page_size = page_size
        self.page_token = page_token
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to

    def execute(self, context: Context):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        CloudComposerEnvironmentsLink.persist(operator_instance=self, context=context)
        result = hook.list_environments(
            project_id=self.project_id,
            region=self.region,
            page_size=self.page_size,
            page_token=self.page_token,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [Environment.to_dict(env) for env in result]


class CloudComposerUpdateEnvironmentOperator(BaseOperator):
    r"""
    Update an environment.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
    :param environment:  A patch environment. Fields specified by the ``updateMask`` will be copied from the
        patch environment into the environment under update.
    :param update_mask:  Required. A comma-separated list of paths, relative to ``Environment``, of fields to
        update. If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.protobuf.field_mask_pb2.FieldMask`
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param deferrable: Run operator in the deferrable mode
    :param pooling_period_seconds: Optional: Control the rate of the poll for the result of deferrable run.
        By default the trigger will poll every 30 seconds.
    """

    template_fields = (
        "project_id",
        "region",
        "environment_id",
        "impersonation_chain",
    )

    operator_extra_links = (CloudComposerEnvironmentLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        environment: dict | Environment,
        update_mask: dict | FieldMask,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        deferrable: bool = False,
        pooling_period_seconds: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.environment = environment
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to
        self.deferrable = deferrable
        self.pooling_period_seconds = pooling_period_seconds

    def execute(self, context: Context):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )

        result = hook.update_environment(
            project_id=self.project_id,
            region=self.region,
            environment_id=self.environment_id,
            environment=self.environment,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        CloudComposerEnvironmentLink.persist(operator_instance=self, context=context)
        if not self.deferrable:
            environment = hook.wait_for_operation(timeout=self.timeout, operation=result)
            return Environment.to_dict(environment)
        else:
            self.defer(
                trigger=CloudComposerExecutionTrigger(
                    project_id=self.project_id,
                    region=self.region,
                    operation_name=result.operation.name,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    delegate_to=self.delegate_to,
                    pooling_period_seconds=self.pooling_period_seconds,
                ),
                method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
            )

    def execute_complete(self, context: Context, event: dict):
        if event["operation_done"]:
            hook = CloudComposerHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                delegate_to=self.delegate_to,
            )

            env = hook.get_environment(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            return Environment.to_dict(env)
        else:
            raise AirflowException(f"Unexpected error in the operation: {event['operation_name']}")


class CloudComposerListImageVersionsOperator(BaseOperator):
    """
    List ImageVersions for provided location.

    :param request:  The request object. List ImageVersions in a project and location.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    """

    template_fields = (
        "project_id",
        "region",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        page_size: int | None = None,
        page_token: str | None = None,
        include_past_releases: bool = False,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.page_size = page_size
        self.page_token = page_token
        self.include_past_releases = include_past_releases
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to

    def execute(self, context: Context):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        result = hook.list_image_versions(
            project_id=self.project_id,
            region=self.region,
            page_size=self.page_size,
            page_token=self.page_token,
            include_past_releases=self.include_past_releases,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [ImageVersion.to_dict(image) for image in result]
