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

import warnings
from typing import TYPE_CHECKING, Sequence

from airflow.providers.google.cloud.links.dataform import (
    DataformRepositoryLink,
    DataformWorkflowInvocationLink,
    DataformWorkspaceLink,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.dataform_v1beta1.types import (
    CompilationResult,
    InstallNpmPackagesResponse,
    MakeDirectoryResponse,
    Repository,
    WorkflowInvocation,
    Workspace,
    WriteFileResponse,
)

from airflow.providers.google.cloud.hooks.dataform import DataformHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator


class DataformCreateCompilationResultOperator(GoogleCloudBaseOperator):
    """
    Creates a new CompilationResult in a given project and location.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
    :param compilation_result:  Required. The compilation result to create.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        compilation_result: CompilationResult | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.compilation_result = compilation_result
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.create_compilation_result(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            compilation_result=self.compilation_result,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return CompilationResult.to_dict(result)


class DataformGetCompilationResultOperator(GoogleCloudBaseOperator):
    """
    Fetches a single CompilationResult.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
    :param compilation_result_id:  The Id of the Dataform Compilation Result
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("repository_id", "compilation_result_id", "delegate_to", "impersonation_chain")

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        compilation_result_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.compilation_result_id = compilation_result_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.get_compilation_result(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            compilation_result_id=self.compilation_result_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return CompilationResult.to_dict(result)


class DataformCreateWorkflowInvocationOperator(GoogleCloudBaseOperator):
    """
    Creates a new WorkflowInvocation in a given Repository.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
    :param workflow_invocation:  Required. The workflow invocation resource to create.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param asynchronous: Flag to return workflow_invocation_id from the Dataform API.
        This is useful for submitting long running workflows and
        waiting on them asynchronously using the DataformWorkflowInvocationStateSensor
    :param wait_time: Number of seconds between checks
    """

    template_fields = ("workflow_invocation", "delegate_to", "impersonation_chain")
    operator_extra_links = (DataformWorkflowInvocationLink(),)

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workflow_invocation: WorkflowInvocation | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: int | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        asynchronous: bool = False,
        wait_time: int = 10,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.workflow_invocation = workflow_invocation
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.asynchronous = asynchronous
        self.wait_time = wait_time

    def execute(self, context: Context):
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.create_workflow_invocation(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workflow_invocation=self.workflow_invocation,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        workflow_invocation_id = result.name.split("/")[-1]
        DataformWorkflowInvocationLink.persist(
            operator_instance=self,
            context=context,
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workflow_invocation_id=workflow_invocation_id,
        )
        if not self.asynchronous:
            hook.wait_for_workflow_invocation(
                workflow_invocation_id=workflow_invocation_id,
                repository_id=self.repository_id,
                project_id=self.project_id,
                region=self.region,
                timeout=self.timeout,
                wait_time=self.wait_time,
            )
        return WorkflowInvocation.to_dict(result)


class DataformGetWorkflowInvocationOperator(GoogleCloudBaseOperator):
    """
    Fetches a single WorkflowInvocation.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
    :param workflow_invocation_id:  the workflow invocation resource's id.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("repository_id", "workflow_invocation_id", "delegate_to", "impersonation_chain")
    operator_extra_links = (DataformWorkflowInvocationLink(),)

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workflow_invocation_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.workflow_invocation_id = workflow_invocation_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.get_workflow_invocation(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workflow_invocation_id=self.workflow_invocation_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return WorkflowInvocation.to_dict(result)


class DataformCancelWorkflowInvocationOperator(GoogleCloudBaseOperator):
    """
    Requests cancellation of a running WorkflowInvocation.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
    :param workflow_invocation_id:  the workflow invocation resource's id.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("repository_id", "workflow_invocation_id", "delegate_to", "impersonation_chain")
    operator_extra_links = (DataformWorkflowInvocationLink(),)

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workflow_invocation_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.workflow_invocation_id = workflow_invocation_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        hook.cancel_workflow_invocation(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workflow_invocation_id=self.workflow_invocation_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataformCreateRepositoryOperator(GoogleCloudBaseOperator):
    """
    Creates repository.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (DataformRepositoryLink(),)
    template_fields = (
        "project_id",
        "repository_id",
        "delegate_to",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id

        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        repository = hook.create_repository(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        DataformRepositoryLink.persist(
            operator_instance=self,
            context=context,
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
        )

        return Repository.to_dict(repository)


class DataformDeleteRepositoryOperator(GoogleCloudBaseOperator):
    """
    Deletes repository.

    :param project_id: Required. The ID of the Google Cloud project where repository located.
    :param region: Required. The ID of the Google Cloud region where repository located.
    :param repository_id: Required. The ID of the Dataform repository that should be deleted.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = (
        "project_id",
        "repository_id",
        "delegate_to",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        force: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.repository_id = repository_id
        self.project_id = project_id
        self.region = region
        self.force = force

        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        hook.delete_repository(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            force=self.force,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataformCreateWorkspaceOperator(GoogleCloudBaseOperator):
    """
    Creates workspace.

    :param project_id: Required. The ID of the Google Cloud project where workspace should be in.
    :param region: Required. Name of the Google Cloud region that where workspace should be in.
    :param repository_id: Required. The ID of the Dataform repository that the workspace belongs to.
    :param workspace_id: Required. The ID of the new workspace that will be created.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (DataformWorkspaceLink(),)
    template_fields = (
        "project_id",
        "repository_id",
        "delegate_to",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.workspace_id = workspace_id
        self.repository_id = repository_id
        self.region = region

        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        workspace = hook.create_workspace(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workspace_id=self.workspace_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        DataformWorkspaceLink.persist(
            operator_instance=self,
            context=context,
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workspace_id=self.workspace_id,
        )

        return Workspace.to_dict(workspace)


class DataformDeleteWorkspaceOperator(GoogleCloudBaseOperator):
    """
    Deletes workspace.

    :param project_id: Required. The ID of the Google Cloud project where workspace located.
    :param region: Required. The ID of the Google Cloud region where workspace located.
    :param repository_id: Required. The ID of the Dataform repository where workspace located.
    :param workspace_id: Required. The ID of the Dataform workspace that should be deleted.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = (
        "project_id",
        "repository_id",
        "workspace_id",
        "delegate_to",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.workspace_id = workspace_id

        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        hook.delete_workspace(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workspace_id=self.workspace_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataformWriteFileOperator(GoogleCloudBaseOperator):
    """
    Writes new file to specified workspace.

    :param project_id: Required. The ID of the Google Cloud project where workspace located.
    :param region: Required. The ID of the Google Cloud region where workspace located.
    :param repository_id: Required. The ID of the Dataform repository where workspace located.
    :param workspace_id: Required. The ID of the Dataform workspace where files should be created.
    :param filepath: Required. Path to file including name of the file relative to workspace root.
    :param contents: Required. Content of the file to be written.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = (
        "project_id",
        "repository_id",
        "workspace_id",
        "delegate_to",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        filepath: str,
        contents: bytes,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.workspace_id = workspace_id
        self.filepath = filepath
        self.contents = contents

        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        write_file_response = hook.write_file(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workspace_id=self.workspace_id,
            filepath=self.filepath,
            contents=self.contents,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return WriteFileResponse.to_dict(write_file_response)


class DataformMakeDirectoryOperator(GoogleCloudBaseOperator):
    """
    Makes new directory in specified workspace.

    :param project_id: Required. The ID of the Google Cloud project where workspace located.
    :param region: Required. The ID of the Google Cloud region where workspace located.
    :param repository_id: Required. The ID of the Dataform repository where workspace located.
    :param workspace_id: Required. The ID of the Dataform workspace where directory should be created.
    :param path: Required. The directory's full path including directory name, relative to the workspace root.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = (
        "project_id",
        "repository_id",
        "workspace_id",
        "delegate_to",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        directory_path: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.workspace_id = workspace_id
        self.directory_path = directory_path

        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        make_directory_response = hook.make_directory(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workspace_id=self.workspace_id,
            path=self.directory_path,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        return MakeDirectoryResponse.to_dict(make_directory_response)


class DataformRemoveFileOperator(GoogleCloudBaseOperator):
    """
    Removes file in specified workspace.

    :param project_id: Required. The ID of the Google Cloud project where workspace located.
    :param region: Required. The ID of the Google Cloud region where workspace located.
    :param repository_id: Required. The ID of the Dataform repository where workspace located.
    :param workspace_id: Required. The ID of the Dataform workspace where directory located.
    :param filepath: Required. The full path including name of the file, relative to the workspace root.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = (
        "project_id",
        "repository_id",
        "workspace_id",
        "delegate_to",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        filepath: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.workspace_id = workspace_id
        self.filepath = filepath

        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        hook.remove_file(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workspace_id=self.workspace_id,
            filepath=self.filepath,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataformRemoveDirectoryOperator(GoogleCloudBaseOperator):
    """
    Removes directory in specified workspace.

    :param project_id: Required. The ID of the Google Cloud project where workspace located.
    :param region: Required. The ID of the Google Cloud region where workspace located.
    :param repository_id: Required. The ID of the Dataform repository where workspace located.
    :param workspace_id: Required. The ID of the Dataform workspace where directory located.
    :param path: Required. The directory's full path including directory name, relative to the workspace root.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = (
        "project_id",
        "repository_id",
        "workspace_id",
        "delegate_to",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        directory_path: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.workspace_id = workspace_id
        self.directory_path = directory_path

        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        hook.remove_directory(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workspace_id=self.workspace_id,
            path=self.directory_path,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataformInstallNpmPackagesOperator(GoogleCloudBaseOperator):
    """
    Installs npm dependencies in the provided workspace. Requires "package.json" to be created in workspace

    :param project_id: Required. The ID of the Google Cloud project where workspace located.
    :param region: Required. The ID of the Google Cloud region where workspace located.
    :param repository_id: Required. The ID of the Dataform repository where workspace located.
    :param workspace_id: Required. The ID of the Dataform workspace.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = (
        "project_id",
        "repository_id",
        "workspace_id",
        "delegate_to",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id
        self.workspace_id = workspace_id

        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        response = hook.install_npm_packages(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workspace_id=self.workspace_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        return InstallNpmPackagesResponse.to_dict(response)
