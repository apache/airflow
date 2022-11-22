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
"""Operators that integrates with Google Cloud Build service."""
from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Sequence
from urllib.parse import unquote, urlsplit

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.devtools.cloudbuild_v1.types import Build, BuildTrigger, RepoSource

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.cloud_build import CloudBuildHook
from airflow.providers.google.cloud.links.cloud_build import (
    CloudBuildLink,
    CloudBuildListLink,
    CloudBuildTriggerDetailsLink,
    CloudBuildTriggersListLink,
)
from airflow.utils import yaml

if TYPE_CHECKING:
    from airflow.utils.context import Context


REGEX_REPO_PATH = re.compile(r"^/(?P<project_id>[^/]+)/(?P<repo_name>[^/]+)[\+/]*(?P<branch_name>[^:]+)?")


class CloudBuildCancelBuildOperator(BaseOperator):
    """
    Cancels a build in progress.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildCancelBuildOperator`

    :param id_: The ID of the build.
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = ("project_id", "id_", "gcp_conn_id")
    operator_extra_links = (CloudBuildLink(),)

    def __init__(
        self,
        *,
        id_: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.id_ = id_
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        result = hook.cancel_build(
            id_=self.id_,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        self.xcom_push(context, key="id", value=result.id)
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                build_id=result.id,
            )
        return Build.to_dict(result)


class CloudBuildCreateBuildOperator(BaseOperator):
    """
    Starts a build with the specified configuration.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildCreateBuildOperator`

    :param build: The build resource to create. If a dict is provided, it must be of
        the same form as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.Build`.
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :param wait: Optional, wait for operation to finish.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = ("project_id", "build", "gcp_conn_id", "impersonation_chain")
    operator_extra_links = (CloudBuildLink(),)

    def __init__(
        self,
        *,
        build: dict | Build,
        project_id: str | None = None,
        wait: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.wait = wait
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.build = build
        # Not template fields to keep original value
        self.build_raw = build

    def prepare_template(self) -> None:
        # if no file is specified, skip
        if not isinstance(self.build_raw, str):
            return
        with open(self.build_raw) as file:
            if any(self.build_raw.endswith(ext) for ext in [".yaml", ".yml"]):
                self.build = yaml.safe_load(file.read())
            if self.build_raw.endswith(".json"):
                self.build = json.loads(file.read())

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        build = BuildProcessor(build=self.build).process_body()

        result = hook.create_build(
            build=build,
            project_id=self.project_id,
            wait=self.wait,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        self.xcom_push(context, key="id", value=result.id)
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                build_id=result.id,
            )
        return Build.to_dict(result)


class CloudBuildCreateBuildTriggerOperator(BaseOperator):
    """
    Creates a new BuildTrigger.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildCreateBuildTriggerOperator`

    :param trigger: The BuildTrigger to create. If a dict is provided, it must be of the same form
        as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = ("project_id", "trigger", "gcp_conn_id")
    operator_extra_links = (
        CloudBuildTriggersListLink(),
        CloudBuildTriggerDetailsLink(),
    )

    def __init__(
        self,
        *,
        trigger: dict | BuildTrigger,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.trigger = trigger
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        result = hook.create_build_trigger(
            trigger=self.trigger,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.xcom_push(context, key="id", value=result.id)
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildTriggerDetailsLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                trigger_id=result.id,
            )
            CloudBuildTriggersListLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
            )
        return BuildTrigger.to_dict(result)


class CloudBuildDeleteBuildTriggerOperator(BaseOperator):
    """
    Deletes a BuildTrigger by its project ID and trigger ID.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildDeleteBuildTriggerOperator`

    :param trigger_id: The ID of the BuildTrigger to delete.
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("project_id", "trigger_id", "gcp_conn_id")
    operator_extra_links = (CloudBuildTriggersListLink(),)

    def __init__(
        self,
        *,
        trigger_id: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.trigger_id = trigger_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        hook.delete_build_trigger(
            trigger_id=self.trigger_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildTriggersListLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
            )


class CloudBuildGetBuildOperator(BaseOperator):
    """
    Returns information about a previously requested build.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildGetBuildOperator`

    :param id_: The ID of the build.
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = ("project_id", "id_", "gcp_conn_id")
    operator_extra_links = (CloudBuildLink(),)

    def __init__(
        self,
        *,
        id_: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.id_ = id_
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        result = hook.get_build(
            id_=self.id_,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                build_id=result.id,
            )
        return Build.to_dict(result)


class CloudBuildGetBuildTriggerOperator(BaseOperator):
    """
    Returns information about a BuildTrigger.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildGetBuildTriggerOperator`

    :param trigger_id: The ID of the BuildTrigger to get.
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = ("project_id", "trigger_id", "gcp_conn_id")
    operator_extra_links = (CloudBuildTriggerDetailsLink(),)

    def __init__(
        self,
        *,
        trigger_id: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.trigger_id = trigger_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        result = hook.get_build_trigger(
            trigger_id=self.trigger_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildTriggerDetailsLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                trigger_id=result.id,
            )
        return BuildTrigger.to_dict(result)


class CloudBuildListBuildTriggersOperator(BaseOperator):
    """
    Lists existing BuildTriggers.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildListBuildTriggersOperator`

    :param location: The location of the project.
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :param page_size: Optional, number of results to return in the list.
    :param page_token: Optional, token to provide to skip to a particular spot in the list.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = ("location", "project_id", "gcp_conn_id")
    operator_extra_links = (CloudBuildTriggersListLink(),)

    def __init__(
        self,
        *,
        location: str,
        project_id: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.page_size = page_size
        self.page_token = page_token
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        results = hook.list_build_triggers(
            project_id=self.project_id,
            location=self.location,
            page_size=self.page_size,
            page_token=self.page_token,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildTriggersListLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
            )
        return [BuildTrigger.to_dict(result) for result in results]


class CloudBuildListBuildsOperator(BaseOperator):
    """
    Lists previously requested builds.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildListBuildsOperator`

    :param location: The location of the project.
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :param page_size: Optional, number of results to return in the list.
    :param filter_: Optional, the raw filter text to constrain the results.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = ("location", "project_id", "gcp_conn_id")
    operator_extra_links = (CloudBuildListLink(),)

    def __init__(
        self,
        *,
        location: str,
        project_id: str | None = None,
        page_size: int | None = None,
        filter_: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.page_size = page_size
        self.filter_ = filter_
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        results = hook.list_builds(
            project_id=self.project_id,
            location=self.location,
            page_size=self.page_size,
            filter_=self.filter_,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildListLink.persist(context=context, task_instance=self, project_id=project_id)
        return [Build.to_dict(result) for result in results]


class CloudBuildRetryBuildOperator(BaseOperator):
    """
    Creates a new build based on the specified build. This method creates a new build
    using the original build request, which may or may not result in an identical build.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildRetryBuildOperator`

    :param id_: Build ID of the original build.
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :param wait: Optional, wait for operation to finish.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = ("project_id", "id_", "gcp_conn_id")
    operator_extra_links = (CloudBuildLink(),)

    def __init__(
        self,
        *,
        id_: str,
        project_id: str | None = None,
        wait: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.id_ = id_
        self.project_id = project_id
        self.wait = wait
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        result = hook.retry_build(
            id_=self.id_,
            project_id=self.project_id,
            wait=self.wait,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        self.xcom_push(context, key="id", value=result.id)
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                build_id=result.id,
            )
        return Build.to_dict(result)


class CloudBuildRunBuildTriggerOperator(BaseOperator):
    """
    Runs a BuildTrigger at a particular source revision.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildRunBuildTriggerOperator`

    :param trigger_id: The ID of the trigger.
    :param source: Source to build against this trigger. If a dict is provided, it must be of the same form
        as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.RepoSource`
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :param wait: Optional, wait for operation to finish.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = ("project_id", "trigger_id", "source", "gcp_conn_id")
    operator_extra_links = (CloudBuildLink(),)

    def __init__(
        self,
        *,
        trigger_id: str,
        source: dict | RepoSource,
        project_id: str | None = None,
        wait: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.trigger_id = trigger_id
        self.source = source
        self.project_id = project_id
        self.wait = wait
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        result = hook.run_build_trigger(
            trigger_id=self.trigger_id,
            source=self.source,
            project_id=self.project_id,
            wait=self.wait,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.xcom_push(context, key="id", value=result.id)
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                build_id=result.id,
            )
        return Build.to_dict(result)


class CloudBuildUpdateBuildTriggerOperator(BaseOperator):
    """
    Updates a BuildTrigger by its project ID and trigger ID.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildUpdateBuildTriggerOperator`

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
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = ("project_id", "trigger_id", "trigger", "gcp_conn_id")
    operator_extra_links = (CloudBuildTriggerDetailsLink(),)

    def __init__(
        self,
        *,
        trigger_id: str,
        trigger: dict | BuildTrigger,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.trigger_id = trigger_id
        self.trigger = trigger
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        result = hook.update_build_trigger(
            trigger_id=self.trigger_id,
            trigger=self.trigger,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.xcom_push(context, key="id", value=result.id)
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBuildTriggerDetailsLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                trigger_id=result.id,
            )
        return BuildTrigger.to_dict(result)


class BuildProcessor:
    """
    Processes build configurations to add additional functionality to support the use of operators.
    The following improvements are made:
    * It is required to provide the source and only one type can be given,
    * It is possible to provide the source as the URL address instead dict.

    :param build: The request body of the build.
        See: https://cloud.google.com/cloud-build/docs/api/reference/rest/Shared.Types/Build
    """

    def __init__(self, build: dict | Build) -> None:
        self.build = deepcopy(build)

    def _verify_source(self) -> None:
        if not (("storage_source" in self.build["source"]) ^ ("repo_source" in self.build["source"])):
            raise AirflowException(
                "The source could not be determined. Please choose one data source from: "
                "storage_source and repo_source."
            )

    def _reformat_source(self) -> None:
        self._reformat_repo_source()
        self._reformat_storage_source()

    def _reformat_repo_source(self) -> None:
        if "repo_source" not in self.build["source"]:
            return

        repo_source = self.build["source"]["repo_source"]

        if not isinstance(repo_source, str):
            return

        self.build["source"]["repo_source"] = self._convert_repo_url_to_dict(repo_source)

    def _reformat_storage_source(self) -> None:
        if "storage_source" not in self.build["source"]:
            return

        storage_source = self.build["source"]["storage_source"]

        if not isinstance(storage_source, str):
            return

        self.build["source"]["storage_source"] = self._convert_storage_url_to_dict(storage_source)

    def process_body(self) -> Build:
        """
        Processes the body passed in the constructor

        :return: the body.
        """
        if "source" in self.build:
            self._verify_source()
            self._reformat_source()
        return Build(self.build)

    @staticmethod
    def _convert_repo_url_to_dict(source: str) -> dict[str, Any]:
        """
        Convert url to repository in Google Cloud Source to a format supported by the API

        Example valid input:

        .. code-block:: none

            https://source.cloud.google.com/airflow-project/airflow-repo/+/branch-name:

        """
        url_parts = urlsplit(source)

        match = REGEX_REPO_PATH.search(url_parts.path)

        if url_parts.scheme != "https" or url_parts.hostname != "source.cloud.google.com" or not match:
            raise AirflowException(
                "Invalid URL. You must pass the URL in the format: "
                "https://source.cloud.google.com/airflow-project/airflow-repo/+/branch-name:"
            )

        project_id = unquote(match.group("project_id"))
        repo_name = unquote(match.group("repo_name"))
        branch_name = unquote(match.group("branch_name")) if match.group("branch_name") else "master"

        source_dict = {
            "project_id": project_id,
            "repo_name": repo_name,
            "branch_name": branch_name,
        }

        return source_dict

    @staticmethod
    def _convert_storage_url_to_dict(storage_url: str) -> dict[str, Any]:
        """
        Convert url to object in Google Cloud Storage to a format supported by the API

        Example valid input:

        .. code-block:: none

            gs://bucket-name/object-name.tar.gz

        """
        url_parts = urlsplit(storage_url)

        if url_parts.scheme != "gs" or not url_parts.hostname or not url_parts.path or url_parts.path == "/":
            raise AirflowException(
                "Invalid URL. You must pass the URL in the format: "
                "gs://bucket-name/object-name.tar.gz#24565443"
            )

        source_dict: dict[str, Any] = {
            "bucket": url_parts.hostname,
            "object_": url_parts.path[1:],
        }

        if url_parts.fragment:
            source_dict["generation"] = int(url_parts.fragment)

        return source_dict
