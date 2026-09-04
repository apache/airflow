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
"""This module contains Google DataFusion operators."""

from __future__ import annotations

import time
import warnings
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from google.api_core.retry import exponential_sleep_generator
from googleapiclient.errors import HttpError

from airflow.providers.common.compat.sdk import AirflowException, conf
from airflow.providers.google.cloud.hooks.datafusion import (
    FAILURE_STATES,
    SUCCESS_STATES,
    DataFusionHook,
    PipelineStates,
)
from airflow.providers.google.cloud.links.datafusion import (
    DataFusionInstanceLink,
    DataFusionPipelineLink,
    DataFusionPipelinesLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.datafusion import DataFusionStartPipelineTrigger
from airflow.providers.google.cloud.utils.datafusion import DataFusionPipelineType
from airflow.providers.google.cloud.utils.helpers import resource_path_to_dict
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

_DURABLE_UNSET = object()


def _warn_and_disable_durable_pre_3_3(durable: Any) -> bool:
    """Disable durable execution below Airflow 3.3 and warn when explicitly set."""
    if durable is not _DURABLE_UNSET:
        warnings.warn(
            "`durable` has no effect on Airflow versions below 3.3.",
            UserWarning,
            stacklevel=3,
        )
    return False


try:
    from airflow.sdk import ResumableJobMixin
except ImportError:

    class ResumableJobMixin:  # type: ignore[no-redef]
        """Airflow <3.3 stub that always submits a fresh job."""

        external_id_key: str = "datafusion_pipeline_run_id"

        def __init__(self, *, durable: Any = _DURABLE_UNSET, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.durable = _warn_and_disable_durable_pre_3_3(durable)

        def execute_resumable(self, context: Context) -> Any:
            external_id = self.submit_job(context=context)  # type: ignore[attr-defined]
            self.poll_until_complete(external_id=external_id, context=context)  # type: ignore[attr-defined]
            return self.get_job_result(external_id=external_id, context=context)  # type: ignore[attr-defined]


if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.providers.common.compat.sdk import Context
    from airflow.providers.openlineage.extractors import OperatorLineage


class CloudDataFusionRestartInstanceOperator(GoogleCloudBaseOperator):
    """
    Restart a single Data Fusion instance.

    At the end of an operation instance is fully restarted.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionRestartInstanceOperator`

    :param instance_name: The name of the instance to restart.
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param project_id: The ID of the Google Cloud project that the instance belongs to.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "impersonation_chain",
    )
    operator_extra_links = (DataFusionInstanceLink(),)

    def __init__(
        self,
        *,
        instance_name: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Restarting Data Fusion instance: %s", self.instance_name)
        operation = hook.restart_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        instance = hook.wait_for_operation(operation)
        self.log.info("Instance %s restarted successfully", self.instance_name)

        project_id = resource_path_to_dict(resource_name=instance["name"])["projects"]
        DataFusionInstanceLink.persist(
            context=context,
            project_id=project_id,
            instance_name=self.instance_name,
            region=self.location,
        )


class CloudDataFusionDeleteInstanceOperator(GoogleCloudBaseOperator):
    """
    Deletes a single Date Fusion instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionDeleteInstanceOperator`

    :param instance_name: The name of the instance to restart.
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param project_id: The ID of the Google Cloud project that the instance belongs to.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        instance_name: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deleting Data Fusion instance: %s", self.instance_name)
        operation = hook.delete_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        hook.wait_for_operation(operation)
        self.log.info("Instance %s deleted successfully", self.instance_name)


class CloudDataFusionCreateInstanceOperator(GoogleCloudBaseOperator):
    """
    Creates a new Data Fusion instance in the specified project and location.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionCreateInstanceOperator`

    :param instance_name: The name of the instance to create.
    :param instance: An instance of Instance.
        https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param project_id: The ID of the Google Cloud project that the instance belongs to.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "instance",
        "impersonation_chain",
    )
    operator_extra_links = (DataFusionInstanceLink(),)

    def __init__(
        self,
        *,
        instance_name: str,
        instance: dict[str, Any],
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.instance = instance
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating Data Fusion instance: %s", self.instance_name)
        try:
            operation = hook.create_instance(
                instance_name=self.instance_name,
                instance=self.instance,
                location=self.location,
                project_id=self.project_id,
            )
            instance = hook.wait_for_operation(operation)
            self.log.info("Instance %s created successfully", self.instance_name)
        except HttpError as err:
            if err.resp.status not in (409, "409"):
                raise
            self.log.info("Instance %s already exists", self.instance_name)
            instance = hook.get_instance(
                instance_name=self.instance_name, location=self.location, project_id=self.project_id
            )
            # Wait for instance to be ready
            for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
                if instance["state"] != "CREATING":
                    break
                time.sleep(time_to_wait)
                instance = hook.get_instance(
                    instance_name=self.instance_name, location=self.location, project_id=self.project_id
                )

        project_id = resource_path_to_dict(resource_name=instance["name"])["projects"]
        DataFusionInstanceLink.persist(
            context=context,
            project_id=project_id,
            instance_name=self.instance_name,
            region=self.location,
        )
        return instance


class CloudDataFusionUpdateInstanceOperator(GoogleCloudBaseOperator):
    """
    Updates a single Data Fusion instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionUpdateInstanceOperator`

    :param instance_name: The name of the instance to create.
    :param instance: An instance of Instance.
        https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
    :param update_mask: Field mask is used to specify the fields that the update will overwrite
        in an instance resource. The fields specified in the updateMask are relative to the resource,
        not the full request. A field will be overwritten if it is in the mask. If the user does not
        provide a mask, all the supported fields (labels and options currently) will be overwritten.
        A comma-separated list of fully qualified names of fields. Example: "user.displayName,photo".
        https://developers.google.com/protocol-buffers/docs/reference/google.protobuf?_ga=2.205612571.-968688242.1573564810#google.protobuf.FieldMask
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param project_id: The ID of the Google Cloud project that the instance belongs to.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "instance",
        "impersonation_chain",
    )
    operator_extra_links = (DataFusionInstanceLink(),)

    def __init__(
        self,
        *,
        instance_name: str,
        instance: dict[str, Any],
        update_mask: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.update_mask = update_mask
        self.instance_name = instance_name
        self.instance = instance
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Updating Data Fusion instance: %s", self.instance_name)
        operation = hook.patch_instance(
            instance_name=self.instance_name,
            instance=self.instance,
            update_mask=self.update_mask,
            location=self.location,
            project_id=self.project_id,
        )
        instance = hook.wait_for_operation(operation)
        self.log.info("Instance %s updated successfully", self.instance_name)

        project_id = resource_path_to_dict(resource_name=instance["name"])["projects"]
        DataFusionInstanceLink.persist(
            context=context,
            project_id=project_id,
            instance_name=self.instance_name,
            region=self.location,
        )


class CloudDataFusionGetInstanceOperator(GoogleCloudBaseOperator):
    """
    Gets details of a single Data Fusion instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionGetInstanceOperator`

    :param instance_name: The name of the instance.
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param project_id: The ID of the Google Cloud project that the instance belongs to.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "impersonation_chain",
    )
    operator_extra_links = (DataFusionInstanceLink(),)

    def __init__(
        self,
        *,
        instance_name: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Retrieving Data Fusion instance: %s", self.instance_name)
        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )

        project_id = resource_path_to_dict(resource_name=instance["name"])["projects"]
        DataFusionInstanceLink.persist(
            context=context,
            project_id=project_id,
            instance_name=self.instance_name,
            region=self.location,
        )
        return instance


class CloudDataFusionCreatePipelineOperator(GoogleCloudBaseOperator):
    """
    Creates a Cloud Data Fusion pipeline.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionCreatePipelineOperator`

    :param pipeline_name: Your pipeline name.
    :param pipeline: The pipeline definition. For more information check:
        https://docs.cdap.io/cdap/current/en/developer-manual/pipelines/developing-pipelines.html#pipeline-configuration-file-format
    :param instance_name: The name of the instance.
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (DataFusionPipelineLink(),)

    template_fields: Sequence[str] = (
        "instance_name",
        "pipeline_name",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        pipeline_name: str,
        pipeline: dict[str, Any],
        instance_name: str,
        location: str,
        namespace: str = "default",
        project_id: str = PROVIDE_PROJECT_ID,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.pipeline_name = pipeline_name
        self.pipeline = pipeline
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating Data Fusion pipeline: %s", self.pipeline_name)
        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        api_url = instance["apiEndpoint"]
        hook.create_pipeline(
            pipeline_name=self.pipeline_name,
            pipeline=self.pipeline,
            instance_url=api_url,
            namespace=self.namespace,
        )
        DataFusionPipelineLink.persist(
            context=context,
            uri=instance["serviceEndpoint"],
            pipeline_name=self.pipeline_name,
            namespace=self.namespace,
        )
        self.log.info("Pipeline %s created", self.pipeline_name)


class CloudDataFusionDeletePipelineOperator(GoogleCloudBaseOperator):
    """
    Deletes a Cloud Data Fusion pipeline.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionDeletePipelineOperator`

    :param pipeline_name: Your pipeline name.
    :param version_id: Version of pipeline to delete
    :param instance_name: The name of the instance.
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "version_id",
        "pipeline_name",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        pipeline_name: str,
        instance_name: str,
        location: str,
        version_id: str | None = None,
        namespace: str = "default",
        project_id: str = PROVIDE_PROJECT_ID,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.pipeline_name = pipeline_name
        self.version_id = version_id
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deleting Data Fusion pipeline: %s", self.pipeline_name)
        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        api_url = instance["apiEndpoint"]
        hook.delete_pipeline(
            pipeline_name=self.pipeline_name,
            version_id=self.version_id,
            instance_url=api_url,
            namespace=self.namespace,
        )
        self.log.info("Pipeline deleted")


class CloudDataFusionListPipelinesOperator(GoogleCloudBaseOperator):
    """
    Lists Cloud Data Fusion pipelines.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionListPipelinesOperator`


    :param instance_name: The name of the instance.
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param artifact_version: Artifact version to filter instances
    :param artifact_name: Artifact name to filter instances
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "artifact_name",
        "artifact_version",
        "impersonation_chain",
    )
    operator_extra_links = (DataFusionPipelinesLink(),)

    def __init__(
        self,
        *,
        instance_name: str,
        location: str,
        artifact_name: str | None = None,
        artifact_version: str | None = None,
        namespace: str = "default",
        project_id: str = PROVIDE_PROJECT_ID,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.artifact_version = artifact_version
        self.artifact_name = artifact_name
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Listing Data Fusion pipelines")
        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        api_url = instance["apiEndpoint"]
        service_endpoint = instance["serviceEndpoint"]
        pipelines = hook.list_pipelines(
            instance_url=api_url,
            namespace=self.namespace,
            artifact_version=self.artifact_version,
            artifact_name=self.artifact_name,
        )
        self.log.info("Pipelines: %s", pipelines)

        DataFusionPipelinesLink.persist(
            context=context,
            uri=service_endpoint,
            namespace=self.namespace,
        )
        return pipelines


class CloudDataFusionStartPipelineOperator(ResumableJobMixin, GoogleCloudBaseOperator):
    """
    Starts a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionStartPipelineOperator`

    :param pipeline_name: Your pipeline name.
    :param pipeline_type: Optional pipeline type (BATCH by default).
    :param instance_name: The name of the instance.
    :param success_states: If provided the operator will wait for pipeline to be in one of
        the provided states.
    :param pipeline_timeout: How long (in seconds) operator should wait for the pipeline to be in one of
        ``success_states``. Works only if ``success_states`` are provided.
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param runtime_args: Optional runtime args to be passed to the pipeline
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param asynchronous: Flag to return after submitting the pipeline ID to the Data Fusion API.
        This is useful for submitting long-running pipelines and
        waiting on them asynchronously using the CloudDataFusionPipelineStateSensor
    :param deferrable: Run operator in the deferrable mode. Is not related to asynchronous parameter. While
        asynchronous parameter gives a possibility to wait until pipeline reaches terminate state using
        sleep() method, deferrable mode checks for the state using asynchronous calls. It is not possible to
        use both asynchronous and deferrable parameters at the same time.
    :param poll_interval: Polling period in seconds to check for the status. Used only in deferrable mode.
    :param durable: When ``True`` (the default) and waiting synchronously, persist the pipeline run ID
        before polling so a worker retry reconnects to that run. Set to ``False`` to submit a new run
        on retry. Requires Airflow 3.3+; no-op on earlier versions.
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "pipeline_name",
        "runtime_args",
        "impersonation_chain",
    )
    operator_extra_links = (DataFusionPipelineLink(),)
    external_id_key = "datafusion_pipeline_run_id"

    def __init__(
        self,
        *,
        pipeline_name: str,
        instance_name: str,
        location: str,
        pipeline_type: DataFusionPipelineType = DataFusionPipelineType.BATCH,
        runtime_args: dict[str, Any] | None = None,
        success_states: list[str] | None = None,
        namespace: str = "default",
        pipeline_timeout: int = 5 * 60,
        project_id: str = PROVIDE_PROJECT_ID,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        asynchronous: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 3.0,
        durable: bool | None = None,
        **kwargs: Any,
    ) -> None:
        if durable is not None:
            kwargs["durable"] = durable
        super().__init__(**kwargs)
        self.pipeline_name = pipeline_name
        self.pipeline_type = pipeline_type
        self.runtime_args = runtime_args
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.asynchronous = asynchronous
        self.pipeline_timeout = pipeline_timeout
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.pipeline_id: str | None = None
        self._api_url: str | None = None

        if success_states:
            self.success_states = success_states
        else:
            self.success_states = [*SUCCESS_STATES, PipelineStates.RUNNING]

    @cached_property
    def hook(self) -> DataFusionHook:
        return DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

    @property
    def _resolved_api_url(self) -> str:
        if self._api_url is None:
            raise RuntimeError("Data Fusion API endpoint is not initialized")
        return self._api_url

    def submit_job(self, context: Context) -> str:
        self.log.info("Starting Data Fusion pipeline: %s", self.pipeline_name)
        self.pipeline_id = self.hook.start_pipeline(
            pipeline_name=self.pipeline_name,
            pipeline_type=self.pipeline_type,
            instance_url=self._resolved_api_url,
            namespace=self.namespace,
            runtime_args=self.runtime_args,
        )
        self.log.info("Pipeline %s submitted successfully.", self.pipeline_id)
        return self.pipeline_id

    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        self.pipeline_id = cast("str", external_id)
        workflow = self.hook.get_pipeline_workflow(
            pipeline_name=self.pipeline_name,
            pipeline_type=self.pipeline_type,
            namespace=self.namespace,
            instance_url=self._resolved_api_url,
            pipeline_id=self.pipeline_id,
        )
        return workflow["status"]

    def is_job_active(self, status: str) -> bool:
        return status not in SUCCESS_STATES and status not in FAILURE_STATES

    def is_job_succeeded(self, status: str) -> bool:
        return status in SUCCESS_STATES

    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        self.pipeline_id = cast("str", external_id)
        self.log.info("Waiting when pipeline %s will be in one of the success states", self.pipeline_id)
        self.hook.wait_for_pipeline_state(
            success_states=self.success_states,
            pipeline_id=self.pipeline_id,
            pipeline_name=self.pipeline_name,
            pipeline_type=self.pipeline_type,
            namespace=self.namespace,
            instance_url=self._resolved_api_url,
            timeout=self.pipeline_timeout,
        )
        self.log.info("Pipeline %s discovered success state.", self.pipeline_id)

    def get_job_result(self, external_id: JsonValue, context: Context) -> str:
        self.pipeline_id = cast("str", external_id)
        return self.pipeline_id

    def execute(self, context: Context) -> str:
        instance = self.hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        self._api_url = instance["apiEndpoint"]

        DataFusionPipelineLink.persist(
            context=context,
            uri=instance["serviceEndpoint"],
            pipeline_name=self.pipeline_name,
            namespace=self.namespace,
        )

        if not self.asynchronous and not self.deferrable:
            self.execute_resumable(context=context)
            return cast("str", self.pipeline_id)

        self.submit_job(context=context)
        if self.deferrable:
            if self.asynchronous:
                raise AirflowException(
                    "Both asynchronous and deferrable parameters were passed. Please, provide only one."
                )
            self.defer(
                trigger=DataFusionStartPipelineTrigger(
                    success_states=self.success_states,
                    instance_url=self._resolved_api_url,
                    namespace=self.namespace,
                    pipeline_name=self.pipeline_name,
                    pipeline_type=self.pipeline_type.value,
                    pipeline_id=cast("str", self.pipeline_id),
                    poll_interval=self.poll_interval,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                ),
                method_name="execute_complete",
            )
        return cast("str", self.pipeline_id)

    def on_kill(self) -> None:
        if self.pipeline_id is None or self._api_url is None:
            return
        self.hook.stop_pipeline(
            instance_url=self._api_url,
            pipeline_name=self.pipeline_name,
            namespace=self.namespace,
            pipeline_type=self.pipeline_type,
            run_id=self.pipeline_id,
        )

    def execute_complete(self, context: Context, event: dict[str, Any]):
        """
        Act as a callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )
        return event["pipeline_id"]

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage | None:
        """Build and return OpenLineage facets and datasets for the completed pipeline start."""
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.google.cloud.openlineage.facets import DataFusionRunFacet
        from airflow.providers.openlineage.extractors import OperatorLineage

        pipeline_resource = f"{self.project_id}:{self.location}:{self.instance_name}:{self.pipeline_name}"

        inputs = [Dataset(namespace="datafusion", name=pipeline_resource)]

        if self.pipeline_id:
            output_name = f"{pipeline_resource}:{self.pipeline_id}"
        else:
            output_name = f"{pipeline_resource}:unknown"
        outputs = [Dataset(namespace="datafusion", name=output_name)]

        run_facets = {
            "dataFusionRun": DataFusionRunFacet(
                runId=self.pipeline_id,
                runtimeArgs=self.runtime_args,
            )
        }

        return OperatorLineage(inputs=inputs, outputs=outputs, run_facets=run_facets, job_facets={})


class CloudDataFusionStopPipelineOperator(GoogleCloudBaseOperator):
    """
    Stops a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionStopPipelineOperator`

    :param pipeline_name: Your pipeline name.
    :param instance_name: The name of the instance.
    :param pipeline_type: Can be either BATCH or STREAM.
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param run_id: The specific run_id to stop execution if available; when absent it will stop all runs under pipeline_name.
    """

    template_fields: Sequence[str] = ("instance_name", "pipeline_name", "impersonation_chain", "run_id")
    operator_extra_links = (DataFusionPipelineLink(),)

    def __init__(
        self,
        *,
        pipeline_name: str,
        instance_name: str,
        pipeline_type: DataFusionPipelineType = DataFusionPipelineType.BATCH,
        location: str,
        namespace: str = "default",
        project_id: str = PROVIDE_PROJECT_ID,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        run_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.pipeline_name = pipeline_name
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.run_id = run_id
        self.pipeline_type = pipeline_type

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Data Fusion pipeline: %s is going to be stopped", self.pipeline_name)
        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        api_url = instance["apiEndpoint"]

        DataFusionPipelineLink.persist(
            context=context,
            uri=instance["serviceEndpoint"],
            pipeline_name=self.pipeline_name,
            namespace=self.namespace,
        )
        hook.stop_pipeline(
            pipeline_name=self.pipeline_name,
            pipeline_type=self.pipeline_type,
            instance_url=api_url,
            namespace=self.namespace,
            run_id=self.run_id,
        )
        if self.run_id:
            self.log.info(
                "Stopped Cloud Data Fusion pipeline '%s' (namespace: '%s') on instance '%s'. Terminated run id: '%s'.",
                self.pipeline_name,
                self.namespace,
                self.instance_name,
                self.run_id,
            )
        else:
            self.log.info(
                "Stopped Cloud Data Fusion pipeline '%s' (namespace: '%s') on instance '%s'.",
                self.pipeline_name,
                self.namespace,
                self.instance_name,
            )
