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

from time import sleep
from typing import TYPE_CHECKING, Any, Sequence

from google.api_core.retry import exponential_sleep_generator
from googleapiclient.errors import HttpError

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.datafusion import SUCCESS_STATES, DataFusionHook, PipelineStates
from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context


BASE_LINK = "https://console.cloud.google.com/data-fusion"
DATAFUSION_INSTANCE_LINK = BASE_LINK + "/locations/{region}/instances/{instance_name}?project={project_id}"
DATAFUSION_PIPELINES_LINK = "{uri}/cdap/ns/default/pipelines"
DATAFUSION_PIPELINE_LINK = "{uri}/pipelines/ns/default/view/{pipeline_name}"


class DataFusionPipelineLinkHelper:
    """Helper class for Pipeline links"""

    @staticmethod
    def get_project_id(instance):
        instance = instance["name"]
        project_id = [x for x in instance.split("/") if x.startswith("airflow")][0]
        return project_id


class DataFusionInstanceLink(BaseGoogleLink):
    """Helper class for constructing Data Fusion Instance link"""

    name = "Data Fusion Instance"
    key = "instance_conf"
    format_str = DATAFUSION_INSTANCE_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: (
            CloudDataFusionRestartInstanceOperator
            | CloudDataFusionCreateInstanceOperator
            | CloudDataFusionUpdateInstanceOperator
            | CloudDataFusionGetInstanceOperator
        ),
        project_id: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataFusionInstanceLink.key,
            value={
                "region": task_instance.location,
                "instance_name": task_instance.instance_name,
                "project_id": project_id,
            },
        )


class DataFusionPipelineLink(BaseGoogleLink):
    """Helper class for constructing Data Fusion Pipeline link"""

    name = "Data Fusion Pipeline"
    key = "pipeline_conf"
    format_str = DATAFUSION_PIPELINE_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: (
            CloudDataFusionCreatePipelineOperator
            | CloudDataFusionStartPipelineOperator
            | CloudDataFusionStopPipelineOperator
        ),
        uri: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataFusionPipelineLink.key,
            value={
                "uri": uri,
                "pipeline_name": task_instance.pipeline_name,
            },
        )


class DataFusionPipelinesLink(BaseGoogleLink):
    """Helper class for constructing list of Data Fusion Pipelines link"""

    name = "Data Fusion Pipelines"
    key = "pipelines_conf"
    format_str = DATAFUSION_PIPELINES_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: CloudDataFusionListPipelinesOperator,
        uri: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataFusionPipelinesLink.key,
            value={
                "uri": uri,
            },
        )


class CloudDataFusionRestartInstanceOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        project_id: str | None = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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

        project_id = self.project_id or DataFusionPipelineLinkHelper.get_project_id(instance)
        DataFusionInstanceLink.persist(context=context, task_instance=self, project_id=project_id)


class CloudDataFusionDeleteInstanceOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        project_id: str | None = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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


class CloudDataFusionCreateInstanceOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        project_id: str | None = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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
                sleep(time_to_wait)
                instance = hook.get_instance(
                    instance_name=self.instance_name, location=self.location, project_id=self.project_id
                )

        project_id = self.project_id or DataFusionPipelineLinkHelper.get_project_id(instance)
        DataFusionInstanceLink.persist(context=context, task_instance=self, project_id=project_id)
        return instance


class CloudDataFusionUpdateInstanceOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        project_id: str | None = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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

        project_id = self.project_id or DataFusionPipelineLinkHelper.get_project_id(instance)
        DataFusionInstanceLink.persist(context=context, task_instance=self, project_id=project_id)


class CloudDataFusionGetInstanceOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        project_id: str | None = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Retrieving Data Fusion instance: %s", self.instance_name)
        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )

        project_id = self.project_id or DataFusionPipelineLinkHelper.get_project_id(instance)
        DataFusionInstanceLink.persist(context=context, task_instance=self, project_id=project_id)
        return instance


class CloudDataFusionCreatePipelineOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        "pipeline_name",
        "impersonation_chain",
    )
    operator_extra_links = (DataFusionPipelineLink(),)

    def __init__(
        self,
        *,
        pipeline_name: str,
        pipeline: dict[str, Any],
        instance_name: str,
        location: str,
        namespace: str = "default",
        project_id: str | None = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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

        DataFusionPipelineLink.persist(context=context, task_instance=self, uri=instance["serviceEndpoint"])
        self.log.info("Pipeline created")


class CloudDataFusionDeletePipelineOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        project_id: str | None = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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


class CloudDataFusionListPipelinesOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        project_id: str | None = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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
        pipelines = hook.list_pipelines(
            instance_url=api_url,
            namespace=self.namespace,
            artifact_version=self.artifact_version,
            artifact_name=self.artifact_name,
        )
        self.log.info("%s", pipelines)

        DataFusionPipelinesLink.persist(context=context, task_instance=self, uri=instance["serviceEndpoint"])
        return pipelines


class CloudDataFusionStartPipelineOperator(BaseOperator):
    """
    Starts a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionStartPipelineOperator`

    :param pipeline_name: Your pipeline name.
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param asynchronous: Flag to return after submitting the pipeline Id to the Data Fusion API.
        This is useful for submitting long running pipelines and
        waiting on them asynchronously using the CloudDataFusionPipelineStateSensor
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "pipeline_name",
        "runtime_args",
        "impersonation_chain",
    )
    operator_extra_links = (DataFusionPipelineLink(),)

    def __init__(
        self,
        *,
        pipeline_name: str,
        instance_name: str,
        location: str,
        runtime_args: dict[str, Any] | None = None,
        success_states: list[str] | None = None,
        namespace: str = "default",
        pipeline_timeout: int = 5 * 60,
        project_id: str | None = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        asynchronous=False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.pipeline_name = pipeline_name
        self.runtime_args = runtime_args
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.asynchronous = asynchronous
        self.pipeline_timeout = pipeline_timeout

        if success_states:
            self.success_states = success_states
        else:
            self.success_states = SUCCESS_STATES + [PipelineStates.RUNNING]

    def execute(self, context: Context) -> str:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Starting Data Fusion pipeline: %s", self.pipeline_name)
        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        api_url = instance["apiEndpoint"]
        pipeline_id = hook.start_pipeline(
            pipeline_name=self.pipeline_name,
            instance_url=api_url,
            namespace=self.namespace,
            runtime_args=self.runtime_args,
        )
        self.log.info("Pipeline %s submitted successfully.", pipeline_id)

        DataFusionPipelineLink.persist(context=context, task_instance=self, uri=instance["serviceEndpoint"])

        if not self.asynchronous:
            self.log.info("Waiting when pipeline %s will be in one of the success states", pipeline_id)
            hook.wait_for_pipeline_state(
                success_states=self.success_states,
                pipeline_id=pipeline_id,
                pipeline_name=self.pipeline_name,
                namespace=self.namespace,
                instance_url=api_url,
                timeout=self.pipeline_timeout,
            )
            self.log.info("Job %s discover success state.", pipeline_id)
        return pipeline_id


class CloudDataFusionStopPipelineOperator(BaseOperator):
    """
    Stops a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionStopPipelineOperator`

    :param pipeline_name: Your pipeline name.
    :param instance_name: The name of the instance.
    :param location: The Cloud Data Fusion location in which to handle the request.
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        "pipeline_name",
        "impersonation_chain",
    )
    operator_extra_links = (DataFusionPipelineLink(),)

    def __init__(
        self,
        *,
        pipeline_name: str,
        instance_name: str,
        location: str,
        namespace: str = "default",
        project_id: str | None = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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

        DataFusionPipelineLink.persist(context=context, task_instance=self, uri=instance["serviceEndpoint"])
        hook.stop_pipeline(
            pipeline_name=self.pipeline_name,
            instance_url=api_url,
            namespace=self.namespace,
        )
        self.log.info("Pipeline stopped")
