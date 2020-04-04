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
This module contains Google DataFusion operators.
"""
from typing import Any, Dict, Optional

from googleapiclient.errors import HttpError

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.datafusion import DataFusionHook
from airflow.utils.decorators import apply_defaults


class CloudDataFusionRestartInstanceOperator(BaseOperator):
    """
    Restart a single Data Fusion instance.
    At the end of an operation instance is fully restarted.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionRestartInstanceOperator`

    :param instance_name: The name of the instance to restart.
    :type instance_name: str
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param project_id: The ID of the Google Cloud Platform project that the instance belongs to.
    :type project_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("instance_name",)

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        location: str,
        project_id: Optional[str] = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Restarting Data Fusion instace: %s", self.instance_name)
        operation = hook.restart_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        hook.wait_for_operation(operation)
        self.log.info("Instance %s restarted successfully", self.instance_name)


class CloudDataFusionDeleteInstanceOperator(BaseOperator):
    """
    Deletes a single Date Fusion instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionDeleteInstanceOperator`

    :param instance_name: The name of the instance to restart.
    :type instance_name: str
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param project_id: The ID of the Google Cloud Platform project that the instance belongs to.
    :type project_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("instance_name",)

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        location: str,
        project_id: Optional[str] = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Deleting Data Fusion instace: %s", self.instance_name)
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
    :type instance_name: str
    :param instance: An instance of Instance.
        https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
    :type instance: Dict[str, Any]
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param project_id: The ID of the Google Cloud Platform project that the instance belongs to.
    :type project_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("instance_name", "instance")

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        instance: Dict[str, Any],
        location: str,
        project_id: Optional[str] = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.instance_name = instance_name
        self.instance = instance
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Creating Data Fusion instace: %s", self.instance_name)
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
            if err.resp.status != 409:
                raise
            self.log.info("Instance %s already exists", self.instance_name)
            instance = hook.get_instance(
                instance_name=self.instance_name, location=self.location, project_id=self.project_id
            )

        return instance


class CloudDataFusionUpdateInstanceOperator(BaseOperator):
    """
    Updates a single Data Fusion instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionUpdateInstanceOperator`

    :param instance_name: The name of the instance to create.
    :type instance_name: str
    :param instance: An instance of Instance.
        https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
    :type instance: Dict[str, Any]
    :param update_mask: Field mask is used to specify the fields that the update will overwrite
        in an instance resource. The fields specified in the updateMask are relative to the resource,
        not the full request. A field will be overwritten if it is in the mask. If the user does not
        provide a mask, all the supported fields (labels and options currently) will be overwritten.
        A comma-separated list of fully qualified names of fields. Example: "user.displayName,photo".
        https://developers.google.com/protocol-buffers/docs/reference/google.protobuf?_ga=2.205612571.-968688242.1573564810#google.protobuf.FieldMask
    :type update_mask: str
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param project_id: The ID of the Google Cloud Platform project that the instance belongs to.
    :type project_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("instance_name", "instance")

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        instance: Dict[str, Any],
        update_mask: str,
        location: str,
        project_id: Optional[str] = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.update_mask = update_mask
        self.instance_name = instance_name
        self.instance = instance
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Updating Data Fusion instace: %s", self.instance_name)
        operation = hook.patch_instance(
            instance_name=self.instance_name,
            instance=self.instance,
            update_mask=self.update_mask,
            location=self.location,
            project_id=self.project_id,
        )
        hook.wait_for_operation(operation)
        self.log.info("Instance %s updated successfully", self.instance_name)


class CloudDataFusionGetInstanceOperator(BaseOperator):
    """
    Gets details of a single Data Fusion instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionGetInstanceOperator`

    :param instance_name: The name of the instance.
    :type instance_name: str
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param project_id: The ID of the Google Cloud Platform project that the instance belongs to.
    :type project_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("instance_name",)

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        location: str,
        project_id: Optional[str] = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Retrieving Data Fusion instance: %s", self.instance_name)
        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        return instance


class CloudDataFusionCreatePipelineOperator(BaseOperator):
    """
    Creates a Cloud Data Fusion pipeline.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionCreatePipelineOperator`

    :param pipeline_name: Your pipeline name.
    :type pipeline_name: str
    :param pipeline: The pipeline definition. For more information check:
        https://docs.cdap.io/cdap/current/en/developer-manual/pipelines/developing-pipelines.html#pipeline-configuration-file-format
    :type pipeline: Dict[str, Any]
    :param instance_name: The name of the instance.
    :type instance_name: str
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :type namespace: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("instance_name", "pipeline_name")

    @apply_defaults
    def __init__(
        self,
        pipeline_name: str,
        pipeline: Dict[str, Any],
        instance_name: str,
        location: str,
        namespace: str = "default",
        project_id: Optional[str] = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.pipeline_name = pipeline_name
        self.pipeline = pipeline
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
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
        self.log.info("Pipeline created")


class CloudDataFusionDeletePipelineOperator(BaseOperator):
    """
    Deletes a Cloud Data Fusion pipeline.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionDeletePipelineOperator`

    :param pipeline_name: Your pipeline name.
    :type pipeline_name: str
    :param version_id: Version of pipeline to delete
    :type version_id: Optional[str]
    :param instance_name: The name of the instance.
    :type instance_name: str
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :type namespace: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("instance_name", "version_id", "pipeline_name")

    @apply_defaults
    def __init__(
        self,
        pipeline_name: str,
        instance_name: str,
        location: str,
        version_id: Optional[str] = None,
        namespace: str = "default",
        project_id: Optional[str] = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.pipeline_name = pipeline_name
        self.version_id = version_id
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
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
    :type instance_name: str
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param artifact_version: Artifact version to filter instances
    :type artifact_version: Optional[str]
    :param artifact_name: Artifact name to filter instances
    :type artifact_name: Optional[str]
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :type namespace: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("instance_name", "artifact_name", "artifact_version")

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        location: str,
        artifact_name: Optional[str] = None,
        artifact_version: Optional[str] = None,
        namespace: str = "default",
        project_id: Optional[str] = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.artifact_version = artifact_version
        self.artifact_name = artifact_name
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
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
        return pipelines


class CloudDataFusionStartPipelineOperator(BaseOperator):
    """
    Starts a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionStartPipelineOperator`

    :param pipeline_name: Your pipeline name.
    :type pipeline_name: str
    :param instance_name: The name of the instance.
    :type instance_name: str
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param runtime_args: Optional runtime args to be passed to the pipeline
    :type runtime_args: dict
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :type namespace: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("instance_name", "pipeline_name", "runtime_args")

    @apply_defaults
    def __init__(
        self,
        pipeline_name: str,
        instance_name: str,
        location: str,
        runtime_args: Optional[Dict[str, Any]] = None,
        namespace: str = "default",
        project_id: Optional[str] = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.pipeline_name = pipeline_name
        self.runtime_args = runtime_args
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Starting Data Fusion pipeline: %s", self.pipeline_name)
        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        api_url = instance["apiEndpoint"]
        hook.start_pipeline(
            pipeline_name=self.pipeline_name,
            instance_url=api_url,
            namespace=self.namespace,
            runtime_args=self.runtime_args

        )
        self.log.info("Pipeline started")


class CloudDataFusionStopPipelineOperator(BaseOperator):
    """
    Stops a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataFusionStopPipelineOperator`

    :param pipeline_name: Your pipeline name.
    :type pipeline_name: str
    :param instance_name: The name of the instance.
    :type instance_name: str
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :type namespace: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("instance_name", "pipeline_name")

    @apply_defaults
    def __init__(
        self,
        pipeline_name: str,
        instance_name: str,
        location: str,
        namespace: str = "default",
        project_id: Optional[str] = None,
        api_version: str = "v1beta1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.pipeline_name = pipeline_name
        self.namespace = namespace
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Starting Data Fusion pipeline: %s", self.pipeline_name)
        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        api_url = instance["apiEndpoint"]
        hook.stop_pipeline(
            pipeline_name=self.pipeline_name,
            instance_url=api_url,
            namespace=self.namespace,
        )
        self.log.info("Pipeline started")
