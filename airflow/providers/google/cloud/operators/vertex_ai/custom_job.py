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
#
"""This module contains Google Vertex AI operators."""

from typing import Dict, List, Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.cloud.aiplatform.models import Model
from google.cloud.aiplatform_v1.types.dataset import Dataset

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.hooks.vertex_ai.custom_job import CustomJobHook

VERTEX_AI_BASE_LINK = "https://console.cloud.google.com/vertex-ai"
VERTEX_AI_MODEL_LINK = (
    VERTEX_AI_BASE_LINK + "/locations/{region}/models/{model_id}/deploy?project={project_id}"
)
VERTEX_AI_TRAINING_PIPELINES_LINK = VERTEX_AI_BASE_LINK + "/training/training-pipelines?project={project_id}"


class VertexAIModelLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI Model link"""

    name = "Vertex AI Model"

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        model_conf = ti.xcom_pull(task_ids=operator.task_id, key="model_conf")
        return (
            VERTEX_AI_MODEL_LINK.format(
                region=model_conf["region"],
                model_id=model_conf["model_id"],
                project_id=model_conf["project_id"],
            )
            if model_conf
            else ""
        )


class VertexAITrainingPipelinesLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI Training Pipelines link"""

    name = "Vertex AI Training Pipelines"

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        project_id = ti.xcom_pull(task_ids=operator.task_id, key="project_id")
        return (
            VERTEX_AI_TRAINING_PIPELINES_LINK.format(
                project_id=project_id,
            )
            if project_id
            else ""
        )


class _CustomTrainingJobBaseOperator(BaseOperator):
    """The base class for operators that launch Custom jobs on VertexAI."""

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        display_name: str,
        container_uri: str,
        model_serving_container_image_uri: Optional[str] = None,
        model_serving_container_predict_route: Optional[str] = None,
        model_serving_container_health_route: Optional[str] = None,
        model_serving_container_command: Optional[Sequence[str]] = None,
        model_serving_container_args: Optional[Sequence[str]] = None,
        model_serving_container_environment_variables: Optional[Dict[str, str]] = None,
        model_serving_container_ports: Optional[Sequence[int]] = None,
        model_description: Optional[str] = None,
        model_instance_schema_uri: Optional[str] = None,
        model_parameters_schema_uri: Optional[str] = None,
        model_prediction_schema_uri: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        training_encryption_spec_key_name: Optional[str] = None,
        model_encryption_spec_key_name: Optional[str] = None,
        staging_bucket: Optional[str] = None,
        # RUN
        dataset_id: Optional[str] = None,
        annotation_schema_uri: Optional[str] = None,
        model_display_name: Optional[str] = None,
        model_labels: Optional[Dict[str, str]] = None,
        base_output_dir: Optional[str] = None,
        service_account: Optional[str] = None,
        network: Optional[str] = None,
        bigquery_destination: Optional[str] = None,
        args: Optional[List[Union[str, float, int]]] = None,
        environment_variables: Optional[Dict[str, str]] = None,
        replica_count: int = 1,
        machine_type: str = "n1-standard-4",
        accelerator_type: str = "ACCELERATOR_TYPE_UNSPECIFIED",
        accelerator_count: int = 0,
        boot_disk_type: str = "pd-ssd",
        boot_disk_size_gb: int = 100,
        training_fraction_split: Optional[float] = None,
        validation_fraction_split: Optional[float] = None,
        test_fraction_split: Optional[float] = None,
        training_filter_split: Optional[str] = None,
        validation_filter_split: Optional[str] = None,
        test_filter_split: Optional[str] = None,
        predefined_split_column_name: Optional[str] = None,
        timestamp_split_column_name: Optional[str] = None,
        tensorboard: Optional[str] = None,
        sync=True,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.display_name = display_name
        # START Custom
        self.container_uri = container_uri
        self.model_serving_container_image_uri = model_serving_container_image_uri
        self.model_serving_container_predict_route = model_serving_container_predict_route
        self.model_serving_container_health_route = model_serving_container_health_route
        self.model_serving_container_command = model_serving_container_command
        self.model_serving_container_args = model_serving_container_args
        self.model_serving_container_environment_variables = model_serving_container_environment_variables
        self.model_serving_container_ports = model_serving_container_ports
        self.model_description = model_description
        self.model_instance_schema_uri = model_instance_schema_uri
        self.model_parameters_schema_uri = model_parameters_schema_uri
        self.model_prediction_schema_uri = model_prediction_schema_uri
        self.labels = labels
        self.training_encryption_spec_key_name = training_encryption_spec_key_name
        self.model_encryption_spec_key_name = model_encryption_spec_key_name
        self.staging_bucket = staging_bucket
        # END Custom
        # START Run param
        self.dataset = Dataset(name=dataset_id) if dataset_id else None
        self.annotation_schema_uri = annotation_schema_uri
        self.model_display_name = model_display_name
        self.model_labels = model_labels
        self.base_output_dir = base_output_dir
        self.service_account = service_account
        self.network = network
        self.bigquery_destination = bigquery_destination
        self.args = args
        self.environment_variables = environment_variables
        self.replica_count = replica_count
        self.machine_type = machine_type
        self.accelerator_type = accelerator_type
        self.accelerator_count = accelerator_count
        self.boot_disk_type = boot_disk_type
        self.boot_disk_size_gb = boot_disk_size_gb
        self.training_fraction_split = training_fraction_split
        self.validation_fraction_split = validation_fraction_split
        self.test_fraction_split = test_fraction_split
        self.training_filter_split = training_filter_split
        self.validation_filter_split = validation_filter_split
        self.test_filter_split = test_filter_split
        self.predefined_split_column_name = predefined_split_column_name
        self.timestamp_split_column_name = timestamp_split_column_name
        self.tensorboard = tensorboard
        self.sync = sync
        # END Run param
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.hook = CustomJobHook(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)

    def on_kill(self) -> None:
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        self.hook.cancel_training_pipeline(
            project_id=self.project_id,
            region=self.region,
            training_pipeline=self.display_name,
        )
        self.hook.cancel_custom_job(
            project_id=self.project_id, region=self.region, custom_job=f"{self.display_name}-custom-job"
        )


class CreateCustomContainerTrainingJobOperator(_CustomTrainingJobBaseOperator):
    """Create Custom Container Training job"""

    template_fields = [
        'region',
        'command',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(),)

    def __init__(
        self,
        *,
        command: Sequence[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command

    def execute(self, context):
        model = self.hook.create_custom_container_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            container_uri=self.container_uri,
            command=self.command,
            model_serving_container_image_uri=self.model_serving_container_image_uri,
            model_serving_container_predict_route=self.model_serving_container_predict_route,
            model_serving_container_health_route=self.model_serving_container_health_route,
            model_serving_container_command=self.model_serving_container_command,
            model_serving_container_args=self.model_serving_container_args,
            model_serving_container_environment_variables=self.model_serving_container_environment_variables,
            model_serving_container_ports=self.model_serving_container_ports,
            model_description=self.model_description,
            model_instance_schema_uri=self.model_instance_schema_uri,
            model_parameters_schema_uri=self.model_parameters_schema_uri,
            model_prediction_schema_uri=self.model_prediction_schema_uri,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            staging_bucket=self.staging_bucket,
            # RUN
            dataset=self.dataset,
            annotation_schema_uri=self.annotation_schema_uri,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            base_output_dir=self.base_output_dir,
            service_account=self.service_account,
            network=self.network,
            bigquery_destination=self.bigquery_destination,
            args=self.args,
            environment_variables=self.environment_variables,
            replica_count=self.replica_count,
            machine_type=self.machine_type,
            accelerator_type=self.accelerator_type,
            accelerator_count=self.accelerator_count,
            boot_disk_type=self.boot_disk_type,
            boot_disk_size_gb=self.boot_disk_size_gb,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            training_filter_split=self.training_filter_split,
            validation_filter_split=self.validation_filter_split,
            test_filter_split=self.test_filter_split,
            predefined_split_column_name=self.predefined_split_column_name,
            timestamp_split_column_name=self.timestamp_split_column_name,
            tensorboard=self.tensorboard,
            sync=True,
        )
        model_id = self.hook.extract_model_id(model)
        self.xcom_push(
            context,
            key="model_conf",
            value={
                "model_id": model_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return Model.to_dict(model)


class CreateCustomPythonPackageTrainingJobOperator(_CustomTrainingJobBaseOperator):
    """Create Custom Python Package Training job"""

    template_fields = [
        'region',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(),)

    def __init__(
        self,
        *,
        python_package_gcs_uri: str,
        python_module_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.python_package_gcs_uri = python_package_gcs_uri
        self.python_module_name = python_module_name

    def execute(self, context):
        model = self.hook.create_custom_python_package_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            python_package_gcs_uri=self.python_package_gcs_uri,
            python_module_name=self.python_module_name,
            container_uri=self.container_uri,
            model_serving_container_image_uri=self.model_serving_container_image_uri,
            model_serving_container_predict_route=self.model_serving_container_predict_route,
            model_serving_container_health_route=self.model_serving_container_health_route,
            model_serving_container_command=self.model_serving_container_command,
            model_serving_container_args=self.model_serving_container_args,
            model_serving_container_environment_variables=self.model_serving_container_environment_variables,
            model_serving_container_ports=self.model_serving_container_ports,
            model_description=self.model_description,
            model_instance_schema_uri=self.model_instance_schema_uri,
            model_parameters_schema_uri=self.model_parameters_schema_uri,
            model_prediction_schema_uri=self.model_prediction_schema_uri,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            staging_bucket=self.staging_bucket,
            # RUN
            dataset=self.dataset,
            annotation_schema_uri=self.annotation_schema_uri,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            base_output_dir=self.base_output_dir,
            service_account=self.service_account,
            network=self.network,
            bigquery_destination=self.bigquery_destination,
            args=self.args,
            environment_variables=self.environment_variables,
            replica_count=self.replica_count,
            machine_type=self.machine_type,
            accelerator_type=self.accelerator_type,
            accelerator_count=self.accelerator_count,
            boot_disk_type=self.boot_disk_type,
            boot_disk_size_gb=self.boot_disk_size_gb,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            training_filter_split=self.training_filter_split,
            validation_filter_split=self.validation_filter_split,
            test_filter_split=self.test_filter_split,
            predefined_split_column_name=self.predefined_split_column_name,
            timestamp_split_column_name=self.timestamp_split_column_name,
            tensorboard=self.tensorboard,
            sync=True,
        )

        model_id = self.hook.extract_model_id(model)
        self.xcom_push(
            context,
            key="model_conf",
            value={
                "model_id": model_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return Model.to_dict(model)


class CreateCustomTrainingJobOperator(_CustomTrainingJobBaseOperator):
    """Create Custom Training job"""

    template_fields = [
        'region',
        'script_path',
        'requirements',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(),)

    def __init__(
        self,
        *,
        script_path: str,
        requirements: Optional[Sequence[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.requirements = requirements
        self.script_path = script_path

    def execute(self, context):
        model = self.hook.create_custom_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            script_path=self.script_path,
            container_uri=self.container_uri,
            requirements=self.requirements,
            model_serving_container_image_uri=self.model_serving_container_image_uri,
            model_serving_container_predict_route=self.model_serving_container_predict_route,
            model_serving_container_health_route=self.model_serving_container_health_route,
            model_serving_container_command=self.model_serving_container_command,
            model_serving_container_args=self.model_serving_container_args,
            model_serving_container_environment_variables=self.model_serving_container_environment_variables,
            model_serving_container_ports=self.model_serving_container_ports,
            model_description=self.model_description,
            model_instance_schema_uri=self.model_instance_schema_uri,
            model_parameters_schema_uri=self.model_parameters_schema_uri,
            model_prediction_schema_uri=self.model_prediction_schema_uri,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            staging_bucket=self.staging_bucket,
            # RUN
            dataset=self.dataset,
            annotation_schema_uri=self.annotation_schema_uri,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            base_output_dir=self.base_output_dir,
            service_account=self.service_account,
            network=self.network,
            bigquery_destination=self.bigquery_destination,
            args=self.args,
            environment_variables=self.environment_variables,
            replica_count=self.replica_count,
            machine_type=self.machine_type,
            accelerator_type=self.accelerator_type,
            accelerator_count=self.accelerator_count,
            boot_disk_type=self.boot_disk_type,
            boot_disk_size_gb=self.boot_disk_size_gb,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            training_filter_split=self.training_filter_split,
            validation_filter_split=self.validation_filter_split,
            test_filter_split=self.test_filter_split,
            predefined_split_column_name=self.predefined_split_column_name,
            timestamp_split_column_name=self.timestamp_split_column_name,
            tensorboard=self.tensorboard,
            sync=True,
        )

        model_id = self.hook.extract_model_id(model)
        self.xcom_push(
            context,
            key="model_conf",
            value={
                "model_id": model_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return Model.to_dict(model)


class DeleteCustomTrainingJobOperator(BaseOperator):
    """Deletes a CustomTrainingJob, CustomPythonTrainingJob, or CustomContainerTrainingJob."""

    template_fields = ("region", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        training_pipeline: str,
        region: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.training_pipeline = training_pipeline
        self.region = region
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Dict):
        hook = CustomJobHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Deleting custom training job: %s", self.training_pipeline)
        hook.delete_training_pipeline(
            training_pipeline=self.training_pipeline,
            region=self.region,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.delete_custom_job(
            custom_job=f"{self.training_pipeline}-custom-job",
            region=self.region,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Custom training job deleted.")


class ListCustomTrainingJobOperator(BaseOperator):
    """Lists CustomTrainingJob, CustomPythonTrainingJob, or CustomContainerTrainingJob in a Location."""

    template_fields = [
        "region",
        "project_id",
        "impersonation_chain",
    ]
    operator_extra_links = [
        VertexAITrainingPipelinesLink(),
    ]

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        filter: Optional[str] = None,
        read_mask: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.page_size = page_size
        self.page_token = page_token
        self.filter = filter
        self.read_mask = read_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Dict):
        hook = CustomJobHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        hook.list_training_pipelines(
            region=self.region,
            project_id=self.project_id,
            page_size=self.page_size,
            page_token=self.page_token,
            filter=self.filter,
            read_mask=self.read_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.xcom_push(context, key="project_id", value=self.project_id)
