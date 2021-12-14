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

from google.api_core.exceptions import NotFound
from google.api_core.retry import Retry
from google.cloud.aiplatform import datasets
from google.cloud.aiplatform.models import Model
from google.cloud.aiplatform_v1.types.dataset import Dataset
from google.cloud.aiplatform_v1.types.training_pipeline import TrainingPipeline

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.hooks.vertex_ai.auto_ml import AutoMLHook

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


class _AutoMLTrainingJobBaseOperator(BaseOperator):
    """The base class for operators that launch AutoML jobs on VertexAI."""

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        display_name: str,
        labels: Optional[Dict[str, str]] = None,
        training_encryption_spec_key_name: Optional[str] = None,
        model_encryption_spec_key_name: Optional[str] = None,
        # RUN
        training_fraction_split: Optional[float] = None,
        test_fraction_split: Optional[float] = None,
        model_display_name: Optional[str] = None,
        model_labels: Optional[Dict[str, str]] = None,
        sync: bool = True,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.display_name = display_name
        self.labels = labels
        self.training_encryption_spec_key_name = training_encryption_spec_key_name
        self.model_encryption_spec_key_name = model_encryption_spec_key_name
        # START Run param
        self.training_fraction_split = training_fraction_split
        self.test_fraction_split = test_fraction_split
        self.model_display_name = model_display_name
        self.model_labels = model_labels
        self.sync = sync
        # END Run param
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.hook = AutoMLHook(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)

    def on_kill(self) -> None:
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        self.hook.cancel_auto_ml_job()


class CreateAutoMLForecastingTrainingJobOperator(_AutoMLTrainingJobBaseOperator):
    """Create AutoML Forecasting Training job"""

    template_fields = [
        'region',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        target_column: str,
        time_column: str,
        time_series_identifier_column: str,
        unavailable_at_forecast_columns: List[str],
        available_at_forecast_columns: List[str],
        forecast_horizon: int,
        data_granularity_unit: str,
        data_granularity_count: int,
        optimization_objective: Optional[str] = None,
        column_specs: Optional[Dict[str, str]] = None,
        column_transformations: Optional[List[Dict[str, Dict[str, str]]]] = None,
        validation_fraction_split: Optional[float] = None,
        predefined_split_column_name: Optional[str] = None,
        weight_column: Optional[str] = None,
        time_series_attribute_columns: Optional[List[str]] = None,
        context_window: Optional[int] = None,
        export_evaluated_data_items: bool = False,
        export_evaluated_data_items_bigquery_destination_uri: Optional[str] = None,
        export_evaluated_data_items_override_destination: bool = False,
        quantiles: Optional[List[float]] = None,
        validation_options: Optional[str] = None,
        budget_milli_node_hours: int = 1000,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset = datasets.TimeSeriesDataset(dataset_name=dataset_id) if dataset_id else None
        self.target_column = target_column
        self.time_column = time_column
        self.time_series_identifier_column = time_series_identifier_column
        self.unavailable_at_forecast_columns = unavailable_at_forecast_columns
        self.available_at_forecast_columns = available_at_forecast_columns
        self.forecast_horizon = forecast_horizon
        self.data_granularity_unit = data_granularity_unit
        self.data_granularity_count = data_granularity_count
        self.optimization_objective = optimization_objective
        self.column_specs = column_specs
        self.column_transformations = column_transformations
        self.validation_fraction_split = validation_fraction_split
        self.predefined_split_column_name = predefined_split_column_name
        self.weight_column = weight_column
        self.time_series_attribute_columns = time_series_attribute_columns
        self.context_window = context_window
        self.export_evaluated_data_items = export_evaluated_data_items
        self.export_evaluated_data_items_bigquery_destination_uri = (
            export_evaluated_data_items_bigquery_destination_uri
        )
        self.export_evaluated_data_items_override_destination = (
            export_evaluated_data_items_override_destination
        )
        self.quantiles = quantiles
        self.validation_options = validation_options
        self.budget_milli_node_hours = budget_milli_node_hours

    def execute(self, context):
        model = self.hook.create_auto_ml_forecasting_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            dataset=self.dataset,
            target_column=self.target_column,
            time_column=self.time_column,
            time_series_identifier_column=self.time_series_identifier_column,
            unavailable_at_forecast_columns=self.unavailable_at_forecast_columns,
            available_at_forecast_columns=self.available_at_forecast_columns,
            forecast_horizon=self.forecast_horizon,
            data_granularity_unit=self.data_granularity_unit,
            data_granularity_count=self.data_granularity_count,
            optimization_objective=self.optimization_objective,
            column_specs=self.column_specs,
            column_transformations=self.column_transformations,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            predefined_split_column_name=self.predefined_split_column_name,
            weight_column=self.weight_column,
            time_series_attribute_columns=self.time_series_attribute_columns,
            context_window=self.context_window,
            export_evaluated_data_items=self.export_evaluated_data_items,
            export_evaluated_data_items_bigquery_destination_uri=self.export_evaluated_data_items_bigquery_destination_uri,
            export_evaluated_data_items_override_destination=self.export_evaluated_data_items_override_destination,
            quantiles=self.quantiles,
            validation_options=self.validation_options,
            budget_milli_node_hours=self.budget_milli_node_hours,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            sync=self.sync,
        )

        result = Model.to_dict(model)
        model_id = self.hook.extract_model_id(result)
        self.xcom_push(
            context,
            key="model_conf",
            value={
                "model_id": model_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return result


class CreateAutoMLImageTrainingJobOperator(_AutoMLTrainingJobBaseOperator):
    """Create Auto ML Image Training job"""

    template_fields = [
        'region',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        prediction_type: str = "classification",
        multi_label: bool = False,
        model_type: str = "CLOUD",
        base_model: Optional[Model] = None,
        validation_fraction_split: Optional[float] = None,
        training_filter_split: Optional[str] = None,
        validation_filter_split: Optional[str] = None,
        test_filter_split: Optional[str] = None,
        budget_milli_node_hours: Optional[int] = None,
        disable_early_stopping: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset = datasets.ImageDataset(dataset_name=dataset_id) if dataset_id else None
        self.prediction_type = prediction_type
        self.multi_label = multi_label
        self.model_type = model_type
        self.base_model = base_model
        self.validation_fraction_split = validation_fraction_split
        self.training_filter_split = training_filter_split
        self.validation_filter_split = validation_filter_split
        self.test_filter_split = test_filter_split
        self.budget_milli_node_hours = budget_milli_node_hours
        self.disable_early_stopping = disable_early_stopping

    def execute(self, context):
        model = self.hook.create_auto_ml_image_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            dataset=self.dataset,
            prediction_type=self.prediction_type,
            multi_label=self.multi_label,
            model_type=self.model_type,
            base_model=self.base_model,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            training_filter_split=self.training_filter_split,
            validation_filter_split=self.validation_filter_split,
            test_filter_split=self.test_filter_split,
            budget_milli_node_hours=self.budget_milli_node_hours,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            disable_early_stopping=self.disable_early_stopping,
            sync=self.sync,
        )

        result = Model.to_dict(model)
        model_id = self.hook.extract_model_id(result)
        self.xcom_push(
            context,
            key="model_conf",
            value={
                "model_id": model_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return result


class CreateAutoMLTabularTrainingJobOperator(_AutoMLTrainingJobBaseOperator):
    """Create Auto ML Tabular Training job"""

    template_fields = [
        'region',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(),)

    def __init__(
        self,
        *,
        target_column: str,
        optimization_prediction_type: str,
        optimization_objective: Optional[str] = None,
        column_specs: Optional[Dict[str, str]] = None,
        column_transformations: Optional[List[Dict[str, Dict[str, str]]]] = None,
        optimization_objective_recall_value: Optional[float] = None,
        optimization_objective_precision_value: Optional[float] = None,
        validation_fraction_split: Optional[float] = None,
        predefined_split_column_name: Optional[str] = None,
        timestamp_split_column_name: Optional[str] = None,
        weight_column: Optional[str] = None,
        budget_milli_node_hours: int = 1000,
        disable_early_stopping: bool = False,
        export_evaluated_data_items: bool = False,
        export_evaluated_data_items_bigquery_destination_uri: Optional[str] = None,
        export_evaluated_data_items_override_destination: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.target_column = target_column
        self.optimization_prediction_type = optimization_prediction_type
        self.optimization_objective = optimization_objective
        self.column_specs = column_specs
        self.column_transformations = column_transformations
        self.optimization_objective_recall_value = optimization_objective_recall_value
        self.optimization_objective_precision_value = optimization_objective_precision_value
        self.validation_fraction_split = validation_fraction_split
        self.predefined_split_column_name = predefined_split_column_name
        self.timestamp_split_column_name = timestamp_split_column_name
        self.weight_column = weight_column
        self.budget_milli_node_hours = budget_milli_node_hours
        self.disable_early_stopping = disable_early_stopping
        self.export_evaluated_data_items = export_evaluated_data_items
        self.export_evaluated_data_items_bigquery_destination_uri = (
            export_evaluated_data_items_bigquery_destination_uri
        )
        self.export_evaluated_data_items_override_destination = (
            export_evaluated_data_items_override_destination
        )

    def execute(self, context):
        model = self.hook.create_auto_ml_tabular_training_job()

        result = Model.to_dict(model)
        model_id = self.hook.extract_model_id(result)
        self.xcom_push(
            context,
            key="model_conf",
            value={
                "model_id": model_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return result


class CreateAutoMLTextTrainingJobOperator(_AutoMLTrainingJobBaseOperator):
    """Create Auto ML Text Training job"""

    template_fields = [
        'region',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(),)

    def __init__(
        self,
        *,
        prediction_type: str,
        multi_label: bool = False,
        sentiment_max: int = 10,
        validation_fraction_split: Optional[float] = None,
        training_filter_split: Optional[str] = None,
        validation_filter_split: Optional[str] = None,
        test_filter_split: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.prediction_type = prediction_type
        self.multi_label = multi_label
        self.sentiment_max = sentiment_max
        self.validation_fraction_split = validation_fraction_split
        self.training_filter_split = training_filter_split
        self.validation_filter_split = validation_filter_split
        self.test_filter_split = test_filter_split

    def execute(self, context):
        model = self.hook.create_auto_ml_text_training_job()

        result = Model.to_dict(model)
        model_id = self.hook.extract_model_id(result)
        self.xcom_push(
            context,
            key="model_conf",
            value={
                "model_id": model_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return result


class CreateAutoMLVideoTrainingJobOperator(_AutoMLTrainingJobBaseOperator):
    """Create Auto ML Video Training job"""

    template_fields = [
        'region',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(),)

    def __init__(
        self,
        *,
        prediction_type: str = "classification",
        model_type: str = "CLOUD",
        training_filter_split: Optional[str] = None,
        test_filter_split: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.prediction_type = prediction_type
        self.model_type = model_type
        self.training_filter_split = training_filter_split
        self.test_filter_split = test_filter_split

    def execute(self, context):
        model = self.hook.create_auto_ml_video_training_job()

        result = Model.to_dict(model)
        model_id = self.hook.extract_model_id(result)
        self.xcom_push(
            context,
            key="model_conf",
            value={
                "model_id": model_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return result


class DeleteCustomTrainingJobOperator(BaseOperator):
    """Deletes a CustomTrainingJob, CustomPythonTrainingJob, or CustomContainerTrainingJob."""

    template_fields = ("region", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        training_pipeline_id: str,
        custom_job_id: str,
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
        self.training_pipeline = training_pipeline_id
        self.custom_job = custom_job_id
        self.region = region
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Dict):
        hook = CustomJobHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        try:
            self.log.info("Deleting custom training pipeline: %s", self.training_pipeline)
            training_pipeline_operation = hook.delete_training_pipeline(
                training_pipeline=self.training_pipeline,
                region=self.region,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=training_pipeline_operation)
            self.log.info("Training pipeline was deleted.")
        except NotFound:
            self.log.info("The Training Pipeline ID %s does not exist.", self.training_pipeline)
        try:
            self.log.info("Deleting custom job: %s", self.custom_job)
            custom_job_operation = hook.delete_custom_job(
                custom_job=self.custom_job,
                region=self.region,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=custom_job_operation)
            self.log.info("Custom job was deleted.")
        except NotFound:
            self.log.info("The Custom Job ID %s does not exist.", self.custom_job)


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
        results = hook.list_training_pipelines(
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
        return [TrainingPipeline.to_dict(result) for result in results]
