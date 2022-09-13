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
"""This module contains Google Vertex AI operators."""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.aiplatform import datasets
from google.cloud.aiplatform.models import Model
from google.cloud.aiplatform_v1.types.training_pipeline import TrainingPipeline

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.vertex_ai.auto_ml import AutoMLHook
from airflow.providers.google.cloud.links.vertex_ai import VertexAIModelLink, VertexAITrainingPipelinesLink

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AutoMLTrainingJobBaseOperator(BaseOperator):
    """The base class for operators that launch AutoML jobs on VertexAI."""

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        display_name: str,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        # RUN
        training_fraction_split: float | None = None,
        test_fraction_split: float | None = None,
        model_display_name: str | None = None,
        model_labels: dict[str, str] | None = None,
        sync: bool = True,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook = None  # type: Optional[AutoMLHook]

    def on_kill(self) -> None:
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        if self.hook:
            self.hook.cancel_auto_ml_job()


class CreateAutoMLForecastingTrainingJobOperator(AutoMLTrainingJobBaseOperator):
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
        unavailable_at_forecast_columns: list[str],
        available_at_forecast_columns: list[str],
        forecast_horizon: int,
        data_granularity_unit: str,
        data_granularity_count: int,
        optimization_objective: str | None = None,
        column_specs: dict[str, str] | None = None,
        column_transformations: list[dict[str, dict[str, str]]] | None = None,
        validation_fraction_split: float | None = None,
        predefined_split_column_name: str | None = None,
        weight_column: str | None = None,
        time_series_attribute_columns: list[str] | None = None,
        context_window: int | None = None,
        export_evaluated_data_items: bool = False,
        export_evaluated_data_items_bigquery_destination_uri: str | None = None,
        export_evaluated_data_items_override_destination: bool = False,
        quantiles: list[float] | None = None,
        validation_options: str | None = None,
        budget_milli_node_hours: int = 1000,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
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

    def execute(self, context: Context):
        self.hook = AutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        model = self.hook.create_auto_ml_forecasting_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            dataset=datasets.TimeSeriesDataset(dataset_name=self.dataset_id),
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
            export_evaluated_data_items_bigquery_destination_uri=(
                self.export_evaluated_data_items_bigquery_destination_uri
            ),
            export_evaluated_data_items_override_destination=(
                self.export_evaluated_data_items_override_destination
            ),
            quantiles=self.quantiles,
            validation_options=self.validation_options,
            budget_milli_node_hours=self.budget_milli_node_hours,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            sync=self.sync,
        )

        result = Model.to_dict(model)
        model_id = self.hook.extract_model_id(result)
        VertexAIModelLink.persist(context=context, task_instance=self, model_id=model_id)
        return result


class CreateAutoMLImageTrainingJobOperator(AutoMLTrainingJobBaseOperator):
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
        base_model: Model | None = None,
        validation_fraction_split: float | None = None,
        training_filter_split: str | None = None,
        validation_filter_split: str | None = None,
        test_filter_split: str | None = None,
        budget_milli_node_hours: int | None = None,
        disable_early_stopping: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
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

    def execute(self, context: Context):
        self.hook = AutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        model = self.hook.create_auto_ml_image_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            dataset=datasets.ImageDataset(dataset_name=self.dataset_id),
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
        VertexAIModelLink.persist(context=context, task_instance=self, model_id=model_id)
        return result


class CreateAutoMLTabularTrainingJobOperator(AutoMLTrainingJobBaseOperator):
    """Create Auto ML Tabular Training job"""

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
        optimization_prediction_type: str,
        optimization_objective: str | None = None,
        column_specs: dict[str, str] | None = None,
        column_transformations: list[dict[str, dict[str, str]]] | None = None,
        optimization_objective_recall_value: float | None = None,
        optimization_objective_precision_value: float | None = None,
        validation_fraction_split: float | None = None,
        predefined_split_column_name: str | None = None,
        timestamp_split_column_name: str | None = None,
        weight_column: str | None = None,
        budget_milli_node_hours: int = 1000,
        disable_early_stopping: bool = False,
        export_evaluated_data_items: bool = False,
        export_evaluated_data_items_bigquery_destination_uri: str | None = None,
        export_evaluated_data_items_override_destination: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
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

    def execute(self, context: Context):
        self.hook = AutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        model = self.hook.create_auto_ml_tabular_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            dataset=datasets.TabularDataset(dataset_name=self.dataset_id),
            target_column=self.target_column,
            optimization_prediction_type=self.optimization_prediction_type,
            optimization_objective=self.optimization_objective,
            column_specs=self.column_specs,
            column_transformations=self.column_transformations,
            optimization_objective_recall_value=self.optimization_objective_recall_value,
            optimization_objective_precision_value=self.optimization_objective_precision_value,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            predefined_split_column_name=self.predefined_split_column_name,
            timestamp_split_column_name=self.timestamp_split_column_name,
            weight_column=self.weight_column,
            budget_milli_node_hours=self.budget_milli_node_hours,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            disable_early_stopping=self.disable_early_stopping,
            export_evaluated_data_items=self.export_evaluated_data_items,
            export_evaluated_data_items_bigquery_destination_uri=(
                self.export_evaluated_data_items_bigquery_destination_uri
            ),
            export_evaluated_data_items_override_destination=(
                self.export_evaluated_data_items_override_destination
            ),
            sync=self.sync,
        )

        result = Model.to_dict(model)
        model_id = self.hook.extract_model_id(result)
        VertexAIModelLink.persist(context=context, task_instance=self, model_id=model_id)
        return result


class CreateAutoMLTextTrainingJobOperator(AutoMLTrainingJobBaseOperator):
    """Create Auto ML Text Training job"""

    template_fields = [
        'region',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        prediction_type: str,
        multi_label: bool = False,
        sentiment_max: int = 10,
        validation_fraction_split: float | None = None,
        training_filter_split: str | None = None,
        validation_filter_split: str | None = None,
        test_filter_split: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.prediction_type = prediction_type
        self.multi_label = multi_label
        self.sentiment_max = sentiment_max
        self.validation_fraction_split = validation_fraction_split
        self.training_filter_split = training_filter_split
        self.validation_filter_split = validation_filter_split
        self.test_filter_split = test_filter_split

    def execute(self, context: Context):
        self.hook = AutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        model = self.hook.create_auto_ml_text_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            dataset=datasets.TextDataset(dataset_name=self.dataset_id),
            prediction_type=self.prediction_type,
            multi_label=self.multi_label,
            sentiment_max=self.sentiment_max,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            training_filter_split=self.training_filter_split,
            validation_filter_split=self.validation_filter_split,
            test_filter_split=self.test_filter_split,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            sync=self.sync,
        )

        result = Model.to_dict(model)
        model_id = self.hook.extract_model_id(result)
        VertexAIModelLink.persist(context=context, task_instance=self, model_id=model_id)
        return result


class CreateAutoMLVideoTrainingJobOperator(AutoMLTrainingJobBaseOperator):
    """Create Auto ML Video Training job"""

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
        model_type: str = "CLOUD",
        training_filter_split: str | None = None,
        test_filter_split: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.prediction_type = prediction_type
        self.model_type = model_type
        self.training_filter_split = training_filter_split
        self.test_filter_split = test_filter_split

    def execute(self, context: Context):
        self.hook = AutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        model = self.hook.create_auto_ml_video_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            dataset=datasets.VideoDataset(dataset_name=self.dataset_id),
            prediction_type=self.prediction_type,
            model_type=self.model_type,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            training_fraction_split=self.training_fraction_split,
            test_fraction_split=self.test_fraction_split,
            training_filter_split=self.training_filter_split,
            test_filter_split=self.test_filter_split,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            sync=self.sync,
        )

        result = Model.to_dict(model)
        model_id = self.hook.extract_model_id(result)
        VertexAIModelLink.persist(context=context, task_instance=self, model_id=model_id)
        return result


class DeleteAutoMLTrainingJobOperator(BaseOperator):
    """Deletes an AutoMLForecastingTrainingJob, AutoMLImageTrainingJob, AutoMLTabularTrainingJob,
    AutoMLTextTrainingJob, or AutoMLVideoTrainingJob.
    """

    template_fields = ("region", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        training_pipeline_id: str,
        region: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.training_pipeline = training_pipeline_id
        self.region = region
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = AutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            self.log.info("Deleting Auto ML training pipeline: %s", self.training_pipeline)
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


class ListAutoMLTrainingJobOperator(BaseOperator):
    """Lists AutoMLForecastingTrainingJob, AutoMLImageTrainingJob, AutoMLTabularTrainingJob,
    AutoMLTextTrainingJob, or AutoMLVideoTrainingJob in a Location.
    """

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
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        read_mask: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = AutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
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
        VertexAITrainingPipelinesLink.persist(context=context, task_instance=self)
        return [TrainingPipeline.to_dict(result) for result in results]
