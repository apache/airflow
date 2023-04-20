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
"""
This module contains a Google Cloud Vertex AI hook.

.. spelling::

    aiplatform
    au
    codepoints
    milli
    mae
    quantile
    quantiles
    Quantiles
    rmse
    rmsle
    rmspe
    wape
    prc
    roc
    Jetson
    forecasted
    Struct
    sentimentMax
    TrainingPipeline
    targetColumn
    optimizationObjective
"""
from __future__ import annotations

import warnings
from typing import Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.aiplatform import (
    AutoMLForecastingTrainingJob,
    AutoMLImageTrainingJob,
    AutoMLTabularTrainingJob,
    AutoMLTextTrainingJob,
    AutoMLVideoTrainingJob,
    datasets,
    models,
)
from google.cloud.aiplatform_v1 import JobServiceClient, PipelineServiceClient
from google.cloud.aiplatform_v1.services.pipeline_service.pagers import ListTrainingPipelinesPager
from google.cloud.aiplatform_v1.types import TrainingPipeline

from airflow import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class AutoMLHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Auto ML APIs."""

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self._job: None | (
            AutoMLForecastingTrainingJob
            | AutoMLImageTrainingJob
            | AutoMLTabularTrainingJob
            | AutoMLTextTrainingJob
            | AutoMLVideoTrainingJob
        ) = None

    def get_pipeline_service_client(
        self,
        region: str | None = None,
    ) -> PipelineServiceClient:
        """Returns PipelineServiceClient."""
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()

        return PipelineServiceClient(
            credentials=self.get_credentials(), client_info=self.client_info, client_options=client_options
        )

    def get_job_service_client(
        self,
        region: str | None = None,
    ) -> JobServiceClient:
        """Returns JobServiceClient"""
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()

        return JobServiceClient(
            credentials=self.get_credentials(), client_info=self.client_info, client_options=client_options
        )

    def get_auto_ml_tabular_training_job(
        self,
        display_name: str,
        optimization_prediction_type: str,
        optimization_objective: str | None = None,
        column_specs: dict[str, str] | None = None,
        column_transformations: list[dict[str, dict[str, str]]] | None = None,
        optimization_objective_recall_value: float | None = None,
        optimization_objective_precision_value: float | None = None,
        project: str | None = None,
        location: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
    ) -> AutoMLTabularTrainingJob:
        """Returns AutoMLTabularTrainingJob object"""
        return AutoMLTabularTrainingJob(
            display_name=display_name,
            optimization_prediction_type=optimization_prediction_type,
            optimization_objective=optimization_objective,
            column_specs=column_specs,
            column_transformations=column_transformations,
            optimization_objective_recall_value=optimization_objective_recall_value,
            optimization_objective_precision_value=optimization_objective_precision_value,
            project=project,
            location=location,
            credentials=self.get_credentials(),
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
        )

    def get_auto_ml_forecasting_training_job(
        self,
        display_name: str,
        optimization_objective: str | None = None,
        column_specs: dict[str, str] | None = None,
        column_transformations: list[dict[str, dict[str, str]]] | None = None,
        project: str | None = None,
        location: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
    ) -> AutoMLForecastingTrainingJob:
        """Returns AutoMLForecastingTrainingJob object"""
        return AutoMLForecastingTrainingJob(
            display_name=display_name,
            optimization_objective=optimization_objective,
            column_specs=column_specs,
            column_transformations=column_transformations,
            project=project,
            location=location,
            credentials=self.get_credentials(),
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
        )

    def get_auto_ml_image_training_job(
        self,
        display_name: str,
        prediction_type: str = "classification",
        multi_label: bool = False,
        model_type: str = "CLOUD",
        base_model: models.Model | None = None,
        project: str | None = None,
        location: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
    ) -> AutoMLImageTrainingJob:
        """Returns AutoMLImageTrainingJob object"""
        return AutoMLImageTrainingJob(
            display_name=display_name,
            prediction_type=prediction_type,
            multi_label=multi_label,
            model_type=model_type,
            base_model=base_model,
            project=project,
            location=location,
            credentials=self.get_credentials(),
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
        )

    def get_auto_ml_text_training_job(
        self,
        display_name: str,
        prediction_type: str,
        multi_label: bool = False,
        sentiment_max: int = 10,
        project: str | None = None,
        location: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
    ) -> AutoMLTextTrainingJob:
        """Returns AutoMLTextTrainingJob object"""
        return AutoMLTextTrainingJob(
            display_name=display_name,
            prediction_type=prediction_type,
            multi_label=multi_label,
            sentiment_max=sentiment_max,
            project=project,
            location=location,
            credentials=self.get_credentials(),
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
        )

    def get_auto_ml_video_training_job(
        self,
        display_name: str,
        prediction_type: str = "classification",
        model_type: str = "CLOUD",
        project: str | None = None,
        location: str | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
    ) -> AutoMLVideoTrainingJob:
        """Returns AutoMLVideoTrainingJob object"""
        return AutoMLVideoTrainingJob(
            display_name=display_name,
            prediction_type=prediction_type,
            model_type=model_type,
            project=project,
            location=location,
            credentials=self.get_credentials(),
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
        )

    @staticmethod
    def extract_model_id(obj: dict) -> str:
        """Returns unique id of the Model."""
        return obj["name"].rpartition("/")[-1]

    @staticmethod
    def extract_training_id(resource_name: str) -> str:
        """Returns unique id of the Training pipeline."""
        return resource_name.rpartition("/")[-1]

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def cancel_auto_ml_job(self) -> None:
        """Cancel Auto ML Job for training pipeline"""
        if self._job:
            self._job.cancel()

    @GoogleBaseHook.fallback_to_default_project_id
    def create_auto_ml_tabular_training_job(
        self,
        project_id: str,
        region: str,
        display_name: str,
        dataset: datasets.TabularDataset,
        target_column: str,
        optimization_prediction_type: str,
        optimization_objective: str | None = None,
        column_specs: dict[str, str] | None = None,
        column_transformations: list[dict[str, dict[str, str]]] | None = None,
        optimization_objective_recall_value: float | None = None,
        optimization_objective_precision_value: float | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        training_fraction_split: float | None = None,
        validation_fraction_split: float | None = None,
        test_fraction_split: float | None = None,
        predefined_split_column_name: str | None = None,
        timestamp_split_column_name: str | None = None,
        weight_column: str | None = None,
        budget_milli_node_hours: int = 1000,
        model_display_name: str | None = None,
        model_labels: dict[str, str] | None = None,
        disable_early_stopping: bool = False,
        export_evaluated_data_items: bool = False,
        export_evaluated_data_items_bigquery_destination_uri: str | None = None,
        export_evaluated_data_items_override_destination: bool = False,
        sync: bool = True,
    ) -> tuple[models.Model | None, str]:
        """
        Create an AutoML Tabular Training Job.

        :param project_id: Required. Project to run training in.
        :param region: Required. Location to run training in.
        :param display_name: Required. The user-defined name of this TrainingPipeline.
        :param dataset: Required. The dataset within the same Project from which data will be used to train
            the Model. The Dataset must use schema compatible with Model being trained, and what is
            compatible should be described in the used TrainingPipeline's [training_task_definition]
            [google.cloud.aiplatform.v1beta1.TrainingPipeline.training_task_definition]. For tabular
            Datasets, all their data is exported to training, to pick and choose from.
        :param target_column: Required. The name of the column values of which the Model is to predict.
        :param optimization_prediction_type: The type of prediction the Model is to produce.
            "classification" - Predict one out of multiple target values is picked for each row.
            "regression" - Predict a value based on its relation to other values. This type is available only
            to columns that contain semantically numeric values, i.e. integers or floating point number, even
            if stored as e.g. strings.
        :param optimization_objective: Optional. Objective function the Model is to be optimized towards.
            The training task creates a Model that maximizes/minimizes the value of the objective function
            over the validation set.

            The supported optimization objectives depend on the prediction type, and in the case of
            classification also the number of distinct values in the target column (two distinct values
            -> binary, 3 or more distinct values -> multi class). If the field is not set, the default
            objective function is used.

            Classification (binary):
            "maximize-au-roc" (default) - Maximize the area under the receiver operating characteristic (ROC)
            curve.
            "minimize-log-loss" - Minimize log loss.
            "maximize-au-prc" - Maximize the area under the precision-recall curve.
            "maximize-precision-at-recall" - Maximize precision for a specified recall value.
            "maximize-recall-at-precision" - Maximize recall for a specified precision value.

            Classification (multi class):
            "minimize-log-loss" (default) - Minimize log loss.

            Regression:
            "minimize-rmse" (default) - Minimize root-mean-squared error (RMSE).
            "minimize-mae" - Minimize mean-absolute error (MAE).
            "minimize-rmsle" - Minimize root-mean-squared log error (RMSLE).
        :param column_specs: Optional. Alternative to column_transformations where the keys of the dict are
            column names and their respective values are one of AutoMLTabularTrainingJob.column_data_types.
            When creating transformation for BigQuery Struct column, the column should be flattened using "."
            as the delimiter. Only columns with no child should have a transformation. If an input column has
            no transformations on it, such a column is ignored by the training, except for the targetColumn,
            which should have no transformations defined on. Only one of column_transformations or
            column_specs should be passed.
        :param column_transformations: Optional. Transformations to apply to the input columns (i.e. columns
            other than the targetColumn). Each transformation may produce multiple result values from the
            column's value, and all are used for training. When creating transformation for BigQuery Struct
            column, the column should be flattened using "." as the delimiter. Only columns with no child
            should have a transformation. If an input column has no transformations on it, such a column is
            ignored by the training, except for the targetColumn, which should have no transformations
            defined on. Only one of column_transformations or column_specs should be passed. Consider using
            column_specs as column_transformations will be deprecated eventually.
        :param optimization_objective_recall_value: Optional. Required when maximize-precision-at-recall
            optimizationObjective was picked, represents the recall value at which the optimization is done.
            The minimum value is 0 and the maximum is 1.0.
        :param optimization_objective_precision_value: Optional. Required when maximize-recall-at-precision
            optimizationObjective was picked, represents the precision value at which the optimization is
            done.
            The minimum value is 0 and the maximum is 1.0.
        :param labels: Optional. The labels with user-defined metadata to organize TrainingPipelines. Label
            keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the training pipeline. Has the form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``. The key needs to be
            in the same region as where the compute resource is created. If set, this TrainingPipeline will
            be secured by this key.
            Note: Model trained by this TrainingPipeline is also secured by this key if ``model_to_upload``
            is not set separately.
        :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the model. Has the form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``. The key needs to be
            in the same region as where the compute resource is created. If set, the trained Model will be
            secured by this key.
        :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
            the Model. This is ignored if Dataset is not provided.
        :param validation_fraction_split: Optional. The fraction of the input data that is to be used to
            validate the Model. This is ignored if Dataset is not provided.
        :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
            the Model. This is ignored if Dataset is not provided.
        :param predefined_split_column_name: Optional. The key is a name of one of the Dataset's data
            columns. The value of the key (either the label's value or value in the column) must be one of
            {``training``, ``validation``, ``test``}, and it defines to which set the given piece of data is
            assigned. If for a piece of data the key is not present or has an invalid value, that piece is
            ignored by the pipeline. Supported only for tabular and time series Datasets.
        :param timestamp_split_column_name: Optional. The key is a name of one of the Dataset's data columns.
            The value of the key values of the key (the values in the column) must be in RFC 3339 `date-time`
            format, where `time-offset` = `"Z"` (e.g. 1985-04-12T23:20:50.52Z). If for a piece of data the
            key is not present or has an invalid value, that piece is ignored by the pipeline. Supported only
            for tabular and time series Datasets. This parameter must be used with training_fraction_split,
            validation_fraction_split and test_fraction_split.
        :param weight_column: Optional. Name of the column that should be used as the weight column. Higher
            values in this column give more importance to the row during Model training. The column must have
            numeric values between 0 and 10000 inclusively, and 0 value means that the row is ignored. If the
            weight column field is not set, then all rows are assumed to have equal weight of 1.
        :param budget_milli_node_hours (int): Optional. The train budget of creating this Model, expressed in
            milli node hours i.e. 1,000 value in this field means 1 node hour. The training cost of the model
            will not exceed this budget. The final cost will be attempted to be close to the budget, though
            may end up being (even) noticeably smaller - at the backend's discretion. This especially may
            happen when further model training ceases to provide any improvements. If the budget is set to a
            value known to be insufficient to train a Model for the given training set, the training won't be
            attempted and will error. The minimum value is 1000 and the maximum is 72000.
        :param model_display_name: Optional. If the script produces a managed Vertex AI Model. The display
            name of the Model. The name can be up to 128 characters long and can be consist of any UTF-8
            characters. If not provided upon creation, the job's display_name is used.
        :param model_labels: Optional. The labels with user-defined metadata to organize your Models. Label
            keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param disable_early_stopping: Required. If true, the entire budget is used. This disables the early
            stopping feature. By default, the early stopping feature is enabled, which means that training
            might stop before the entire training budget has been used, if further training does no longer
            brings significant improvement to the model.
        :param export_evaluated_data_items: Whether to export the test set predictions to a BigQuery table.
            If False, then the export is not performed.
        :param export_evaluated_data_items_bigquery_destination_uri: Optional. URI of desired destination
            BigQuery table for exported test set predictions.

            Expected format: ``bq://<project_id>:<dataset_id>:<table>``

            If not specified, then results are exported to the following auto-created BigQuery table:
            ``<project_id>:export_evaluated_examples_<model_name>_<yyyy_MM_dd'T'HH_mm_ss_SSS'Z'>
            .evaluated_examples``

            Applies only if [export_evaluated_data_items] is True.
        :param export_evaluated_data_items_override_destination: Whether to override the contents of
            [export_evaluated_data_items_bigquery_destination_uri], if the table exists, for exported test
            set predictions. If False, and the table exists, then the training job will fail. Applies only if
            [export_evaluated_data_items] is True and [export_evaluated_data_items_bigquery_destination_uri]
            is specified.
        :param sync: Whether to execute this method synchronously. If False, this method will be executed in
            concurrent Future and any downstream object will be immediately returned and synced when the
            Future has completed.
        """
        if column_transformations:
            warnings.warn(
                "Consider using column_specs as column_transformations will be deprecated eventually.",
                DeprecationWarning,
                stacklevel=2,
            )

        self._job = self.get_auto_ml_tabular_training_job(
            project=project_id,
            location=region,
            display_name=display_name,
            optimization_prediction_type=optimization_prediction_type,
            optimization_objective=optimization_objective,
            column_specs=column_specs,
            column_transformations=column_transformations,
            optimization_objective_recall_value=optimization_objective_recall_value,
            optimization_objective_precision_value=optimization_objective_precision_value,
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
        )

        if not self._job:
            raise AirflowException("AutoMLTabularTrainingJob was not created")

        model = self._job.run(
            dataset=dataset,
            target_column=target_column,
            training_fraction_split=training_fraction_split,
            validation_fraction_split=validation_fraction_split,
            test_fraction_split=test_fraction_split,
            predefined_split_column_name=predefined_split_column_name,
            timestamp_split_column_name=timestamp_split_column_name,
            weight_column=weight_column,
            budget_milli_node_hours=budget_milli_node_hours,
            model_display_name=model_display_name,
            model_labels=model_labels,
            disable_early_stopping=disable_early_stopping,
            export_evaluated_data_items=export_evaluated_data_items,
            export_evaluated_data_items_bigquery_destination_uri=(
                export_evaluated_data_items_bigquery_destination_uri
            ),
            export_evaluated_data_items_override_destination=export_evaluated_data_items_override_destination,
            sync=sync,
        )
        training_id = self.extract_training_id(self._job.resource_name)
        if model:
            model.wait()
        else:
            self.log.warning(
                "Training did not produce a Managed Model returning None. Training Pipeline is not "
                "configured to upload a Model."
            )
        return model, training_id

    @GoogleBaseHook.fallback_to_default_project_id
    def create_auto_ml_forecasting_training_job(
        self,
        project_id: str,
        region: str,
        display_name: str,
        dataset: datasets.TimeSeriesDataset,
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
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        training_fraction_split: float | None = None,
        validation_fraction_split: float | None = None,
        test_fraction_split: float | None = None,
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
        model_display_name: str | None = None,
        model_labels: dict[str, str] | None = None,
        sync: bool = True,
    ) -> tuple[models.Model | None, str]:
        """
        Create an AutoML Forecasting Training Job.

        :param project_id: Required. Project to run training in.
        :param region: Required. Location to run training in.
        :param display_name: Required. The user-defined name of this TrainingPipeline.
        :param dataset: Required. The dataset within the same Project from which data will be used to train
            the Model. The Dataset must use schema compatible with Model being trained, and what is
            compatible should be described in the used TrainingPipeline's [training_task_definition]
            [google.cloud.aiplatform.v1beta1.TrainingPipeline.training_task_definition]. For time series
            Datasets, all their data is exported to training, to pick and choose from.
        :param target_column: Required. Name of the column that the Model is to predict values for.
        :param time_column: Required. Name of the column that identifies time order in the time series.
        :param time_series_identifier_column: Required. Name of the column that identifies the time series.
        :param unavailable_at_forecast_columns: Required. Column names of columns that are unavailable at
            forecast. Each column contains information for the given entity (identified by the
            [time_series_identifier_column]) that is unknown before the forecast (e.g. population of a city
            in a given year, or weather on a given day).
        :param available_at_forecast_columns: Required. Column names of columns that are available at
            forecast. Each column contains information for the given entity (identified by the
            [time_series_identifier_column]) that is known at forecast.
        :param forecast_horizon: Required. The amount of time into the future for which forecasted values for
            the target are returned. Expressed in number of units defined by the [data_granularity_unit] and
            [data_granularity_count] field. Inclusive.
        :param data_granularity_unit: Required. The data granularity unit. Accepted values are ``minute``,
            ``hour``, ``day``, ``week``, ``month``, ``year``.
        :param data_granularity_count: Required. The number of data granularity units between data points in
            the training data. If [data_granularity_unit] is `minute`, can be 1, 5, 10, 15, or 30. For all
            other values of [data_granularity_unit], must be 1.
        :param optimization_objective: Optional. Objective function the model is to be optimized towards. The
            training process creates a Model that optimizes the value of the objective function over the
            validation set. The supported optimization objectives:
            "minimize-rmse" (default) - Minimize root-mean-squared error (RMSE).
            "minimize-mae" - Minimize mean-absolute error (MAE).
            "minimize-rmsle" - Minimize root-mean-squared log error (RMSLE).
            "minimize-rmspe" - Minimize root-mean-squared percentage error (RMSPE).
            "minimize-wape-mae" - Minimize the combination of weighted absolute percentage error (WAPE) and
            mean-absolute-error (MAE).
            "minimize-quantile-loss" - Minimize the quantile loss at the defined quantiles. (Set this
            objective to build quantile forecasts.)
        :param column_specs: Optional. Alternative to column_transformations where the keys of the dict are
            column names and their respective values are one of AutoMLTabularTrainingJob.column_data_types.
            When creating transformation for BigQuery Struct column, the column should be flattened using "."
            as the delimiter. Only columns with no child should have a transformation. If an input column has
            no transformations on it, such a column is ignored by the training, except for the targetColumn,
            which should have no transformations defined on. Only one of column_transformations or
            column_specs should be passed.
        :param column_transformations: Optional. Transformations to apply to the input columns (i.e. columns
            other than the targetColumn). Each transformation may produce multiple result values from the
            column's value, and all are used for training. When creating transformation for BigQuery Struct
            column, the column should be flattened using "." as the delimiter. Only columns with no child
            should have a transformation. If an input column has no transformations on it, such a column is
            ignored by the training, except for the targetColumn, which should have no transformations
            defined on. Only one of column_transformations or column_specs should be passed. Consider using
            column_specs as column_transformations will be deprecated eventually.
        :param labels: Optional. The labels with user-defined metadata to organize TrainingPipelines. Label
            keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the training pipeline. Has the form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``. The key needs to be
            in the same region as where the compute resource is created. If set, this TrainingPipeline will
            be secured by this key.
            Note: Model trained by this TrainingPipeline is also secured by this key if ``model_to_upload``
            is not set separately.
        :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the model. Has the form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``. The key needs to be
            in the same region as where the compute resource is created.
            If set, the trained Model will be secured by this key.
        :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
            the Model. This is ignored if Dataset is not provided.
        :param validation_fraction_split: Optional. The fraction of the input data that is to be used to
            validate the Model. This is ignored if Dataset is not provided.
        :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
            the Model. This is ignored if Dataset is not provided.
        :param predefined_split_column_name: Optional. The key is a name of one of the Dataset's data
            columns. The value of the key (either the label's value or value in the column) must be one of
            {``TRAIN``, ``VALIDATE``, ``TEST``}, and it defines to which set the given piece of data is
            assigned. If for a piece of data the key is not present or has an invalid value, that piece is
            ignored by the pipeline.
            Supported only for tabular and time series Datasets.
        :param weight_column: Optional. Name of the column that should be used as the weight column. Higher
            values in this column give more importance to the row during Model training. The column must have
            numeric values between 0 and 10000 inclusively, and 0 value means that the row is ignored. If the
            weight column field is not set, then all rows are assumed to have equal weight of 1.
        :param time_series_attribute_columns: Optional. Column names that should be used as attribute
            columns. Each column is constant within a time series.
        :param context_window: Optional. The amount of time into the past training and prediction data is
            used for model training and prediction respectively. Expressed in number of units defined by the
            [data_granularity_unit] and [data_granularity_count] fields. When not provided uses the default
            value of 0 which means the model sets each series context window to be 0 (also known as "cold
            start"). Inclusive.
        :param export_evaluated_data_items: Whether to export the test set predictions to a BigQuery table.
            If False, then the export is not performed.
        :param export_evaluated_data_items_bigquery_destination_uri: Optional. URI of desired destination
            BigQuery table for exported test set predictions. Expected format:
            ``bq://<project_id>:<dataset_id>:<table>``
            If not specified, then results are exported to the following auto-created BigQuery table:
            ``<project_id>:export_evaluated_examples_<model_name>_<yyyy_MM_dd'T'HH_mm_ss_SSS'Z'>
            .evaluated_examples``
            Applies only if [export_evaluated_data_items] is True.
        :param export_evaluated_data_items_override_destination: Whether to override the contents of
            [export_evaluated_data_items_bigquery_destination_uri], if the table exists, for exported test
            set predictions. If False, and the table exists, then the training job will fail.
            Applies only if [export_evaluated_data_items] is True and
            [export_evaluated_data_items_bigquery_destination_uri] is specified.
        :param quantiles: Quantiles to use for the `minizmize-quantile-loss`
            [AutoMLForecastingTrainingJob.optimization_objective]. This argument is required in this case.
            Accepts up to 5 quantiles in the form of a double from 0 to 1, exclusive. Each quantile must be
            unique.
        :param validation_options: Validation options for the data validation component. The available
            options are: "fail-pipeline" - (default), will validate against the validation and fail the
            pipeline if it fails. "ignore-validation" - ignore the results of the validation and continue the
            pipeline
        :param budget_milli_node_hours: Optional. The train budget of creating this Model, expressed in milli
            node hours i.e. 1,000 value in this field means 1 node hour. The training cost of the model will
            not exceed this budget. The final cost will be attempted to be close to the budget, though may
            end up being (even) noticeably smaller - at the backend's discretion. This especially may happen
            when further model training ceases to provide any improvements. If the budget is set to a value
            known to be insufficient to train a Model for the given training set, the training won't be
            attempted and will error. The minimum value is 1000 and the maximum is 72000.
        :param model_display_name: Optional. If the script produces a managed Vertex AI Model. The display
            name of the Model. The name can be up to 128 characters long and can be consist of any UTF-8
            characters. If not provided upon creation, the job's display_name is used.
        :param model_labels: Optional. The labels with user-defined metadata to organize your Models. Label
            keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param sync: Whether to execute this method synchronously. If False, this method will be executed in
            concurrent Future and any downstream object will be immediately returned and synced when the
            Future has completed.
        """
        if column_transformations:
            warnings.warn(
                "Consider using column_specs as column_transformations will be deprecated eventually.",
                DeprecationWarning,
                stacklevel=2,
            )

        self._job = self.get_auto_ml_forecasting_training_job(
            project=project_id,
            location=region,
            display_name=display_name,
            optimization_objective=optimization_objective,
            column_specs=column_specs,
            column_transformations=column_transformations,
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
        )

        if not self._job:
            raise AirflowException("AutoMLForecastingTrainingJob was not created")

        model = self._job.run(
            dataset=dataset,
            target_column=target_column,
            time_column=time_column,
            time_series_identifier_column=time_series_identifier_column,
            unavailable_at_forecast_columns=unavailable_at_forecast_columns,
            available_at_forecast_columns=available_at_forecast_columns,
            forecast_horizon=forecast_horizon,
            data_granularity_unit=data_granularity_unit,
            data_granularity_count=data_granularity_count,
            training_fraction_split=training_fraction_split,
            validation_fraction_split=validation_fraction_split,
            test_fraction_split=test_fraction_split,
            predefined_split_column_name=predefined_split_column_name,
            weight_column=weight_column,
            time_series_attribute_columns=time_series_attribute_columns,
            context_window=context_window,
            export_evaluated_data_items=export_evaluated_data_items,
            export_evaluated_data_items_bigquery_destination_uri=(
                export_evaluated_data_items_bigquery_destination_uri
            ),
            export_evaluated_data_items_override_destination=export_evaluated_data_items_override_destination,
            quantiles=quantiles,
            validation_options=validation_options,
            budget_milli_node_hours=budget_milli_node_hours,
            model_display_name=model_display_name,
            model_labels=model_labels,
            sync=sync,
        )
        training_id = self.extract_training_id(self._job.resource_name)
        if model:
            model.wait()
        else:
            self.log.warning(
                "Training did not produce a Managed Model returning None. Training Pipeline is not "
                "configured to upload a Model."
            )
        return model, training_id

    @GoogleBaseHook.fallback_to_default_project_id
    def create_auto_ml_image_training_job(
        self,
        project_id: str,
        region: str,
        display_name: str,
        dataset: datasets.ImageDataset,
        prediction_type: str = "classification",
        multi_label: bool = False,
        model_type: str = "CLOUD",
        base_model: models.Model | None = None,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        training_fraction_split: float | None = None,
        validation_fraction_split: float | None = None,
        test_fraction_split: float | None = None,
        training_filter_split: str | None = None,
        validation_filter_split: str | None = None,
        test_filter_split: str | None = None,
        budget_milli_node_hours: int | None = None,
        model_display_name: str | None = None,
        model_labels: dict[str, str] | None = None,
        disable_early_stopping: bool = False,
        sync: bool = True,
    ) -> tuple[models.Model | None, str]:
        """
        Create an AutoML Image Training Job.

        :param project_id: Required. Project to run training in.
        :param region: Required. Location to run training in.
        :param display_name: Required. The user-defined name of this TrainingPipeline.
        :param dataset: Required. The dataset within the same Project from which data will be used to train
            the Model. The Dataset must use schema compatible with Model being trained, and what is
            compatible should be described in the used TrainingPipeline's [training_task_definition]
            [google.cloud.aiplatform.v1beta1.TrainingPipeline.training_task_definition]. For tabular
            Datasets, all their data is exported to training, to pick and choose from.
        :param prediction_type: The type of prediction the Model is to produce, one of:
            "classification" - Predict one out of multiple target values is picked for each row.
            "object_detection" - Predict a value based on its relation to other values. This type is
            available only to columns that contain semantically numeric values, i.e. integers or floating
            point number, even if stored as e.g. strings.
        :param multi_label: Required. Default is False. If false, a single-label (multi-class) Model will be
            trained (i.e. assuming that for each image just up to one annotation may be applicable). If true,
            a multi-label Model will be trained (i.e. assuming that for each image multiple annotations may
            be applicable).
            This is only applicable for the "classification" prediction_type and will be ignored otherwise.
        :param model_type: Required. One of the following:
            "CLOUD" - Default for Image Classification. A Model best tailored to be used within Google Cloud,
            and which cannot be exported.
            "CLOUD_HIGH_ACCURACY_1" - Default for Image Object Detection. A model best tailored to be used
            within Google Cloud, and which cannot be exported. Expected to have a higher latency, but should
            also have a higher prediction quality than other cloud models.
            "CLOUD_LOW_LATENCY_1" - A model best tailored to be used within Google Cloud, and which cannot be
            exported. Expected to have a low latency, but may have lower prediction quality than other cloud
            models.
            "MOBILE_TF_LOW_LATENCY_1" - A model that, in addition to being available within Google Cloud, can
            also be exported as TensorFlow or Core ML model and used on a mobile or edge device afterwards.
            Expected to have low latency, but may have lower prediction quality than other mobile models.
            "MOBILE_TF_VERSATILE_1" - A model that, in addition to being available within Google Cloud, can
            also be exported as TensorFlow or Core ML model and used on a mobile or edge device with
            afterwards.
            "MOBILE_TF_HIGH_ACCURACY_1" - A model that, in addition to being available within Google Cloud,
            can also be exported as TensorFlow or Core ML model and used on a mobile or edge device
            afterwards. Expected to have a higher latency, but should also have a higher prediction quality
            than other mobile models.
        :param base_model: Optional. Only permitted for Image Classification models. If it is specified, the
            new model will be trained based on the `base` model. Otherwise, the new model will be trained
            from scratch. The `base` model must be in the same Project and Location as the new Model to
            train, and have the same model_type.
        :param labels: Optional. The labels with user-defined metadata to organize TrainingPipelines. Label
            keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the training pipeline. Has the form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``. The key needs to be
            in the same region as where the compute resource is created. If set, this TrainingPipeline will
            be secured by this key.
            Note: Model trained by this TrainingPipeline is also secured by this key if ``model_to_upload``
            is not set separately.
        :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the model. Has the form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute resource is created.
            If set, the trained Model will be secured by this key.
        :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
            the Model. This is ignored if Dataset is not provided.
        :param validation_fraction_split: Optional. The fraction of the input data that is to be used to
            validate the Model. This is ignored if Dataset is not provided.
        :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
            the Model. This is ignored if Dataset is not provided.
        :param training_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to train the Model. A filter with same syntax as the one used in
            DatasetService.ListDataItems may be used. If a single DataItem is matched by more than one of the
            FilterSplit filters, then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
        :param validation_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to validate the Model. A filter with same syntax as the one used in
            DatasetService.ListDataItems may be used. If a single DataItem is matched by more than one of the
            FilterSplit filters, then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
        :param test_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match this
            filter are used to test the Model. A filter with same syntax as the one used in
            DatasetService.ListDataItems may be used. If a single DataItem is matched by more than one of the
            FilterSplit filters, then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
        :param budget_milli_node_hours: Optional. The train budget of creating this Model, expressed in milli
            node hours i.e. 1,000 value in this field means 1 node hour.
            Defaults by `prediction_type`:
            `classification` - For Cloud models the budget must be: 8,000 - 800,000 milli node hours
            (inclusive). The default value is 192,000 which represents one day in wall time, assuming 8 nodes
            are used.
            `object_detection` - For Cloud models the budget must be: 20,000 - 900,000 milli node hours
            (inclusive). The default value is 216,000 which represents one day in wall time, assuming 9 nodes
            are used.
            The training cost of the model will not exceed this budget. The final cost will be attempted to
            be close to the budget, though may end up being (even) noticeably smaller - at the backend's
            discretion. This especially may happen when further model training ceases to provide any
            improvements. If the budget is set to a value known to be insufficient to train a Model for the
            given training set, the training won't be attempted and will error.
        :param model_display_name: Optional. The display name of the managed Vertex AI Model. The name can be
            up to 128 characters long and can be consist of any UTF-8 characters. If not provided upon
            creation, the job's display_name is used.
        :param model_labels: Optional. The labels with user-defined metadata to organize your Models. Label
            keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param disable_early_stopping: Required. If true, the entire budget is used. This disables the early
            stopping feature. By default, the early stopping feature is enabled, which means that training
            might stop before the entire training budget has been used, if further training does no longer
            brings significant improvement to the model.
        :param sync: Whether to execute this method synchronously. If False, this method will be executed in
            concurrent Future and any downstream object will be immediately returned and synced when the
            Future has completed.
        """
        self._job = self.get_auto_ml_image_training_job(
            project=project_id,
            location=region,
            display_name=display_name,
            prediction_type=prediction_type,
            multi_label=multi_label,
            model_type=model_type,
            base_model=base_model,
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
        )

        if not self._job:
            raise AirflowException("AutoMLImageTrainingJob was not created")

        model = self._job.run(
            dataset=dataset,
            training_fraction_split=training_fraction_split,
            validation_fraction_split=validation_fraction_split,
            test_fraction_split=test_fraction_split,
            training_filter_split=training_filter_split,
            validation_filter_split=validation_filter_split,
            test_filter_split=test_filter_split,
            budget_milli_node_hours=budget_milli_node_hours,
            model_display_name=model_display_name,
            model_labels=model_labels,
            disable_early_stopping=disable_early_stopping,
            sync=sync,
        )
        training_id = self.extract_training_id(self._job.resource_name)
        if model:
            model.wait()
        else:
            self.log.warning(
                "Training did not produce a Managed Model returning None. AutoML Image Training "
                "Pipeline is not configured to upload a Model."
            )
        return model, training_id

    @GoogleBaseHook.fallback_to_default_project_id
    def create_auto_ml_text_training_job(
        self,
        project_id: str,
        region: str,
        display_name: str,
        dataset: datasets.TextDataset,
        prediction_type: str,
        multi_label: bool = False,
        sentiment_max: int = 10,
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        training_fraction_split: float | None = None,
        validation_fraction_split: float | None = None,
        test_fraction_split: float | None = None,
        training_filter_split: str | None = None,
        validation_filter_split: str | None = None,
        test_filter_split: str | None = None,
        model_display_name: str | None = None,
        model_labels: dict[str, str] | None = None,
        sync: bool = True,
    ) -> tuple[models.Model | None, str]:
        """
        Create an AutoML Text Training Job.

        :param project_id: Required. Project to run training in.
        :param region: Required. Location to run training in.
        :param display_name: Required. The user-defined name of this TrainingPipeline.
        :param dataset: Required. The dataset within the same Project from which data will be used to train
            the Model. The Dataset must use schema compatible with Model being trained, and what is
            compatible should be described in the used TrainingPipeline's [training_task_definition]
            [google.cloud.aiplatform.v1beta1.TrainingPipeline.training_task_definition].
        :param prediction_type: The type of prediction the Model is to produce, one of:
            "classification" - A classification model analyzes text data and returns a list of categories
            that apply to the text found in the data. Vertex AI offers both single-label and multi-label text
            classification models.
            "extraction" - An entity extraction model inspects text data for known entities referenced in the
            data and labels those entities in the text.
            "sentiment" - A sentiment analysis model inspects text data and identifies the prevailing
            emotional opinion within it, especially to determine a writer's attitude as positive, negative,
            or neutral.
        :param multi_label: Required and only applicable for text classification task. If false, a
            single-label (multi-class) Model will be trained (i.e. assuming that for each text snippet just
            up to one annotation may be applicable). If true, a multi-label Model will be trained (i.e.
            assuming that for each text snippet multiple annotations may be applicable).
        :param sentiment_max: Required and only applicable for sentiment task. A sentiment is expressed as an
            integer ordinal, where higher value means a more positive sentiment. The range of sentiments that
            will be used is between 0 and sentimentMax (inclusive on both ends), and all the values in the
            range must be represented in the dataset before a model can be created. Only the Annotations with
            this sentimentMax will be used for training. sentimentMax value must be between 1 and 10
            (inclusive).
        :param labels: Optional. The labels with user-defined metadata to organize TrainingPipelines. Label
            keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the training pipeline. Has the form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute resource is created.
            If set, this TrainingPipeline will be secured by this key.
            Note: Model trained by this TrainingPipeline is also secured by this key if ``model_to_upload``
            is not set separately.
        :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the model. Has the form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute resource is created.
            If set, the trained Model will be secured by this key.
        :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
            the Model. This is ignored if Dataset is not provided.
        :param validation_fraction_split: Optional. The fraction of the input data that is to be used to
            validate the Model. This is ignored if Dataset is not provided.
        :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
            the Model. This is ignored if Dataset is not provided.
        :param training_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to train the Model. A filter with same syntax as the one used in
            DatasetService.ListDataItems may be used. If a single DataItem is matched by more than one of the
            FilterSplit filters, then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
        :param validation_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to validate the Model. A filter with same syntax as the one used in
            DatasetService.ListDataItems may be used. If a single DataItem is matched by more than one of the
            FilterSplit filters, then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
        :param test_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match this
            filter are used to test the Model. A filter with same syntax as the one used in
            DatasetService.ListDataItems may be used. If a single DataItem is matched by more than one of the
            FilterSplit filters, then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
        :param model_display_name: Optional. The display name of the managed Vertex AI Model. The name can be
            up to 128 characters long and can consist of any UTF-8 characters.
            If not provided upon creation, the job's display_name is used.
        :param model_labels: Optional. The labels with user-defined metadata to organize your Models. Label
            keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param sync: Whether to execute this method synchronously. If False, this method will be executed in
            concurrent Future and any downstream object will be immediately returned and synced when the
            Future has completed.
        """
        self._job = self.get_auto_ml_text_training_job(
            project=project_id,
            location=region,
            display_name=display_name,
            prediction_type=prediction_type,
            multi_label=multi_label,
            sentiment_max=sentiment_max,
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
        )

        if not self._job:
            raise AirflowException("AutoMLTextTrainingJob was not created")

        model = self._job.run(
            dataset=dataset,
            training_fraction_split=training_fraction_split,
            validation_fraction_split=validation_fraction_split,
            test_fraction_split=test_fraction_split,
            training_filter_split=training_filter_split,
            validation_filter_split=validation_filter_split,
            test_filter_split=test_filter_split,
            model_display_name=model_display_name,
            model_labels=model_labels,
            sync=sync,
        )
        training_id = self.extract_training_id(self._job.resource_name)
        if model:
            model.wait()
        else:
            self.log.warning(
                "Training did not produce a Managed Model returning None. AutoML Text Training "
                "Pipeline is not configured to upload a Model."
            )
        return model, training_id

    @GoogleBaseHook.fallback_to_default_project_id
    def create_auto_ml_video_training_job(
        self,
        project_id: str,
        region: str,
        display_name: str,
        dataset: datasets.VideoDataset,
        prediction_type: str = "classification",
        model_type: str = "CLOUD",
        labels: dict[str, str] | None = None,
        training_encryption_spec_key_name: str | None = None,
        model_encryption_spec_key_name: str | None = None,
        training_fraction_split: float | None = None,
        test_fraction_split: float | None = None,
        training_filter_split: str | None = None,
        test_filter_split: str | None = None,
        model_display_name: str | None = None,
        model_labels: dict[str, str] | None = None,
        sync: bool = True,
    ) -> tuple[models.Model | None, str]:
        """
        Create an AutoML Video Training Job.

        :param project_id: Required. Project to run training in.
        :param region: Required. Location to run training in.
        :param display_name: Required. The user-defined name of this TrainingPipeline.
        :param dataset: Required. The dataset within the same Project from which data will be used to train
            the Model. The Dataset must use schema compatible with Model being trained, and what is
            compatible should be described in the used TrainingPipeline's [training_task_definition]
            [google.cloud.aiplatform.v1beta1.TrainingPipeline.training_task_definition]. For tabular
            Datasets, all their data is exported to training, to pick and choose from.
        :param prediction_type: The type of prediction the Model is to produce, one of:
            "classification" - A video classification model classifies shots and segments in your videos
            according to your own defined labels.
            "object_tracking" - A video object tracking model detects and tracks multiple objects in shots
            and segments. You can use these models to track objects in your videos according to your own
            pre-defined, custom labels.
            "action_recognition" - A video action recognition model pinpoints the location of actions with
            short temporal durations (~1 second).
        :param model_type: Required. One of the following:
            "CLOUD" - available for "classification", "object_tracking" and "action_recognition" A Model best
            tailored to be used within Google Cloud, and which cannot be exported.
            "MOBILE_VERSATILE_1" - available for "classification", "object_tracking" and "action_recognition"
            A model that, in addition to being available within Google Cloud, can also be exported (see
            ModelService.ExportModel) as a TensorFlow or TensorFlow Lite model and used on a mobile or edge
            device with afterwards.
            "MOBILE_CORAL_VERSATILE_1" - available only for "object_tracking" A versatile model that is meant
            to be exported (see ModelService.ExportModel) and used on a Google Coral device.
            "MOBILE_CORAL_LOW_LATENCY_1" - available only for "object_tracking" A model that trades off
            quality for low latency, to be exported (see ModelService.ExportModel) and used on a Google Coral
            device.
            "MOBILE_JETSON_VERSATILE_1" - available only for "object_tracking" A versatile model that is
            meant to be exported (see ModelService.ExportModel) and used on an NVIDIA Jetson device.
            "MOBILE_JETSON_LOW_LATENCY_1" - available only for "object_tracking" A model that trades off
            quality for low latency, to be exported (see ModelService.ExportModel) and used on an NVIDIA
            Jetson device.
        :param labels: Optional. The labels with user-defined metadata to organize TrainingPipelines. Label
            keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the training pipeline. Has the form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute resource is created.
            If set, this TrainingPipeline will be secured by this key.
            Note: Model trained by this TrainingPipeline is also secured by this key if ``model_to_upload``
            is not set separately.
        :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the model. Has the form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute resource is created.
            If set, the trained Model will be secured by this key.
        :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
            the Model. This is ignored if Dataset is not provided.
        :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
            the Model. This is ignored if Dataset is not provided.
        :param training_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to train the Model. A filter with same syntax as the one used in
            DatasetService.ListDataItems may be used. If a single DataItem is matched by more than one of the
            FilterSplit filters, then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
        :param test_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match this
            filter are used to test the Model. A filter with same syntax as the one used in
            DatasetService.ListDataItems may be used. If a single DataItem is matched by more than one of the
            FilterSplit filters, then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
        :param model_display_name: Optional. The display name of the managed Vertex AI Model. The name can be
            up to 128 characters long and can be consist of any UTF-8 characters. If not provided upon
            creation, the job's display_name is used.
        :param model_labels: Optional. The labels with user-defined metadata to organize your Models. Label
            keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are
            allowed. See https://goo.gl/xmQnxf for more information and examples of labels.
        :param sync: Whether to execute this method synchronously. If False, this method will be executed in
            concurrent Future and any downstream object will be immediately returned and synced when the
            Future has completed.
        """
        self._job = self.get_auto_ml_video_training_job(
            project=project_id,
            location=region,
            display_name=display_name,
            prediction_type=prediction_type,
            model_type=model_type,
            labels=labels,
            training_encryption_spec_key_name=training_encryption_spec_key_name,
            model_encryption_spec_key_name=model_encryption_spec_key_name,
        )

        if not self._job:
            raise AirflowException("AutoMLVideoTrainingJob was not created")

        model = self._job.run(
            dataset=dataset,
            training_fraction_split=training_fraction_split,
            test_fraction_split=test_fraction_split,
            training_filter_split=training_filter_split,
            test_filter_split=test_filter_split,
            model_display_name=model_display_name,
            model_labels=model_labels,
            sync=sync,
        )
        training_id = self.extract_training_id(self._job.resource_name)
        if model:
            model.wait()
        else:
            self.log.warning(
                "Training did not produce a Managed Model returning None. AutoML Video Training "
                "Pipeline is not configured to upload a Model."
            )
        return model, training_id

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_training_pipeline(
        self,
        project_id: str,
        region: str,
        training_pipeline: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Deletes a TrainingPipeline.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param training_pipeline: Required. The name of the TrainingPipeline resource to be deleted.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        name = client.training_pipeline_path(project_id, region, training_pipeline)

        result = client.delete_training_pipeline(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_training_pipeline(
        self,
        project_id: str,
        region: str,
        training_pipeline: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> TrainingPipeline:
        """
        Gets a TrainingPipeline.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param training_pipeline: Required. The name of the TrainingPipeline resource.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        name = client.training_pipeline_path(project_id, region, training_pipeline)

        result = client.get_training_pipeline(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_training_pipelines(
        self,
        project_id: str,
        region: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        read_mask: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListTrainingPipelinesPager:
        """
        Lists TrainingPipelines in a Location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param filter: Optional. The standard list filter. Supported fields:

            -  ``display_name`` supports = and !=.

            -  ``state`` supports = and !=.

            Some examples of using the filter are:

            -  ``state="PIPELINE_STATE_SUCCEEDED" AND display_name="my_pipeline"``

            -  ``state="PIPELINE_STATE_RUNNING" OR display_name="my_pipeline"``

            -  ``NOT display_name="my_pipeline"``

            -  ``state="PIPELINE_STATE_FAILED"``
        :param page_size: Optional. The standard list page size.
        :param page_token: Optional. The standard list page token. Typically obtained via
            [ListTrainingPipelinesResponse.next_page_token][google.cloud.aiplatform.v1.ListTrainingPipelinesResponse.next_page_token]
            of the previous
            [PipelineService.ListTrainingPipelines][google.cloud.aiplatform.v1.PipelineService.ListTrainingPipelines]
            call.
        :param read_mask: Optional. Mask specifying which fields to read.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_training_pipelines(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter,
                "read_mask": read_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
