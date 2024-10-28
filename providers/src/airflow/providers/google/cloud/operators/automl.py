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
"""This module contains Google AutoML operators."""

from __future__ import annotations

import ast
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Sequence, Tuple, cast

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.automl_v1beta1 import (
    BatchPredictResult,
    ColumnSpec,
    Dataset,
    Model,
    PredictResponse,
    TableSpec,
)

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
from airflow.providers.google.cloud.hooks.vertex_ai.prediction_service import (
    PredictionServiceHook,
)
from airflow.providers.google.cloud.links.translate import (
    TranslationDatasetListLink,
    TranslationLegacyDatasetLink,
    TranslationLegacyModelLink,
    TranslationLegacyModelPredictLink,
    TranslationLegacyModelTrainLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from google.api_core.retry import Retry

    from airflow.utils.context import Context

MetaData = Sequence[Tuple[str, str]]


def _raise_exception_for_deprecated_operator(
    deprecated_class_name: str, alternative_class_names: str | list[str]
):
    if isinstance(alternative_class_names, str):
        alternative_class_name_str = alternative_class_names
    elif len(alternative_class_names) == 1:
        alternative_class_name_str = alternative_class_names[0]
    else:
        alternative_class_name_str = ", ".join(
            f"`{cls_name}`" for cls_name in alternative_class_names[:-1]
        )
        alternative_class_name_str += f" or `{alternative_class_names[-1]}`"

    raise AirflowException(
        f"{deprecated_class_name} for text, image, and video prediction has been "
        f"deprecated and no longer available. All the functionality of "
        f"legacy AutoML Natural Language, Vision, Video Intelligence and Tables "
        f"and new features are available on the Vertex AI platform. "
        f"Please use {alternative_class_name_str} from Vertex AI."
    )


class AutoMLTrainModelOperator(GoogleCloudBaseOperator):
    """
    Creates Google Cloud AutoML model.

    AutoMLTrainModelOperator for tables, video intelligence, vision and natural language has been deprecated
    and no longer available. Please use
    :class:`airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLTabularTrainingJobOperator`,
    :class:`airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLVideoTrainingJobOperator`,
    :class:`airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLImageTrainingJobOperator`,
    :class:`airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLTextTrainingJobOperator`,
    instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLTrainModelOperator`

    :param model: Model definition.
    :param project_id: ID of the Google Cloud project where model will be created if None then
        default project_id is used.
    :param location: The location of the project.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "model",
        "location",
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (
        TranslationLegacyModelTrainLink(),
        TranslationLegacyModelLink(),
    )

    def __init__(
        self,
        *,
        model: dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.model = model
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        # Raise exception if running not AutoML Translation prediction job
        if "translation_model_metadata" not in self.model:
            _raise_exception_for_deprecated_operator(
                self.__class__.__name__,
                [
                    "CreateAutoMLTabularTrainingJobOperator",
                    "CreateAutoMLVideoTrainingJobOperator",
                    "CreateAutoMLImageTrainingJobOperator",
                    "CreateAutoMLTextTrainingJobOperator",
                ],
            )
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating model %s...", self.model["display_name"])
        operation = hook.create_model(
            model=self.model,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        project_id = self.project_id or hook.project_id
        if project_id:
            TranslationLegacyModelTrainLink.persist(
                context=context, task_instance=self, project_id=project_id
            )
        operation_result = hook.wait_for_operation(
            timeout=self.timeout, operation=operation
        )
        result = Model.to_dict(operation_result)
        model_id = hook.extract_object_id(result)
        self.log.info("Model is created, model_id: %s", model_id)

        self.xcom_push(context, key="model_id", value=model_id)
        if project_id:
            TranslationLegacyModelLink.persist(
                context=context,
                task_instance=self,
                dataset_id=self.model["dataset_id"] or "-",
                model_id=model_id,
                project_id=project_id,
            )
        return result


class AutoMLPredictOperator(GoogleCloudBaseOperator):
    """
    Runs prediction operation on Google Cloud AutoML.

    AutoMLPredictOperator for text, image, and video prediction has been deprecated.
    Please use endpoint_id param instead of model_id param.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLPredictOperator`

    :param model_id: Name of the model requested to serve the batch prediction.
    :param endpoint_id: Name of the endpoint used for the prediction.
    :param payload: Name of the model used for the prediction.
    :param project_id: ID of the Google Cloud project where model is located if None then
        default project_id is used.
    :param location: The location of the project.
    :param operation_params: Additional domain-specific parameters for the predictions.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "model_id",
        "location",
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (TranslationLegacyModelPredictLink(),)

    def __init__(
        self,
        *,
        model_id: str | None = None,
        endpoint_id: str | None = None,
        location: str,
        payload: dict,
        operation_params: dict[str, str] | None = None,
        instances: list[str] | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.model_id = model_id
        self.endpoint_id = endpoint_id
        self.operation_params = operation_params  # type: ignore
        self.instances = instances
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.payload = payload
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @cached_property
    def hook(self) -> CloudAutoMLHook | PredictionServiceHook:
        if self.model_id:
            return CloudAutoMLHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )
        else:  # endpoint_id defined
            return PredictionServiceHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )

    @cached_property
    def model(self) -> Model | None:
        if self.model_id:
            hook = cast(CloudAutoMLHook, self.hook)
            return hook.get_model(
                model_id=self.model_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        return None

    def _check_model_type(self):
        if not hasattr(self.model, "translation_model_metadata"):
            raise AirflowException(
                "AutoMLPredictOperator for text, image, and video prediction has been deprecated. "
                "Please use endpoint_id param instead of model_id param."
            )

    def execute(self, context: Context):
        if self.model_id is None and self.endpoint_id is None:
            raise AirflowException("You must specify model_id or endpoint_id!")

        if self.model_id:
            self._check_model_type()

        hook = self.hook
        if self.model_id:
            result = hook.predict(
                model_id=self.model_id,
                payload=self.payload,
                location=self.location,
                project_id=self.project_id,
                params=self.operation_params,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        else:  # self.endpoint_id is defined
            result = hook.predict(
                endpoint_id=self.endpoint_id,
                instances=self.instances,
                payload=self.payload,
                location=self.location,
                project_id=self.project_id,
                parameters=self.operation_params,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )

        project_id = self.project_id or hook.project_id
        dataset_id: str | None = self.model.dataset_id if self.model else None
        if project_id and self.model_id and dataset_id:
            TranslationLegacyModelPredictLink.persist(
                context=context,
                task_instance=self,
                model_id=self.model_id,
                dataset_id=dataset_id,
                project_id=project_id,
            )
        return PredictResponse.to_dict(result)


@deprecated(
    planned_removal_date="January 01, 2025",
    use_instead="airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job",
    category=AirflowProviderDeprecationWarning,
)
class AutoMLBatchPredictOperator(GoogleCloudBaseOperator):
    """
    Perform a batch prediction on Google Cloud AutoML.

    AutoMLBatchPredictOperator for tables, video intelligence, vision and natural language has been deprecated
    and no longer available. Please use
    :class:`airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.CreateBatchPredictionJobOperator`,
    :class:`airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.GetBatchPredictionJobOperator`,
    :class:`airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.ListBatchPredictionJobsOperator`,
    :class:`airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.DeleteBatchPredictionJobOperator`,
    instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLBatchPredictOperator`

    :param project_id: ID of the Google Cloud project where model will be created if None then
        default project_id is used.
    :param location: The location of the project.
    :param model_id: Name of the model_id requested to serve the batch prediction.
    :param input_config: Required. The input configuration for batch prediction.
        If a dict is provided, it must be of the same form as the protobuf message
        `google.cloud.automl_v1beta1.types.BatchPredictInputConfig`
    :param output_config: Required. The Configuration specifying where output predictions should be
        written. If a dict is provided, it must be of the same form as the protobuf message
        `google.cloud.automl_v1beta1.types.BatchPredictOutputConfig`
    :param prediction_params: Additional domain-specific parameters for the predictions,
        any string must be up to 25000 characters long.
    :param project_id: ID of the Google Cloud project where model is located if None then
        default project_id is used.
    :param location: The location of the project.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "model_id",
        "input_config",
        "output_config",
        "location",
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (TranslationLegacyModelPredictLink(),)

    def __init__(
        self,
        *,
        model_id: str,
        input_config: dict,
        output_config: dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        prediction_params: dict[str, str] | None = None,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.model_id = model_id
        self.location = location
        self.project_id = project_id
        self.prediction_params = prediction_params
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.input_config = input_config
        self.output_config = output_config

    @cached_property
    def hook(self) -> CloudAutoMLHook:
        return CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    @cached_property
    def model(self) -> Model:
        return self.hook.get_model(
            model_id=self.model_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

    def execute(self, context: Context):
        if not hasattr(self.model, "translation_model_metadata"):
            _raise_exception_for_deprecated_operator(
                self.__class__.__name__,
                [
                    "CreateBatchPredictionJobOperator",
                    "GetBatchPredictionJobOperator",
                    "ListBatchPredictionJobsOperator",
                    "DeleteBatchPredictionJobOperator",
                ],
            )
        self.log.info("Fetch batch prediction.")
        operation = self.hook.batch_predict(
            model_id=self.model_id,
            input_config=self.input_config,
            output_config=self.output_config,
            project_id=self.project_id,
            location=self.location,
            params=self.prediction_params,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        operation_result = self.hook.wait_for_operation(
            timeout=self.timeout, operation=operation
        )
        result = BatchPredictResult.to_dict(operation_result)
        self.log.info("Batch prediction is ready.")
        project_id = self.project_id or self.hook.project_id
        if project_id:
            TranslationLegacyModelPredictLink.persist(
                context=context,
                task_instance=self,
                model_id=self.model_id,
                project_id=project_id,
                dataset_id=self.model.dataset_id,
            )
        return result


class AutoMLCreateDatasetOperator(GoogleCloudBaseOperator):
    """
    Creates a Google Cloud AutoML dataset.

    AutoMLCreateDatasetOperator for tables, video intelligence, vision and natural language has been
    deprecated and no longer available. Please use
    :class:`airflow.providers.google.cloud.operators.vertex_ai.dataset.CreateDatasetOperator` instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLCreateDatasetOperator`

    :param dataset: The dataset to create. If a dict is provided, it must be of the
        same form as the protobuf message Dataset.
    :param project_id: ID of the Google Cloud project where dataset is located if None then
        default project_id is used.
    :param location: The location of the project.
    :param params: Additional domain-specific parameters for the predictions.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "dataset",
        "location",
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (TranslationLegacyDatasetLink(),)

    def __init__(
        self,
        *,
        dataset: dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.dataset = dataset
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        if "translation_dataset_metadata" not in self.dataset:
            _raise_exception_for_deprecated_operator(
                self.__class__.__name__, "CreateDatasetOperator"
            )
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating dataset %s...", self.dataset)
        result = hook.create_dataset(
            dataset=self.dataset,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = Dataset.to_dict(result)
        dataset_id = hook.extract_object_id(result)
        self.log.info("Creating completed. Dataset id: %s", dataset_id)

        self.xcom_push(context, key="dataset_id", value=dataset_id)
        project_id = self.project_id or hook.project_id
        if project_id:
            TranslationLegacyDatasetLink.persist(
                context=context,
                task_instance=self,
                dataset_id=dataset_id,
                project_id=project_id,
            )
        return result


class AutoMLImportDataOperator(GoogleCloudBaseOperator):
    """
    Imports data to a Google Cloud AutoML dataset.

    AutoMLImportDataOperator for tables, video intelligence, vision and natural language has been deprecated
    and no longer available. Please use
    :class:`airflow.providers.google.cloud.operators.vertex_ai.dataset.ImportDataOperator` instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLImportDataOperator`

    :param dataset_id: ID of dataset to be updated.
    :param input_config: The desired input location and its domain specific semantics, if any.
        If a dict is provided, it must be of the same form as the protobuf message InputConfig.
    :param project_id: ID of the Google Cloud project where dataset is located if None then
        default project_id is used.
    :param location: The location of the project.
    :param params: Additional domain-specific parameters for the predictions.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "dataset_id",
        "input_config",
        "location",
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (TranslationLegacyDatasetLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        location: str,
        input_config: dict,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.dataset_id = dataset_id
        self.input_config = input_config
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        dataset: Dataset = hook.get_dataset(
            dataset_id=self.dataset_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        if not hasattr(dataset, "translation_dataset_metadata"):
            _raise_exception_for_deprecated_operator(
                self.__class__.__name__, "ImportDataOperator"
            )
        self.log.info("Importing data to dataset...")
        operation = hook.import_data(
            dataset_id=self.dataset_id,
            input_config=self.input_config,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Import is completed")
        project_id = self.project_id or hook.project_id
        if project_id:
            TranslationLegacyDatasetLink.persist(
                context=context,
                task_instance=self,
                dataset_id=self.dataset_id,
                project_id=project_id,
            )


class AutoMLTablesListColumnSpecsOperator(GoogleCloudBaseOperator):
    """
    Lists column specs in a table.

    Operator AutoMLTablesListColumnSpecsOperator has been deprecated due to shutdown of
    a legacy version of AutoML Tables on March 31, 2024. For additional information
    see: https://cloud.google.com/automl-tables/docs/deprecations.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLTablesListColumnSpecsOperator`

    :param dataset_id: Name of the dataset.
    :param table_spec_id: table_spec_id for path builder.
    :param field_mask: Mask specifying which fields to read. If a dict is provided, it must be of the same
        form as the protobuf message `google.cloud.automl_v1beta1.types.FieldMask`
    :param filter_: Filter expression, see go/filtering.
    :param page_size: The maximum number of resources contained in the
        underlying API response. If page streaming is performed per
        resource, this parameter does not affect the return value. If page
        streaming is performed per page, this determines the maximum number
        of resources in a page.
    :param project_id: ID of the Google Cloud project where dataset is located if None then
        default project_id is used.
    :param location: The location of the project.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "dataset_id",
        "table_spec_id",
        "field_mask",
        "filter_",
        "location",
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (TranslationLegacyDatasetLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        table_spec_id: str,
        location: str,
        field_mask: dict | None = None,
        filter_: str | None = None,
        page_size: int | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.table_spec_id = table_spec_id
        self.field_mask = field_mask
        self.filter_ = filter_
        self.page_size = page_size
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        raise AirflowException(
            "Operator AutoMLTablesListColumnSpecsOperator has been deprecated due to shutdown of "
            "a legacy version of AutoML Tables on March 31, 2024. "
            "For additional information see: https://cloud.google.com/automl-tables/docs/deprecations."
        )

    def execute(self, context: Context):
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Requesting column specs.")
        page_iterator = hook.list_column_specs(
            dataset_id=self.dataset_id,
            table_spec_id=self.table_spec_id,
            field_mask=self.field_mask,
            filter_=self.filter_,
            page_size=self.page_size,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = [ColumnSpec.to_dict(spec) for spec in page_iterator]
        self.log.info("Columns specs obtained.")
        project_id = self.project_id or hook.project_id
        if project_id:
            TranslationLegacyDatasetLink.persist(
                context=context,
                task_instance=self,
                dataset_id=self.dataset_id,
                project_id=project_id,
            )
        return result


class AutoMLTablesUpdateDatasetOperator(GoogleCloudBaseOperator):
    """
    Updates a dataset.

    Operator AutoMLTablesUpdateDatasetOperator has been deprecated due to shutdown of
    a legacy version of AutoML Tables on March 31, 2024. For additional information
    see: https://cloud.google.com/automl-tables/docs/deprecations.
    Please use :class:`airflow.providers.google.cloud.operators.vertex_ai.dataset.UpdateDatasetOperator`
    instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLTablesUpdateDatasetOperator`

    :param dataset: The dataset which replaces the resource on the server.
        If a dict is provided, it must be of the same form as the protobuf message Dataset.
    :param update_mask: The update mask applies to the resource.  If a dict is provided, it must
        be of the same form as the protobuf message FieldMask.
    :param location: The location of the project.
    :param params: Additional domain-specific parameters for the predictions.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "dataset",
        "update_mask",
        "location",
        "impersonation_chain",
    )
    operator_extra_links = (TranslationLegacyDatasetLink(),)

    def __init__(
        self,
        *,
        dataset: dict,
        location: str,
        update_mask: dict | None = None,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.dataset = dataset
        self.update_mask = update_mask
        self.location = location
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        raise AirflowException(
            "Operator AutoMLTablesUpdateDatasetOperator has been deprecated due to shutdown of "
            "a legacy version of AutoML Tables on March 31, 2024. "
            "For additional information see: https://cloud.google.com/automl-tables/docs/deprecations. "
            "Please use UpdateDatasetOperator from Vertex AI instead."
        )

    def execute(self, context: Context):
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Updating AutoML dataset %s.", self.dataset["name"])
        result = hook.update_dataset(
            dataset=self.dataset,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Dataset updated.")
        project_id = hook.project_id
        if project_id:
            TranslationLegacyDatasetLink.persist(
                context=context,
                task_instance=self,
                dataset_id=hook.extract_object_id(self.dataset),
                project_id=project_id,
            )
        return Dataset.to_dict(result)


class AutoMLGetModelOperator(GoogleCloudBaseOperator):
    """
    Get Google Cloud AutoML model.

    AutoMLGetModelOperator for tables, video intelligence, vision and natural language has been deprecated
    and no longer available. Please use
    :class:`airflow.providers.google.cloud.operators.vertex_ai.model_service.GetModelOperator` instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLGetModelOperator`

    :param model_id: Name of the model requested to serve the prediction.
    :param project_id: ID of the Google Cloud project where model is located if None then
        default project_id is used.
    :param location: The location of the project.
    :param params: Additional domain-specific parameters for the predictions.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "model_id",
        "location",
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (TranslationLegacyModelLink(),)

    def __init__(
        self,
        *,
        model_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.model_id = model_id
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.get_model(
            model_id=self.model_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        if not hasattr(result, "translation_model_metadata"):
            _raise_exception_for_deprecated_operator(
                self.__class__.__name__, "GetModelOperator"
            )
        model = Model.to_dict(result)
        project_id = self.project_id or hook.project_id
        if project_id:
            TranslationLegacyModelLink.persist(
                context=context,
                task_instance=self,
                dataset_id=model["dataset_id"],
                model_id=self.model_id,
                project_id=project_id,
            )
        return model


class AutoMLDeleteModelOperator(GoogleCloudBaseOperator):
    """
    Delete Google Cloud AutoML model.

    AutoMLDeleteModelOperator for tables, video intelligence, vision and natural language has been deprecated
    and no longer available. Please use
    :class:`airflow.providers.google.cloud.operators.vertex_ai.model_service.DeleteModelOperator` instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLDeleteModelOperator`

    :param model_id: Name of the model requested to serve the prediction.
    :param project_id: ID of the Google Cloud project where model is located if None then
        default project_id is used.
    :param location: The location of the project.
    :param params: Additional domain-specific parameters for the predictions.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "model_id",
        "location",
        "project_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        model_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.model_id = model_id
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        model: Model = hook.get_model(
            model_id=self.model_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        if not hasattr(model, "translation_model_metadata"):
            _raise_exception_for_deprecated_operator(
                self.__class__.__name__, "DeleteModelOperator"
            )
        operation = hook.delete_model(
            model_id=self.model_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Deletion is completed")


class AutoMLDeployModelOperator(GoogleCloudBaseOperator):
    """
    Deploys a model; if a model is already deployed, deploying it with the same parameters has no effect.

    Deploying with different parameters (as e.g. changing node_number) will
    reset the deployment state without pausing the model_id's availability.

    Only applicable for Text Classification, Image Object Detection and Tables; all other
    domains manage deployment automatically.

    Operator AutoMLDeployModelOperator has been deprecated due to shutdown of a legacy version
    of AutoML Natural Language, Vision, Video Intelligence on March 31, 2024.
    For additional information see: https://cloud.google.com/vision/automl/docs/deprecations .
    Please use :class:`airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.DeployModelOperator`
    instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLDeployModelOperator`

    :param model_id: Name of the model to be deployed.
    :param image_detection_metadata: Model deployment metadata specific to Image Object Detection.
        If a dict is provided, it must be of the same form as the protobuf message
        ImageObjectDetectionModelDeploymentMetadata
    :param project_id: ID of the Google Cloud project where model is located if None then
        default project_id is used.
    :param location: The location of the project.
    :param params: Additional domain-specific parameters for the predictions.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "model_id",
        "location",
        "project_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        model_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        image_detection_metadata: dict | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.model_id = model_id
        self.image_detection_metadata = image_detection_metadata
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        raise AirflowException(
            "Operator AutoMLDeployModelOperator has been deprecated due to shutdown of "
            "a legacy version of AutoML AutoML Natural Language, Vision, Video Intelligence "
            "on March 31, 2024. "
            "For additional information see: https://cloud.google.com/vision/automl/docs/deprecations. "
            "Please use DeployModelOperator from Vertex AI instead."
        )

    def execute(self, context: Context):
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deploying model_id %s", self.model_id)
        operation = hook.deploy_model(
            model_id=self.model_id,
            location=self.location,
            project_id=self.project_id,
            image_detection_metadata=self.image_detection_metadata,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Model was deployed successfully.")


class AutoMLTablesListTableSpecsOperator(GoogleCloudBaseOperator):
    """
    Lists table specs in a dataset.

    Operator AutoMLTablesListTableSpecsOperator has been deprecated due to shutdown of
    a legacy version of AutoML Tables on March 31, 2024. For additional information
    see: https://cloud.google.com/automl-tables/docs/deprecations.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLTablesListTableSpecsOperator`

    :param dataset_id: Name of the dataset.
    :param filter_: Filter expression, see go/filtering.
    :param page_size: The maximum number of resources contained in the
        underlying API response. If page streaming is performed per
        resource, this parameter does not affect the return value. If page
        streaming is performed per-page, this determines the maximum number
        of resources in a page.
    :param project_id: ID of the Google Cloud project if None then
        default project_id is used.
    :param location: The location of the project.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "dataset_id",
        "filter_",
        "location",
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (TranslationLegacyDatasetLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        location: str,
        page_size: int | None = None,
        filter_: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.filter_ = filter_
        self.page_size = page_size
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        raise AirflowException(
            "Operator AutoMLTablesListTableSpecsOperator has been deprecated due to shutdown of "
            "a legacy version of AutoML Tables on March 31, 2024. "
            "For additional information see: https://cloud.google.com/automl-tables/docs/deprecations. "
        )

    def execute(self, context: Context):
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Requesting table specs for %s.", self.dataset_id)
        page_iterator = hook.list_table_specs(
            dataset_id=self.dataset_id,
            filter_=self.filter_,
            page_size=self.page_size,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = [TableSpec.to_dict(spec) for spec in page_iterator]
        self.log.info(result)
        self.log.info("Table specs obtained.")
        project_id = self.project_id or hook.project_id
        if project_id:
            TranslationLegacyDatasetLink.persist(
                context=context,
                task_instance=self,
                dataset_id=self.dataset_id,
                project_id=project_id,
            )
        return result


class AutoMLListDatasetOperator(GoogleCloudBaseOperator):
    """
    Lists AutoML Datasets in project.

    AutoMLListDatasetOperator for tables, video intelligence, vision and natural language has been deprecated
    and no longer available. Please use
    :class:`airflow.providers.google.cloud.operators.vertex_ai.dataset.ListDatasetsOperator` instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLListDatasetOperator`

    :param project_id: ID of the Google Cloud project where datasets are located if None then
        default project_id is used.
    :param location: The location of the project.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "location",
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (TranslationDatasetListLink(),)

    def __init__(
        self,
        *,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Requesting datasets")
        page_iterator = hook.list_datasets(
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = []
        for dataset in page_iterator:
            if not hasattr(dataset, "translation_dataset_metadata"):
                warnings.warn(
                    "Class `AutoMLListDatasetOperator` has been deprecated and no longer available. "
                    "Please use `ListDatasetsOperator` instead.",
                    stacklevel=2,
                )
            else:
                result.append(Dataset.to_dict(dataset))
        self.log.info("Datasets obtained.")

        self.xcom_push(
            context,
            key="dataset_id_list",
            value=[hook.extract_object_id(d) for d in result],
        )
        project_id = self.project_id or hook.project_id
        if project_id:
            TranslationDatasetListLink.persist(
                context=context, task_instance=self, project_id=project_id
            )
        return result


class AutoMLDeleteDatasetOperator(GoogleCloudBaseOperator):
    """
    Deletes a dataset and all of its contents.

    AutoMLDeleteDatasetOperator for tables, video intelligence, vision and natural language has been
    deprecated and no longer available. Please use
    :class:`airflow.providers.google.cloud.operators.vertex_ai.dataset.DeleteDatasetOperator` instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLDeleteDatasetOperator`

    :param dataset_id: Name of the dataset_id, list of dataset_id or string of dataset_id
        coma separated to be deleted.
    :param project_id: ID of the Google Cloud project where dataset is located if None then
        default project_id is used.
    :param location: The location of the project.
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
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
        "dataset_id",
        "location",
        "project_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        dataset_id: str | list[str],
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: MetaData = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.dataset_id = dataset_id
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @staticmethod
    def _parse_dataset_id(dataset_id: str | list[str]) -> list[str]:
        if not isinstance(dataset_id, str):
            return dataset_id
        try:
            return ast.literal_eval(dataset_id)
        except (SyntaxError, ValueError):
            return dataset_id.split(",")

    def execute(self, context: Context):
        hook = CloudAutoMLHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        dataset: Dataset = hook.get_dataset(
            dataset_id=self.dataset_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        if not hasattr(dataset, "translation_dataset_metadata"):
            _raise_exception_for_deprecated_operator(
                self.__class__.__name__, "DeleteDatasetOperator"
            )
        dataset_id_list = self._parse_dataset_id(self.dataset_id)
        for dataset_id in dataset_id_list:
            self.log.info("Deleting dataset %s", dataset_id)
            hook.delete_dataset(
                dataset_id=dataset_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Dataset deleted.")
