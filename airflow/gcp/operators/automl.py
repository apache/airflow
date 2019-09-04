# -*- coding: utf-8 -*-
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
# pylint:disable=too-many-lines
"""
This module contains Google AutoML operators.
"""
import ast
from typing import Sequence, Tuple, Union, List, Dict

from google.api_core.retry import Retry
from google.protobuf.json_format import MessageToDict

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.gcp.hooks.automl import CloudAutoMLHook


class AutoMLTrainModelOperator(BaseOperator):
    """
    Creates Google Cloud AutoML model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLTrainModelOperator`

    :param model: Model definition.
    :type model: dict
    :param project_id: ID of the Google Cloud project where model will be created if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
     """

    template_fields = ("model", "location", "project_id")

    @apply_defaults
    def __init__(
        self,
        model: dict,
        location: str,
        project_id: str = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.model = model
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info("Creating model.")
        operation = hook.create_model(
            model=self.model,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = MessageToDict(operation.result())
        model_id = hook.extract_object_id(result)
        self.log.info("Model created: %s", model_id)

        self.xcom_push(context, key="model_id", value=model_id)
        return result


class AutoMLPredictOperator(BaseOperator):
    """
    Runs prediction operation on Google Cloud AutoML.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLPredictOperator`

    :param model_id: Name of the model requested to serve the batch prediction.
    :type model_id: str
    :param payload: Name od the model used for the prediction.
    :type payload: dict
    :param project_id: ID of the Google Cloud project where model is located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param params: Additional domain-specific parameters for the predictions.
    :type params: Optional[Dict[str, str]]
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ("model_id", "location", "project_id")

    @apply_defaults
    def __init__(
        self,
        model_id: str,
        location: str,
        payload: dict,
        params: Dict[str, str] = None,
        project_id: str = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.model_id = model_id
        self.params = params  # type: ignore
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.payload = payload
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.predict(
            model_id=self.model_id,
            payload=self.payload,
            location=self.location,
            project_id=self.project_id,
            params=self.params,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)


class AutoMLBatchPredictOperator(BaseOperator):
    """
    Perform a batch prediction on Google Cloud AutoML.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLBatchPredictOperator`

    :param project_id: ID of the Google Cloud project where model will be created if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param model_id: Name of the model_id requested to serve the batch prediction.
    :type model_id: str
    :param input_config: Required. The input configuration for batch prediction.
        If a dict is provided, it must be of the same form as the protobuf message
        `google.cloud.automl_v1beta1.types.BatchPredictInputConfig`
    :type input_config: Union[dict, ~google.cloud.automl_v1beta1.types.BatchPredictInputConfig]
    :param output_config: Required. The Configuration specifying where output predictions should be
        written. If a dict is provided, it must be of the same form as the protobuf message
        `google.cloud.automl_v1beta1.types.BatchPredictOutputConfig`
    :type output_config: Union[dict, ~google.cloud.automl_v1beta1.types.BatchPredictOutputConfig]
    :param params: Additional domain-specific parameters for the predictions, any string must be up to
        25000 characters long.
    :type params: Optional[Dict[str, str]]
    :param project_id: ID of the Google Cloud project where model is located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = (
        "model_id",
        "input_config",
        "output_config",
        "location",
        "project_id",
    )

    @apply_defaults
    def __init__(  # pylint:disable=too-many-arguments
        self,
        model_id: str,
        input_config: dict,
        output_config: dict,
        location: str,
        project_id: str = None,
        params: Dict[str, str] = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.model_id = model_id
        self.location = location
        self.project_id = project_id
        self.params = params  # type: ignore
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.input_config = input_config
        self.output_config = output_config

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info("Fetch batch prediction.")
        operation = hook.batch_predict(
            model_id=self.model_id,
            input_config=self.input_config,
            output_config=self.output_config,
            project_id=self.project_id,
            location=self.location,
            params=self.params,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = MessageToDict(operation.result())
        self.log.info("Batch prediction ready.")
        return result


class AutoMLCreateDatasetOperator(BaseOperator):
    """
    Creates a Google Cloud AutoML dataset.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLCreateDatasetOperator`

    :param dataset: The dataset to create. If a dict is provided, it must be of the
        same form as the protobuf message Dataset.
    :type dataset: Union[dict, Dataset]
    :param project_id: ID of the Google Cloud project where dataset is located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param params: Additional domain-specific parameters for the predictions.
    :type params: Optional[Dict[str, str]]
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ("dataset", "location", "project_id")

    @apply_defaults
    def __init__(
        self,
        dataset: dict,
        location: str,
        project_id: str = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.dataset = dataset
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info("Creating dataset")
        result = hook.create_dataset(
            dataset=self.dataset,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = MessageToDict(result)
        dataset_id = hook.extract_object_id(result)
        self.log.info("Creating completed. Dataset id: %s", dataset_id)

        self.xcom_push(context, key="dataset_id", value=dataset_id)
        return result


class AutoMLImportDataOperator(BaseOperator):
    """
    Imports data to a Google Cloud AutoML dataset.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLImportDataOperator`

    :param dataset_id: ID of dataset to be updated.
    :type dataset_id: str
    :param input_config: The desired input location and its domain specific semantics, if any.
        If a dict is provided, it must be of the same form as the protobuf message InputConfig.
    :type input_config: dict
    :param project_id: ID of the Google Cloud project where dataset is located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param params: Additional domain-specific parameters for the predictions.
    :type params: Optional[Dict[str, str]]
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ("dataset_id", "input_config", "location", "project_id")

    @apply_defaults
    def __init__(
        self,
        dataset_id: str,
        location: str,
        input_config: dict,
        project_id: str = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.dataset_id = dataset_id
        self.input_config = input_config
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info("Importing dataset")
        operation = hook.import_data(
            dataset_id=self.dataset_id,
            input_config=self.input_config,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = MessageToDict(operation.result())
        self.log.info("Import completed")
        return result


class AutoMLTablesListColumnSpecsOperator(BaseOperator):
    """
    Lists column specs in a table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLTablesListColumnSpecsOperator`

    :param dataset_id: Name of the dataset.
    :type dataset_id: str
    :param table_spec_id: table_spec_id for path builder.
    :type table_spec_id: str
    :param field_mask: Mask specifying which fields to read. If a dict is provided, it must be of the same
        form as the protobuf message `google.cloud.automl_v1beta1.types.FieldMask`
    :type field_mask: Union[dict, google.cloud.automl_v1beta1.types.FieldMask]
    :param filter_: Filter expression, see go/filtering.
    :type filter_: str
    :param page_size: The maximum number of resources contained in the
        underlying API response. If page streaming is performed per
        resource, this parameter does not affect the return value. If page
        streaming is performed per page, this determines the maximum number
        of resources in a page.
    :type page_size: int
    :param project_id: ID of the Google Cloud project where dataset is located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = (
        "dataset_id",
        "table_spec_id",
        "field_mask",
        "filter_",
        "location",
        "project_id",
    )

    @apply_defaults
    def __init__(  # pylint:disable=too-many-arguments
        self,
        dataset_id: str,
        table_spec_id: str,
        location: str,
        field_mask: dict = None,
        filter_: str = None,
        page_size: int = None,
        project_id: str = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
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

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
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
        result = [MessageToDict(spec) for spec in page_iterator]
        self.log.info("Columns specs obtained.")

        return result


class AutoMLTablesUpdateDatasetOperator(BaseOperator):
    """
    Updates a dataset.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLTablesUpdateDatasetOperator`

    :param dataset: The dataset which replaces the resource on the server.
        If a dict is provided, it must be of the same form as the protobuf message Dataset.
    :type dataset: Union[dict, Dataset]
    :param update_mask: The update mask applies to the resource.  If a dict is provided, it must
        be of the same form as the protobuf message FieldMask.
    :type update_mask: Union[dict, FieldMask]
    :param project_id: ID of the Google Cloud project where dataset is located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param params: Additional domain-specific parameters for the predictions.
    :type params: Optional[Dict[str, str]]
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ("dataset", "update_mask", "location", "project_id")

    @apply_defaults
    def __init__(
        self,
        dataset: dict,
        location: str,
        project_id: str = None,
        update_mask: dict = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.dataset = dataset
        self.update_mask = update_mask
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info("Updating AutoML dataset %s.", self.dataset["name"])
        result = hook.update_dataset(
            dataset=self.dataset,
            update_mask=self.update_mask,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Dataset updated.")
        return MessageToDict(result)


class AutoMLGetModelOperator(BaseOperator):
    """
    Get Google Cloud AutoML model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLGetModelOperator`

    :param model_id: Name of the model requested to serve the prediction.
    :type model_id: str
    :param project_id: ID of the Google Cloud project where model is located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param params: Additional domain-specific parameters for the predictions.
    :type params: Optional[Dict[str, str]]
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ("model_id", "location", "project_id")

    @apply_defaults
    def __init__(
        self,
        model_id: str,
        location: str,
        project_id: str = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.model_id = model_id
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.get_model(
            model_id=self.model_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)


class AutoMLDeleteModelOperator(BaseOperator):
    """
    Delete Google Cloud AutoML model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLDeleteModelOperator`

    :param model_id: Name of the model requested to serve the prediction.
    :type model_id: str
    :param project_id: ID of the Google Cloud project where model is located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param params: Additional domain-specific parameters for the predictions.
    :type params: Optional[Dict[str, str]]
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ("model_id", "location", "project_id")

    @apply_defaults
    def __init__(
        self,
        model_id: str,
        location: str,
        project_id: str = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.model_id = model_id
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
        operation = hook.delete_model(
            model_id=self.model_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = MessageToDict(operation.result())
        return result


class AutoMLDeployModelOperator(BaseOperator):
    """
    Deploys a model. If a model is already deployed, deploying it with the same parameters
    has no effect. Deploying with different parametrs (as e.g. changing node_number) will
    reset the deployment state without pausing the model_id’s availability.

    Only applicable for Text Classification, Image Object Detection and Tables; all other
    domains manage deployment automatically.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLDeployModelOperator`

    :param model_id: Name of the model to be deployed.
    :type model_id: str
    :param image_detection_metadata: Model deployment metadata specific to Image Object Detection.
        If a dict is provided, it must be of the same form as the protobuf message
        ImageObjectDetectionModelDeploymentMetadata
    :type image_detection_metadata: dict
    :param project_id: ID of the Google Cloud project where model is located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param params: Additional domain-specific parameters for the predictions.
    :type params: Optional[Dict[str, str]]
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ("model_id", "location", "project_id")

    @apply_defaults
    def __init__(
        self,
        model_id: str,
        location: str,
        project_id: str = None,
        image_detection_metadata: dict = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.model_id = model_id
        self.image_detection_metadata = image_detection_metadata
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
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
        result = MessageToDict(operation.result())
        self.log.info("Model deployed.")
        return result


class AutoMLTablesListTableSpecsOperator(BaseOperator):
    """
    Lists table specs in a dataset.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLTablesListTableSpecsOperator`

    :param dataset_id: Name of the dataset.
    :type dataset_id: str
    :param filter_: Filter expression, see go/filtering.
    :type filter_: str
    :param page_size: The maximum number of resources contained in the
        underlying API response. If page streaming is performed per
        resource, this parameter does not affect the return value. If page
        streaming is performed per-page, this determines the maximum number
        of resources in a page.
    :type page_size: int
    :param project_id: ID of the Google Cloud project if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ("dataset_id", "filter_", "location", "project_id")

    @apply_defaults
    def __init__(
        self,
        dataset_id: str,
        location: str,
        page_size: int = None,
        filter_: str = None,
        project_id: str = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.dataset_id = dataset_id
        self.filter_ = filter_
        self.page_size = page_size
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
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
        result = [MessageToDict(spec) for spec in page_iterator]
        self.log.info(result)
        self.log.info("Table specs obtained.")
        return result


class AutoMLListDatasetOperator(BaseOperator):
    """
    Lists AutoML Datasets in project.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLListDatasetOperator`

    :param project_id: ID of the Google Cloud project where datasets are located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ("location", "project_id")

    @apply_defaults
    def __init__(
        self,
        location: str,
        project_id: str = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info("Requesting datasets")
        page_iterator = hook.list_datasets(
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = [MessageToDict(dataset) for dataset in page_iterator]
        self.log.info("Datasets obtained.")

        self.xcom_push(
            context,
            key="dataset_id_list",
            value=[hook.extract_object_id(d) for d in result],
        )
        return result


class AutoMLDeleteDatasetOperator(BaseOperator):
    """
    Deletes a dataset and all of its contents.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AutoMLDeleteDatasetOperator`

    :param dataset_id: Name of the dataset_id, list of dataset_id or string of dataset_id
        coma separated to be deleted.
    :type dataset_id: Union[str, List[str]]
    :param project_id: ID of the Google Cloud project where dataset is located if None then
        default project_id is used.
    :type project_id: str
    :param location: The location of the project.
    :type location: str
    :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
        retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ("dataset_id", "location", "project_id")

    @apply_defaults
    def __init__(
        self,
        dataset_id: Union[str, List[str]],
        location: str,
        project_id: str = None,
        metadata: Sequence[Tuple[str, str]] = None,
        timeout: float = None,
        retry: Retry = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.dataset_id = dataset_id
        self.location = location
        self.project_id = project_id
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id

    @staticmethod
    def _parse_dataset_id(dataset_id: Union[str, List[str]]) -> List[str]:
        if not isinstance(dataset_id, str):
            return dataset_id
        try:
            return ast.literal_eval(dataset_id)
        except (SyntaxError, ValueError):
            return dataset_id.split(",")

    def execute(self, context):
        hook = CloudAutoMLHook(gcp_conn_id=self.gcp_conn_id)
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
