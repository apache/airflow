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
"""
This module contains a Google AutoML hook.

.. spelling::

    PredictResponse
"""
from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.automl_v1beta1 import (
    AutoMlClient,
    BatchPredictInputConfig,
    BatchPredictOutputConfig,
    Dataset,
    ExamplePayload,
    ImageObjectDetectionModelDeploymentMetadata,
    InputConfig,
    Model,
    PredictionServiceClient,
    PredictResponse,
)
from google.cloud.automl_v1beta1.services.auto_ml.pagers import (
    ListColumnSpecsPager,
    ListDatasetsPager,
    ListTableSpecsPager,
)
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.compat.functools import cached_property
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook


class CloudAutoMLHook(GoogleBaseHook):
    """
    Google Cloud AutoML hook.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self._client = None  # type: Optional[AutoMlClient]

    @staticmethod
    def extract_object_id(obj: Dict) -> str:
        """Returns unique id of the object."""
        return obj["name"].rpartition("/")[-1]

    def get_conn(self) -> AutoMlClient:
        """
        Retrieves connection to AutoML.

        :return: Google Cloud AutoML client object.
        :rtype: google.cloud.automl_v1beta1.AutoMlClient
        """
        if self._client is None:
            self._client = AutoMlClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._client

    @cached_property
    def prediction_client(self) -> PredictionServiceClient:
        """
        Creates PredictionServiceClient.

        :return: Google Cloud AutoML PredictionServiceClient client object.
        :rtype: google.cloud.automl_v1beta1.PredictionServiceClient
        """
        return PredictionServiceClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_model(
        self,
        model: Union[dict, Model],
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        retry: Union[Retry, _MethodDefault] = DEFAULT,
    ) -> Operation:
        """
        Creates a model_id. Returns a Model in the `response` field when it
        completes. When you create a model, several model evaluations are
        created for it: a global evaluation, and one evaluation for each
        annotation spec.

        :param model: The model_id to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.automl_v1beta1.types.Model`
        :param project_id: ID of the Google Cloud project where model will be created if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}"
        return client.create_model(
            request={'parent': parent, 'model': model},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def batch_predict(
        self,
        model_id: str,
        input_config: Union[dict, BatchPredictInputConfig],
        output_config: Union[dict, BatchPredictOutputConfig],
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        params: Optional[Dict[str, str]] = None,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Perform a batch prediction. Unlike the online `Predict`, batch
        prediction result won't be immediately available in the response.
        Instead, a long running operation object is returned.

        :param model_id: Name of the model_id requested to serve the batch prediction.
        :param input_config: Required. The input configuration for batch prediction.
            If a dict is provided, it must be of the same form as the protobuf message
            `google.cloud.automl_v1beta1.types.BatchPredictInputConfig`
        :param output_config: Required. The Configuration specifying where output predictions should be
            written. If a dict is provided, it must be of the same form as the protobuf message
            `google.cloud.automl_v1beta1.types.BatchPredictOutputConfig`
        :param params: Additional domain-specific parameters for the predictions, any string must be up to
            25000 characters long.
        :param project_id: ID of the Google Cloud project where model is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance
        """
        client = self.prediction_client
        name = f"projects/{project_id}/locations/{location}/models/{model_id}"
        result = client.batch_predict(
            request={
                'name': name,
                'input_config': input_config,
                'output_config': output_config,
                'params': params,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def predict(
        self,
        model_id: str,
        payload: Union[dict, ExamplePayload],
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        params: Optional[Dict[str, str]] = None,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> PredictResponse:
        """
        Perform an online prediction. The prediction result will be directly
        returned in the response.

        :param model_id: Name of the model_id requested to serve the prediction.
        :param payload: Required. Payload to perform a prediction on. The payload must match the problem type
            that the model_id was trained to solve. If a dict is provided, it must be of
            the same form as the protobuf message `google.cloud.automl_v1beta1.types.ExamplePayload`
        :param params: Additional domain-specific parameters, any string must be up to 25000 characters long.
        :param project_id: ID of the Google Cloud project where model is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types.PredictResponse` instance
        """
        client = self.prediction_client
        name = f"projects/{project_id}/locations/{location}/models/{model_id}"
        result = client.predict(
            request={'name': name, 'payload': payload, 'params': params},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_dataset(
        self,
        dataset: Union[dict, Dataset],
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Dataset:
        """
        Creates a dataset.

        :param dataset: The dataset to create. If a dict is provided, it must be of the
            same form as the protobuf message Dataset.
        :param project_id: ID of the Google Cloud project where dataset is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types.Dataset` instance.
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}"
        result = client.create_dataset(
            request={'parent': parent, 'dataset': dataset},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def import_data(
        self,
        dataset_id: str,
        location: str,
        input_config: Union[dict, InputConfig],
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Imports data into a dataset. For Tables this method can only be called on an empty Dataset.

        :param dataset_id: Name of the AutoML dataset.
        :param input_config: The desired input location and its domain specific semantics, if any.
            If a dict is provided, it must be of the same form as the protobuf message InputConfig.
        :param project_id: ID of the Google Cloud project where dataset is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/datasets/{dataset_id}"
        result = client.import_data(
            request={'name': name, 'input_config': input_config},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_column_specs(
        self,
        dataset_id: str,
        table_spec_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        field_mask: Optional[Union[dict, FieldMask]] = None,
        filter_: Optional[str] = None,
        page_size: Optional[int] = None,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> ListColumnSpecsPager:
        """
        Lists column specs in a table spec.

        :param dataset_id: Name of the AutoML dataset.
        :param table_spec_id: table_spec_id for path builder.
        :param field_mask: Mask specifying which fields to read. If a dict is provided, it must be of the same
            form as the protobuf message `google.cloud.automl_v1beta1.types.FieldMask`
        :param filter_: Filter expression, see go/filtering.
        :param page_size: The maximum number of resources contained in the
            underlying API response. If page streaming is performed per
            resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number
            of resources in a page.
        :param project_id: ID of the Google Cloud project where dataset is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types.ColumnSpec` instance.
        """
        client = self.get_conn()
        parent = client.table_spec_path(
            project=project_id,
            location=location,
            dataset=dataset_id,
            table_spec=table_spec_id,
        )
        result = client.list_column_specs(
            request={'parent': parent, 'field_mask': field_mask, 'filter': filter_, 'page_size': page_size},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_model(
        self,
        model_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Model:
        """
        Gets a AutoML model.

        :param model_id: Name of the model.
        :param project_id: ID of the Google Cloud project where model is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types.Model` instance.
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/models/{model_id}"
        result = client.get_model(
            request={'name': name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_model(
        self,
        model_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Deletes a AutoML model.

        :param model_id: Name of the model.
        :param project_id: ID of the Google Cloud project where model is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance.
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/models/{model_id}"
        result = client.delete_model(
            request={'name': name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def update_dataset(
        self,
        dataset: Union[dict, Dataset],
        update_mask: Optional[Union[dict, FieldMask]] = None,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Dataset:
        """
        Updates a dataset.

        :param dataset: The dataset which replaces the resource on the server.
            If a dict is provided, it must be of the same form as the protobuf message Dataset.
        :param update_mask: The update mask applies to the resource.  If a dict is provided, it must
            be of the same form as the protobuf message FieldMask.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types.Dataset` instance..
        """
        client = self.get_conn()
        result = client.update_dataset(
            request={'dataset': dataset, 'update_mask': update_mask},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def deploy_model(
        self,
        model_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        image_detection_metadata: Optional[Union[ImageObjectDetectionModelDeploymentMetadata, dict]] = None,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Deploys a model. If a model is already deployed, deploying it with the same parameters
        has no effect. Deploying with different parameters (as e.g. changing node_number) will
        reset the deployment state without pausing the model_id's availability.

        Only applicable for Text Classification, Image Object Detection and Tables; all other
        domains manage deployment automatically.

        :param model_id: Name of the model requested to serve the prediction.
        :param image_detection_metadata: Model deployment metadata specific to Image Object Detection.
            If a dict is provided, it must be of the same form as the protobuf message
            ImageObjectDetectionModelDeploymentMetadata
        :param project_id: ID of the Google Cloud project where model will be created if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance.
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/models/{model_id}"
        result = client.deploy_model(
            request={
                'name': name,
                'image_object_detection_model_deployment_metadata': image_detection_metadata,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def list_table_specs(
        self,
        dataset_id: str,
        location: str,
        project_id: Optional[str] = None,
        filter_: Optional[str] = None,
        page_size: Optional[int] = None,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> ListTableSpecsPager:
        """
        Lists table specs in a dataset_id.

        :param dataset_id: Name of the dataset.
        :param filter_: Filter expression, see go/filtering.
        :param page_size: The maximum number of resources contained in the
            underlying API response. If page streaming is performed per
            resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number
            of resources in a page.
        :param project_id: ID of the Google Cloud project where dataset is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: A `google.gax.PageIterator` instance. By default, this
            is an iterable of `google.cloud.automl_v1beta1.types.TableSpec` instances.
            This object can also be configured to iterate over the pages
            of the response through the `options` parameter.
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}/datasets/{dataset_id}"
        result = client.list_table_specs(
            request={'parent': parent, 'filter': filter_, 'page_size': page_size},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_datasets(
        self,
        location: str,
        project_id: str,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> ListDatasetsPager:
        """
        Lists datasets in a project.

        :param project_id: ID of the Google Cloud project where dataset is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: A `google.gax.PageIterator` instance. By default, this
            is an iterable of `google.cloud.automl_v1beta1.types.Dataset` instances.
            This object can also be configured to iterate over the pages
            of the response through the `options` parameter.
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}"
        result = client.list_datasets(
            request={'parent': parent},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_dataset(
        self,
        dataset_id: str,
        location: str,
        project_id: str,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Deletes a dataset and all of its contents.

        :param dataset_id: ID of dataset to be deleted.
        :param project_id: ID of the Google Cloud project where dataset is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/datasets/{dataset_id}"
        result = client.delete_dataset(
            request={'name': name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
