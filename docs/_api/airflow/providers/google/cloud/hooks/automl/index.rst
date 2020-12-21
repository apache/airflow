:mod:`airflow.providers.google.cloud.hooks.automl`
==================================================

.. py:module:: airflow.providers.google.cloud.hooks.automl

.. autoapi-nested-parse::

   This module contains a Google AutoML hook.



Module Contents
---------------

.. py:class:: CloudAutoMLHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Google Cloud AutoML hook.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. staticmethod:: extract_object_id(obj: Dict)

      Returns unique id of the object.



   
   .. method:: get_conn(self)

      Retrieves connection to AutoML.

      :return: Google Cloud AutoML client object.
      :rtype: google.cloud.automl_v1beta1.AutoMlClient



   
   .. method:: prediction_client(self)

      Creates PredictionServiceClient.

      :return: Google Cloud AutoML PredictionServiceClient client object.
      :rtype: google.cloud.automl_v1beta1.PredictionServiceClient



   
   .. method:: create_model(self, model: Union[dict, Model], location: str, project_id: str, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, retry: Optional[Retry] = None)

      Creates a model_id. Returns a Model in the `response` field when it
      completes. When you create a model, several model evaluations are
      created for it: a global evaluation, and one evaluation for each
      annotation spec.

      :param model: The model_id to create. If a dict is provided, it must be of the same form
          as the protobuf message `google.cloud.automl_v1beta1.types.Model`
      :type model: Union[dict, google.cloud.automl_v1beta1.types.Model]
      :param project_id: ID of the Google Cloud project where model will be created if None then
          default project_id is used.
      :type project_id: str
      :param location: The location of the project.
      :type location: str
      :param retry: A retry object used  to retry requests. If `None` is specified, requests
          will not be retried.
      :type retry: Optional[google.api_core.retry.Retry]
      :param timeout: The amount of time, in seconds, to wait for the request to complete.
          Note that if `retry` is specified, the timeout applies to each individual attempt.
      :type timeout: Optional[float]
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Optional[Sequence[Tuple[str, str]]]

      :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance



   
   .. method:: batch_predict(self, model_id: str, input_config: Union[dict, BatchPredictInputConfig], output_config: Union[dict, BatchPredictOutputConfig], location: str, project_id: str, params: Optional[Dict[str, str]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Perform a batch prediction. Unlike the online `Predict`, batch
      prediction result won't be immediately available in the response.
      Instead, a long running operation object is returned.

      :param model_id: Name of the model_id requested to serve the batch prediction.
      :type model_id: str
      :param input_config: Required. The input configuration for batch prediction.
          If a dict is provided, it must be of the same form as the protobuf message
          `google.cloud.automl_v1beta1.types.BatchPredictInputConfig`
      :type input_config: Union[dict, google.cloud.automl_v1beta1.types.BatchPredictInputConfig]
      :param output_config: Required. The Configuration specifying where output predictions should be
          written. If a dict is provided, it must be of the same form as the protobuf message
          `google.cloud.automl_v1beta1.types.BatchPredictOutputConfig`
      :type output_config: Union[dict, google.cloud.automl_v1beta1.types.BatchPredictOutputConfig]
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

      :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance



   
   .. method:: predict(self, model_id: str, payload: Union[dict, ExamplePayload], location: str, project_id: str, params: Optional[Dict[str, str]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Perform an online prediction. The prediction result will be directly
      returned in the response.

      :param model_id: Name of the model_id requested to serve the prediction.
      :type model_id: str
      :param payload: Required. Payload to perform a prediction on. The payload must match the problem type
          that the model_id was trained to solve. If a dict is provided, it must be of
          the same form as the protobuf message `google.cloud.automl_v1beta1.types.ExamplePayload`
      :type payload: Union[dict, google.cloud.automl_v1beta1.types.ExamplePayload]
      :param params: Additional domain-specific parameters, any string must be up to 25000 characters long.
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

      :return: `google.cloud.automl_v1beta1.types.PredictResponse` instance



   
   .. method:: create_dataset(self, dataset: Union[dict, Dataset], location: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Creates a dataset.

      :param dataset: The dataset to create. If a dict is provided, it must be of the
          same form as the protobuf message Dataset.
      :type dataset: Union[dict, Dataset]
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

      :return: `google.cloud.automl_v1beta1.types.Dataset` instance.



   
   .. method:: import_data(self, dataset_id: str, location: str, input_config: Union[dict, InputConfig], project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Imports data into a dataset. For Tables this method can only be called on an empty Dataset.

      :param dataset_id: Name of the AutoML dataset.
      :type dataset_id: str
      :param input_config: The desired input location and its domain specific semantics, if any.
          If a dict is provided, it must be of the same form as the protobuf message InputConfig.
      :type input_config: Union[dict, InputConfig]
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

      :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance



   
   .. method:: list_column_specs(self, dataset_id: str, table_spec_id: str, location: str, project_id: str, field_mask: Union[dict, FieldMask] = None, filter_: Optional[str] = None, page_size: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists column specs in a table spec.

      :param dataset_id: Name of the AutoML dataset.
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
          streaming is performed per-page, this determines the maximum number
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

      :return: `google.cloud.automl_v1beta1.types.ColumnSpec` instance.



   
   .. method:: get_model(self, model_id: str, location: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets a AutoML model.

      :param model_id: Name of the model.
      :type model_id: str
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

      :return: `google.cloud.automl_v1beta1.types.Model` instance.



   
   .. method:: delete_model(self, model_id: str, location: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Deletes a AutoML model.

      :param model_id: Name of the model.
      :type model_id: str
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

      :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance.



   
   .. method:: update_dataset(self, dataset: Union[dict, Dataset], update_mask: Union[dict, FieldMask] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Updates a dataset.

      :param dataset: The dataset which replaces the resource on the server.
          If a dict is provided, it must be of the same form as the protobuf message Dataset.
      :type dataset: Union[dict, Dataset]
      :param update_mask: The update mask applies to the resource.  If a dict is provided, it must
          be of the same form as the protobuf message FieldMask.
      :type update_mask: Union[dict, FieldMask]
      :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
          retried.
      :type retry: Optional[google.api_core.retry.Retry]
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          `retry` is specified, the timeout applies to each individual attempt.
      :type timeout: Optional[float]
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Optional[Sequence[Tuple[str, str]]]

      :return: `google.cloud.automl_v1beta1.types.Dataset` instance..



   
   .. method:: deploy_model(self, model_id: str, location: str, project_id: str, image_detection_metadata: Union[ImageObjectDetectionModelDeploymentMetadata, dict] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Deploys a model. If a model is already deployed, deploying it with the same parameters
      has no effect. Deploying with different parameters (as e.g. changing node_number) will
      reset the deployment state without pausing the model_id’s availability.

      Only applicable for Text Classification, Image Object Detection and Tables; all other
      domains manage deployment automatically.

      :param model_id: Name of the model requested to serve the prediction.
      :type model_id: str
      :param image_detection_metadata: Model deployment metadata specific to Image Object Detection.
          If a dict is provided, it must be of the same form as the protobuf message
          ImageObjectDetectionModelDeploymentMetadata
      :type image_detection_metadata: Union[ImageObjectDetectionModelDeploymentMetadata, dict]
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

      :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance.



   
   .. method:: list_table_specs(self, dataset_id: str, location: str, project_id: Optional[str] = None, filter_: Optional[str] = None, page_size: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists table specs in a dataset_id.

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

      :return: A `google.gax.PageIterator` instance. By default, this
          is an iterable of `google.cloud.automl_v1beta1.types.TableSpec` instances.
          This object can also be configured to iterate over the pages
          of the response through the `options` parameter.



   
   .. method:: list_datasets(self, location: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists datasets in a project.

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

      :return: A `google.gax.PageIterator` instance. By default, this
          is an iterable of `google.cloud.automl_v1beta1.types.Dataset` instances.
          This object can also be configured to iterate over the pages
          of the response through the `options` parameter.



   
   .. method:: delete_dataset(self, dataset_id: str, location: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Deletes a dataset and all of its contents.

      :param dataset_id: ID of dataset to be deleted.
      :type dataset_id: str
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

      :return: `google.cloud.automl_v1beta1.types._OperationFuture` instance




