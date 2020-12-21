:mod:`airflow.providers.google.cloud.operators.automl`
======================================================

.. py:module:: airflow.providers.google.cloud.operators.automl

.. autoapi-nested-parse::

   This module contains Google AutoML operators.



Module Contents
---------------

.. data:: MetaData
   

   

.. py:class:: AutoMLTrainModelOperator(*, model: dict, location: str, project_id: Optional[str] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['model', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLPredictOperator(*, model_id: str, location: str, payload: dict, params: Optional[Dict[str, str]] = None, project_id: Optional[str] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['model_id', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLBatchPredictOperator(*, model_id: str, input_config: dict, output_config: dict, location: str, project_id: Optional[str] = None, prediction_params: Optional[Dict[str, str]] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param prediction_params: Additional domain-specific parameters for the predictions,
       any string must be up to 25000 characters long.
   :type prediction_params: Optional[Dict[str, str]]
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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['model_id', 'input_config', 'output_config', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLCreateDatasetOperator(*, dataset: dict, location: str, project_id: Optional[str] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['dataset', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLImportDataOperator(*, dataset_id: str, location: str, input_config: dict, project_id: Optional[str] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['dataset_id', 'input_config', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLTablesListColumnSpecsOperator(*, dataset_id: str, table_spec_id: str, location: str, field_mask: Optional[dict] = None, filter_: Optional[str] = None, page_size: Optional[int] = None, project_id: Optional[str] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['dataset_id', 'table_spec_id', 'field_mask', 'filter_', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLTablesUpdateDatasetOperator(*, dataset: dict, location: str, update_mask: Optional[dict] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['dataset', 'update_mask', 'location', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLGetModelOperator(*, model_id: str, location: str, project_id: Optional[str] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['model_id', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLDeleteModelOperator(*, model_id: str, location: str, project_id: Optional[str] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['model_id', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLDeployModelOperator(*, model_id: str, location: str, project_id: Optional[str] = None, image_detection_metadata: Optional[dict] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deploys a model. If a model is already deployed, deploying it with the same parameters
   has no effect. Deploying with different parameters (as e.g. changing node_number) will
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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['model_id', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLTablesListTableSpecsOperator(*, dataset_id: str, location: str, page_size: Optional[int] = None, filter_: Optional[str] = None, project_id: Optional[str] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['dataset_id', 'filter_', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLListDatasetOperator(*, location: str, project_id: Optional[str] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: AutoMLDeleteDatasetOperator(*, dataset_id: Union[str, List[str]], location: str, project_id: Optional[str] = None, metadata: Optional[MetaData] = None, timeout: Optional[float] = None, retry: Optional[Retry] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['dataset_id', 'location', 'project_id', 'impersonation_chain']

      

   
   .. staticmethod:: _parse_dataset_id(dataset_id: Union[str, List[str]])



   
   .. method:: execute(self, context)




