:mod:`airflow.providers.google.cloud.operators.cloud_storage_transfer_service`
==============================================================================

.. py:module:: airflow.providers.google.cloud.operators.cloud_storage_transfer_service

.. autoapi-nested-parse::

   This module contains Google Cloud Transfer operators.



Module Contents
---------------

.. py:class:: TransferJobPreprocessor(body: dict, aws_conn_id: str = 'aws_default', default_schedule: bool = False)

   Helper class for preprocess of transfer job body.

   
   .. method:: _inject_aws_credentials(self)



   
   .. method:: _reformat_date(self, field_key: str)



   
   .. method:: _reformat_time(self, field_key: str)



   
   .. method:: _reformat_schedule(self)



   
   .. method:: process_body(self)

      Injects AWS credentials into body if needed and
      reformats schedule information.

      :return: Preprocessed body
      :rtype: dict



   
   .. staticmethod:: _convert_date_to_dict(field_date: date)

      Convert native python ``datetime.date`` object  to a format supported by the API



   
   .. staticmethod:: _convert_time_to_dict(time_object: time)

      Convert native python ``datetime.time`` object  to a format supported by the API




.. py:class:: TransferJobValidator(body: dict)

   Helper class for validating transfer job body.

   
   .. method:: _verify_data_source(self)



   
   .. method:: _restrict_aws_credentials(self)



   
   .. method:: validate_body(self)

      Validates the body. Checks if body specifies `transferSpec`
      if yes, then check if AWS credentials are passed correctly and
      no more than 1 data source was selected.

      :raises: AirflowException




.. py:class:: CloudDataTransferServiceCreateJobOperator(*, body: dict, aws_conn_id: str = 'aws_default', gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a transfer job that runs periodically.

   .. warning::

       This operator is NOT idempotent in the following cases:

       * `name` is not passed in body param
       * transfer job `name` has been soft deleted. In this case,
         each new task will receive a unique suffix

       If you run it many times, many transfer jobs will be created in the Google Cloud.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataTransferServiceCreateJobOperator`

   :param body: (Required) The request body, as described in
       https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs#TransferJob
       With three additional improvements:

       * dates can be given in the form :class:`datetime.date`
       * times can be given in the form :class:`datetime.time`
       * credentials to Amazon Web Service should be stored in the connection and indicated by the
         aws_conn_id parameter

   :type body: dict
   :param aws_conn_id: The connection ID used to retrieve credentials to
       Amazon Web Service.
   :type aws_conn_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1).
   :type api_version: str
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['body', 'gcp_conn_id', 'aws_conn_id', 'google_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudDataTransferServiceUpdateJobOperator(*, job_name: str, body: dict, aws_conn_id: str = 'aws_default', gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates a transfer job that runs periodically.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataTransferServiceUpdateJobOperator`

   :param job_name: (Required) Name of the job to be updated
   :type job_name: str
   :param body: (Required) The request body, as described in
       https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
       With three additional improvements:

       * dates can be given in the form :class:`datetime.date`
       * times can be given in the form :class:`datetime.time`
       * credentials to Amazon Web Service should be stored in the connection and indicated by the
         aws_conn_id parameter

   :type body: dict
   :param aws_conn_id: The connection ID used to retrieve credentials to
       Amazon Web Service.
   :type aws_conn_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1).
   :type api_version: str
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['job_name', 'body', 'gcp_conn_id', 'aws_conn_id', 'google_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudDataTransferServiceDeleteJobOperator(*, job_name: str, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', project_id: Optional[str] = None, google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Delete a transfer job. This is a soft delete. After a transfer job is
   deleted, the job and all the transfer executions are subject to garbage
   collection. Transfer jobs become eligible for garbage collection
   30 days after soft delete.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataTransferServiceDeleteJobOperator`

   :param job_name: (Required) Name of the TRANSFER operation
   :type job_name: str
   :param project_id: (Optional) the ID of the project that owns the Transfer
       Job. If set to None or missing, the default project_id from the Google Cloud
       connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1).
   :type api_version: str
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['job_name', 'project_id', 'gcp_conn_id', 'api_version', 'google_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudDataTransferServiceGetOperationOperator(*, operation_name: str, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets the latest state of a long-running operation in Google Storage Transfer
   Service.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataTransferServiceGetOperationOperator`

   :param operation_name: (Required) Name of the transfer operation.
   :type operation_name: str
   :param gcp_conn_id: The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1).
   :type api_version: str
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['operation_name', 'gcp_conn_id', 'google_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudDataTransferServiceListOperationsOperator(request_filter: Optional[Dict] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists long-running operations in Google Storage Transfer
   Service that match the specified filter.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataTransferServiceListOperationsOperator`

   :param request_filter: (Required) A request filter, as described in
           https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
   :type request_filter: dict
   :param gcp_conn_id: The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1).
   :type api_version: str
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['filter', 'gcp_conn_id', 'google_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudDataTransferServicePauseOperationOperator(*, operation_name: str, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Pauses a transfer operation in Google Storage Transfer Service.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataTransferServicePauseOperationOperator`

   :param operation_name: (Required) Name of the transfer operation.
   :type operation_name: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version:  API version used (e.g. v1).
   :type api_version: str
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['operation_name', 'gcp_conn_id', 'api_version', 'google_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudDataTransferServiceResumeOperationOperator(*, operation_name: str, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Resumes a transfer operation in Google Storage Transfer Service.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataTransferServiceResumeOperationOperator`

   :param operation_name: (Required) Name of the transfer operation.
   :type operation_name: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :param api_version: API version used (e.g. v1).
   :type api_version: str
   :type gcp_conn_id: str
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['operation_name', 'gcp_conn_id', 'api_version', 'google_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudDataTransferServiceCancelOperationOperator(*, operation_name: str, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Cancels a transfer operation in Google Storage Transfer Service.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataTransferServiceCancelOperationOperator`

   :param operation_name: (Required) Name of the transfer operation.
   :type operation_name: str
   :param api_version: API version used (e.g. v1).
   :type api_version: str
   :param gcp_conn_id: The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['operation_name', 'gcp_conn_id', 'api_version', 'google_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudDataTransferServiceS3ToGCSOperator(*, s3_bucket: str, gcs_bucket: str, project_id: Optional[str] = None, aws_conn_id: str = 'aws_default', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, description: Optional[str] = None, schedule: Optional[Dict] = None, object_conditions: Optional[Dict] = None, transfer_options: Optional[Dict] = None, wait: bool = True, timeout: Optional[float] = None, google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Synchronizes an S3 bucket with a Google Cloud Storage bucket using the
   Google Cloud Storage Transfer Service.

   .. warning::

       This operator is NOT idempotent. If you run it many times, many transfer
       jobs will be created in the Google Cloud.

   **Example**:

   .. code-block:: python

      s3_to_gcs_transfer_op = S3ToGoogleCloudStorageTransferOperator(
           task_id='s3_to_gcs_transfer_example',
           s3_bucket='my-s3-bucket',
           project_id='my-gcp-project',
           gcs_bucket='my-gcs-bucket',
           dag=my_dag)

   :param s3_bucket: The S3 bucket where to find the objects. (templated)
   :type s3_bucket: str
   :param gcs_bucket: The destination Google Cloud Storage bucket
       where you want to store the files. (templated)
   :type gcs_bucket: str
   :param project_id: Optional ID of the Google Cloud Console project that
       owns the job
   :type project_id: str
   :param aws_conn_id: The source S3 connection
   :type aws_conn_id: str
   :param gcp_conn_id: The destination connection ID to use
       when connecting to Google Cloud Storage.
   :type gcp_conn_id: str
   :param delegate_to: Google account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :param description: Optional transfer service job description
   :type description: str
   :param schedule: Optional transfer service schedule;
       If not set, run transfer job once as soon as the operator runs
       The format is described
       https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs.
       With two additional improvements:

       * dates they can be passed as :class:`datetime.date`
       * times they can be passed as :class:`datetime.time`

   :type schedule: dict
   :param object_conditions: Optional transfer service object conditions; see
       https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec
   :type object_conditions: dict
   :param transfer_options: Optional transfer service transfer options; see
       https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec
   :type transfer_options: dict
   :param wait: Wait for transfer to finish
   :type wait: bool
   :param timeout: Time to wait for the operation to end in seconds. Defaults to 60 seconds if not specified.
   :type timeout: Optional[Union[float, timedelta]]
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['gcp_conn_id', 's3_bucket', 'gcs_bucket', 'description', 'object_conditions', 'google_impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #e09411

      

   
   .. method:: execute(self, context)



   
   .. method:: _create_body(self)




.. py:class:: CloudDataTransferServiceGCSToGCSOperator(*, source_bucket: str, destination_bucket: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, description: Optional[str] = None, schedule: Optional[Dict] = None, object_conditions: Optional[Dict] = None, transfer_options: Optional[Dict] = None, wait: bool = True, timeout: Optional[float] = None, google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Copies objects from a bucket to another using the Google Cloud Storage Transfer Service.

   .. warning::

       This operator is NOT idempotent. If you run it many times, many transfer
       jobs will be created in the Google Cloud.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSToGCSOperator`

   **Example**:

   .. code-block:: python

      gcs_to_gcs_transfer_op = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
           task_id='gcs_to_gcs_transfer_example',
           source_bucket='my-source-bucket',
           destination_bucket='my-destination-bucket',
           project_id='my-gcp-project',
           dag=my_dag)

   :param source_bucket: The source Google Cloud Storage bucket where the
        object is. (templated)
   :type source_bucket: str
   :param destination_bucket: The destination Google Cloud Storage bucket
       where the object should be. (templated)
   :type destination_bucket: str
   :param project_id: The ID of the Google Cloud Console project that
       owns the job
   :type project_id: str
   :param gcp_conn_id: Optional connection ID to use when connecting to Google Cloud
       Storage.
   :type gcp_conn_id: str
   :param delegate_to: Google account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :param description: Optional transfer service job description
   :type description: str
   :param schedule: Optional transfer service schedule;
       If not set, run transfer job once as soon as the operator runs
       See:
       https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs.
       With two additional improvements:

       * dates they can be passed as :class:`datetime.date`
       * times they can be passed as :class:`datetime.time`

   :type schedule: dict
   :param object_conditions: Optional transfer service object conditions; see
       https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec#ObjectConditions
   :type object_conditions: dict
   :param transfer_options: Optional transfer service transfer options; see
       https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec#TransferOptions
   :type transfer_options: dict
   :param wait: Wait for transfer to finish; defaults to `True`
   :type wait: bool
   :param timeout: Time to wait for the operation to end in seconds. Defaults to 60 seconds if not specified.
   :type timeout: Optional[Union[float, timedelta]]
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['gcp_conn_id', 'source_bucket', 'destination_bucket', 'description', 'object_conditions', 'google_impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #e09411

      

   
   .. method:: execute(self, context)



   
   .. method:: _create_body(self)




