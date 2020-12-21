:mod:`airflow.providers.google.cloud.operators.bigquery_dts`
============================================================

.. py:module:: airflow.providers.google.cloud.operators.bigquery_dts

.. autoapi-nested-parse::

   This module contains Google BigQuery Data Transfer Service operators.



Module Contents
---------------

.. py:class:: BigQueryCreateDataTransferOperator(*, transfer_config: dict, project_id: Optional[str] = None, authorization_code: Optional[str] = None, retry: Retry = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id='google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new data transfer configuration.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryCreateDataTransferOperator`

   :param transfer_config: Data transfer configuration to create.
   :type transfer_config: dict
   :param project_id: The BigQuery project id where the transfer configuration should be
           created. If set to None or missing, the default project_id from the Google Cloud connection
           is used.
   :type project_id: str
   :param authorization_code: authorization code to use with this transfer configuration.
       This is required if new credentials are needed.
   :type authorization_code: Optional[str]
   :param retry: A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: Optional[google.api_core.retry.Retry]
   :param timeout: The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: Optional[float]
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Optional[Sequence[Tuple[str, str]]]
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
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
      :annotation: = ['transfer_config', 'project_id', 'authorization_code', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryDeleteDataTransferConfigOperator(*, transfer_config_id: str, project_id: Optional[str] = None, retry: Retry = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id='google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes transfer configuration.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryDeleteDataTransferConfigOperator`

   :param transfer_config_id: Id of transfer config to be used.
   :type transfer_config_id: str
   :param project_id: The BigQuery project id where the transfer configuration should be
       created. If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: Optional[google.api_core.retry.Retry]
   :param timeout: The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: Optional[float]
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Optional[Sequence[Tuple[str, str]]]
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
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
      :annotation: = ['transfer_config_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryDataTransferServiceStartTransferRunsOperator(*, transfer_config_id: str, project_id: Optional[str] = None, requested_time_range: Optional[dict] = None, requested_run_time: Optional[dict] = None, retry: Retry = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id='google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Start manual transfer runs to be executed now with schedule_time equal
   to current time. The transfer runs can be created for a time range where
   the run_time is between start_time (inclusive) and end_time
   (exclusive), or for a specific run_time.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryDataTransferServiceStartTransferRunsOperator`

   :param transfer_config_id: Id of transfer config to be used.
   :type transfer_config_id: str
   :param requested_time_range: Time range for the transfer runs that should be started.
       If a dict is provided, it must be of the same form as the protobuf
       message `~google.cloud.bigquery_datatransfer_v1.types.TimeRange`
   :type requested_time_range: Union[dict, ~google.cloud.bigquery_datatransfer_v1.types.TimeRange]
   :param requested_run_time: Specific run_time for a transfer run to be started. The
       requested_run_time must not be in the future.  If a dict is provided, it
       must be of the same form as the protobuf message
       `~google.cloud.bigquery_datatransfer_v1.types.Timestamp`
   :type requested_run_time: Union[dict, ~google.cloud.bigquery_datatransfer_v1.types.Timestamp]
   :param project_id: The BigQuery project id where the transfer configuration should be
       created. If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: Optional[google.api_core.retry.Retry]
   :param timeout: The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: Optional[float]
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Optional[Sequence[Tuple[str, str]]]
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
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
      :annotation: = ['transfer_config_id', 'project_id', 'requested_time_range', 'requested_run_time', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




