:mod:`airflow.providers.google.cloud.hooks.bigquery_dts`
========================================================

.. py:module:: airflow.providers.google.cloud.hooks.bigquery_dts

.. autoapi-nested-parse::

   This module contains a BigQuery Hook.



Module Contents
---------------

.. function:: get_object_id(obj: dict) -> str
   Returns unique id of the object.


.. py:class:: BiqQueryDataTransferServiceHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Bigquery Transfer API.

   All the methods in the hook where ``project_id`` is used must be called with
   keyword arguments rather than positional.

   .. attribute:: _conn
      :annotation: :Optional[Resource]

      

   
   .. staticmethod:: _disable_auto_scheduling(config: Union[dict, TransferConfig])

      In the case of Airflow, the customer needs to create a transfer config
      with the automatic scheduling disabled (UI, CLI or an Airflow operator) and
      then trigger a transfer run using a specialized Airflow operator that will
      call start_manual_transfer_runs.

      :param config: Data transfer configuration to create.
      :type config: Union[dict, google.cloud.bigquery_datatransfer_v1.types.TransferConfig]



   
   .. method:: get_conn(self)

      Retrieves connection to Google Bigquery.

      :return: Google Bigquery API client
      :rtype: google.cloud.bigquery_datatransfer_v1.DataTransferServiceClient



   
   .. method:: create_transfer_config(self, transfer_config: Union[dict, TransferConfig], project_id: str, authorization_code: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Creates a new data transfer configuration.

      :param transfer_config: Data transfer configuration to create.
      :type transfer_config: Union[dict, google.cloud.bigquery_datatransfer_v1.types.TransferConfig]
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
      :return: A ``google.cloud.bigquery_datatransfer_v1.types.TransferConfig`` instance.



   
   .. method:: delete_transfer_config(self, transfer_config_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Deletes transfer configuration.

      :param transfer_config_id: Id of transfer config to be used.
      :type transfer_config_id: str
      :param project_id: The BigQuery project id where the transfer configuration should be
          created. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
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
      :return: None



   
   .. method:: start_manual_transfer_runs(self, transfer_config_id: str, project_id: str, requested_time_range: Optional[dict] = None, requested_run_time: Optional[dict] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Start manual transfer runs to be executed now with schedule_time equal
      to current time. The transfer runs can be created for a time range where
      the run_time is between start_time (inclusive) and end_time
      (exclusive), or for a specific run_time.

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
          created. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
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
      :return: An ``google.cloud.bigquery_datatransfer_v1.types.StartManualTransferRunsResponse`` instance.



   
   .. method:: get_transfer_run(self, run_id: str, transfer_config_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Returns information about the particular transfer run.

      :param run_id: ID of the transfer run.
      :type run_id: str
      :param transfer_config_id: ID of transfer config to be used.
      :type transfer_config_id: str
      :param project_id: The BigQuery project id where the transfer configuration should be
          created. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
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
      :return: An ``google.cloud.bigquery_datatransfer_v1.types.TransferRun`` instance.




