:mod:`airflow.providers.google.cloud.sensors.bigquery_dts`
==========================================================

.. py:module:: airflow.providers.google.cloud.sensors.bigquery_dts

.. autoapi-nested-parse::

   This module contains a Google BigQuery Data Transfer Service sensor.



Module Contents
---------------

.. py:class:: BigQueryDataTransferServiceTransferRunSensor(*, run_id: str, transfer_config_id: str, expected_statuses: Union[Set[str], str] = 'SUCCEEDED', project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', retry: Optional[Retry] = None, request_timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for Data Transfer Service run to complete.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/operator:BigQueryDataTransferServiceTransferRunSensor`

   :param expected_statuses: The expected state of the operation.
       See:
       https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
   :type expected_statuses: Union[Set[str], str]
   :param run_id: ID of the transfer run.
   :type run_id: str
   :param transfer_config_id: ID of transfer config to be used.
   :type transfer_config_id: str
   :param project_id: The BigQuery project id where the transfer configuration should be
       created. If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: Optional[google.api_core.retry.Retry]
   :param request_timeout: The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type request_timeout: Optional[float]
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Optional[Sequence[Tuple[str, str]]]
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :return: An ``google.cloud.bigquery_datatransfer_v1.types.TransferRun`` instance.

   .. attribute:: template_fields
      :annotation: = ['run_id', 'transfer_config_id', 'expected_statuses', 'project_id', 'impersonation_chain']

      

   
   .. method:: poke(self, context: dict)




