:mod:`airflow.providers.google.cloud.sensors.cloud_storage_transfer_service`
============================================================================

.. py:module:: airflow.providers.google.cloud.sensors.cloud_storage_transfer_service

.. autoapi-nested-parse::

   This module contains a Google Cloud Transfer sensor.



Module Contents
---------------

.. py:class:: CloudDataTransferServiceJobStatusSensor(*, job_name: str, expected_statuses: Union[Set[str], str], project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for at least one operation belonging to the job to have the
   expected status.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataTransferServiceJobStatusSensor`

   :param job_name: The name of the transfer job
   :type job_name: str
   :param expected_statuses: The expected state of the operation.
       See:
       https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
   :type expected_statuses: set[str] or string
   :param project_id: (Optional) the ID of the project that owns the Transfer
       Job. If set to None or missing, the default project_id from the Google Cloud
       connection is used.
   :type project_id: str
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
      :annotation: = ['job_name', 'impersonation_chain']

      

   
   .. method:: poke(self, context: dict)




