:mod:`airflow.providers.google.marketing_platform.sensors.display_video`
========================================================================

.. py:module:: airflow.providers.google.marketing_platform.sensors.display_video

.. autoapi-nested-parse::

   Sensor for detecting the completion of DV360 reports.



Module Contents
---------------

.. py:class:: GoogleDisplayVideo360ReportSensor(*, report_id: str, api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Sensor for detecting the completion of DV360 reports.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleDisplayVideo360ReportSensor`

   :param report_id: Report ID to delete.
   :type report_id: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['report_id', 'impersonation_chain']

      

   
   .. method:: poke(self, context: dict)




.. py:class:: GoogleDisplayVideo360GetSDFDownloadOperationSensor(operation_name: str, api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, mode: str = 'reschedule', poke_interval: int = 60 * 5, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, *args, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Sensor for detecting the completion of SDF operation.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleDisplayVideo360GetSDFDownloadOperationSensor`

   :param name: The name of the operation resource
   :type name: Dict[str, Any]
   :param api_version: The version of the api that will be requested for example 'v1'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['operation_name', 'impersonation_chain']

      

   
   .. method:: poke(self, context: dict)




