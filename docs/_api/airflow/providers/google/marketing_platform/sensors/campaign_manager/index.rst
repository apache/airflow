:mod:`airflow.providers.google.marketing_platform.sensors.campaign_manager`
===========================================================================

.. py:module:: airflow.providers.google.marketing_platform.sensors.campaign_manager

.. autoapi-nested-parse::

   This module contains Google Campaign Manager sensor.



Module Contents
---------------

.. py:class:: GoogleCampaignManagerReportSensor(*, profile_id: str, report_id: str, file_id: str, api_version: str = 'v3.3', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, mode: str = 'reschedule', poke_interval: int = 60 * 5, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Check if report is ready.

   .. seealso::
       Check official API docs:
       https://developers.google.com/doubleclick-advertisers/v3.3/reports/get

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleCampaignManagerReportSensor`

   :param profile_id: The DFA user profile ID.
   :type profile_id: str
   :param report_id: The ID of the report.
   :type report_id: str
   :param file_id: The ID of the report file.
   :type file_id: str
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
      :annotation: = ['profile_id', 'report_id', 'file_id', 'impersonation_chain']

      

   
   .. method:: poke(self, context: Dict)




