:mod:`airflow.providers.google.marketing_platform.sensors.search_ads`
=====================================================================

.. py:module:: airflow.providers.google.marketing_platform.sensors.search_ads

.. autoapi-nested-parse::

   This module contains Google Search Ads sensor.



Module Contents
---------------

.. py:class:: GoogleSearchAdsReportSensor(*, report_id: str, api_version: str = 'v2', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, mode: str = 'reschedule', poke_interval: int = 5 * 60, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Polls for the status of a report request.

   .. seealso::
       For API documentation check:
       https://developers.google.com/search-ads/v2/reference/reports/get

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleSearchAdsReportSensor`

   :param report_id: ID of the report request being polled.
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




