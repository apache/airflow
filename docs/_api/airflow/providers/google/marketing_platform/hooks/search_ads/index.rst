:mod:`airflow.providers.google.marketing_platform.hooks.search_ads`
===================================================================

.. py:module:: airflow.providers.google.marketing_platform.hooks.search_ads

.. autoapi-nested-parse::

   This module contains Google Search Ads 360 hook.



Module Contents
---------------

.. py:class:: GoogleSearchAdsHook(api_version: str = 'v2', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Search Ads 360.

   .. attribute:: _conn
      :annotation: :Optional[Any]

      

   
   .. method:: get_conn(self)

      Retrieves connection to Google SearchAds.



   
   .. method:: insert_report(self, report: Dict[str, Any])

      Inserts a report request into the reporting system.

      :param report: Report to be generated.
      :type report: Dict[str, Any]



   
   .. method:: get(self, report_id: str)

      Polls for the status of a report request.

      :param report_id: ID of the report request being polled.
      :type report_id: str



   
   .. method:: get_file(self, report_fragment: int, report_id: str)

      Downloads a report file encoded in UTF-8.

      :param report_fragment: The index of the report fragment to download.
      :type report_fragment: int
      :param report_id: ID of the report.
      :type report_id: str




