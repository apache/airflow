:mod:`airflow.providers.facebook.ads.hooks.ads`
===============================================

.. py:module:: airflow.providers.facebook.ads.hooks.ads

.. autoapi-nested-parse::

   This module contains Facebook Ads Reporting hooks



Module Contents
---------------

.. py:class:: JobStatus

   Bases: :class:`enum.Enum`

   Available options for facebook async task status

   .. attribute:: COMPLETED
      :annotation: = Job Completed

      

   .. attribute:: STARTED
      :annotation: = Job Started

      

   .. attribute:: RUNNING
      :annotation: = Job Running

      

   .. attribute:: FAILED
      :annotation: = Job Failed

      

   .. attribute:: SKIPPED
      :annotation: = Job Skipped

      


.. py:class:: FacebookAdsReportingHook(facebook_conn_id: str = 'facebook_default', api_version: str = 'v6.0')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Hook for the Facebook Ads API

   .. seealso::
       For more information on the Facebook Ads API, take a look at the API docs:
       https://developers.facebook.com/docs/marketing-apis/

   :param facebook_conn_id: Airflow Facebook Ads connection ID
   :type facebook_conn_id: str
   :param api_version: The version of Facebook API. Default to v6.0
   :type api_version: str

   
   .. method:: _get_service(self)

      Returns Facebook Ads Client using a service account



   
   .. method:: facebook_ads_config(self)

      Gets Facebook ads connection from meta db and sets
      facebook_ads_config attribute with returned config file



   
   .. method:: bulk_facebook_report(self, params: Dict[str, Any], fields: List[str], sleep_time: int = 5)

      Pulls data from the Facebook Ads API

      :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
          https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
      :type fields: List[str]
      :param params: Parameters that determine the query for Facebook
          https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
      :type fields: Dict[str, Any]
      :param sleep_time: Time to sleep when async call is happening
      :type sleep_time: int

      :return: Facebook Ads API response, converted to Facebook Ads Row objects
      :rtype: List[AdsInsights]




