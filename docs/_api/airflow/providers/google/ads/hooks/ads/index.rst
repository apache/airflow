:mod:`airflow.providers.google.ads.hooks.ads`
=============================================

.. py:module:: airflow.providers.google.ads.hooks.ads

.. autoapi-nested-parse::

   This module contains Google Ad hook.



Module Contents
---------------

.. py:class:: GoogleAdsHook(gcp_conn_id: str = 'google_cloud_default', google_ads_conn_id: str = 'google_ads_default', api_version: str = 'v3')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Hook for the Google Ads API.

   This hook requires two connections:

       - gcp_conn_id - provides service account details (like any other GCP connection)
       - google_ads_conn_id - which contains information from Google Ads config.yaml file
         in the ``extras``. Example of the ``extras``:

       .. code-block:: json

           {
               "google_ads_client": {
                   "developer_token": "{{ INSERT_TOKEN }}",
                   "path_to_private_key_file": null,
                   "delegated_account": "{{ INSERT_DELEGATED_ACCOUNT }}"
               }
           }

       The ``path_to_private_key_file`` is resolved by the hook using credentials from gcp_conn_id.
       https://developers.google.com/google-ads/api/docs/client-libs/python/oauth-service

   .. seealso::
       For more information on how Google Ads authentication flow works take a look at:
       https://developers.google.com/google-ads/api/docs/client-libs/python/oauth-service

   .. seealso::
       For more information on the Google Ads API, take a look at the API docs:
       https://developers.google.com/google-ads/api/docs/start

   :param gcp_conn_id: The connection ID with the service account details.
   :type gcp_conn_id: str
   :param google_ads_conn_id: The connection ID with the details of Google Ads config.yaml file.
   :type google_ads_conn_id: str

   :return: list of Google Ads Row object(s)
   :rtype: list[GoogleAdsRow]

   
   .. method:: _get_service(self)

      Connects and authenticates with the Google Ads API using a service account



   
   .. method:: _get_customer_service(self)

      Connects and authenticates with the Google Ads API using a service account



   
   .. method:: _get_config(self)

      Gets google ads connection from meta db and sets google_ads_config attribute with returned config
      file



   
   .. method:: _update_config_with_secret(self, secrets_temp: IO[str])

      Gets Google Cloud secret from connection and saves the contents to the temp file
      Updates google ads config with file path of the temp file containing the secret
      Note, the secret must be passed as a file path for Google Ads API



   
   .. method:: search(self, client_ids: List[str], query: str, page_size: int = 10000, **kwargs)

      Pulls data from the Google Ads API

      :param client_ids: Google Ads client ID(s) to query the API for.
      :type client_ids: List[str]
      :param query: Google Ads Query Language query.
      :type query: str
      :param page_size: Number of results to return per page. Max 10000.
      :type page_size: int

      :return: Google Ads API response, converted to Google Ads Row objects
      :rtype: list[GoogleAdsRow]



   
   .. method:: _extract_rows(self, iterators: Generator[GRPCIterator, None, None])

      Convert Google Page Iterator (GRPCIterator) objects to Google Ads Rows

      :param iterators: List of Google Page Iterator (GRPCIterator) objects
      :type iterators: generator[GRPCIterator, None, None]

      :return: API response for all clients in the form of Google Ads Row object(s)
      :rtype: list[GoogleAdsRow]



   
   .. method:: list_accessible_customers(self)

      Returns resource names of customers directly accessible by the user authenticating the call.
      The resulting list of customers is based on your OAuth credentials. The request returns a list
      of all accounts that you are able to act upon directly given your current credentials. This will
      not necessarily include all accounts within the account hierarchy; rather, it will only include
      accounts where your authenticated user has been added with admin or other rights in the account.

      ..seealso::
          https://developers.google.com/google-ads/api/reference/rpc

      :return: List of names of customers




