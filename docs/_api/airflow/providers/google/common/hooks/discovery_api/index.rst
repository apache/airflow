:mod:`airflow.providers.google.common.hooks.discovery_api`
==========================================================

.. py:module:: airflow.providers.google.common.hooks.discovery_api

.. autoapi-nested-parse::

   This module allows you to connect to the Google Discovery API Service and query it.



Module Contents
---------------

.. py:class:: GoogleDiscoveryApiHook(api_service_name: str, api_version: str, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   A hook to use the Google API Discovery Service.

   :param api_service_name: The name of the api service that is needed to get the data
       for example 'youtube'.
   :type api_service_name: str
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
       account from the list granting this role to the originating account.
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: _conn
      :annotation: :Optional[Resource]

      

   
   .. method:: get_conn(self)

      Creates an authenticated api client for the given api service name and credentials.

      :return: the authenticated api service.
      :rtype: Resource



   
   .. method:: query(self, endpoint: str, data: dict, paginate: bool = False, num_retries: int = 0)

      Creates a dynamic API call to any Google API registered in Google's API Client Library
      and queries it.

      :param endpoint: The client libraries path to the api call's executing method.
          For example: 'analyticsreporting.reports.batchGet'

          .. seealso:: https://developers.google.com/apis-explorer
              for more information on what methods are available.
      :type endpoint: str
      :param data: The data (endpoint params) needed for the specific request to given endpoint.
      :type data: dict
      :param paginate: If set to True, it will collect all pages of data.
      :type paginate: bool
      :param num_retries: Define the number of retries for the requests being made if it fails.
      :type num_retries: int
      :return: the API response from the passed endpoint.
      :rtype: dict



   
   .. method:: _call_api_request(self, google_api_conn_client, endpoint, data, paginate, num_retries)



   
   .. method:: _build_api_request(self, google_api_conn_client, api_sub_functions, api_endpoint_params)



   
   .. method:: _paginate_api(self, google_api_endpoint_instance, google_api_conn_client, api_endpoint_parts, num_retries)



   
   .. method:: _build_next_api_request(self, google_api_conn_client, api_sub_functions, api_endpoint_instance, api_response)




