:mod:`airflow.providers.microsoft.azure.hooks.adx`
==================================================

.. py:module:: airflow.providers.microsoft.azure.hooks.adx

.. autoapi-nested-parse::

   This module contains Azure Data Explorer hook



Module Contents
---------------

.. py:class:: AzureDataExplorerHook(azure_data_explorer_conn_id: str = 'azure_data_explorer_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interacts with Azure Data Explorer (Kusto).

   Extra JSON field contains the following parameters:

   .. code-block:: json

       {
           "tenant": "<Tenant ID>",
           "auth_method": "<Authentication method>",
           "certificate": "<Application PEM certificate>",
           "thumbprint": "<Application certificate thumbprint>"
       }

   **Cluster**:

   Azure Data Explorer cluster is specified by a URL, for example: "https://help.kusto.windows.net".
   The parameter must be provided through `Host` connection detail.

   **Tenant ID**:

   To learn about tenants refer to: https://docs.microsoft.com/en-us/onedrive/find-your-office-365-tenant-id

   **Authentication methods**:

   Authentication method must be provided through "auth_method" extra parameter.
   Available authentication methods are:

     - AAD_APP : Authentication with AAD application certificate. Extra parameters:
                 "tenant" is required when using this method. Provide application ID
                 and application key through username and password parameters.

     - AAD_APP_CERT: Authentication with AAD application certificate. Extra parameters:
                     "tenant", "certificate" and "thumbprint" are required
                     when using this method.

     - AAD_CREDS : Authentication with AAD username and password. Extra parameters:
                   "tenant" is required when using this method. Username and password
                   parameters are used for authentication with AAD.

     - AAD_DEVICE : Authenticate with AAD device code. Please note that if you choose
                    this option, you'll need to authenticate for every new instance
                    that is initialized. It is highly recommended to create one instance
                    and use it for all queries.

   :param azure_data_explorer_conn_id: Reference to the Azure Data Explorer connection.
   :type azure_data_explorer_conn_id: str

   
   .. method:: get_conn(self)

      Return a KustoClient object.



   
   .. method:: run_query(self, query: str, database: str, options: Optional[Dict] = None)

      Run KQL query using provided configuration, and return
      `azure.kusto.data.response.KustoResponseDataSet` instance.
      If query is unsuccessful AirflowException is raised.

      :param query: KQL query to run
      :type query: str
      :param database: Database to run the query on.
      :type database: str
      :param options: Optional query options. See:
         https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
      :type options: dict
      :return: dict




