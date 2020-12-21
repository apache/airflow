:mod:`airflow.providers.microsoft.azure.hooks.base_azure`
=========================================================

.. py:module:: airflow.providers.microsoft.azure.hooks.base_azure


Module Contents
---------------

.. py:class:: AzureBaseHook(sdk_client: Any, conn_id: str = 'azure_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   This hook acts as a base hook for azure services. It offers several authentication mechanisms to
   authenticate the client library used for upstream azure hooks.

   :param sdk_client: The SDKClient to use.
   :param conn_id: The azure connection id which refers to the information to connect to the service.

   
   .. method:: get_conn(self)

      Authenticates the resource using the connection id passed during init.

      :return: the authenticated client.




