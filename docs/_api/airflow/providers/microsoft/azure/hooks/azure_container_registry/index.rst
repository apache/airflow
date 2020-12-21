:mod:`airflow.providers.microsoft.azure.hooks.azure_container_registry`
=======================================================================

.. py:module:: airflow.providers.microsoft.azure.hooks.azure_container_registry

.. autoapi-nested-parse::

   Hook for Azure Container Registry



Module Contents
---------------

.. py:class:: AzureContainerRegistryHook(conn_id: str = 'azure_registry')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   A hook to communicate with a Azure Container Registry.

   :param conn_id: connection id of a service principal which will be used
       to start the container instance
   :type conn_id: str

   
   .. method:: get_conn(self)




