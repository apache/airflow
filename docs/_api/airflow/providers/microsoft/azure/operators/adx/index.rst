:mod:`airflow.providers.microsoft.azure.operators.adx`
======================================================

.. py:module:: airflow.providers.microsoft.azure.operators.adx

.. autoapi-nested-parse::

   This module contains Azure Data Explorer operators



Module Contents
---------------

.. py:class:: AzureDataExplorerQueryOperator(*, query: str, database: str, options: Optional[dict] = None, azure_data_explorer_conn_id: str = 'azure_data_explorer_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Operator for querying Azure Data Explorer (Kusto).

   :param query: KQL query to run (templated).
   :type query: str
   :param database: Database to run the query on (templated).
   :type database: str
   :param options: Optional query options. See:
     https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
   :type options: dict
   :param azure_data_explorer_conn_id: Azure Data Explorer connection to use.
   :type azure_data_explorer_conn_id: str

   .. attribute:: ui_color
      :annotation: = #00a1f2

      

   .. attribute:: template_fields
      :annotation: = ['query', 'database']

      

   .. attribute:: template_ext
      :annotation: = ['.kql']

      

   
   .. method:: get_hook(self)

      Returns new instance of AzureDataExplorerHook



   
   .. method:: execute(self, context: dict)

      Run KQL Query on Azure Data Explorer (Kusto).
      Returns `PrimaryResult` of Query v2 HTTP response contents
      (https://docs.microsoft.com/en-us/azure/kusto/api/rest/response2)




