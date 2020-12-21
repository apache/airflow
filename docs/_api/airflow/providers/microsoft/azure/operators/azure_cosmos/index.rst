:mod:`airflow.providers.microsoft.azure.operators.azure_cosmos`
===============================================================

.. py:module:: airflow.providers.microsoft.azure.operators.azure_cosmos


Module Contents
---------------

.. py:class:: AzureCosmosInsertDocumentOperator(*, database_name: str, collection_name: str, document: dict, azure_cosmos_conn_id: str = 'azure_cosmos_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Inserts a new document into the specified Cosmos database and collection
   It will create both the database and collection if they do not already exist

   :param database_name: The name of the database. (templated)
   :type database_name: str
   :param collection_name: The name of the collection. (templated)
   :type collection_name: str
   :param document: The document to insert
   :type document: dict
   :param azure_cosmos_conn_id: reference to a CosmosDB connection.
   :type azure_cosmos_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['database_name', 'collection_name']

      

   .. attribute:: ui_color
      :annotation: = #e4f0e8

      

   
   .. method:: execute(self, context: dict)




