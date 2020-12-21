:mod:`airflow.providers.microsoft.azure.hooks.azure_cosmos`
===========================================================

.. py:module:: airflow.providers.microsoft.azure.hooks.azure_cosmos

.. autoapi-nested-parse::

   This module contains integration with Azure CosmosDB.

   AzureCosmosDBHook communicates via the Azure Cosmos library. Make sure that a
   Airflow connection of type `azure_cosmos` exists. Authorization can be done by supplying a
   login (=Endpoint uri), password (=secret key) and extra fields database_name and collection_name to specify
   the default database and collection to use (see connection `azure_cosmos_default` for an example).



Module Contents
---------------

.. py:class:: AzureCosmosDBHook(azure_cosmos_conn_id: str = 'azure_cosmos_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interacts with Azure CosmosDB.

   login should be the endpoint uri, password should be the master key
   optionally, you can use the following extras to default these values
   {"database_name": "<DATABASE_NAME>", "collection_name": "COLLECTION_NAME"}.

   :param azure_cosmos_conn_id: Reference to the Azure CosmosDB connection.
   :type azure_cosmos_conn_id: str

   
   .. method:: get_conn(self)

      Return a cosmos db client.



   
   .. method:: __get_database_name(self, database_name: Optional[str] = None)



   
   .. method:: __get_collection_name(self, collection_name: Optional[str] = None)



   
   .. method:: does_collection_exist(self, collection_name: str, database_name: str)

      Checks if a collection exists in CosmosDB.



   
   .. method:: create_collection(self, collection_name: str, database_name: Optional[str] = None)

      Creates a new collection in the CosmosDB database.



   
   .. method:: does_database_exist(self, database_name: str)

      Checks if a database exists in CosmosDB.



   
   .. method:: create_database(self, database_name: str)

      Creates a new database in CosmosDB.



   
   .. method:: delete_database(self, database_name: str)

      Deletes an existing database in CosmosDB.



   
   .. method:: delete_collection(self, collection_name: str, database_name: Optional[str] = None)

      Deletes an existing collection in the CosmosDB database.



   
   .. method:: upsert_document(self, document, database_name=None, collection_name=None, document_id=None)

      Inserts a new document (or updates an existing one) into an existing
      collection in the CosmosDB database.



   
   .. method:: insert_documents(self, documents, database_name: Optional[str] = None, collection_name: Optional[str] = None)

      Insert a list of new documents into an existing collection in the CosmosDB database.



   
   .. method:: delete_document(self, document_id: str, database_name: Optional[str] = None, collection_name: Optional[str] = None)

      Delete an existing document out of a collection in the CosmosDB database.



   
   .. method:: get_document(self, document_id: str, database_name: Optional[str] = None, collection_name: Optional[str] = None)

      Get a document from an existing collection in the CosmosDB database.



   
   .. method:: get_documents(self, sql_string: str, database_name: Optional[str] = None, collection_name: Optional[str] = None, partition_key: Optional[str] = None)

      Get a list of documents from an existing collection in the CosmosDB database via SQL query.




.. function:: get_database_link(database_id: str) -> str
   Get Azure CosmosDB database link


.. function:: get_collection_link(database_id: str, collection_id: str) -> str
   Get Azure CosmosDB collection link


.. function:: get_document_link(database_id: str, collection_id: str, document_id: str) -> str
   Get Azure CosmosDB document link


