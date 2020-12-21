:mod:`airflow.providers.microsoft.azure.sensors.azure_cosmos`
=============================================================

.. py:module:: airflow.providers.microsoft.azure.sensors.azure_cosmos


Module Contents
---------------

.. py:class:: AzureCosmosDocumentSensor(*, database_name: str, collection_name: str, document_id: str, azure_cosmos_conn_id: str = 'azure_cosmos_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for the existence of a document which
   matches the given query in CosmosDB. Example:

   >>> azure_cosmos_sensor = AzureCosmosDocumentSensor(database_name="somedatabase_name",
   ...                            collection_name="somecollection_name",
   ...                            document_id="unique-doc-id",
   ...                            azure_cosmos_conn_id="azure_cosmos_default",
   ...                            task_id="azure_cosmos_sensor")

   :param database_name: Target CosmosDB database_name.
   :type database_name: str
   :param collection_name: Target CosmosDB collection_name.
   :type collection_name: str
   :param document_id: The ID of the target document.
   :type query: str
   :param azure_cosmos_conn_id: Reference to the Azure CosmosDB connection.
   :type azure_cosmos_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['database_name', 'collection_name', 'document_id']

      

   
   .. method:: poke(self, context: dict)




