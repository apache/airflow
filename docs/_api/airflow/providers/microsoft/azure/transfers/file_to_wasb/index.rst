:mod:`airflow.providers.microsoft.azure.transfers.file_to_wasb`
===============================================================

.. py:module:: airflow.providers.microsoft.azure.transfers.file_to_wasb


Module Contents
---------------

.. py:class:: FileToWasbOperator(*, file_path: str, container_name: str, blob_name: str, wasb_conn_id: str = 'wasb_default', load_options: Optional[dict] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Uploads a file to Azure Blob Storage.

   :param file_path: Path to the file to load. (templated)
   :type file_path: str
   :param container_name: Name of the container. (templated)
   :type container_name: str
   :param blob_name: Name of the blob. (templated)
   :type blob_name: str
   :param wasb_conn_id: Reference to the wasb connection.
   :type wasb_conn_id: str
   :param load_options: Optional keyword arguments that
       `WasbHook.load_file()` takes.
   :type load_options: Optional[dict]

   .. attribute:: template_fields
      :annotation: = ['file_path', 'container_name', 'blob_name']

      

   
   .. method:: execute(self, context: dict)

      Upload a file to Azure Blob Storage.




