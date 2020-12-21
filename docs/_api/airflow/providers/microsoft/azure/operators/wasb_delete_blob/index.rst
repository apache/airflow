:mod:`airflow.providers.microsoft.azure.operators.wasb_delete_blob`
===================================================================

.. py:module:: airflow.providers.microsoft.azure.operators.wasb_delete_blob


Module Contents
---------------

.. py:class:: WasbDeleteBlobOperator(*, container_name: str, blob_name: str, wasb_conn_id: str = 'wasb_default', check_options: Any = None, is_prefix: bool = False, ignore_if_missing: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes blob(s) on Azure Blob Storage.

   :param container_name: Name of the container. (templated)
   :type container_name: str
   :param blob_name: Name of the blob. (templated)
   :type blob_name: str
   :param wasb_conn_id: Reference to the wasb connection.
   :type wasb_conn_id: str
   :param check_options: Optional keyword arguments that
       `WasbHook.check_for_blob()` takes.
   :param is_prefix: If blob_name is a prefix, delete all files matching prefix.
   :type is_prefix: bool
   :param ignore_if_missing: if True, then return success even if the
       blob does not exist.
   :type ignore_if_missing: bool

   .. attribute:: template_fields
      :annotation: = ['container_name', 'blob_name']

      

   
   .. method:: execute(self, context: dict)




