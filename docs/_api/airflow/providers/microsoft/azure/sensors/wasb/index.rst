:mod:`airflow.providers.microsoft.azure.sensors.wasb`
=====================================================

.. py:module:: airflow.providers.microsoft.azure.sensors.wasb


Module Contents
---------------

.. py:class:: WasbBlobSensor(*, container_name: str, blob_name: str, wasb_conn_id: str = 'wasb_default', check_options: Optional[dict] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a blob to arrive on Azure Blob Storage.

   :param container_name: Name of the container.
   :type container_name: str
   :param blob_name: Name of the blob.
   :type blob_name: str
   :param wasb_conn_id: Reference to the wasb connection.
   :type wasb_conn_id: str
   :param check_options: Optional keyword arguments that
       `WasbHook.check_for_blob()` takes.
   :type check_options: dict

   .. attribute:: template_fields
      :annotation: = ['container_name', 'blob_name']

      

   
   .. method:: poke(self, context: dict)




.. py:class:: WasbPrefixSensor(*, container_name: str, prefix: str, wasb_conn_id: str = 'wasb_default', check_options: Optional[dict] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for blobs matching a prefix to arrive on Azure Blob Storage.

   :param container_name: Name of the container.
   :type container_name: str
   :param prefix: Prefix of the blob.
   :type prefix: str
   :param wasb_conn_id: Reference to the wasb connection.
   :type wasb_conn_id: str
   :param check_options: Optional keyword arguments that
       `WasbHook.check_for_prefix()` takes.
   :type check_options: dict

   .. attribute:: template_fields
      :annotation: = ['container_name', 'prefix']

      

   
   .. method:: poke(self, context: dict)




