:mod:`airflow.providers.apache.hive.sensors.named_hive_partition`
=================================================================

.. py:module:: airflow.providers.apache.hive.sensors.named_hive_partition


Module Contents
---------------

.. py:class:: NamedHivePartitionSensor(*, partition_names: List[str], metastore_conn_id: str = 'metastore_default', poke_interval: int = 60 * 3, hook: Any = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a set of partitions to show up in Hive.

   :param partition_names: List of fully qualified names of the
       partitions to wait for. A fully qualified name is of the
       form ``schema.table/pk1=pv1/pk2=pv2``, for example,
       default.users/ds=2016-01-01. This is passed as is to the metastore
       Thrift client ``get_partitions_by_name`` method. Note that
       you cannot use logical or comparison operators as in
       HivePartitionSensor.
   :type partition_names: list[str]
   :param metastore_conn_id: reference to the metastore thrift service
       connection id
   :type metastore_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['partition_names']

      

   .. attribute:: ui_color
      :annotation: = #8d99ae

      

   .. attribute:: poke_context_fields
      :annotation: = ['partition_names', 'metastore_conn_id']

      

   
   .. staticmethod:: parse_partition_name(partition: str)

      Get schema, table, and partition info.



   
   .. method:: poke_partition(self, partition: str)

      Check for a named partition.



   
   .. method:: poke(self, context: Dict[str, Any])



   
   .. method:: is_smart_sensor_compatible(self)




