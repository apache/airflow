:mod:`airflow.providers.apache.hive.sensors.hive_partition`
===========================================================

.. py:module:: airflow.providers.apache.hive.sensors.hive_partition


Module Contents
---------------

.. py:class:: HivePartitionSensor(*, table: str, partition: Optional[str] = "ds='{{ ds }}'", metastore_conn_id: str = 'metastore_default', schema: str = 'default', poke_interval: int = 60 * 3, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a partition to show up in Hive.

   Note: Because ``partition`` supports general logical operators, it
   can be inefficient. Consider using NamedHivePartitionSensor instead if
   you don't need the full flexibility of HivePartitionSensor.

   :param table: The name of the table to wait for, supports the dot
       notation (my_database.my_table)
   :type table: str
   :param partition: The partition clause to wait for. This is passed as
       is to the metastore Thrift client ``get_partitions_by_filter`` method,
       and apparently supports SQL like notation as in ``ds='2015-01-01'
       AND type='value'`` and comparison operators as in ``"ds>=2015-01-01"``
   :type partition: str
   :param metastore_conn_id: reference to the metastore thrift service
       connection id
   :type metastore_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['schema', 'table', 'partition']

      

   .. attribute:: ui_color
      :annotation: = #C5CAE9

      

   
   .. method:: poke(self, context: Dict[str, Any])




