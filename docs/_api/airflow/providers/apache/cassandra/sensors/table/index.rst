:mod:`airflow.providers.apache.cassandra.sensors.table`
=======================================================

.. py:module:: airflow.providers.apache.cassandra.sensors.table

.. autoapi-nested-parse::

   This module contains sensor that check the existence
   of a table in a Cassandra cluster.



Module Contents
---------------

.. py:class:: CassandraTableSensor(*, table: str, cassandra_conn_id: str, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for the existence of a table in a Cassandra cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CassandraTableSensor`


   For example, if you want to wait for a table called 't' to be created
   in a keyspace 'k', instantiate it as follows:

   >>> cassandra_sensor = CassandraTableSensor(table="k.t",
   ...                                         cassandra_conn_id="cassandra_default",
   ...                                         task_id="cassandra_sensor")

   :param table: Target Cassandra table.
       Use dot notation to target a specific keyspace.
   :type table: str
   :param cassandra_conn_id: The connection ID to use
       when connecting to Cassandra cluster
   :type cassandra_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['table']

      

   
   .. method:: poke(self, context: Dict[Any, Any])




