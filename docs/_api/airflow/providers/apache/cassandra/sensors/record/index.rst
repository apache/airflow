:mod:`airflow.providers.apache.cassandra.sensors.record`
========================================================

.. py:module:: airflow.providers.apache.cassandra.sensors.record

.. autoapi-nested-parse::

   This module contains sensor that check the existence
   of a record in a Cassandra cluster.



Module Contents
---------------

.. py:class:: CassandraRecordSensor(*, table: str, keys: Dict[str, str], cassandra_conn_id: str, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for the existence of a record in a Cassandra cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CassandraRecordSensor`

   For example, if you want to wait for a record that has values 'v1' and 'v2' for each
   primary keys 'p1' and 'p2' to be populated in keyspace 'k' and table 't',
   instantiate it as follows:

   >>> cassandra_sensor = CassandraRecordSensor(table="k.t",
   ...                                          keys={"p1": "v1", "p2": "v2"},
   ...                                          cassandra_conn_id="cassandra_default",
   ...                                          task_id="cassandra_sensor")

   :param table: Target Cassandra table.
       Use dot notation to target a specific keyspace.
   :type table: str
   :param keys: The keys and their values to be monitored
   :type keys: dict
   :param cassandra_conn_id: The connection ID to use
       when connecting to Cassandra cluster
   :type cassandra_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['table', 'keys']

      

   
   .. method:: poke(self, context: Dict[str, str])




