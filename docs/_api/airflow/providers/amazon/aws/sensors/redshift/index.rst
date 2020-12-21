:mod:`airflow.providers.amazon.aws.sensors.redshift`
====================================================

.. py:module:: airflow.providers.amazon.aws.sensors.redshift


Module Contents
---------------

.. py:class:: AwsRedshiftClusterSensor(*, cluster_identifier: str, target_status: str = 'available', aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a Redshift cluster to reach a specific status.

   :param cluster_identifier: The identifier for the cluster being pinged.
   :type cluster_identifier: str
   :param target_status: The cluster status desired.
   :type target_status: str

   .. attribute:: template_fields
      :annotation: = ['cluster_identifier', 'target_status']

      

   
   .. method:: poke(self, context)



   
   .. method:: get_hook(self)

      Create and return a RedshiftHook




