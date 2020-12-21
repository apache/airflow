:mod:`airflow.providers.apache.hdfs.sensors.web_hdfs`
=====================================================

.. py:module:: airflow.providers.apache.hdfs.sensors.web_hdfs


Module Contents
---------------

.. py:class:: WebHdfsSensor(*, filepath: str, webhdfs_conn_id: str = 'webhdfs_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a file or folder to land in HDFS

   .. attribute:: template_fields
      :annotation: = ['filepath']

      

   
   .. method:: poke(self, context: Dict[Any, Any])




