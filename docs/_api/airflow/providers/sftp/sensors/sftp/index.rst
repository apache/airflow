:mod:`airflow.providers.sftp.sensors.sftp`
==========================================

.. py:module:: airflow.providers.sftp.sensors.sftp

.. autoapi-nested-parse::

   This module contains SFTP sensor.



Module Contents
---------------

.. py:class:: SFTPSensor(*, path: str, sftp_conn_id: str = 'sftp_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a file or directory to be present on SFTP.

   :param path: Remote file or directory path
   :type path: str
   :param sftp_conn_id: The connection to run the sensor against
   :type sftp_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['path']

      

   
   .. method:: poke(self, context: dict)




