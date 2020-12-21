:mod:`airflow.sensors.filesystem`
=================================

.. py:module:: airflow.sensors.filesystem


Module Contents
---------------

.. py:class:: FileSensor(*, filepath, fs_conn_id='fs_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a file or folder to land in a filesystem.

   If the path given is a directory then this sensor will only return true if
   any files exist inside it (either directly, or within a subdirectory)

   :param fs_conn_id: reference to the File (path)
       connection id
   :type fs_conn_id: str
   :param filepath: File or folder name (relative to
       the base path set within the connection), can be a glob.
   :type filepath: str

   .. attribute:: template_fields
      :annotation: = ['filepath']

      

   .. attribute:: ui_color
      :annotation: = #91818a

      

   
   .. method:: poke(self, context)




