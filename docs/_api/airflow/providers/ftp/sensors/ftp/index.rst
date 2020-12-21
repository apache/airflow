:mod:`airflow.providers.ftp.sensors.ftp`
========================================

.. py:module:: airflow.providers.ftp.sensors.ftp


Module Contents
---------------

.. py:class:: FTPSensor(*, path: str, ftp_conn_id: str = 'ftp_default', fail_on_transient_errors: bool = True, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a file or directory to be present on FTP.

   :param path: Remote file or directory path
   :type path: str
   :param fail_on_transient_errors: Fail on all errors,
       including 4xx transient errors. Default True.
   :type fail_on_transient_errors: bool
   :param ftp_conn_id: The connection to run the sensor against
   :type ftp_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['path']

      Errors that are transient in nature, and where action can be retried


   .. attribute:: transient_errors
      :annotation: = [421, 425, 426, 434, 450, 451, 452]

      

   .. attribute:: error_code_pattern
      

      

   
   .. method:: _create_hook(self)

      Return connection hook.



   
   .. method:: _get_error_code(self, e)

      Extract error code from ftp exception



   
   .. method:: poke(self, context: dict)




.. py:class:: FTPSSensor

   Bases: :class:`airflow.providers.ftp.sensors.ftp.FTPSensor`

   Waits for a file or directory to be present on FTP over SSL.

   
   .. method:: _create_hook(self)

      Return connection hook.




