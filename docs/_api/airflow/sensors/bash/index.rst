:mod:`airflow.sensors.bash`
===========================

.. py:module:: airflow.sensors.bash


Module Contents
---------------

.. py:class:: BashSensor(*, bash_command, env=None, output_encoding='utf-8', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Executes a bash command/script and returns True if and only if the
   return code is 0.

   :param bash_command: The command, set of commands or reference to a
       bash script (must be '.sh') to be executed.
   :type bash_command: str

   :param env: If env is not None, it must be a mapping that defines the
       environment variables for the new process; these are used instead
       of inheriting the current process environment, which is the default
       behavior. (templated)
   :type env: dict
   :param output_encoding: output encoding of bash command.
   :type output_encoding: str

   .. attribute:: template_fields
      :annotation: = ['bash_command', 'env']

      

   
   .. method:: poke(self, context)

      Execute the bash command in a temporary directory
      which will be cleaned afterwards




