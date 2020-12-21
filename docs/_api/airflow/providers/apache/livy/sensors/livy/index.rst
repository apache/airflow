:mod:`airflow.providers.apache.livy.sensors.livy`
=================================================

.. py:module:: airflow.providers.apache.livy.sensors.livy

.. autoapi-nested-parse::

   This module contains the Apache Livy sensor.



Module Contents
---------------

.. py:class:: LivySensor(*, batch_id: Union[int, str], livy_conn_id: str = 'livy_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Monitor a Livy sessions for termination.

   :param livy_conn_id: reference to a pre-defined Livy connection
   :type livy_conn_id: str
   :param batch_id: identifier of the monitored batch
   :type batch_id: Union[int, str]

   .. attribute:: template_fields
      :annotation: = ['batch_id']

      

   
   .. method:: get_hook(self)

      Get valid hook.

      :return: hook
      :rtype: LivyHook



   
   .. method:: poke(self, context: Dict[Any, Any])




