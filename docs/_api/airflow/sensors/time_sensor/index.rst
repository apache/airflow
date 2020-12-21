:mod:`airflow.sensors.time_sensor`
==================================

.. py:module:: airflow.sensors.time_sensor


Module Contents
---------------

.. py:class:: TimeSensor(*, target_time, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits until the specified time of the day.

   :param target_time: time after which the job succeeds
   :type target_time: datetime.time

   
   .. method:: poke(self, context)




