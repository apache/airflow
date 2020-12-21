:mod:`airflow.sensors.time_delta_sensor`
========================================

.. py:module:: airflow.sensors.time_delta_sensor


Module Contents
---------------

.. py:class:: TimeDeltaSensor(*, delta, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a timedelta after the task's execution_date + schedule_interval.
   In Airflow, the daily task stamped with ``execution_date``
   2016-01-01 can only start running on 2016-01-02. The timedelta here
   represents the time after the execution period has closed.

   :param delta: time length to wait after execution_date before succeeding
   :type delta: datetime.timedelta

   
   .. method:: poke(self, context)




