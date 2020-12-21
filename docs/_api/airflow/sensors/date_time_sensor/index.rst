:mod:`airflow.sensors.date_time_sensor`
=======================================

.. py:module:: airflow.sensors.date_time_sensor


Module Contents
---------------

.. py:class:: DateTimeSensor(*, target_time: Union[str, datetime.datetime], **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits until the specified datetime.

   A major advantage of this sensor is idempotence for the ``target_time``.
   It handles some cases for which ``TimeSensor`` and ``TimeDeltaSensor`` are not suited.

   **Example** 1 :
       If a task needs to wait for 11am on each ``execution_date``. Using
       ``TimeSensor`` or ``TimeDeltaSensor``, all backfill tasks started at
       1am have to wait for 10 hours. This is unnecessary, e.g. a backfill
       task with ``{{ ds }} = '1970-01-01'`` does not need to wait because
       ``1970-01-01T11:00:00`` has already passed.

   **Example** 2 :
       If a DAG is scheduled to run at 23:00 daily, but one of the tasks is
       required to run at 01:00 next day, using ``TimeSensor`` will return
       ``True`` immediately because 23:00 > 01:00. Instead, we can do this:

       .. code-block:: python

           DateTimeSensor(
               task_id='wait_for_0100',
               target_time='{{ next_execution_date.tomorrow().replace(hour=1) }}',
           )

   :param target_time: datetime after which the job succeeds. (templated)
   :type target_time: str or datetime.datetime

   .. attribute:: template_fields
      :annotation: = ['target_time']

      

   
   .. method:: poke(self, context: Dict)




