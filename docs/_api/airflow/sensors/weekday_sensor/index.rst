:mod:`airflow.sensors.weekday_sensor`
=====================================

.. py:module:: airflow.sensors.weekday_sensor


Module Contents
---------------

.. py:class:: DayOfWeekSensor(*, week_day, use_task_execution_day=False, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits until the first specified day of the week. For example, if the execution
   day of the task is '2018-12-22' (Saturday) and you pass 'FRIDAY', the task will wait
   until next Friday.

   **Example** (with single day): ::

       weekend_check = DayOfWeekSensor(
           task_id='weekend_check',
           week_day='Saturday',
           use_task_execution_day=True,
           dag=dag)

   **Example** (with multiple day using set): ::

       weekend_check = DayOfWeekSensor(
           task_id='weekend_check',
           week_day={'Saturday', 'Sunday'},
           use_task_execution_day=True,
           dag=dag)

   **Example** (with :class:`~airflow.utils.weekday.WeekDay` enum): ::

       # import WeekDay Enum
       from airflow.utils.weekday import WeekDay

       weekend_check = DayOfWeekSensor(
           task_id='weekend_check',
           week_day={WeekDay.SATURDAY, WeekDay.SUNDAY},
           use_task_execution_day=True,
           dag=dag)

   :param week_day: Day of the week to check (full name). Optionally, a set
       of days can also be provided using a set.
       Example values:

           * ``"MONDAY"``,
           * ``{"Saturday", "Sunday"}``
           * ``{WeekDay.TUESDAY}``
           * ``{WeekDay.SATURDAY, WeekDay.SUNDAY}``

   :type week_day: set or str or airflow.utils.weekday.WeekDay
   :param use_task_execution_day: If ``True``, uses task's execution day to compare
       with week_day. Execution Date is Useful for backfilling.
       If ``False``, uses system's day of the week. Useful when you
       don't want to run anything on weekdays on the system.
   :type use_task_execution_day: bool

   
   .. method:: poke(self, context)




