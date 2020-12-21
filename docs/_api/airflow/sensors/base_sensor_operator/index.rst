:mod:`airflow.sensors.base_sensor_operator`
===========================================

.. py:module:: airflow.sensors.base_sensor_operator


Module Contents
---------------

.. py:class:: BaseSensorOperator(*, poke_interval: float = 60, timeout: float = 60 * 60 * 24 * 7, soft_fail: bool = False, mode: str = 'poke', exponential_backoff: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`, :class:`airflow.models.skipmixin.SkipMixin`

   Sensor operators are derived from this class and inherit these attributes.

   Sensor operators keep executing at a time interval and succeed when
   a criteria is met and fail if and when they time out.

   :param soft_fail: Set to true to mark the task as SKIPPED on failure
   :type soft_fail: bool
   :param poke_interval: Time in seconds that the job should wait in
       between each tries
   :type poke_interval: float
   :param timeout: Time, in seconds before the task times out and fails.
   :type timeout: float
   :param mode: How the sensor operates.
       Options are: ``{ poke | reschedule }``, default is ``poke``.
       When set to ``poke`` the sensor is taking up a worker slot for its
       whole execution time and sleeps between pokes. Use this mode if the
       expected runtime of the sensor is short or if a short poke interval
       is required. Note that the sensor will hold onto a worker slot and
       a pool slot for the duration of the sensor's runtime in this mode.
       When set to ``reschedule`` the sensor task frees the worker slot when
       the criteria is not yet met and it's rescheduled at a later time. Use
       this mode if the time before the criteria is met is expected to be
       quite long. The poke interval should be more than one minute to
       prevent too much load on the scheduler.
   :type mode: str
   :param exponential_backoff: allow progressive longer waits between
       pokes by using exponential backoff algorithm
   :type exponential_backoff: bool

   .. attribute:: ui_color
      :annotation: :str = #e6f1f2

      

   .. attribute:: valid_modes
      :annotation: :Iterable[str] = ['poke', 'reschedule']

      

   .. attribute:: execution_fields
      :annotation: = ['poke_interval', 'retries', 'execution_timeout', 'timeout', 'email', 'email_on_retry', 'email_on_failure']

      

   .. attribute:: reschedule
      

      Define mode rescheduled sensors.


   .. attribute:: deps
      

      Adds one additional dependency for all sensor operators that
      checks if a sensor task instance can be rescheduled.


   
   .. method:: _validate_input_values(self)



   
   .. method:: poke(self, context: Dict)

      Function that the sensors defined while deriving this class should
      override.



   
   .. method:: is_smart_sensor_compatible(self)



   
   .. method:: register_in_sensor_service(self, ti, context)

      Register ti in smart sensor service

      :param ti: Task instance object.
      :param context: TaskInstance template context from the ti.
      :return: boolean



   
   .. method:: get_poke_context(self, context)

      Return a dictionary with all attributes in poke_context_fields. The
      poke_context with operator class can be used to identify a unique
      sensor job.

      :param context: TaskInstance template context.
      :return: A dictionary with key in poke_context_fields.



   
   .. method:: get_execution_context(self, context)

      Return a dictionary with all attributes in execution_fields. The
      execution_context include execution requirement for each sensor task
      such as timeout setup, email_alert setup.

      :param context: TaskInstance template context.
      :return: A dictionary with key in execution_fields.



   
   .. method:: execute(self, context: Dict)



   
   .. method:: _get_next_poke_interval(self, started_at, try_number)

      Using the similar logic which is used for exponential backoff retry delay for operators.



   
   .. method:: prepare_for_execution(self)




.. function:: poke_mode_only(cls)
   Class Decorator for child classes of BaseSensorOperator to indicate
   that instances of this class are only safe to use poke mode.

   Will decorate all methods in the class to assert they did not change
   the mode from 'poke'.

   :param cls: BaseSensor class to enforce methods only use 'poke' mode.
   :type cls: type


.. data:: apply_defaults
   

   

