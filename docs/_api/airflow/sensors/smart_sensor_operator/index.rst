:mod:`airflow.sensors.smart_sensor_operator`
============================================

.. py:module:: airflow.sensors.smart_sensor_operator


Module Contents
---------------

.. data:: config
   

   

.. data:: handler_config
   

   

.. data:: formatter_config
   

   

.. data:: dictConfigurator
   

   

.. py:class:: SensorWork(si)

   This class stores a sensor work with decoded context value. It is only used
   inside of smart sensor. Create a sensor work based on sensor instance record.
   A sensor work object has the following attributes:
   `dag_id`: sensor_instance dag_id.
   `task_id`: sensor_instance task_id.
   `execution_date`: sensor_instance execution_date.
   `try_number`: sensor_instance try_number
   `poke_context`: Decoded poke_context for the sensor task.
   `execution_context`: Decoded execution_context.
   `hashcode`: This is the signature of poking job.
   `operator`: The sensor operator class.
   `op_classpath`: The sensor operator class path
   `encoded_poke_context`: The raw data from sensor_instance poke_context column.
   `log`: The sensor work logger which will mock the corresponding task instance log.

   :param si: The sensor_instance ORM object.

   .. attribute:: ti_key
      

      Key for the task instance that maps to the sensor work.


   .. attribute:: cache_key
      

      Key used to query in smart sensor for cached sensor work.


   
   .. method:: __eq__(self, other)



   
   .. staticmethod:: create_new_task_handler()

      Create task log handler for a sensor work.
      :return: log handler



   
   .. method:: _get_sensor_logger(self, si)

      Return logger for a sensor instance object.



   
   .. method:: close_sensor_logger(self)

      Close log handler for a sensor work.




.. py:class:: CachedPokeWork

   Wrapper class for the poke work inside smart sensor. It saves
   the sensor_task used to poke and recent poke result state.
   state: poke state.
   sensor_task: The cached object for executing the poke function.
   last_poke_time: The latest time this cached work being called.
   to_flush: If we should flush the cached work.

   
   .. method:: set_state(self, state)

      Set state for cached poke work.
      :param state: The sensor_instance state.



   
   .. method:: clear_state(self)

      Clear state for cached poke work.



   
   .. method:: set_to_flush(self)

      Mark this poke work to be popped from cached dict after current loop.



   
   .. method:: is_expired(self)

      The cached task object expires if there is no poke for 20 minutes.
      :return: Boolean




.. py:class:: SensorExceptionInfo(exception_info, is_infra_failure=False, infra_failure_retry_window=datetime.timedelta(minutes=130))

   Hold sensor exception information and the type of exception. For possible transient
   infra failure, give the task more chance to retry before fail it.

   .. attribute:: exception_info
      

      :return: exception msg.


   .. attribute:: is_infra_failure
      

      :return: If the exception is an infra failure
      :type: boolean


   
   .. method:: set_latest_exception(self, exception_info, is_infra_failure=False)

      This function set the latest exception information for sensor exception. If the exception
      implies an infra failure, this function will check the recorded infra failure timeout
      which was set at the first infra failure exception arrives. There is a 6 hours window
      for retry without failing current run.

      :param exception_info: Details of the exception information.
      :param is_infra_failure: If current exception was caused by transient infra failure.
          There is a retry window _infra_failure_retry_window that the smart sensor will
          retry poke function without failing current task run.



   
   .. method:: set_infra_failure_timeout(self)

      Set the time point when the sensor should be failed if it kept getting infra
      failure.
      :return:



   
   .. method:: should_fail_current_run(self)

      :return: Should the sensor fail
      :type: boolean



   
   .. method:: is_expired(self)

      :return: If current exception need to be kept.
      :type: boolean




.. py:class:: SmartSensorOperator(poke_interval=180, smart_sensor_timeout=60 * 60 * 24 * 7, soft_fail=False, shard_min=0, shard_max=100000, poke_timeout=6.0, *args, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`, :class:`airflow.models.SkipMixin`

   Smart sensor operators are derived from this class.

   Smart Sensor operators keep refresh a dictionary by visiting DB.
   Taking qualified active sensor tasks. Different from sensor operator,
   Smart sensor operators poke for all sensor tasks in the dictionary at
   a time interval. When a criteria is met or fail by time out, it update
   all sensor task state in task_instance table

   :param soft_fail: Set to true to mark the task as SKIPPED on failure
   :type soft_fail: bool
   :param poke_interval: Time in seconds that the job should wait in
       between each tries.
   :type poke_interval: int
   :param smart_sensor_timeout: Time, in seconds before the internal sensor
       job times out if poke_timeout is not defined.
   :type smart_sensor_timeout: float
   :param shard_min: shard code lower bound (inclusive)
   :type shard_min: int
   :param shard_max: shard code upper bound (exclusive)
   :type shard_max: int
   :param poke_timeout: Time, in seconds before the task times out and fails.
   :type poke_timeout: float

   .. attribute:: ui_color
      :annotation: = #e6f1f2

      

   
   .. method:: _validate_input_values(self)



   
   .. method:: _load_sensor_works(self, session=None)

      Refresh sensor instances need to be handled by this operator. Create smart sensor
      internal object based on the information persisted in the sensor_instance table.



   
   .. method:: _update_ti_hostname(self, sensor_works, session=None)

      Update task instance hostname for new sensor works.

      :param sensor_works: Smart sensor internal object for a sensor task.
      :param session: The sqlalchemy session.



   
   .. method:: _mark_multi_state(self, operator, poke_hash, encoded_poke_context, state, session=None)

      Mark state for multiple tasks in the task_instance table to a new state if they have
      the same signature as the poke_hash.

      :param operator: The sensor's operator class name.
      :param poke_hash: The hash code generated from sensor's poke context.
      :param encoded_poke_context: The raw encoded poke_context.
      :param state: Set multiple sensor tasks to this state.
      :param session: The sqlalchemy session.



   
   .. method:: _retry_or_fail_task(self, sensor_work, error, session=None)

      Change single task state for sensor task. For final state, set the end_date.
      Since smart sensor take care all retries in one process. Failed sensor tasks
      logically experienced all retries and the try_number should be set to max_tries.

      :param sensor_work: The sensor_work with exception.
      :type sensor_work: SensorWork
      :param error: The error message for this sensor_work.
      :type error: str.
      :param session: The sqlalchemy session.



   
   .. method:: _check_and_handle_ti_timeout(self, sensor_work)

      Check if a sensor task in smart sensor is timeout. Could be either sensor operator timeout
      or general operator execution_timeout.

      :param sensor_work: SensorWork



   
   .. method:: _handle_poke_exception(self, sensor_work)

      Fail task if accumulated exceptions exceeds retries.

      :param sensor_work: SensorWork



   
   .. method:: _process_sensor_work_with_cached_state(self, sensor_work, state)



   
   .. method:: _execute_sensor_work(self, sensor_work)



   
   .. method:: flush_cached_sensor_poke_results(self)

      Flush outdated cached sensor states saved in previous loop.



   
   .. method:: poke(self, sensor_work)

      Function that the sensors defined while deriving this class should
      override.



   
   .. method:: _emit_loop_stats(self)



   
   .. method:: execute(self, context)



   
   .. method:: on_kill(self)




