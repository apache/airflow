:mod:`airflow.providers.celery.sensors.celery_queue`
====================================================

.. py:module:: airflow.providers.celery.sensors.celery_queue


Module Contents
---------------

.. py:class:: CeleryQueueSensor(*, celery_queue: str, target_task_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a Celery queue to be empty. By default, in order to be considered
   empty, the queue must not have any tasks in the ``reserved``, ``scheduled``
   or ``active`` states.

   :param celery_queue: The name of the Celery queue to wait for.
   :type celery_queue: str
   :param target_task_id: Task id for checking
   :type target_task_id: str

   
   .. method:: _check_task_id(self, context: Dict[str, Any])

      Gets the returned Celery result from the Airflow task
      ID provided to the sensor, and returns True if the
      celery result has been finished execution.

      :param context: Airflow's execution context
      :type context: dict
      :return: True if task has been executed, otherwise False
      :rtype: bool



   
   .. method:: poke(self, context: Dict[str, Any])




