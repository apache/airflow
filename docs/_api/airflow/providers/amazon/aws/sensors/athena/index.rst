:mod:`airflow.providers.amazon.aws.sensors.athena`
==================================================

.. py:module:: airflow.providers.amazon.aws.sensors.athena


Module Contents
---------------

.. py:class:: AthenaSensor(*, query_execution_id: str, max_retries: Optional[int] = None, aws_conn_id: str = 'aws_default', sleep_time: int = 10, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Asks for the state of the Query until it reaches a failure state or success state.
   If the query fails, the task will fail.

   :param query_execution_id: query_execution_id to check the state of
   :type query_execution_id: str
   :param max_retries: Number of times to poll for query state before
       returning the current state, defaults to None
   :type max_retries: int
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'
   :type aws_conn_id: str
   :param sleep_time: Time in seconds to wait between two consecutive call to
       check query status on athena, defaults to 10
   :type sleep_time: int

   .. attribute:: INTERMEDIATE_STATES
      :annotation: = ['QUEUED', 'RUNNING']

      

   .. attribute:: FAILURE_STATES
      :annotation: = ['FAILED', 'CANCELLED']

      

   .. attribute:: SUCCESS_STATES
      :annotation: = ['SUCCEEDED']

      

   .. attribute:: template_fields
      :annotation: = ['query_execution_id']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #66c3ff

      

   
   .. method:: poke(self, context: dict)



   
   .. method:: hook(self)

      Create and return an AWSAthenaHook




