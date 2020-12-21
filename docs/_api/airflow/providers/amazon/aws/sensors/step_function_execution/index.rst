:mod:`airflow.providers.amazon.aws.sensors.step_function_execution`
===================================================================

.. py:module:: airflow.providers.amazon.aws.sensors.step_function_execution


Module Contents
---------------

.. py:class:: StepFunctionExecutionSensor(*, execution_arn: str, aws_conn_id: str = 'aws_default', region_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Asks for the state of the Step Function State Machine Execution until it
   reaches a failure state or success state.
   If it fails, failing the task.

   On successful completion of the Execution the Sensor will do an XCom Push
   of the State Machine's output to `output`

   :param execution_arn: execution_arn to check the state of
   :type execution_arn: str
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'
   :type aws_conn_id: str

   .. attribute:: INTERMEDIATE_STATES
      :annotation: = ['RUNNING']

      

   .. attribute:: FAILURE_STATES
      :annotation: = ['FAILED', 'TIMED_OUT', 'ABORTED']

      

   .. attribute:: SUCCESS_STATES
      :annotation: = ['SUCCEEDED']

      

   .. attribute:: template_fields
      :annotation: = ['execution_arn']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #66c3ff

      

   
   .. method:: poke(self, context)



   
   .. method:: get_hook(self)

      Create and return a StepFunctionHook




