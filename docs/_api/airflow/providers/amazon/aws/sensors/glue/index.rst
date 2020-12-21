:mod:`airflow.providers.amazon.aws.sensors.glue`
================================================

.. py:module:: airflow.providers.amazon.aws.sensors.glue


Module Contents
---------------

.. py:class:: AwsGlueJobSensor(*, job_name: str, run_id: str, aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for an AWS Glue Job to reach any of the status below
   'FAILED', 'STOPPED', 'SUCCEEDED'

   :param job_name: The AWS Glue Job unique name
   :type job_name: str
   :param run_id: The AWS Glue current running job identifier
   :type run_id: str

   .. attribute:: template_fields
      :annotation: = ['job_name', 'run_id']

      

   
   .. method:: poke(self, context)




