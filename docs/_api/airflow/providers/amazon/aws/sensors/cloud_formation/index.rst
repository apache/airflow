:mod:`airflow.providers.amazon.aws.sensors.cloud_formation`
===========================================================

.. py:module:: airflow.providers.amazon.aws.sensors.cloud_formation

.. autoapi-nested-parse::

   This module contains sensors for AWS CloudFormation.



Module Contents
---------------

.. py:class:: CloudFormationCreateStackSensor(*, stack_name, aws_conn_id='aws_default', region_name=None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a stack to be created successfully on AWS CloudFormation.

   :param stack_name: The name of the stack to wait for (templated)
   :type stack_name: str
   :param aws_conn_id: ID of the Airflow connection where credentials and extra configuration are
       stored
   :type aws_conn_id: str
   :param poke_interval: Time in seconds that the job should wait between each try
   :type poke_interval: int

   .. attribute:: template_fields
      :annotation: = ['stack_name']

      

   .. attribute:: ui_color
      :annotation: = #C5CAE9

      

   
   .. method:: poke(self, context)




.. py:class:: CloudFormationDeleteStackSensor(*, stack_name: str, aws_conn_id: str = 'aws_default', region_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a stack to be deleted successfully on AWS CloudFormation.

   :param stack_name: The name of the stack to wait for (templated)
   :type stack_name: str
   :param aws_conn_id: ID of the Airflow connection where credentials and extra configuration are
       stored
   :type aws_conn_id: str
   :param poke_interval: Time in seconds that the job should wait between each try
   :type poke_interval: int

   .. attribute:: template_fields
      :annotation: = ['stack_name']

      

   .. attribute:: ui_color
      :annotation: = #C5CAE9

      

   
   .. method:: poke(self, context)



   
   .. method:: get_hook(self)

      Create and return an AWSCloudFormationHook




