:mod:`airflow.providers.amazon.aws.sensors.ec2_instance_state`
==============================================================

.. py:module:: airflow.providers.amazon.aws.sensors.ec2_instance_state


Module Contents
---------------

.. py:class:: EC2InstanceStateSensor(*, target_state: str, instance_id: str, aws_conn_id: str = 'aws_default', region_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Check the state of the AWS EC2 instance until
   state of the instance become equal to the target state.

   :param target_state: target state of instance
   :type target_state: str
   :param instance_id: id of the AWS EC2 instance
   :type instance_id: str
   :param region_name: (optional) aws region name associated with the client
   :type region_name: Optional[str]

   .. attribute:: template_fields
      :annotation: = ['target_state', 'instance_id', 'region_name']

      

   .. attribute:: ui_color
      :annotation: = #cc8811

      

   .. attribute:: ui_fgcolor
      :annotation: = #ffffff

      

   .. attribute:: valid_states
      :annotation: = ['running', 'stopped', 'terminated']

      

   
   .. method:: poke(self, context)




