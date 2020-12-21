:mod:`airflow.providers.amazon.aws.hooks.ec2`
=============================================

.. py:module:: airflow.providers.amazon.aws.hooks.ec2


Module Contents
---------------

.. py:class:: EC2Hook(*args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS EC2 Service.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. method:: get_instance(self, instance_id: str)

      Get EC2 instance by id and return it.

      :param instance_id: id of the AWS EC2 instance
      :type instance_id: str
      :return: Instance object
      :rtype: ec2.Instance



   
   .. method:: get_instance_state(self, instance_id: str)

      Get EC2 instance state by id and return it.

      :param instance_id: id of the AWS EC2 instance
      :type instance_id: str
      :return: current state of the instance
      :rtype: str



   
   .. method:: wait_for_state(self, instance_id: str, target_state: str, check_interval: float)

      Wait EC2 instance until its state is equal to the target_state.

      :param instance_id: id of the AWS EC2 instance
      :type instance_id: str
      :param target_state: target state of instance
      :type target_state: str
      :param check_interval: time in seconds that the job should wait in
          between each instance state checks until operation is completed
      :type check_interval: float
      :return: None
      :rtype: None




