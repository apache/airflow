:mod:`airflow.providers.amazon.aws.operators.ec2_start_instance`
================================================================

.. py:module:: airflow.providers.amazon.aws.operators.ec2_start_instance


Module Contents
---------------

.. py:class:: EC2StartInstanceOperator(*, instance_id: str, aws_conn_id: str = 'aws_default', region_name: Optional[str] = None, check_interval: float = 15, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Start AWS EC2 instance using boto3.

   :param instance_id: id of the AWS EC2 instance
   :type instance_id: str
   :param aws_conn_id: aws connection to use
   :type aws_conn_id: str
   :param region_name: (optional) aws region name associated with the client
   :type region_name: Optional[str]
   :param check_interval: time in seconds that the job should wait in
       between each instance state checks until operation is completed
   :type check_interval: float

   .. attribute:: template_fields
      :annotation: = ['instance_id', 'region_name']

      

   .. attribute:: ui_color
      :annotation: = #eeaa11

      

   .. attribute:: ui_fgcolor
      :annotation: = #ffffff

      

   
   .. method:: execute(self, context)




