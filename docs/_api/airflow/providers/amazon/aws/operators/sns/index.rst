:mod:`airflow.providers.amazon.aws.operators.sns`
=================================================

.. py:module:: airflow.providers.amazon.aws.operators.sns

.. autoapi-nested-parse::

   Publish message to SNS queue



Module Contents
---------------

.. py:class:: SnsPublishOperator(*, target_arn: str, message: str, aws_conn_id: str = 'aws_default', subject: Optional[str] = None, message_attributes: Optional[dict] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Publish a message to Amazon SNS.

   :param aws_conn_id: aws connection to use
   :type aws_conn_id: str
   :param target_arn: either a TopicArn or an EndpointArn
   :type target_arn: str
   :param message: the default message you want to send (templated)
   :type message: str
   :param subject: the message subject you want to send (templated)
   :type subject: str
   :param message_attributes: the message attributes you want to send as a flat dict (data type will be
       determined automatically)
   :type message_attributes: dict

   .. attribute:: template_fields
      :annotation: = ['message', 'subject', 'message_attributes']

      

   .. attribute:: template_ext
      :annotation: = []

      

   
   .. method:: execute(self, context)




