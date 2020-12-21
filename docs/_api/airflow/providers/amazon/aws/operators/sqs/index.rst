:mod:`airflow.providers.amazon.aws.operators.sqs`
=================================================

.. py:module:: airflow.providers.amazon.aws.operators.sqs

.. autoapi-nested-parse::

   Publish message to SQS queue



Module Contents
---------------

.. py:class:: SQSPublishOperator(*, sqs_queue: str, message_content: str, message_attributes: Optional[dict] = None, delay_seconds: int = 0, aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Publish message to a SQS queue.

   :param sqs_queue: The SQS queue url (templated)
   :type sqs_queue: str
   :param message_content: The message content (templated)
   :type message_content: str
   :param message_attributes: additional attributes for the message (default: None)
       For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`
   :type message_attributes: dict
   :param delay_seconds: message delay (templated) (default: 1 second)
   :type delay_seconds: int
   :param aws_conn_id: AWS connection id (default: aws_default)
   :type aws_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['sqs_queue', 'message_content', 'delay_seconds']

      

   .. attribute:: ui_color
      :annotation: = #6ad3fa

      

   
   .. method:: execute(self, context)

      Publish the message to SQS queue

      :param context: the context object
      :type context: dict
      :return: dict with information about the message sent
          For details of the returned dict see :py:meth:`botocore.client.SQS.send_message`
      :rtype: dict




