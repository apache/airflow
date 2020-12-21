:mod:`airflow.providers.amazon.aws.hooks.sqs`
=============================================

.. py:module:: airflow.providers.amazon.aws.hooks.sqs

.. autoapi-nested-parse::

   This module contains AWS SQS hook



Module Contents
---------------

.. py:class:: SQSHook(*args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Simple Queue Service.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. method:: create_queue(self, queue_name: str, attributes: Optional[Dict] = None)

      Create queue using connection object

      :param queue_name: name of the queue.
      :type queue_name: str
      :param attributes: additional attributes for the queue (default: None)
          For details of the attributes parameter see :py:meth:`SQS.create_queue`
      :type attributes: dict

      :return: dict with the information about the queue
          For details of the returned value see :py:meth:`SQS.create_queue`
      :rtype: dict



   
   .. method:: send_message(self, queue_url: str, message_body: str, delay_seconds: int = 0, message_attributes: Optional[Dict] = None)

      Send message to the queue

      :param queue_url: queue url
      :type queue_url: str
      :param message_body: the contents of the message
      :type message_body: str
      :param delay_seconds: seconds to delay the message
      :type delay_seconds: int
      :param message_attributes: additional attributes for the message (default: None)
          For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`
      :type message_attributes: dict

      :return: dict with the information about the message sent
          For details of the returned value see :py:meth:`botocore.client.SQS.send_message`
      :rtype: dict




