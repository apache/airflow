:mod:`airflow.providers.amazon.aws.hooks.sns`
=============================================

.. py:module:: airflow.providers.amazon.aws.hooks.sns

.. autoapi-nested-parse::

   This module contains AWS SNS hook



Module Contents
---------------

.. function:: _get_message_attribute(o)

.. py:class:: AwsSnsHook(*args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Simple Notification Service.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. method:: publish_to_target(self, target_arn: str, message: str, subject: Optional[str] = None, message_attributes: Optional[dict] = None)

      Publish a message to a topic or an endpoint.

      :param target_arn: either a TopicArn or an EndpointArn
      :type target_arn: str
      :param message: the default message you want to send
      :param message: str
      :param subject: subject of message
      :type subject: str
      :param message_attributes: additional attributes to publish for message filtering. This should be
          a flat dict; the DataType to be sent depends on the type of the value:

          - bytes = Binary
          - str = String
          - int, float = Number
          - iterable = String.Array

      :type message_attributes: dict




