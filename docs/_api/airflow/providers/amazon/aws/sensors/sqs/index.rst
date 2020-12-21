:mod:`airflow.providers.amazon.aws.sensors.sqs`
===============================================

.. py:module:: airflow.providers.amazon.aws.sensors.sqs

.. autoapi-nested-parse::

   Reads and then deletes the message from SQS queue



Module Contents
---------------

.. py:class:: SQSSensor(*, sqs_queue, aws_conn_id: str = 'aws_default', max_messages: int = 5, wait_time_seconds: int = 1, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Get messages from an SQS queue and then deletes  the message from the SQS queue.
   If deletion of messages fails an AirflowException is thrown otherwise, the message
   is pushed through XCom with the key ``message``.

   :param aws_conn_id: AWS connection id
   :type aws_conn_id: str
   :param sqs_queue: The SQS queue url (templated)
   :type sqs_queue: str
   :param max_messages: The maximum number of messages to retrieve for each poke (templated)
   :type max_messages: int
   :param wait_time_seconds: The time in seconds to wait for receiving messages (default: 1 second)
   :type wait_time_seconds: int

   .. attribute:: template_fields
      :annotation: = ['sqs_queue', 'max_messages']

      

   
   .. method:: poke(self, context)

      Check for message on subscribed queue and write to xcom the message with key ``messages``

      :param context: the context object
      :type context: dict
      :return: ``True`` if message is available or ``False``



   
   .. method:: get_hook(self)

      Create and return an SQSHook




