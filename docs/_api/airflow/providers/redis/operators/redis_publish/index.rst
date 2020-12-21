:mod:`airflow.providers.redis.operators.redis_publish`
======================================================

.. py:module:: airflow.providers.redis.operators.redis_publish


Module Contents
---------------

.. py:class:: RedisPublishOperator(*, channel: str, message: str, redis_conn_id: str = 'redis_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Publish a message to Redis.

   :param channel: redis channel to which the message is published (templated)
   :type channel: str
   :param message: the message to publish (templated)
   :type message: str
   :param redis_conn_id: redis connection to use
   :type redis_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['channel', 'message']

      

   
   .. method:: execute(self, context: Dict)

      Publish the message to Redis channel

      :param context: the context object
      :type context: dict




