:mod:`airflow.providers.redis.sensors.redis_pub_sub`
====================================================

.. py:module:: airflow.providers.redis.sensors.redis_pub_sub


Module Contents
---------------

.. py:class:: RedisPubSubSensor(*, channels: Union[List[str], str], redis_conn_id: str, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Redis sensor for reading a message from pub sub channels

   :param channels: The channels to be subscribed to (templated)
   :type channels: str or list of str
   :param redis_conn_id: the redis connection id
   :type redis_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['channels']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: poke(self, context: Dict)

      Check for message on subscribed channels and write to xcom the message with key ``message``

      An example of message ``{'type': 'message', 'pattern': None, 'channel': b'test', 'data': b'hello'}``

      :param context: the context object
      :type context: dict
      :return: ``True`` if message (with type 'message') is available or ``False`` if not




