:mod:`airflow.providers.redis.sensors.redis_key`
================================================

.. py:module:: airflow.providers.redis.sensors.redis_key


Module Contents
---------------

.. py:class:: RedisKeySensor(*, key: str, redis_conn_id: str, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for the existence of a key in a Redis

   .. attribute:: template_fields
      :annotation: = ['key']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: poke(self, context: Dict)




