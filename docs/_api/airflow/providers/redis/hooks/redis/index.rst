:mod:`airflow.providers.redis.hooks.redis`
==========================================

.. py:module:: airflow.providers.redis.hooks.redis

.. autoapi-nested-parse::

   RedisHook module



Module Contents
---------------

.. py:class:: RedisHook(redis_conn_id: str = 'redis_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Wrapper for connection to interact with Redis in-memory data structure store

   You can set your db in the extra field of your connection as ``{"db": 3}``.
   Also you can set ssl parameters as:
   ``{"ssl": true, "ssl_cert_reqs": "require", "ssl_cert_file": "/path/to/cert.pem", etc}``.

   
   .. method:: get_conn(self)

      Returns a Redis connection.




