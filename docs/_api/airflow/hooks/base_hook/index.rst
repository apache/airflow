:mod:`airflow.hooks.base_hook`
==============================

.. py:module:: airflow.hooks.base_hook

.. autoapi-nested-parse::

   Base class for all hooks



Module Contents
---------------

.. data:: log
   

   

.. py:class:: BaseHook

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Abstract base class for hooks, hooks are meant as an interface to
   interact with external systems. MySqlHook, HiveHook, PigHook return
   object that can handle the connection and interaction to specific
   instances of these systems, and expose consistent methods to interact
   with them.

   
   .. classmethod:: get_connections(cls, conn_id: str)

      Get all connections as an iterable.

      :param conn_id: connection id
      :return: array of connections



   
   .. classmethod:: get_connection(cls, conn_id: str)

      Get random connection selected from all connections configured with this connection id.

      :param conn_id: connection id
      :return: connection



   
   .. classmethod:: get_hook(cls, conn_id: str)

      Returns default hook for this connection id.

      :param conn_id: connection id
      :return: default hook for this connection



   
   .. method:: get_conn(self)

      Returns connection for the hook.




