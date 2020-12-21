:mod:`airflow.providers.sqlite.hooks.sqlite`
============================================

.. py:module:: airflow.providers.sqlite.hooks.sqlite


Module Contents
---------------

.. py:class:: SqliteHook

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with SQLite.

   .. attribute:: conn_name_attr
      :annotation: = sqlite_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = sqlite_default

      

   
   .. method:: get_conn(self)

      Returns a sqlite connection object




