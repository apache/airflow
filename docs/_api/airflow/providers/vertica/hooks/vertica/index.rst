:mod:`airflow.providers.vertica.hooks.vertica`
==============================================

.. py:module:: airflow.providers.vertica.hooks.vertica


Module Contents
---------------

.. py:class:: VerticaHook

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with Vertica.

   .. attribute:: conn_name_attr
      :annotation: = vertica_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = vertica_default

      

   .. attribute:: supports_autocommit
      :annotation: = True

      

   
   .. method:: get_conn(self)

      Return verticaql connection object




