:mod:`airflow.providers.samba.hooks.samba`
==========================================

.. py:module:: airflow.providers.samba.hooks.samba


Module Contents
---------------

.. py:class:: SambaHook(samba_conn_id: str)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Allows for interaction with an samba server.

   
   .. method:: get_conn(self)



   
   .. method:: push_from_local(self, destination_filepath: str, local_filepath: str)

      Push local file to samba server




