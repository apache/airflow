:mod:`airflow.hooks.filesystem`
===============================

.. py:module:: airflow.hooks.filesystem


Module Contents
---------------

.. py:class:: FSHook(conn_id='fs_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Allows for interaction with an file server.

   Connection should have a name and a path specified under extra:

   example:
   Conn Id: fs_test
   Conn Type: File (path)
   Host, Schema, Login, Password, Port: empty
   Extra: {"path": "/tmp"}

   
   .. method:: get_conn(self)



   
   .. method:: get_path(self)

      Get the path to the filesystem location.

      :return: the path.




