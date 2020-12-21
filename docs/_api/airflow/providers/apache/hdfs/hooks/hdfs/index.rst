:mod:`airflow.providers.apache.hdfs.hooks.hdfs`
===============================================

.. py:module:: airflow.providers.apache.hdfs.hooks.hdfs

.. autoapi-nested-parse::

   Hook for HDFS operations



Module Contents
---------------

.. data:: snakebite_loaded
   :annotation: = True

   

.. py:exception:: HDFSHookException

   Bases: :class:`airflow.exceptions.AirflowException`

   Exception specific for HDFS


.. py:class:: HDFSHook(hdfs_conn_id: str = 'hdfs_default', proxy_user: Optional[str] = None, autoconfig: bool = False)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interact with HDFS. This class is a wrapper around the snakebite library.

   :param hdfs_conn_id: Connection id to fetch connection info
   :type hdfs_conn_id: str
   :param proxy_user: effective user for HDFS operations
   :type proxy_user: str
   :param autoconfig: use snakebite's automatically configured client
   :type autoconfig: bool

   
   .. method:: get_conn(self)

      Returns a snakebite HDFSClient object.




