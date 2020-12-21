:mod:`airflow.providers.apache.hdfs.hooks.webhdfs`
==================================================

.. py:module:: airflow.providers.apache.hdfs.hooks.webhdfs

.. autoapi-nested-parse::

   Hook for Web HDFS



Module Contents
---------------

.. data:: log
   

   

.. data:: _kerberos_security_mode
   

   

.. py:exception:: AirflowWebHDFSHookException

   Bases: :class:`airflow.exceptions.AirflowException`

   Exception specific for WebHDFS hook


.. py:class:: WebHDFSHook(webhdfs_conn_id: str = 'webhdfs_default', proxy_user: Optional[str] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interact with HDFS. This class is a wrapper around the hdfscli library.

   :param webhdfs_conn_id: The connection id for the webhdfs client to connect to.
   :type webhdfs_conn_id: str
   :param proxy_user: The user used to authenticate.
   :type proxy_user: str

   
   .. method:: get_conn(self)

      Establishes a connection depending on the security mode set via config or environment variable.
      :return: a hdfscli InsecureClient or KerberosClient object.
      :rtype: hdfs.InsecureClient or hdfs.ext.kerberos.KerberosClient



   
   .. method:: _find_valid_server(self)



   
   .. method:: _get_client(self, connection: Connection)



   
   .. method:: check_for_path(self, hdfs_path: str)

      Check for the existence of a path in HDFS by querying FileStatus.

      :param hdfs_path: The path to check.
      :type hdfs_path: str
      :return: True if the path exists and False if not.
      :rtype: bool



   
   .. method:: load_file(self, source: str, destination: str, overwrite: bool = True, parallelism: int = 1, **kwargs)

      Uploads a file to HDFS.

      :param source: Local path to file or folder.
          If it's a folder, all the files inside of it will be uploaded.
          .. note:: This implies that folders empty of files will not be created remotely.

      :type source: str
      :param destination: PTarget HDFS path.
          If it already exists and is a directory, files will be uploaded inside.
      :type destination: str
      :param overwrite: Overwrite any existing file or directory.
      :type overwrite: bool
      :param parallelism: Number of threads to use for parallelization.
          A value of `0` (or negative) uses as many threads as there are files.
      :type parallelism: int
      :param \**kwargs: Keyword arguments forwarded to :meth:`hdfs.client.Client.upload`.




