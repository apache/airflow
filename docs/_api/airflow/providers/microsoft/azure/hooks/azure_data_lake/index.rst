:mod:`airflow.providers.microsoft.azure.hooks.azure_data_lake`
==============================================================

.. py:module:: airflow.providers.microsoft.azure.hooks.azure_data_lake

.. autoapi-nested-parse::

   This module contains integration with Azure Data Lake.

   AzureDataLakeHook communicates via a REST API compatible with WebHDFS. Make sure that a
   Airflow connection of type `azure_data_lake` exists. Authorization can be done by supplying a
   login (=Client ID), password (=Client Secret) and extra fields tenant (Tenant) and account_name (Account Name)
   (see connection `azure_data_lake_default` for an example).



Module Contents
---------------

.. py:class:: AzureDataLakeHook(azure_data_lake_conn_id: str = 'azure_data_lake_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interacts with Azure Data Lake.

   Client ID and client secret should be in user and password parameters.
   Tenant and account name should be extra field as
   {"tenant": "<TENANT>", "account_name": "ACCOUNT_NAME"}.

   :param azure_data_lake_conn_id: Reference to the Azure Data Lake connection.
   :type azure_data_lake_conn_id: str

   
   .. method:: get_conn(self)

      Return a AzureDLFileSystem object.



   
   .. method:: check_for_file(self, file_path: str)

      Check if a file exists on Azure Data Lake.

      :param file_path: Path and name of the file.
      :type file_path: str
      :return: True if the file exists, False otherwise.
      :rtype: bool



   
   .. method:: upload_file(self, local_path: str, remote_path: str, nthreads: int = 64, overwrite: bool = True, buffersize: int = 4194304, blocksize: int = 4194304, **kwargs)

      Upload a file to Azure Data Lake.

      :param local_path: local path. Can be single file, directory (in which case,
          upload recursively) or glob pattern. Recursive glob patterns using `**`
          are not supported.
      :type local_path: str
      :param remote_path: Remote path to upload to; if multiple files, this is the
          directory root to write within.
      :type remote_path: str
      :param nthreads: Number of threads to use. If None, uses the number of cores.
      :type nthreads: int
      :param overwrite: Whether to forcibly overwrite existing files/directories.
          If False and remote path is a directory, will quit regardless if any files
          would be overwritten or not. If True, only matching filenames are actually
          overwritten.
      :type overwrite: bool
      :param buffersize: int [2**22]
          Number of bytes for internal buffer. This block cannot be bigger than
          a chunk and cannot be smaller than a block.
      :type buffersize: int
      :param blocksize: int [2**22]
          Number of bytes for a block. Within each chunk, we write a smaller
          block for each API call. This block cannot be bigger than a chunk.
      :type blocksize: int



   
   .. method:: download_file(self, local_path: str, remote_path: str, nthreads: int = 64, overwrite: bool = True, buffersize: int = 4194304, blocksize: int = 4194304, **kwargs)

      Download a file from Azure Blob Storage.

      :param local_path: local path. If downloading a single file, will write to this
          specific file, unless it is an existing directory, in which case a file is
          created within it. If downloading multiple files, this is the root
          directory to write within. Will create directories as required.
      :type local_path: str
      :param remote_path: remote path/globstring to use to find remote files.
          Recursive glob patterns using `**` are not supported.
      :type remote_path: str
      :param nthreads: Number of threads to use. If None, uses the number of cores.
      :type nthreads: int
      :param overwrite: Whether to forcibly overwrite existing files/directories.
          If False and remote path is a directory, will quit regardless if any files
          would be overwritten or not. If True, only matching filenames are actually
          overwritten.
      :type overwrite: bool
      :param buffersize: int [2**22]
          Number of bytes for internal buffer. This block cannot be bigger than
          a chunk and cannot be smaller than a block.
      :type buffersize: int
      :param blocksize: int [2**22]
          Number of bytes for a block. Within each chunk, we write a smaller
          block for each API call. This block cannot be bigger than a chunk.
      :type blocksize: int



   
   .. method:: list(self, path: str)

      List files in Azure Data Lake Storage

      :param path: full path/globstring to use to list files in ADLS
      :type path: str




