:mod:`airflow.providers.microsoft.azure.transfers.local_to_adls`
================================================================

.. py:module:: airflow.providers.microsoft.azure.transfers.local_to_adls


Module Contents
---------------

.. py:class:: LocalToAzureDataLakeStorageOperator(*, local_path: str, remote_path: str, overwrite: bool = True, nthreads: int = 64, buffersize: int = 4194304, blocksize: int = 4194304, extra_upload_options: Optional[Dict[str, Any]] = None, azure_data_lake_conn_id: str = 'azure_data_lake_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Upload file(s) to Azure Data Lake

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:LocalToAzureDataLakeStorageOperator`

   :param local_path: local path. Can be single file, directory (in which case,
           upload recursively) or glob pattern. Recursive glob patterns using `**`
           are not supported
   :type local_path: str
   :param remote_path: Remote path to upload to; if multiple files, this is the
           directory root to write within
   :type remote_path: str
   :param nthreads: Number of threads to use. If None, uses the number of cores.
   :type nthreads: int
   :param overwrite: Whether to forcibly overwrite existing files/directories.
           If False and remote path is a directory, will quit regardless if any files
           would be overwritten or not. If True, only matching filenames are actually
           overwritten
   :type overwrite: bool
   :param buffersize: int [2**22]
           Number of bytes for internal buffer. This block cannot be bigger than
           a chunk and cannot be smaller than a block
   :type buffersize: int
   :param blocksize: int [2**22]
           Number of bytes for a block. Within each chunk, we write a smaller
           block for each API call. This block cannot be bigger than a chunk
   :type blocksize: int
   :param extra_upload_options: Extra upload options to add to the hook upload method
   :type extra_upload_options: dict
   :param azure_data_lake_conn_id: Reference to the Azure Data Lake connection
   :type azure_data_lake_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['local_path', 'remote_path']

      

   .. attribute:: ui_color
      :annotation: = #e4f0e8

      

   
   .. method:: execute(self, context: dict)




