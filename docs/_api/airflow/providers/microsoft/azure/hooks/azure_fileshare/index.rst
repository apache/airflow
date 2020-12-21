:mod:`airflow.providers.microsoft.azure.hooks.azure_fileshare`
==============================================================

.. py:module:: airflow.providers.microsoft.azure.hooks.azure_fileshare


Module Contents
---------------

.. py:class:: AzureFileShareHook(wasb_conn_id: str = 'wasb_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interacts with Azure FileShare Storage.

   Additional options passed in the 'extra' field of the connection will be
   passed to the `FileService()` constructor.

   :param wasb_conn_id: Reference to the wasb connection.
   :type wasb_conn_id: str

   
   .. method:: get_conn(self)

      Return the FileService object.



   
   .. method:: check_for_directory(self, share_name: str, directory_name: str, **kwargs)

      Check if a directory exists on Azure File Share.

      :param share_name: Name of the share.
      :type share_name: str
      :param directory_name: Name of the directory.
      :type directory_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.exists()` takes.
      :type kwargs: object
      :return: True if the file exists, False otherwise.
      :rtype: bool



   
   .. method:: check_for_file(self, share_name: str, directory_name: str, file_name: str, **kwargs)

      Check if a file exists on Azure File Share.

      :param share_name: Name of the share.
      :type share_name: str
      :param directory_name: Name of the directory.
      :type directory_name: str
      :param file_name: Name of the file.
      :type file_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.exists()` takes.
      :type kwargs: object
      :return: True if the file exists, False otherwise.
      :rtype: bool



   
   .. method:: list_directories_and_files(self, share_name: str, directory_name: Optional[str] = None, **kwargs)

      Return the list of directories and files stored on a Azure File Share.

      :param share_name: Name of the share.
      :type share_name: str
      :param directory_name: Name of the directory.
      :type directory_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.list_directories_and_files()` takes.
      :type kwargs: object
      :return: A list of files and directories
      :rtype: list



   
   .. method:: list_files(self, share_name: str, directory_name: Optional[str] = None, **kwargs)

      Return the list of files stored on a Azure File Share.

      :param share_name: Name of the share.
      :type share_name: str
      :param directory_name: Name of the directory.
      :type directory_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.list_directories_and_files()` takes.
      :type kwargs: object
      :return: A list of files
      :rtype: list



   
   .. method:: create_share(self, share_name: str, **kwargs)

      Create new Azure File Share.

      :param share_name: Name of the share.
      :type share_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.create_share()` takes.
      :type kwargs: object
      :return: True if share is created, False if share already exists.
      :rtype: bool



   
   .. method:: delete_share(self, share_name: str, **kwargs)

      Delete existing Azure File Share.

      :param share_name: Name of the share.
      :type share_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.delete_share()` takes.
      :type kwargs: object
      :return: True if share is deleted, False if share does not exist.
      :rtype: bool



   
   .. method:: create_directory(self, share_name: str, directory_name: str, **kwargs)

      Create a new directory on a Azure File Share.

      :param share_name: Name of the share.
      :type share_name: str
      :param directory_name: Name of the directory.
      :type directory_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.create_directory()` takes.
      :type kwargs: object
      :return: A list of files and directories
      :rtype: list



   
   .. method:: get_file(self, file_path: str, share_name: str, directory_name: str, file_name: str, **kwargs)

      Download a file from Azure File Share.

      :param file_path: Where to store the file.
      :type file_path: str
      :param share_name: Name of the share.
      :type share_name: str
      :param directory_name: Name of the directory.
      :type directory_name: str
      :param file_name: Name of the file.
      :type file_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.get_file_to_path()` takes.
      :type kwargs: object



   
   .. method:: get_file_to_stream(self, stream: str, share_name: str, directory_name: str, file_name: str, **kwargs)

      Download a file from Azure File Share.

      :param stream: A filehandle to store the file to.
      :type stream: file-like object
      :param share_name: Name of the share.
      :type share_name: str
      :param directory_name: Name of the directory.
      :type directory_name: str
      :param file_name: Name of the file.
      :type file_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.get_file_to_stream()` takes.
      :type kwargs: object



   
   .. method:: load_file(self, file_path: str, share_name: str, directory_name: str, file_name: str, **kwargs)

      Upload a file to Azure File Share.

      :param file_path: Path to the file to load.
      :type file_path: str
      :param share_name: Name of the share.
      :type share_name: str
      :param directory_name: Name of the directory.
      :type directory_name: str
      :param file_name: Name of the file.
      :type file_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.create_file_from_path()` takes.
      :type kwargs: object



   
   .. method:: load_string(self, string_data: str, share_name: str, directory_name: str, file_name: str, **kwargs)

      Upload a string to Azure File Share.

      :param string_data: String to load.
      :type string_data: str
      :param share_name: Name of the share.
      :type share_name: str
      :param directory_name: Name of the directory.
      :type directory_name: str
      :param file_name: Name of the file.
      :type file_name: str
      :param kwargs: Optional keyword arguments that
          `FileService.create_file_from_text()` takes.
      :type kwargs: object



   
   .. method:: load_stream(self, stream: str, share_name: str, directory_name: str, file_name: str, count: str, **kwargs)

      Upload a stream to Azure File Share.

      :param stream: Opened file/stream to upload as the file content.
      :type stream: file-like
      :param share_name: Name of the share.
      :type share_name: str
      :param directory_name: Name of the directory.
      :type directory_name: str
      :param file_name: Name of the file.
      :type file_name: str
      :param count: Size of the stream in bytes
      :type count: int
      :param kwargs: Optional keyword arguments that
          `FileService.create_file_from_stream()` takes.
      :type kwargs: object




