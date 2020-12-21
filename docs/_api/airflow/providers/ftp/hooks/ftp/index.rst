:mod:`airflow.providers.ftp.hooks.ftp`
======================================

.. py:module:: airflow.providers.ftp.hooks.ftp


Module Contents
---------------

.. py:class:: FTPHook(ftp_conn_id: str = 'ftp_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interact with FTP.

   Errors that may occur throughout but should be handled downstream.
   You can specify mode for data transfers in the extra field of your
   connection as ``{"passive": "true"}``.

   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any)



   
   .. method:: get_conn(self)

      Returns a FTP connection object



   
   .. method:: close_conn(self)

      Closes the connection. An error will occur if the
      connection wasn't ever opened.



   
   .. method:: describe_directory(self, path: str)

      Returns a dictionary of {filename: {attributes}} for all files
      on the remote system (where the MLSD command is supported).

      :param path: full path to the remote directory
      :type path: str



   
   .. method:: list_directory(self, path: str)

      Returns a list of files on the remote system.

      :param path: full path to the remote directory to list
      :type path: str



   
   .. method:: create_directory(self, path: str)

      Creates a directory on the remote system.

      :param path: full path to the remote directory to create
      :type path: str



   
   .. method:: delete_directory(self, path: str)

      Deletes a directory on the remote system.

      :param path: full path to the remote directory to delete
      :type path: str



   
   .. method:: retrieve_file(self, remote_full_path, local_full_path_or_buffer, callback=None)

      Transfers the remote file to a local location.

      If local_full_path_or_buffer is a string path, the file will be put
      at that location; if it is a file-like buffer, the file will
      be written to the buffer but not closed.

      :param remote_full_path: full path to the remote file
      :type remote_full_path: str
      :param local_full_path_or_buffer: full path to the local file or a
          file-like buffer
      :type local_full_path_or_buffer: str or file-like buffer
      :param callback: callback which is called each time a block of data
          is read. if you do not use a callback, these blocks will be written
          to the file or buffer passed in. if you do pass in a callback, note
          that writing to a file or buffer will need to be handled inside the
          callback.
          [default: output_handle.write()]
      :type callback: callable

      .. code-block:: python

          hook = FTPHook(ftp_conn_id='my_conn')

          remote_path = '/path/to/remote/file'
          local_path = '/path/to/local/file'

          # with a custom callback (in this case displaying progress on each read)
          def print_progress(percent_progress):
              self.log.info('Percent Downloaded: %s%%' % percent_progress)

          total_downloaded = 0
          total_file_size = hook.get_size(remote_path)
          output_handle = open(local_path, 'wb')
          def write_to_file_with_progress(data):
              total_downloaded += len(data)
              output_handle.write(data)
              percent_progress = (total_downloaded / total_file_size) * 100
              print_progress(percent_progress)
          hook.retrieve_file(remote_path, None, callback=write_to_file_with_progress)

          # without a custom callback data is written to the local_path
          hook.retrieve_file(remote_path, local_path)



   
   .. method:: store_file(self, remote_full_path: str, local_full_path_or_buffer: Any)

      Transfers a local file to the remote location.

      If local_full_path_or_buffer is a string path, the file will be read
      from that location; if it is a file-like buffer, the file will
      be read from the buffer but not closed.

      :param remote_full_path: full path to the remote file
      :type remote_full_path: str
      :param local_full_path_or_buffer: full path to the local file or a
          file-like buffer
      :type local_full_path_or_buffer: str or file-like buffer



   
   .. method:: delete_file(self, path: str)

      Removes a file on the FTP Server.

      :param path: full path to the remote file
      :type path: str



   
   .. method:: rename(self, from_name: str, to_name: str)

      Rename a file.

      :param from_name: rename file from name
      :param to_name: rename file to name



   
   .. method:: get_mod_time(self, path: str)

      Returns a datetime object representing the last time the file was modified

      :param path: remote file path
      :type path: str



   
   .. method:: get_size(self, path: str)

      Returns the size of a file (in bytes)

      :param path: remote file path
      :type path: str




.. py:class:: FTPSHook

   Bases: :class:`airflow.providers.ftp.hooks.ftp.FTPHook`

   Interact with FTPS.

   
   .. method:: get_conn(self)

      Returns a FTPS connection object.




