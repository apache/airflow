:mod:`airflow.providers.sftp.hooks.sftp`
========================================

.. py:module:: airflow.providers.sftp.hooks.sftp

.. autoapi-nested-parse::

   This module contains SFTP hook.



Module Contents
---------------

.. py:class:: SFTPHook(ftp_conn_id: str = 'sftp_default', *args, **kwargs)

   Bases: :class:`airflow.providers.ssh.hooks.ssh.SSHHook`

   This hook is inherited from SSH hook. Please refer to SSH hook for the input
   arguments.

   Interact with SFTP. Aims to be interchangeable with FTPHook.

   :Pitfalls::

       - In contrast with FTPHook describe_directory only returns size, type and
         modify. It doesn't return unix.owner, unix.mode, perm, unix.group and
         unique.
       - retrieve_file and store_file only take a local full path and not a
          buffer.
       - If no mode is passed to create_directory it will be created with 777
         permissions.

   Errors that may occur throughout but should be handled downstream.

   
   .. method:: get_conn(self)

      Returns an SFTP connection object



   
   .. method:: close_conn(self)

      Closes the connection



   
   .. method:: describe_directory(self, path: str)

      Returns a dictionary of {filename: {attributes}} for all files
      on the remote system (where the MLSD command is supported).

      :param path: full path to the remote directory
      :type path: str



   
   .. method:: list_directory(self, path: str)

      Returns a list of files on the remote system.

      :param path: full path to the remote directory to list
      :type path: str



   
   .. method:: create_directory(self, path: str, mode: int = 777)

      Creates a directory on the remote system.

      :param path: full path to the remote directory to create
      :type path: str
      :param mode: int representation of octal mode for directory



   
   .. method:: delete_directory(self, path: str)

      Deletes a directory on the remote system.

      :param path: full path to the remote directory to delete
      :type path: str



   
   .. method:: retrieve_file(self, remote_full_path: str, local_full_path: str)

      Transfers the remote file to a local location.
      If local_full_path is a string path, the file will be put
      at that location

      :param remote_full_path: full path to the remote file
      :type remote_full_path: str
      :param local_full_path: full path to the local file
      :type local_full_path: str



   
   .. method:: store_file(self, remote_full_path: str, local_full_path: str)

      Transfers a local file to the remote location.
      If local_full_path_or_buffer is a string path, the file will be read
      from that location

      :param remote_full_path: full path to the remote file
      :type remote_full_path: str
      :param local_full_path: full path to the local file
      :type local_full_path: str



   
   .. method:: delete_file(self, path: str)

      Removes a file on the FTP Server

      :param path: full path to the remote file
      :type path: str



   
   .. method:: get_mod_time(self, path: str)

      Returns modification time.

      :param path: full path to the remote file
      :type path: str



   
   .. method:: path_exists(self, path: str)

      Returns True if a remote entity exists

      :param path: full path to the remote file or directory
      :type path: str



   
   .. staticmethod:: _is_path_match(path: str, prefix: Optional[str] = None, delimiter: Optional[str] = None)

      Return True if given path starts with prefix (if set) and ends with delimiter (if set).

      :param path: path to be checked
      :type path: str
      :param prefix: if set path will be checked is starting with prefix
      :type prefix: str
      :param delimiter: if set path will be checked is ending with suffix
      :type delimiter: str
      :return: bool



   
   .. method:: get_tree_map(self, path: str, prefix: Optional[str] = None, delimiter: Optional[str] = None)

      Return tuple with recursive lists of files, directories and unknown paths from given path.
      It is possible to filter results by giving prefix and/or delimiter parameters.

      :param path: path from which tree will be built
      :type path: str
      :param prefix: if set paths will be added if start with prefix
      :type prefix: str
      :param delimiter: if set paths will be added if end with delimiter
      :type delimiter: str
      :return: tuple with list of files, dirs and unknown items
      :rtype: Tuple[List[str], List[str], List[str]]




