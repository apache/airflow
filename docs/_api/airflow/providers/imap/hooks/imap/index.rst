:mod:`airflow.providers.imap.hooks.imap`
========================================

.. py:module:: airflow.providers.imap.hooks.imap

.. autoapi-nested-parse::

   This module provides everything to be able to search in mails for a specific attachment
   and also to download it.
   It uses the imaplib library that is already integrated in python 2 and 3.



Module Contents
---------------

.. py:class:: ImapHook(imap_conn_id: str = 'imap_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   This hook connects to a mail server by using the imap protocol.

   .. note:: Please call this Hook as context manager via `with`
       to automatically open and close the connection to the mail server.

   :param imap_conn_id: The connection id that contains the information used to authenticate the client.
   :type imap_conn_id: str

   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, exc_type, exc_val, exc_tb)



   
   .. method:: get_conn(self)

      Login to the mail server.

      .. note:: Please call this Hook as context manager via `with`
          to automatically open and close the connection to the mail server.

      :return: an authorized ImapHook object.
      :rtype: ImapHook



   
   .. method:: has_mail_attachment(self, name: str, *, check_regex: bool = False, mail_folder: str = 'INBOX', mail_filter: str = 'All')

      Checks the mail folder for mails containing attachments with the given name.

      :param name: The name of the attachment that will be searched for.
      :type name: str
      :param check_regex: Checks the name for a regular expression.
      :type check_regex: bool
      :param mail_folder: The mail folder where to look at.
      :type mail_folder: str
      :param mail_filter: If set other than 'All' only specific mails will be checked.
          See :py:meth:`imaplib.IMAP4.search` for details.
      :type mail_filter: str
      :returns: True if there is an attachment with the given name and False if not.
      :rtype: bool



   
   .. method:: retrieve_mail_attachments(self, name: str, *, check_regex: bool = False, latest_only: bool = False, mail_folder: str = 'INBOX', mail_filter: str = 'All', not_found_mode: str = 'raise')

      Retrieves mail's attachments in the mail folder by its name.

      :param name: The name of the attachment that will be downloaded.
      :type name: str
      :param check_regex: Checks the name for a regular expression.
      :type check_regex: bool
      :param latest_only: If set to True it will only retrieve the first matched attachment.
      :type latest_only: bool
      :param mail_folder: The mail folder where to look at.
      :type mail_folder: str
      :param mail_filter: If set other than 'All' only specific mails will be checked.
          See :py:meth:`imaplib.IMAP4.search` for details.
      :type mail_filter: str
      :param not_found_mode: Specify what should happen if no attachment has been found.
          Supported values are 'raise', 'warn' and 'ignore'.
          If it is set to 'raise' it will raise an exception,
          if set to 'warn' it will only print a warning and
          if set to 'ignore' it won't notify you at all.
      :type not_found_mode: str
      :returns: a list of tuple each containing the attachment filename and its payload.
      :rtype: a list of tuple



   
   .. method:: download_mail_attachments(self, name: str, local_output_directory: str, *, check_regex: bool = False, latest_only: bool = False, mail_folder: str = 'INBOX', mail_filter: str = 'All', not_found_mode: str = 'raise')

      Downloads mail's attachments in the mail folder by its name to the local directory.

      :param name: The name of the attachment that will be downloaded.
      :type name: str
      :param local_output_directory: The output directory on the local machine
          where the files will be downloaded to.
      :type local_output_directory: str
      :param check_regex: Checks the name for a regular expression.
      :type check_regex: bool
      :param latest_only: If set to True it will only download the first matched attachment.
      :type latest_only: bool
      :param mail_folder: The mail folder where to look at.
      :type mail_folder: str
      :param mail_filter: If set other than 'All' only specific mails will be checked.
          See :py:meth:`imaplib.IMAP4.search` for details.
      :type mail_filter: str
      :param not_found_mode: Specify what should happen if no attachment has been found.
          Supported values are 'raise', 'warn' and 'ignore'.
          If it is set to 'raise' it will raise an exception,
          if set to 'warn' it will only print a warning and
          if set to 'ignore' it won't notify you at all.
      :type not_found_mode: str



   
   .. method:: _handle_not_found_mode(self, not_found_mode: str)



   
   .. method:: _retrieve_mails_attachments_by_name(self, name: str, check_regex: bool, latest_only: bool, mail_folder: str, mail_filter: str)



   
   .. method:: _list_mail_ids_desc(self, mail_filter: str)



   
   .. method:: _fetch_mail_body(self, mail_id: str)



   
   .. method:: _check_mail_body(self, response_mail_body: str, name: str, check_regex: bool, latest_only: bool)



   
   .. method:: _create_files(self, mail_attachments: List, local_output_directory: str)



   
   .. method:: _is_symlink(self, name: str)



   
   .. method:: _is_escaping_current_directory(self, name: str)



   
   .. method:: _correct_path(self, name: str, local_output_directory: str)



   
   .. method:: _create_file(self, name: str, payload: Any, local_output_directory: str)




.. py:class:: Mail(mail_body: str)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   This class simplifies working with mails returned by the imaplib client.

   :param mail_body: The mail body of a mail received from imaplib client.
   :type mail_body: str

   
   .. method:: has_attachments(self)

      Checks the mail for a attachments.

      :returns: True if it has attachments and False if not.
      :rtype: bool



   
   .. method:: get_attachments_by_name(self, name: str, check_regex: bool, find_first: bool = False)

      Gets all attachments by name for the mail.

      :param name: The name of the attachment to look for.
      :type name: str
      :param check_regex: Checks the name for a regular expression.
      :type check_regex: bool
      :param find_first: If set to True it will only find the first match and then quit.
      :type find_first: bool
      :returns: a list of tuples each containing name and payload
          where the attachments name matches the given name.
      :rtype: list(tuple)



   
   .. method:: _iterate_attachments(self)




.. py:class:: MailPart(part: Any)

   This class is a wrapper for a Mail object's part and gives it more features.

   :param part: The mail part in a Mail object.
   :type part: any

   
   .. method:: is_attachment(self)

      Checks if the part is a valid mail attachment.

      :returns: True if it is an attachment and False if not.
      :rtype: bool



   
   .. method:: has_matching_name(self, name: str)

      Checks if the given name matches the part's name.

      :param name: The name to look for.
      :type name: str
      :returns: True if it matches the name (including regular expression).
      :rtype: tuple



   
   .. method:: has_equal_name(self, name: str)

      Checks if the given name is equal to the part's name.

      :param name: The name to look for.
      :type name: str
      :returns: True if it is equal to the given name.
      :rtype: bool



   
   .. method:: get_file(self)

      Gets the file including name and payload.

      :returns: the part's name and payload.
      :rtype: tuple




