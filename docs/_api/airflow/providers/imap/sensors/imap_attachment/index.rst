:mod:`airflow.providers.imap.sensors.imap_attachment`
=====================================================

.. py:module:: airflow.providers.imap.sensors.imap_attachment

.. autoapi-nested-parse::

   This module allows you to poke for attachments on a mail server.



Module Contents
---------------

.. py:class:: ImapAttachmentSensor(*, attachment_name, check_regex=False, mail_folder='INBOX', mail_filter='All', conn_id='imap_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a specific attachment on a mail server.

   :param attachment_name: The name of the attachment that will be checked.
   :type attachment_name: str
   :param check_regex: If set to True the attachment's name will be parsed as regular expression.
       Through this you can get a broader set of attachments
       that it will look for than just only the equality of the attachment name.
   :type check_regex: bool
   :param mail_folder: The mail folder in where to search for the attachment.
   :type mail_folder: str
   :param mail_filter: If set other than 'All' only specific mails will be checked.
       See :py:meth:`imaplib.IMAP4.search` for details.
   :type mail_filter: str
   :param conn_id: The connection to run the sensor against.
   :type conn_id: str

   .. attribute:: template_fields
      :annotation: = ['attachment_name', 'mail_filter']

      

   
   .. method:: poke(self, context: dict)

      Pokes for a mail attachment on the mail server.

      :param context: The context that is being provided when poking.
      :type context: dict
      :return: True if attachment with the given name is present and False if not.
      :rtype: bool




