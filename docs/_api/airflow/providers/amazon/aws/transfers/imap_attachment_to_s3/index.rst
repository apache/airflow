:mod:`airflow.providers.amazon.aws.transfers.imap_attachment_to_s3`
===================================================================

.. py:module:: airflow.providers.amazon.aws.transfers.imap_attachment_to_s3

.. autoapi-nested-parse::

   This module allows you to transfer mail attachments from a mail server into s3 bucket.



Module Contents
---------------

.. py:class:: ImapAttachmentToS3Operator(*, imap_attachment_name: str, s3_key: str, imap_check_regex: bool = False, imap_mail_folder: str = 'INBOX', imap_mail_filter: str = 'All', s3_overwrite: bool = False, imap_conn_id: str = 'imap_default', s3_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Transfers a mail attachment from a mail server into s3 bucket.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:ImapAttachmentToS3Operator`

   :param imap_attachment_name: The file name of the mail attachment that you want to transfer.
   :type imap_attachment_name: str
   :param s3_key: The destination file name in the s3 bucket for the attachment.
   :type s3_key: str
   :param imap_check_regex: If set checks the `imap_attachment_name` for a regular expression.
   :type imap_check_regex: bool
   :param imap_mail_folder: The folder on the mail server to look for the attachment.
   :type imap_mail_folder: str
   :param imap_mail_filter: If set other than 'All' only specific mails will be checked.
       See :py:meth:`imaplib.IMAP4.search` for details.
   :type imap_mail_filter: str
   :param s3_overwrite: If set overwrites the s3 key if already exists.
   :type s3_overwrite: bool
   :param imap_conn_id: The reference to the connection details of the mail server.
   :type imap_conn_id: str
   :param s3_conn_id: The reference to the s3 connection details.
   :type s3_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['imap_attachment_name', 's3_key', 'imap_mail_filter']

      

   
   .. method:: execute(self, context)

      This function executes the transfer from the email server (via imap) into s3.

      :param context: The context while executing.
      :type context: dict




