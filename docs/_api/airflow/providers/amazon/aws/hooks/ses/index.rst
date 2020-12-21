:mod:`airflow.providers.amazon.aws.hooks.ses`
=============================================

.. py:module:: airflow.providers.amazon.aws.hooks.ses

.. autoapi-nested-parse::

   This module contains AWS SES Hook



Module Contents
---------------

.. py:class:: SESHook(*args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Simple Email Service.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. method:: send_email(self, mail_from: str, to: Union[str, Iterable[str]], subject: str, html_content: str, files: Optional[List[str]] = None, cc: Optional[Union[str, Iterable[str]]] = None, bcc: Optional[Union[str, Iterable[str]]] = None, mime_subtype: str = 'mixed', mime_charset: str = 'utf-8', reply_to: Optional[str] = None, return_path: Optional[str] = None, custom_headers: Optional[Dict[str, Any]] = None)

      Send email using Amazon Simple Email Service

      :param mail_from: Email address to set as email's from
      :param to: List of email addresses to set as email's to
      :param subject: Email's subject
      :param html_content: Content of email in HTML format
      :param files: List of paths of files to be attached
      :param cc: List of email addresses to set as email's CC
      :param bcc: List of email addresses to set as email's BCC
      :param mime_subtype: Can be used to specify the sub-type of the message. Default = mixed
      :param mime_charset: Email's charset. Default = UTF-8.
      :param return_path: The email address to which replies will be sent. By default, replies
          are sent to the original sender's email address.
      :param reply_to: The email address to which message bounces and complaints should be sent.
          "Return-Path" is sometimes called "envelope from," "envelope sender," or "MAIL FROM."
      :param custom_headers: Additional headers to add to the MIME message.
          No validations are run on these values and they should be able to be encoded.
      :return: Response from Amazon SES service with unique message identifier.




