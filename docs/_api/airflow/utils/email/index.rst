:mod:`airflow.utils.email`
==========================

.. py:module:: airflow.utils.email


Module Contents
---------------

.. data:: log
   

   

.. function:: send_email(to: Union[List[str], Iterable[str]], subject: str, html_content: str, files=None, dryrun=False, cc=None, bcc=None, mime_subtype='mixed', mime_charset='utf-8', **kwargs)
   Send email using backend specified in EMAIL_BACKEND.


.. function:: send_email_smtp(to: Union[str, Iterable[str]], subject: str, html_content: str, files: Optional[List[str]] = None, dryrun: bool = False, cc: Optional[Union[str, Iterable[str]]] = None, bcc: Optional[Union[str, Iterable[str]]] = None, mime_subtype: str = 'mixed', mime_charset: str = 'utf-8', **kwargs)
   Send an email with html content

   >>> send_email('test@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True)


.. function:: build_mime_message(mail_from: str, to: Union[str, Iterable[str]], subject: str, html_content: str, files: Optional[List[str]] = None, cc: Optional[Union[str, Iterable[str]]] = None, bcc: Optional[Union[str, Iterable[str]]] = None, mime_subtype: str = 'mixed', mime_charset: str = 'utf-8', custom_headers: Optional[Dict[str, Any]] = None) -> Tuple[MIMEMultipart, List[str]]
   Build a MIME message that can be used to send an email and
   returns full list of recipients.

   :param mail_from: Email address to set as email's from
   :param to: List of email addresses to set as email's to
   :param subject: Email's subject
   :param html_content: Content of email in HTML format
   :param files: List of paths of files to be attached
   :param cc: List of email addresses to set as email's CC
   :param bcc: List of email addresses to set as email's BCC
   :param mime_subtype: Can be used to specify the subtype of the message. Default = mixed
   :param mime_charset: Email's charset. Default = UTF-8.
   :param custom_headers: Additional headers to add to the MIME message.
       No validations are run on these values and they should be able to be encoded.
   :return: Email as MIMEMultipart and list of recipients' addresses.


.. function:: send_mime_email(e_from: str, e_to: List[str], mime_msg: MIMEMultipart, dryrun: bool = False) -> None
   Send MIME email.


.. function:: get_email_address_list(addresses: Union[str, Iterable[str]]) -> List[str]
   Get list of email addresses.


.. function:: _get_email_list_from_str(addresses: str) -> List[str]

