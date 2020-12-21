:mod:`airflow.operators.email`
==============================

.. py:module:: airflow.operators.email


Module Contents
---------------

.. py:class:: EmailOperator(*, to: Union[List[str], str], subject: str, html_content: str, files: Optional[List] = None, cc: Optional[Union[List[str], str]] = None, bcc: Optional[Union[List[str], str]] = None, mime_subtype: str = 'mixed', mime_charset: str = 'utf-8', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Sends an email.

   :param to: list of emails to send the email to. (templated)
   :type to: list or string (comma or semicolon delimited)
   :param subject: subject line for the email. (templated)
   :type subject: str
   :param html_content: content of the email, html markup
       is allowed. (templated)
   :type html_content: str
   :param files: file names to attach in email
   :type files: list
   :param cc: list of recipients to be added in CC field
   :type cc: list or string (comma or semicolon delimited)
   :param bcc: list of recipients to be added in BCC field
   :type bcc: list or string (comma or semicolon delimited)
   :param mime_subtype: MIME sub content type
   :type mime_subtype: str
   :param mime_charset: character set parameter added to the Content-Type
       header.
   :type mime_charset: str

   .. attribute:: template_fields
      :annotation: = ['to', 'subject', 'html_content']

      

   .. attribute:: template_fields_renderers
      

      

   .. attribute:: template_ext
      :annotation: = ['.html']

      

   .. attribute:: ui_color
      :annotation: = #e6faf9

      

   
   .. method:: execute(self, context)




