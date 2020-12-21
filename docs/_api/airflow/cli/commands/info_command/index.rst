:mod:`airflow.cli.commands.info_command`
========================================

.. py:module:: airflow.cli.commands.info_command

.. autoapi-nested-parse::

   Config sub-commands



Module Contents
---------------

.. data:: log
   

   

.. py:class:: Anonymizer

   Bases: :class:`airflow.typing_compat.Protocol`

   Anonymizer protocol.

   
   .. method:: process_path(self, value)

      Remove pii from paths



   
   .. method:: process_username(self, value)

      Remove pii from username



   
   .. method:: process_url(self, value)

      Remove pii from URL




.. py:class:: NullAnonymizer

   Bases: :class:`airflow.cli.commands.info_command.Anonymizer`

   Do nothing.

   
   .. method:: _identity(self, value)




.. py:class:: PiiAnonymizer

   Bases: :class:`airflow.cli.commands.info_command.Anonymizer`

   Remove personally identifiable info from path.

   
   .. method:: process_path(self, value)



   
   .. method:: process_username(self, value)



   
   .. method:: process_url(self, value)




.. py:class:: OperatingSystem

   Operating system

   .. attribute:: WINDOWS
      :annotation: = Windows

      

   .. attribute:: LINUX
      :annotation: = Linux

      

   .. attribute:: MACOSX
      :annotation: = Mac OS

      

   .. attribute:: CYGWIN
      :annotation: = Cygwin

      

   
   .. staticmethod:: get_current()

      Get current operating system




.. py:class:: Architecture

   Compute architecture

   .. attribute:: X86_64
      :annotation: = x86_64

      

   .. attribute:: X86
      :annotation: = x86

      

   .. attribute:: PPC
      :annotation: = ppc

      

   .. attribute:: ARM
      :annotation: = arm

      

   
   .. staticmethod:: get_current()

      Get architecture




.. data:: _MACHINE_TO_ARCHITECTURE
   

   

.. py:class:: AirflowInfo(anonymizer: Anonymizer)

   All information related to Airflow, system and other.

   
   .. method:: __str__(self)




.. py:class:: SystemInfo(anonymizer: Anonymizer)

   Basic system and python information

   
   .. method:: __str__(self)




.. py:class:: PathsInfo(anonymizer: Anonymizer)

   Path information

   
   .. method:: __str__(self)




.. py:class:: ConfigInfo(anonymizer: Anonymizer)

   Most critical config properties

   .. attribute:: task_logging_handler
      

      Returns task logging handler.


   
   .. method:: __str__(self)




.. py:class:: ToolsInfo(anonymize: Anonymizer)

   The versions of the tools that Airflow uses

   
   .. method:: _get_version(self, cmd, grep=None)

      Return tools version.



   
   .. method:: __str__(self)




.. py:exception:: FileIoException

   Bases: :class:`Exception`

   Raises when error happens in FileIo.io integration


.. function:: _upload_text_to_fileio(content)
   Upload text file to File.io service and return lnk


.. function:: _send_report_to_fileio(info)

.. function:: show_info(args)
   Show information related to Airflow, system and other.


