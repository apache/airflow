:mod:`airflow.utils.log.logging_mixin`
======================================

.. py:module:: airflow.utils.log.logging_mixin


Module Contents
---------------

.. data:: ANSI_ESCAPE
   

   

.. function:: remove_escape_codes(text: str) -> str
   Remove ANSI escapes codes from string. It's used to remove
   "colors" from log messages.


.. py:class:: LoggingMixin(context=None)

   Convenience super-class to have a logger configured with the class name

   .. attribute:: log
      

      Returns a logger.


   
   .. method:: _set_context(self, context)




.. py:class:: ExternalLoggingMixin

   Define a log handler based on an external service (e.g. ELK, StackDriver).

   
   .. method:: log_name(self)

      Return log name



   
   .. method:: get_external_log_url(self, task_instance, try_number)

      Return the URL for log visualization in the external service.




.. py:class:: StreamLogWriter(logger, level)

   Allows to redirect stdout and stderr to logger

   .. attribute:: encoding
      :annotation: :Const.NoneType(value=None)

      

   .. attribute:: closed
      

      Returns False to indicate that the stream is not closed (as it will be
      open for the duration of Airflow's lifecycle).

      For compatibility with the io.IOBase interface.


   
   .. method:: close(self)

      Provide close method, for compatibility with the io.IOBase interface.

      This is a no-op method.



   
   .. method:: _propagate_log(self, message)

      Propagate message removing escape codes.



   
   .. method:: write(self, message)

      Do whatever it takes to actually log the specified logging record

      :param message: message to log



   
   .. method:: flush(self)

      Ensure all logging output has been flushed



   
   .. method:: isatty(self)

      Returns False to indicate the fd is not connected to a tty(-like) device.
      For compatibility reasons.




.. py:class:: RedirectStdHandler(stream)

   Bases: :class:`logging.StreamHandler`

   This class is like a StreamHandler using sys.stderr/stdout, but always uses
   whatever sys.stderr/stderr is currently set to rather than the value of
   sys.stderr/stdout at handler construction time.

   .. attribute:: stream
      

      Returns current stream.



.. function:: set_context(logger, value)
   Walks the tree of loggers and tries to set the context for each handler

   :param logger: logger
   :param value: value to set


