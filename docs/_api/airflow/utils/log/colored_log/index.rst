:mod:`airflow.utils.log.colored_log`
====================================

.. py:module:: airflow.utils.log.colored_log

.. autoapi-nested-parse::

   Class responsible for colouring logs based on log level.



Module Contents
---------------

.. data:: DEFAULT_COLORS
   

   

.. data:: BOLD_ON
   

   

.. data:: BOLD_OFF
   

   

.. py:class:: CustomTTYColoredFormatter(*args, **kwargs)

   Bases: :class:`colorlog.TTYColoredFormatter`

   Custom log formatter which extends `colored.TTYColoredFormatter`
   by adding attributes to message arguments and coloring error
   traceback.

   
   .. staticmethod:: _color_arg(arg: Any)



   
   .. staticmethod:: _count_number_of_arguments_in_message(record: LogRecord)



   
   .. method:: _color_record_args(self, record: LogRecord)



   
   .. method:: _color_record_traceback(self, record: LogRecord)



   
   .. method:: format(self, record: LogRecord)




