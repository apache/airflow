:mod:`airflow.utils.platform`
=============================

.. py:module:: airflow.utils.platform

.. autoapi-nested-parse::

   Platform and system specific function.



Module Contents
---------------

.. data:: log
   

   

.. function:: is_tty()
   Checks if the standard output is connected (is associated with a terminal device) to a tty(-like)
   device.


.. function:: is_terminal_support_colors() -> bool
   Try to determine if the current terminal supports colors.


.. function:: get_airflow_git_version()
   Returns the git commit hash representing the current version of the application.


