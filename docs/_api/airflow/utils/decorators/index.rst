:mod:`airflow.utils.decorators`
===============================

.. py:module:: airflow.utils.decorators


Module Contents
---------------

.. data:: signature
   

   

.. data:: T
   

   

.. function:: apply_defaults(func: T) -> T
   Function decorator that Looks for an argument named "default_args", and
   fills the unspecified arguments from it.

   Since python2.* isn't clear about which arguments are missing when
   calling a function, and that this can be quite confusing with multi-level
   inheritance and argument defaults, this decorator also alerts with
   specific information about the missing arguments.


.. data:: apply_defaults
   

   

