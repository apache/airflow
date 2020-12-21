:mod:`airflow.utils.mixins`
===========================

.. py:module:: airflow.utils.mixins


Module Contents
---------------

.. py:class:: MultiprocessingStartMethodMixin

   Convenience class to add support for different types of multiprocessing.

   
   .. method:: _get_multiprocessing_start_method(self)

      Determine method of creating new processes by checking if the
      mp_start_method is set in configs, else, it uses the OS default.




