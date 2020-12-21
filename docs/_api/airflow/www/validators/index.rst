:mod:`airflow.www.validators`
=============================

.. py:module:: airflow.www.validators


Module Contents
---------------

.. py:class:: GreaterEqualThan

   Bases: :class:`wtforms.validators.EqualTo`

   Compares the values of two fields.

   :param fieldname:
       The name of the other field to compare to.
   :param message:
       Error message to raise in case of a validation error. Can be
       interpolated with `%(other_label)s` and `%(other_name)s` to provide a
       more helpful error.

   
   .. method:: __call__(self, form, field)




.. py:class:: ValidJson(message=None)

   Validates data is valid JSON.

   :param message:
       Error message to raise in case of a validation error.

   
   .. method:: __call__(self, form, field)




