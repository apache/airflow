:mod:`airflow.api`
==================

.. py:module:: airflow.api

.. autoapi-nested-parse::

   Authentication backend



Subpackages
-----------
.. toctree::
   :titlesonly:
   :maxdepth: 3

   auth/index.rst
   client/index.rst
   common/index.rst


Package Contents
----------------

.. data:: conf
   

   

.. py:exception:: AirflowConfigException

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when there is configuration problem


.. py:exception:: AirflowException

   Bases: :class:`Exception`

   Base class for all Airflow's errors.
   Each custom exception should be derived from this class

   .. attribute:: status_code
      :annotation: = 500

      


.. data:: log
   

   

.. function:: load_auth()
   Loads authentication backend


