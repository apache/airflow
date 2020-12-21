:mod:`airflow.api.common.experimental.pool`
===========================================

.. py:module:: airflow.api.common.experimental.pool

.. autoapi-nested-parse::

   Pool APIs.



Module Contents
---------------

.. function:: get_pool(name, session=None)
   Get pool by a given name.


.. function:: get_pools(session=None)
   Get all pools.


.. function:: create_pool(name, slots, description, session=None)
   Create a pool with a given parameters.


.. function:: delete_pool(name, session=None)
   Delete pool by a given name.


