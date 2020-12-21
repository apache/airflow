:mod:`airflow.api.auth.backend.deny_all`
========================================

.. py:module:: airflow.api.auth.backend.deny_all

.. autoapi-nested-parse::

   Authentication backend that denies all requests



Module Contents
---------------

.. data:: CLIENT_AUTH
   :annotation: :Optional[Union[Tuple[str, str], AuthBase]]

   

.. function:: init_app(_)
   Initializes authentication


.. data:: T
   

   

.. function:: requires_authentication(function: T)
   Decorator for functions that require authentication


