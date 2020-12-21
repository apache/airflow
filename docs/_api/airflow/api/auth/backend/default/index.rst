:mod:`airflow.api.auth.backend.default`
=======================================

.. py:module:: airflow.api.auth.backend.default

.. autoapi-nested-parse::

   Default authentication backend - everything is allowed



Module Contents
---------------

.. data:: CLIENT_AUTH
   :annotation: :Optional[Union[Tuple[str, str], AuthBase]]

   

.. function:: init_app(_)
   Initializes authentication backend


.. data:: T
   

   

.. function:: requires_authentication(function: T)
   Decorator for functions that require authentication


