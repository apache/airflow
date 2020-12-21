:mod:`airflow.api.auth.backend.basic_auth`
==========================================

.. py:module:: airflow.api.auth.backend.basic_auth

.. autoapi-nested-parse::

   Basic authentication backend



Module Contents
---------------

.. data:: CLIENT_AUTH
   :annotation: :Optional[Union[Tuple[str, str], AuthBase]]

   

.. function:: init_app(_)
   Initializes authentication backend


.. data:: T
   

   

.. function:: auth_current_user() -> Optional[User]
   Authenticate and set current user if Authorization header exists


.. function:: requires_authentication(function: T)
   Decorator for functions that require authentication


