:mod:`airflow.providers.google.common.auth_backend.google_openid`
=================================================================

.. py:module:: airflow.providers.google.common.auth_backend.google_openid

.. autoapi-nested-parse::

   Authentication backend that use Google credentials for authorization.



Module Contents
---------------

.. data:: log
   

   

.. data:: _GOOGLE_ISSUERS
   :annotation: = ['accounts.google.com', 'https://accounts.google.com']

   

.. data:: AUDIENCE
   

   

.. function:: create_client_session()
   Create a HTTP authorized client.


.. function:: init_app(_)
   Initializes authentication.


.. function:: _get_id_token_from_request(request) -> Optional[str]

.. function:: _verify_id_token(id_token: str) -> Optional[str]

.. function:: _lookup_user(user_email: str)

.. function:: _set_current_user(user)

.. data:: T
   

   

.. function:: requires_authentication(function: T)
   Decorator for functions that require authentication.


