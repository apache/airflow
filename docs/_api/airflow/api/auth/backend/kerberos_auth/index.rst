:mod:`airflow.api.auth.backend.kerberos_auth`
=============================================

.. py:module:: airflow.api.auth.backend.kerberos_auth

.. autoapi-nested-parse::

   Kerberos authentication module



Module Contents
---------------

.. data:: log
   

   

.. data:: CLIENT_AUTH
   :annotation: :Optional[Union[Tuple[str, str], AuthBase]]

   

.. py:class:: KerberosService

   Class to keep information about the Kerberos Service initialized


.. data:: _KERBEROS_SERVICE
   

   

.. function:: init_app(app)
   Initializes application with kerberos


.. function:: _unauthorized()
   Indicate that authorization is required
   :return:


.. function:: _forbidden()

.. function:: _gssapi_authenticate(token)

.. data:: T
   

   

.. function:: requires_authentication(function: T)
   Decorator for functions that require authentication with Kerberos


