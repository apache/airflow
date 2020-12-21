:mod:`airflow.api_connexion.exceptions`
=======================================

.. py:module:: airflow.api_connexion.exceptions


Module Contents
---------------

.. data:: doc_link
   :annotation: = https://airflow.readthedocs.io/en/latest/stable-rest-api-ref.html

   

.. data:: EXCEPTIONS_LINK_MAP
   

   

.. function:: common_error_handler(exception)
   Used to capture connexion exceptions and add link to the type field

   :type exception: Exception


.. py:exception:: NotFound(title: str = 'Not Found', detail: Optional[str] = None, headers: Optional[Dict] = None, **kwargs)

   Bases: :class:`connexion.ProblemException`

   Raise when the object cannot be found


.. py:exception:: BadRequest(title: str = 'Bad Request', detail: Optional[str] = None, headers: Optional[Dict] = None, **kwargs)

   Bases: :class:`connexion.ProblemException`

   Raise when the server processes a bad request


.. py:exception:: Unauthenticated(title: str = 'Unauthorized', detail: Optional[str] = None, headers: Optional[Dict] = None, **kwargs)

   Bases: :class:`connexion.ProblemException`

   Raise when the user is not authenticated


.. py:exception:: PermissionDenied(title: str = 'Forbidden', detail: Optional[str] = None, headers: Optional[Dict] = None, **kwargs)

   Bases: :class:`connexion.ProblemException`

   Raise when the user does not have the required permissions


.. py:exception:: AlreadyExists(title='Conflict', detail: Optional[str] = None, headers: Optional[Dict] = None, **kwargs)

   Bases: :class:`connexion.ProblemException`

   Raise when the object already exists


.. py:exception:: Unknown(title: str = 'Internal Server Error', detail: Optional[str] = None, headers: Optional[Dict] = None, **kwargs)

   Bases: :class:`connexion.ProblemException`

   Returns a response body and status code for HTTP 500 exception


