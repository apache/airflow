:mod:`airflow.api_connexion.security`
=====================================

.. py:module:: airflow.api_connexion.security


Module Contents
---------------

.. data:: T
   

   

.. function:: check_authentication() -> None
   Checks that the request has valid authorization information.


.. function:: requires_access(permissions: Optional[Sequence[Tuple[str, str]]] = None) -> Callable[[T], T]
   Factory for decorator that checks current user's permissions against required permissions.


