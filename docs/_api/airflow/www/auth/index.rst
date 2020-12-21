:mod:`airflow.www.auth`
=======================

.. py:module:: airflow.www.auth


Module Contents
---------------

.. data:: T
   

   

.. function:: has_access(permissions: Optional[Sequence[Tuple[str, str]]] = None) -> Callable[[T], T]
   Factory for decorator that checks current user's permissions against required permissions.


