:mod:`airflow.utils.session`
============================

.. py:module:: airflow.utils.session


Module Contents
---------------

.. function:: create_session()
   Contextmanager that will create and teardown a session.


.. data:: RT
   

   

.. function:: provide_session(func: Callable[..., RT]) -> Callable[..., RT]
   Function decorator that provides a session if it isn't provided.
   If you want to reuse a session or run the function as part of a
   database transaction, you pass it to the function, if not this wrapper
   will create one and close it for you.


