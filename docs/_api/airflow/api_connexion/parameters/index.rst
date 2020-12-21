:mod:`airflow.api_connexion.parameters`
=======================================

.. py:module:: airflow.api_connexion.parameters


Module Contents
---------------

.. function:: validate_istimezone(value)
   Validates that a datetime is not naive


.. function:: format_datetime(value: str)
   Datetime format parser for args since connexion doesn't parse datetimes
   https://github.com/zalando/connexion/issues/476

   This should only be used within connection views because it raises 400


.. function:: check_limit(value: int)
   This checks the limit passed to view and raises BadRequest if
   limit exceed user configured value


.. data:: T
   

   

.. function:: format_parameters(params_formatters: Dict[str, Callable[..., bool]]) -> Callable[[T], T]
   Decorator factory that create decorator that convert parameters using given formatters.

   Using it allows you to separate parameter formatting from endpoint logic.

   :param params_formatters: Map of key name and formatter function


