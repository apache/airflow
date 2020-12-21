:mod:`airflow.api_connexion.endpoints.variable_endpoint`
========================================================

.. py:module:: airflow.api_connexion.endpoints.variable_endpoint


Module Contents
---------------

.. function:: delete_variable(variable_key: str) -> Response
   Delete variable


.. function:: get_variable(variable_key: str) -> Response
   Get a variables by key


.. function:: get_variables(session, limit: Optional[int], offset: Optional[int] = None) -> Response
   Get all variable values


.. function:: patch_variable(variable_key: str, update_mask: Optional[List[str]] = None) -> Response
   Update a variable by key


.. function:: post_variables() -> Response
   Create a variable


