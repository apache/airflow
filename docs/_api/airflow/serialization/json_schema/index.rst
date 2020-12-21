:mod:`airflow.serialization.json_schema`
========================================

.. py:module:: airflow.serialization.json_schema

.. autoapi-nested-parse::

   jsonschema for validating serialized DAG and operator.



Module Contents
---------------

.. py:class:: Validator

   Bases: :class:`airflow.typing_compat.Protocol`

   This class is only used for TypeChecking (for IDEs, mypy, pylint, etc)
   due to the way ``Draft7Validator`` is created. They are created or do not inherit
   from proper classes. Hence you can not have ``type: Draft7Validator``.

   
   .. method:: is_valid(self, instance)

      Check if the instance is valid under the current schema



   
   .. method:: validate(self, instance)

      Check if the instance is valid under the current schema, raising validation error if not



   
   .. method:: iter_errors(self, instance)

      Lazily yield each of the validation errors in the given instance




.. function:: load_dag_schema_dict() -> dict
   Load & return Json Schema for DAG as Python dict


.. function:: load_dag_schema() -> Validator
   Load & Validate Json Schema for DAG


