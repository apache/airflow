:mod:`airflow.serialization.helpers`
====================================

.. py:module:: airflow.serialization.helpers

.. autoapi-nested-parse::

   Serialized DAG and BaseOperator



Module Contents
---------------

.. function:: serialize_template_field(template_field: dict) -> Union[str, dict]
   Return a serializable representation of the templated_field.
   If a templated_field contains a Class or Instance for recursive templating, store them
   as strings. If the templated_field is not recursive return the field

   :param template_field: Task's Templated Field


