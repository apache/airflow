:mod:`airflow.utils.json`
=========================

.. py:module:: airflow.utils.json


Module Contents
---------------

.. data:: k8s
   

   

.. py:class:: AirflowJsonEncoder(*args, **kwargs)

   Bases: :class:`json.JSONEncoder`

   Custom Airflow json encoder implementation.

   
   .. staticmethod:: _default(obj)

      Convert dates and numpy objects in a json serializable format.




