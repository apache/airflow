:mod:`airflow.providers.cncf.kubernetes.backcompat.volume`
==========================================================

.. py:module:: airflow.providers.cncf.kubernetes.backcompat.volume

.. autoapi-nested-parse::

   This module is deprecated. Please use `kubernetes.client.models.V1Volume`.



Module Contents
---------------

.. py:class:: Volume(name, configs)

   Backward compatible Volume

   
   .. method:: to_k8s_client_obj(self)

      Converts to k8s object.

      :return Volume Mount k8s object



   
   .. staticmethod:: _convert_to_snake_case(input_string)




