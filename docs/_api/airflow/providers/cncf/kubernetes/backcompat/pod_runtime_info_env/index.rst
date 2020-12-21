:mod:`airflow.providers.cncf.kubernetes.backcompat.pod_runtime_info_env`
========================================================================

.. py:module:: airflow.providers.cncf.kubernetes.backcompat.pod_runtime_info_env

.. autoapi-nested-parse::

   Classes for interacting with Kubernetes API



Module Contents
---------------

.. py:class:: PodRuntimeInfoEnv(name, field_path)

   Defines Pod runtime information as environment variable

   
   .. method:: to_k8s_client_obj(self)

      Converts to k8s object.

      :return: kubernetes.client.models.V1EnvVar




