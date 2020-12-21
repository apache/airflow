:mod:`airflow.kubernetes.secret`
================================

.. py:module:: airflow.kubernetes.secret

.. autoapi-nested-parse::

   Classes for interacting with Kubernetes API



Module Contents
---------------

.. py:class:: Secret(deploy_type, deploy_target, secret, key=None, items=None)

   Bases: :class:`airflow.kubernetes.k8s_model.K8SModel`

   Defines Kubernetes Secret Volume

   
   .. method:: to_env_secret(self)

      Stores es environment secret



   
   .. method:: to_env_from_secret(self)

      Reads from environment to secret



   
   .. method:: to_volume_secret(self)

      Converts to volume secret



   
   .. method:: attach_to_pod(self, pod: k8s.V1Pod)

      Attaches to pod



   
   .. method:: __eq__(self, other)



   
   .. method:: __repr__(self)




