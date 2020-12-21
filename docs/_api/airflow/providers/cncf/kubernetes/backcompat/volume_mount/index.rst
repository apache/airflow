:mod:`airflow.providers.cncf.kubernetes.backcompat.volume_mount`
================================================================

.. py:module:: airflow.providers.cncf.kubernetes.backcompat.volume_mount

.. autoapi-nested-parse::

   Classes for interacting with Kubernetes API



Module Contents
---------------

.. py:class:: VolumeMount(name, mount_path, sub_path, read_only)

   Backward compatible VolumeMount

   .. attribute:: __slots__
      :annotation: = ['name', 'mount_path', 'sub_path', 'read_only']

      

   
   .. method:: to_k8s_client_obj(self)

      Converts to k8s object.

      :return Volume Mount k8s object




