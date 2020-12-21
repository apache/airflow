:mod:`airflow.providers.cncf.kubernetes.backcompat.pod`
=======================================================

.. py:module:: airflow.providers.cncf.kubernetes.backcompat.pod

.. autoapi-nested-parse::

   Classes for interacting with Kubernetes API



Module Contents
---------------

.. py:class:: Resources(request_memory=None, request_cpu=None, request_ephemeral_storage=None, limit_memory=None, limit_cpu=None, limit_gpu=None, limit_ephemeral_storage=None)

   backwards compat for Resources

   .. attribute:: __slots__
      :annotation: = ['request_memory', 'request_cpu', 'limit_memory', 'limit_cpu', 'limit_gpu', 'request_ephemeral_storage', 'limit_ephemeral_storage']

      :param request_memory: requested memory
      :type request_memory: str
      :param request_cpu: requested CPU number
      :type request_cpu: float | str
      :param request_ephemeral_storage: requested ephemeral storage
      :type request_ephemeral_storage: str
      :param limit_memory: limit for memory usage
      :type limit_memory: str
      :param limit_cpu: Limit for CPU used
      :type limit_cpu: float | str
      :param limit_gpu: Limits for GPU used
      :type limit_gpu: int
      :param limit_ephemeral_storage: Limit for ephemeral storage
      :type limit_ephemeral_storage: float | str


   
   .. method:: to_k8s_client_obj(self)

      Converts to k8s object.

      @rtype: object




.. py:class:: Port(name=None, container_port=None)

   POD port

   .. attribute:: __slots__
      :annotation: = ['name', 'container_port']

      

   
   .. method:: to_k8s_client_obj(self)

      Converts to k8s object.

      :rtype: object




