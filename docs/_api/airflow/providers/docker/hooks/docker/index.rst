:mod:`airflow.providers.docker.hooks.docker`
============================================

.. py:module:: airflow.providers.docker.hooks.docker


Module Contents
---------------

.. py:class:: DockerHook(docker_conn_id='docker_default', base_url: Optional[str] = None, version: Optional[str] = None, tls: Optional[str] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Interact with a private Docker registry.

   :param docker_conn_id: ID of the Airflow connection where
       credentials and extra configuration are stored
   :type docker_conn_id: str

   
   .. method:: get_conn(self)



   
   .. method:: __login(self, client)




