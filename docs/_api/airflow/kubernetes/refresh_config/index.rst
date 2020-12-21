:mod:`airflow.kubernetes.refresh_config`
========================================

.. py:module:: airflow.kubernetes.refresh_config

.. autoapi-nested-parse::

   NOTE: this module can be removed once upstream client supports token refresh
   see: https://github.com/kubernetes-client/python/issues/741



Module Contents
---------------

.. function:: _parse_timestamp(ts_str: str) -> int

.. py:class:: RefreshKubeConfigLoader(*args, **kwargs)

   Bases: :class:`kubernetes.config.kube_config.KubeConfigLoader`

   Patched KubeConfigLoader, this subclass takes expirationTimestamp into
   account and sets api key refresh callback hook in Configuration object

   
   .. method:: _load_from_exec_plugin(self)

      We override _load_from_exec_plugin method to also read and store
      expiration timestamp for aws-iam-authenticator. It will be later
      used for api token refresh.



   
   .. method:: refresh_api_key(self, client_configuration)

      Refresh API key if expired



   
   .. method:: load_and_set(self, client_configuration)




.. py:class:: RefreshConfiguration(*args, **kwargs)

   Bases: :class:`kubernetes.client.Configuration`

   Patched Configuration, this subclass taskes api key refresh callback hook
   into account

   
   .. method:: get_api_key_with_prefix(self, identifier)




.. function:: _get_kube_config_loader_for_yaml_file(filename, **kwargs) -> Optional[RefreshKubeConfigLoader]
   Adapted from the upstream _get_kube_config_loader_for_yaml_file function, changed
   KubeConfigLoader to RefreshKubeConfigLoader


.. function:: load_kube_config(client_configuration, config_file=None, context=None)
   Adapted from the upstream load_kube_config function, changes:
   - removed persist_config argument since it's not being used
   - remove `client_configuration is None` branch since we always pass
   in client configuration


