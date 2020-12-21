:mod:`airflow.providers.yandex.hooks.yandex`
============================================

.. py:module:: airflow.providers.yandex.hooks.yandex


Module Contents
---------------

.. py:class:: YandexCloudBaseHook(connection_id: Optional[str] = None, default_folder_id: Union[dict, bool, None] = None, default_public_ssh_key: Optional[str] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   A base hook for Yandex.Cloud related tasks.

   :param connection_id: The connection ID to use when fetching connection info.
   :type connection_id: str

   
   .. method:: _get_credentials(self)



   
   .. method:: _get_field(self, field_name: str, default: Any = None)

      Fetches a field from extras, and returns it.




