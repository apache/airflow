:mod:`airflow.providers.microsoft.azure.hooks.azure_container_volume`
=====================================================================

.. py:module:: airflow.providers.microsoft.azure.hooks.azure_container_volume


Module Contents
---------------

.. py:class:: AzureContainerVolumeHook(wasb_conn_id: str = 'wasb_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   A hook which wraps an Azure Volume.

   :param wasb_conn_id: connection id of a Azure storage account of
       which file shares should be mounted
   :type wasb_conn_id: str

   
   .. method:: get_storagekey(self)

      Get Azure File Volume storage key



   
   .. method:: get_file_volume(self, mount_name: str, share_name: str, storage_account_name: str, read_only: bool = False)

      Get Azure File Volume




