:mod:`airflow.providers.microsoft.azure.hooks.wasb`
===================================================

.. py:module:: airflow.providers.microsoft.azure.hooks.wasb

.. autoapi-nested-parse::

   This module contains integration with Azure Blob Storage.

   It communicate via the Window Azure Storage Blob protocol. Make sure that a
   Airflow connection of type `wasb` exists. Authorization can be done by supplying a
   login (=Storage account name) and password (=KEY), or login and SAS token in the extra
   field (see connection `wasb_default` for an example).



Module Contents
---------------

.. py:class:: WasbHook(wasb_conn_id: str = 'wasb_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interacts with Azure Blob Storage through the ``wasb://`` protocol.

   These parameters have to be passed in Airflow Data Base: account_name and account_key.

   Additional options passed in the 'extra' field of the connection will be
   passed to the `BlockBlockService()` constructor. For example, authenticate
   using a SAS token by adding {"sas_token": "YOUR_TOKEN"}.

   :param wasb_conn_id: Reference to the wasb connection.
   :type wasb_conn_id: str

   
   .. method:: get_conn(self)

      Return the BlockBlobService object.



   
   .. method:: check_for_blob(self, container_name, blob_name, **kwargs)

      Check if a blob exists on Azure Blob Storage.

      :param container_name: Name of the container.
      :type container_name: str
      :param blob_name: Name of the blob.
      :type blob_name: str
      :param kwargs: Optional keyword arguments that
          `BlockBlobService.exists()` takes.
      :type kwargs: object
      :return: True if the blob exists, False otherwise.
      :rtype: bool



   
   .. method:: check_for_prefix(self, container_name: str, prefix: str, **kwargs)

      Check if a prefix exists on Azure Blob storage.

      :param container_name: Name of the container.
      :type container_name: str
      :param prefix: Prefix of the blob.
      :type prefix: str
      :param kwargs: Optional keyword arguments that
          `BlockBlobService.list_blobs()` takes.
      :type kwargs: object
      :return: True if blobs matching the prefix exist, False otherwise.
      :rtype: bool



   
   .. method:: get_blobs_list(self, container_name: str, prefix: str, **kwargs)

      Return a list of blobs from path defined in prefix param

      :param container_name: Name of the container.
      :type container_name: str
      :param prefix: Prefix of the blob.
      :type prefix: str
      :param kwargs: Optional keyword arguments that
          `BlockBlobService.list_blobs()` takes (num_results, include,
          delimiter, marker, timeout)
      :type kwargs: object
      :return: List of blobs.
      :rtype: list(azure.storage.common.models.ListGenerator)



   
   .. method:: load_file(self, file_path: str, container_name: str, blob_name: str, **kwargs)

      Upload a file to Azure Blob Storage.

      :param file_path: Path to the file to load.
      :type file_path: str
      :param container_name: Name of the container.
      :type container_name: str
      :param blob_name: Name of the blob.
      :type blob_name: str
      :param kwargs: Optional keyword arguments that
          `BlockBlobService.create_blob_from_path()` takes.
      :type kwargs: object



   
   .. method:: load_string(self, string_data: str, container_name: str, blob_name: str, **kwargs)

      Upload a string to Azure Blob Storage.

      :param string_data: String to load.
      :type string_data: str
      :param container_name: Name of the container.
      :type container_name: str
      :param blob_name: Name of the blob.
      :type blob_name: str
      :param kwargs: Optional keyword arguments that
          `BlockBlobService.create_blob_from_text()` takes.
      :type kwargs: object



   
   .. method:: get_file(self, file_path: str, container_name: str, blob_name: str, **kwargs)

      Download a file from Azure Blob Storage.

      :param file_path: Path to the file to download.
      :type file_path: str
      :param container_name: Name of the container.
      :type container_name: str
      :param blob_name: Name of the blob.
      :type blob_name: str
      :param kwargs: Optional keyword arguments that
          `BlockBlobService.create_blob_from_path()` takes.
      :type kwargs: object



   
   .. method:: read_file(self, container_name: str, blob_name: str, **kwargs)

      Read a file from Azure Blob Storage and return as a string.

      :param container_name: Name of the container.
      :type container_name: str
      :param blob_name: Name of the blob.
      :type blob_name: str
      :param kwargs: Optional keyword arguments that
          `BlockBlobService.create_blob_from_path()` takes.
      :type kwargs: object



   
   .. method:: delete_file(self, container_name: str, blob_name: str, is_prefix: bool = False, ignore_if_missing: bool = False, **kwargs)

      Delete a file from Azure Blob Storage.

      :param container_name: Name of the container.
      :type container_name: str
      :param blob_name: Name of the blob.
      :type blob_name: str
      :param is_prefix: If blob_name is a prefix, delete all matching files
      :type is_prefix: bool
      :param ignore_if_missing: if True, then return success even if the
          blob does not exist.
      :type ignore_if_missing: bool
      :param kwargs: Optional keyword arguments that
          `BlockBlobService.create_blob_from_path()` takes.
      :type kwargs: object




