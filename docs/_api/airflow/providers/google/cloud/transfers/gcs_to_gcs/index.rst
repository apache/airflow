:mod:`airflow.providers.google.cloud.transfers.gcs_to_gcs`
==========================================================

.. py:module:: airflow.providers.google.cloud.transfers.gcs_to_gcs

.. autoapi-nested-parse::

   This module contains a Google Cloud Storage operator.



Module Contents
---------------

.. data:: WILDCARD
   :annotation: = *

   

.. py:class:: GCSToGCSOperator(*, source_bucket, source_object=None, source_objects=None, destination_bucket=None, destination_object=None, delimiter=None, move_object=False, replace=True, gcp_conn_id='google_cloud_default', google_cloud_storage_conn_id=None, delegate_to=None, last_modified_time=None, maximum_modified_time=None, is_older_than=None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Copies objects from a bucket to another, with renaming if requested.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSToGCSOperator`

   :param source_bucket: The source Google Cloud Storage bucket where the
        object is. (templated)
   :type source_bucket: str
   :param source_object: The source name of the object to copy in the Google cloud
       storage bucket. (templated)
       You can use only one wildcard for objects (filenames) within your
       bucket. The wildcard can appear inside the object name or at the
       end of the object name. Appending a wildcard to the bucket name is
       unsupported.
   :type source_object: str
   :param source_objects: A list of source name of the objects to copy in the Google cloud
       storage bucket. (templated)
   :type source_objects: List[str]
   :param destination_bucket: The destination Google Cloud Storage bucket
       where the object should be. If the destination_bucket is None, it defaults
       to source_bucket. (templated)
   :type destination_bucket: str
   :param destination_object: The destination name of the object in the
       destination Google Cloud Storage bucket. (templated)
       If a wildcard is supplied in the source_object argument, this is the
       prefix that will be prepended to the final destination objects' paths.
       Note that the source path's part before the wildcard will be removed;
       if it needs to be retained it should be appended to destination_object.
       For example, with prefix ``foo/*`` and destination_object ``blah/``, the
       file ``foo/baz`` will be copied to ``blah/baz``; to retain the prefix write
       the destination_object as e.g. ``blah/foo``, in which case the copied file
       will be named ``blah/foo/baz``.
       The same thing applies to source objects inside source_objects.
   :type destination_object: str
   :param move_object: When move object is True, the object is moved instead
       of copied to the new location. This is the equivalent of a mv command
       as opposed to a cp command.
   :type move_object: bool
   :param replace: Whether you want to replace existing destination files or not.
   :type replace: bool
   :param delimiter: This is used to restrict the result to only the 'files' in a given 'folder'.
       If source_objects = ['foo/bah/'] and delimiter = '.avro', then only the 'files' in the
       folder 'foo/bah/' with '.avro' delimiter will be copied to the destination object.
   :type delimiter: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param last_modified_time: When specified, the objects will be copied or moved,
       only if they were modified after last_modified_time.
       If tzinfo has not been set, UTC will be assumed.
   :type last_modified_time: datetime.datetime
   :param maximum_modified_time: When specified, the objects will be copied or moved,
       only if they were modified before maximum_modified_time.
       If tzinfo has not been set, UTC will be assumed.
   :type maximum_modified_time: datetime.datetime
   :param is_older_than: When specified, the objects will be copied if they are older
       than the specified time in seconds.
   :type is_older_than: int
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :Example:

   The following Operator would copy a single file named
   ``sales/sales-2017/january.avro`` in the ``data`` bucket to the file named
   ``copied_sales/2017/january-backup.avro`` in the ``data_backup`` bucket ::

       copy_single_file = GCSToGCSOperator(
           task_id='copy_single_file',
           source_bucket='data',
           source_objects=['sales/sales-2017/january.avro'],
           destination_bucket='data_backup',
           destination_object='copied_sales/2017/january-backup.avro',
           gcp_conn_id=google_cloud_conn_id
       )

   The following Operator would copy all the Avro files from ``sales/sales-2017``
   folder (i.e. with names starting with that prefix) in ``data`` bucket to the
   ``copied_sales/2017`` folder in the ``data_backup`` bucket. ::

       copy_files = GCSToGCSOperator(
           task_id='copy_files',
           source_bucket='data',
           source_objects=['sales/sales-2017'],
           destination_bucket='data_backup',
           destination_object='copied_sales/2017/',
           delimiter='.avro'
           gcp_conn_id=google_cloud_conn_id
       )

       Or ::

       copy_files = GCSToGCSOperator(
           task_id='copy_files',
           source_bucket='data',
           source_object='sales/sales-2017/*.avro',
           destination_bucket='data_backup',
           destination_object='copied_sales/2017/',
           gcp_conn_id=google_cloud_conn_id
       )

   The following Operator would move all the Avro files from ``sales/sales-2017``
   folder (i.e. with names starting with that prefix) in ``data`` bucket to the
   same folder in the ``data_backup`` bucket, deleting the original files in the
   process. ::

       move_files = GCSToGCSOperator(
           task_id='move_files',
           source_bucket='data',
           source_object='sales/sales-2017/*.avro',
           destination_bucket='data_backup',
           move_object=True,
           gcp_conn_id=google_cloud_conn_id
       )

   The following Operator would move all the Avro files from ``sales/sales-2019``
    and ``sales/sales-2020` folder in ``data`` bucket to the same folder in the
    ``data_backup`` bucket, deleting the original files in the process. ::

       move_files = GCSToGCSOperator(
           task_id='move_files',
           source_bucket='data',
           source_objects=['sales/sales-2019/*.avro', 'sales/sales-2020'],
           destination_bucket='data_backup',
           delimiter='.avro',
           move_object=True,
           gcp_conn_id=google_cloud_conn_id
       )

   .. attribute:: template_fields
      :annotation: = ['source_bucket', 'source_object', 'source_objects', 'destination_bucket', 'destination_object', 'delimiter', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: execute(self, context)



   
   .. method:: _copy_source_without_wildcard(self, hook, prefix)



   
   .. method:: _copy_source_with_wildcard(self, hook, prefix)



   
   .. method:: _copy_single_object(self, hook, source_object, destination_object)




