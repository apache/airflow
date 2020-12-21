:mod:`airflow.providers.google.cloud.operators.gcs`
===================================================

.. py:module:: airflow.providers.google.cloud.operators.gcs

.. autoapi-nested-parse::

   This module contains a Google Cloud Storage Bucket operator.



Module Contents
---------------

.. py:class:: GCSCreateBucketOperator(*, bucket_name: str, resource: Optional[Dict] = None, storage_class: str = 'MULTI_REGIONAL', location: str = 'US', project_id: Optional[str] = None, labels: Optional[Dict] = None, gcp_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new bucket. Google Cloud Storage uses a flat namespace,
   so you can't create a bucket with a name that is already in use.

       .. seealso::
           For more information, see Bucket Naming Guidelines:
           https://cloud.google.com/storage/docs/bucketnaming.html#requirements

   :param bucket_name: The name of the bucket. (templated)
   :type bucket_name: str
   :param resource: An optional dict with parameters for creating the bucket.
           For information on available parameters, see Cloud Storage API doc:
           https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
   :type resource: dict
   :param storage_class: This defines how objects in the bucket are stored
           and determines the SLA and the cost of storage (templated). Values include

           - ``MULTI_REGIONAL``
           - ``REGIONAL``
           - ``STANDARD``
           - ``NEARLINE``
           - ``COLDLINE``.

           If this value is not specified when the bucket is
           created, it will default to STANDARD.
   :type storage_class: str
   :param location: The location of the bucket. (templated)
       Object data for objects in the bucket resides in physical storage
       within this region. Defaults to US.

       .. seealso:: https://developers.google.com/storage/docs/bucket-locations

   :type location: str
   :param project_id: The ID of the Google Cloud Project. (templated)
   :type project_id: str
   :param labels: User-provided labels, in key/value pairs.
   :type labels: dict
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   The following Operator would create a new bucket ``test-bucket``
   with ``MULTI_REGIONAL`` storage class in ``EU`` region

   .. code-block:: python

       CreateBucket = GoogleCloudStorageCreateBucketOperator(
           task_id='CreateNewBucket',
           bucket_name='test-bucket',
           storage_class='MULTI_REGIONAL',
           location='EU',
           labels={'env': 'dev', 'team': 'airflow'},
           gcp_conn_id='airflow-conn-id'
       )

   .. attribute:: template_fields
      :annotation: = ['bucket_name', 'storage_class', 'location', 'project_id', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: execute(self, context)




.. py:class:: GCSListObjectsOperator(*, bucket: str, prefix: Optional[str] = None, delimiter: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   List all objects from the bucket with the give string prefix and delimiter in name.

   This operator returns a python list with the name of objects which can be used by
    `xcom` in the downstream task.

   :param bucket: The Google Cloud Storage bucket to find the objects. (templated)
   :type bucket: str
   :param prefix: Prefix string which filters objects whose name begin with
          this prefix. (templated)
   :type prefix: str
   :param delimiter: The delimiter by which you want to filter the objects. (templated)
       For e.g to lists the CSV files from in a directory in GCS you would use
       delimiter='.csv'.
   :type delimiter: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id:
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   **Example**:
       The following Operator would list all the Avro files from ``sales/sales-2017``
       folder in ``data`` bucket. ::

           GCS_Files = GoogleCloudStorageListOperator(
               task_id='GCS_Files',
               bucket='data',
               prefix='sales/sales-2017/',
               delimiter='.avro',
               gcp_conn_id=google_cloud_conn_id
           )

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['bucket', 'prefix', 'delimiter', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: execute(self, context)




.. py:class:: GCSDeleteObjectsOperator(*, bucket_name: str, objects: Optional[Iterable[str]] = None, prefix: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes objects from a Google Cloud Storage bucket, either
   from an explicit list of object names or all objects
   matching a prefix.

   :param bucket_name: The GCS bucket to delete from
   :type bucket_name: str
   :param objects: List of objects to delete. These should be the names
       of objects in the bucket, not including gs://bucket/
   :type objects: Iterable[str]
   :param prefix: Prefix of objects to delete. All objects matching this
       prefix in the bucket will be deleted.
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['bucket_name', 'prefix', 'objects', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: GCSBucketCreateAclEntryOperator(*, bucket: str, entity: str, role: str, user_project: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new ACL entry on the specified bucket.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSBucketCreateAclEntryOperator`

   :param bucket: Name of a bucket.
   :type bucket: str
   :param entity: The entity holding the permission, in one of the following forms:
       user-userId, user-email, group-groupId, group-email, domain-domain,
       project-team-projectId, allUsers, allAuthenticatedUsers
   :type entity: str
   :param role: The access permission for the entity.
       Acceptable values are: "OWNER", "READER", "WRITER".
   :type role: str
   :param user_project: (Optional) The project to be billed for this request.
       Required for Requester Pays buckets.
   :type user_project: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['bucket', 'entity', 'role', 'user_project', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: GCSObjectCreateAclEntryOperator(*, bucket: str, object_name: str, entity: str, role: str, generation: Optional[int] = None, user_project: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new ACL entry on the specified object.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSObjectCreateAclEntryOperator`

   :param bucket: Name of a bucket.
   :type bucket: str
   :param object_name: Name of the object. For information about how to URL encode object
       names to be path safe, see:
       https://cloud.google.com/storage/docs/json_api/#encoding
   :type object_name: str
   :param entity: The entity holding the permission, in one of the following forms:
       user-userId, user-email, group-groupId, group-email, domain-domain,
       project-team-projectId, allUsers, allAuthenticatedUsers
   :type entity: str
   :param role: The access permission for the entity.
       Acceptable values are: "OWNER", "READER".
   :type role: str
   :param generation: Optional. If present, selects a specific revision of this object.
   :type generation: long
   :param user_project: (Optional) The project to be billed for this request.
       Required for Requester Pays buckets.
   :type user_project: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['bucket', 'object_name', 'entity', 'generation', 'role', 'user_project', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: GCSFileTransformOperator(*, source_bucket: str, source_object: str, transform_script: Union[str, List[str]], destination_bucket: Optional[str] = None, destination_object: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Copies data from a source GCS location to a temporary location on the
   local filesystem. Runs a transformation on this file as specified by
   the transformation script and uploads the output to a destination bucket.
   If the output bucket is not specified the original file will be
   overwritten.

   The locations of the source and the destination files in the local
   filesystem is provided as an first and second arguments to the
   transformation script. The transformation script is expected to read the
   data from source, transform it and write the output to the local
   destination file.

   :param source_bucket: The key to be retrieved from S3. (templated)
   :type source_bucket: str
   :param destination_bucket: The key to be written from S3. (templated)
   :type destination_bucket: str
   :param transform_script: location of the executable transformation script or list of arguments
       passed to subprocess ex. `['python', 'script.py', 10]`. (templated)
   :type transform_script: Union[str, List[str]]
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['source_bucket', 'destination_bucket', 'transform_script', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: GCSDeleteBucketOperator(*, bucket_name: str, force: bool = True, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes bucket from a Google Cloud Storage.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSDeleteBucketOperator`

   :param bucket_name: name of the bucket which will be deleted
   :type bucket_name: str
   :param force: false not allow to delete non empty bucket, set force=True
       allows to delete non empty bucket
   :type: bool
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['bucket_name', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: GCSSynchronizeBucketsOperator(*, source_bucket: str, destination_bucket: str, source_object: Optional[str] = None, destination_object: Optional[str] = None, recursive: bool = True, delete_extra_files: bool = False, allow_overwrite: bool = False, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Synchronizes the contents of the buckets or bucket's directories in the Google Cloud Services.

   Parameters ``source_object`` and ``destination_object`` describe the root sync directory. If they are
   not passed, the entire bucket will be synchronized. They should point to directories.

   .. note::
       The synchronization of individual files is not supported. Only entire directories can be
       synchronized.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSSynchronizeBuckets`

   :param source_bucket: The name of the bucket containing the source objects.
   :type source_bucket: str
   :param destination_bucket: The name of the bucket containing the destination objects.
   :type destination_bucket: str
   :param source_object: The root sync directory in the source bucket.
   :type source_object: Optional[str]
   :param destination_object: The root sync directory in the destination bucket.
   :type destination_object: Optional[str]
   :param recursive: If True, subdirectories will be considered
   :type recursive: bool
   :param allow_overwrite: if True, the files will be overwritten if a mismatched file is found.
       By default, overwriting files is not allowed
   :type allow_overwrite: bool
   :param delete_extra_files: if True, deletes additional files from the source that not found in the
       destination. By default extra files are not deleted.

       .. note::
           This option can delete data quickly if you specify the wrong source/destination combination.

   :type delete_extra_files: bool
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['source_bucket', 'destination_bucket', 'source_object', 'destination_object', 'recursive', 'delete_extra_files', 'allow_overwrite', 'gcp_conn_id', 'delegate_to', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




