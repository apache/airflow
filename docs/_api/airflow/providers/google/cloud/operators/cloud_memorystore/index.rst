:mod:`airflow.providers.google.cloud.operators.cloud_memorystore`
=================================================================

.. py:module:: airflow.providers.google.cloud.operators.cloud_memorystore

.. autoapi-nested-parse::

   Operators for Google Cloud Memorystore service



Module Contents
---------------

.. py:class:: CloudMemorystoreCreateInstanceOperator(*, location: str, instance_id: str, instance: Union[Dict, Instance], project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a Redis instance based on the specified tier and memory size.

   By default, the instance is accessible from the project's `default network
   <https://cloud.google.com/compute/docs/networks-and-firewalls#networks>`__.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreCreateInstanceOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance_id: Required. The logical name of the Redis instance in the customer project with the
       following restrictions:

       -  Must contain only lowercase letters, numbers, and hyphens.
       -  Must start with a letter.
       -  Must be between 1-40 characters.
       -  Must end with a number or a letter.
       -  Must be unique within the customer project / location
   :type instance_id: str
   :param instance: Required. A Redis [Instance] resource

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.redis_v1.types.Instance`
   :type instance: Union[Dict, google.cloud.redis_v1.types.Instance]
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['location', 'instance_id', 'instance', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreDeleteInstanceOperator(*, location: str, instance: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a specific Redis instance. Instance stops serving and data is deleted.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreDeleteInstanceOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance: The logical name of the Redis instance in the customer project.
   :type instance: str
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['location', 'instance', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreExportInstanceOperator(*, location: str, instance: str, output_config: Union[Dict, OutputConfig], project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Export Redis instance data into a Redis RDB format file in Cloud Storage.

   Redis will continue serving during this operation.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreExportInstanceOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance: The logical name of the Redis instance in the customer project.
   :type instance: str
   :param output_config: Required. Specify data to be exported.

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.redis_v1.types.OutputConfig`
   :type output_config: Union[Dict, google.cloud.redis_v1.types.OutputConfig]
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['location', 'instance', 'output_config', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreFailoverInstanceOperator(*, location: str, instance: str, data_protection_mode: FailoverInstanceRequest.DataProtectionMode, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Initiates a failover of the master node to current replica node for a specific STANDARD tier Cloud
   Memorystore for Redis instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreFailoverInstanceOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance: The logical name of the Redis instance in the customer project.
   :type instance: str
   :param data_protection_mode: Optional. Available data protection modes that the user can choose. If it's
       unspecified, data protection mode will be LIMITED_DATA_LOSS by default.
   :type data_protection_mode: google.cloud.redis_v1.gapic.enums.FailoverInstanceRequest.DataProtectionMode
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['location', 'instance', 'data_protection_mode', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreGetInstanceOperator(*, location: str, instance: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets the details of a specific Redis instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreGetInstanceOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance: The logical name of the Redis instance in the customer project.
   :type instance: str
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['location', 'instance', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreImportOperator(*, location: str, instance: str, input_config: Union[Dict, InputConfig], project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Import a Redis RDB snapshot file from Cloud Storage into a Redis instance.

   Redis may stop serving during this operation. Instance state will be IMPORTING for entire operation. When
   complete, the instance will contain only data from the imported file.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreImportOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance: The logical name of the Redis instance in the customer project.
   :type instance: str
   :param input_config: Required. Specify data to be imported.

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.redis_v1.types.InputConfig`
   :type input_config: Union[Dict, google.cloud.redis_v1.types.InputConfig]
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['location', 'instance', 'input_config', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreListInstancesOperator(*, location: str, page_size: int, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists all Redis instances owned by a project in either the specified location (region) or all locations.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreListInstancesOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
       If it is specified as ``-`` (wildcard), then all regions available to the project are
       queried, and the results are aggregated.
   :type location: str
   :param page_size: The maximum number of resources contained in the underlying API response. If page
       streaming is performed per- resource, this parameter does not affect the return value. If page
       streaming is performed per-page, this determines the maximum number of resources in a page.
   :type page_size: int
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['location', 'page_size', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreUpdateInstanceOperator(*, update_mask: Union[Dict, FieldMask], instance: Union[Dict, Instance], location: Optional[str] = None, instance_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates the metadata and configuration of a specific Redis instance.

   :param update_mask: Required. Mask of fields to update. At least one path must be supplied in this field.
       The elements of the repeated paths field may only include these fields from ``Instance``:

       -  ``displayName``
       -  ``labels``
       -  ``memorySizeGb``
       -  ``redisConfig``

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.redis_v1.types.FieldMask`

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreUpdateInstanceOperator`

   :type update_mask: Union[Dict, google.cloud.redis_v1.types.FieldMask]
   :param instance: Required. Update description. Only fields specified in update_mask are updated.

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.redis_v1.types.Instance`
   :type instance: Union[Dict, google.cloud.redis_v1.types.Instance]
   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance_id: The logical name of the Redis instance in the customer project.
   :type instance_id: str
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['update_mask', 'instance', 'location', 'instance_id', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreScaleInstanceOperator(*, memory_size_gb: int, location: Optional[str] = None, instance_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates the metadata and configuration of a specific Redis instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreScaleInstanceOperator`

   :param memory_size_gb: Redis memory size in GiB.
   :type memory_size_gb: int
   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance_id: The logical name of the Redis instance in the customer project.
   :type instance_id: str
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['memory_size_gb', 'location', 'instance_id', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreCreateInstanceAndImportOperator(*, location: str, instance_id: str, instance: Union[Dict, Instance], input_config: Union[Dict, InputConfig], project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a Redis instance based on the specified tier and memory size and import a Redis RDB snapshot file
   from Cloud Storage into a this instance.

   By default, the instance is accessible from the project's `default network
   <https://cloud.google.com/compute/docs/networks-and-firewalls#networks>`__.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreCreateInstanceAndImportOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance_id: Required. The logical name of the Redis instance in the customer project with the
       following restrictions:

       -  Must contain only lowercase letters, numbers, and hyphens.
       -  Must start with a letter.
       -  Must be between 1-40 characters.
       -  Must end with a number or a letter.
       -  Must be unique within the customer project / location
   :type instance_id: str
   :param instance: Required. A Redis [Instance] resource

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.redis_v1.types.Instance`
   :type instance: Union[Dict, google.cloud.redis_v1.types.Instance]
   :param input_config: Required. Specify data to be imported.

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.redis_v1.types.InputConfig`
   :type input_config: Union[Dict, google.cloud.redis_v1.types.InputConfig]
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['location', 'instance_id', 'instance', 'input_config', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreExportAndDeleteInstanceOperator(*, location: str, instance: str, output_config: Union[Dict, OutputConfig], project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Export Redis instance data into a Redis RDB format file in Cloud Storage. In next step, deletes a this
   instance.

   Redis will continue serving during this operation.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreExportAndDeleteInstanceOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance: The logical name of the Redis instance in the customer project.
   :type instance: str
   :param output_config: Required. Specify data to be exported.

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.redis_v1.types.OutputConfig`
   :type output_config: Union[Dict, google.cloud.redis_v1.types.OutputConfig]
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['location', 'instance', 'output_config', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudMemorystoreMemcachedApplyParametersOperator(*, node_ids: Sequence[str], apply_all: bool, location: str, instance_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Will update current set of Parameters to the set of specified nodes of the Memcached Instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreMemcachedApplyParametersOperator`

   :param node_ids: Nodes to which we should apply the instance-level parameter group.
   :type node_ids: Sequence[str]
   :param apply_all: Whether to apply instance-level parameter group to all nodes. If set to true,
       will explicitly restrict users from specifying any nodes, and apply parameter group updates
       to all nodes within the instance.
   :type apply_all: bool
   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance_id: The logical name of the Memcached instance in the customer project.
   :type instance_id: str
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]

   .. attribute:: template_fields
      :annotation: = ['node_ids', 'apply_all', 'location', 'instance_id', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: Dict)




.. py:class:: CloudMemorystoreMemcachedCreateInstanceOperator(location: str, instance_id: str, instance: Union[Dict, cloud_memcache.Instance], project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', *args, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a Memcached instance based on the specified tier and memory size.

   By default, the instance is accessible from the project's `default network
   <https://cloud.google.com/compute/docs/networks-and-firewalls#networks>`__.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreMemcachedCreateInstanceOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance_id: Required. The logical name of the Memcached instance in the customer project with the
       following restrictions:

       -  Must contain only lowercase letters, numbers, and hyphens.
       -  Must start with a letter.
       -  Must be between 1-40 characters.
       -  Must end with a number or a letter.
       -  Must be unique within the customer project / location
   :type instance_id: str
   :param instance: Required. A Memcached [Instance] resource

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.Instance`
   :type instance: Union[Dict, google.cloud.memcache_v1beta2.types.cloud_memcache.Instance]
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the GCP connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
   :type gcp_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['location', 'instance_id', 'instance', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id']

      

   
   .. method:: execute(self, context: Dict)




.. py:class:: CloudMemorystoreMemcachedDeleteInstanceOperator(location: str, instance: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', *args, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a specific Memcached instance. Instance stops serving and data is deleted.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreMemcachedDeleteInstanceOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance: The logical name of the Memcached instance in the customer project.
   :type instance: str
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the GCP connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
   :type gcp_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['location', 'instance', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id']

      

   
   .. method:: execute(self, context: Dict)




.. py:class:: CloudMemorystoreMemcachedGetInstanceOperator(*, location: str, instance: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets the details of a specific Memcached instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreMemcachedGetInstanceOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance: The logical name of the Memcached instance in the customer project.
   :type instance: str
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
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
      :annotation: = ['location', 'instance', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: Dict)




.. py:class:: CloudMemorystoreMemcachedListInstancesOperator(*, location: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists all Memcached instances owned by a project in either the specified location (region) or all
       locations.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreMemcachedListInstancesOperator`

   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
       If it is specified as ``-`` (wildcard), then all regions available to the project are
       queried, and the results are aggregated.
   :type location: str
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['location', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: Dict)




.. py:class:: CloudMemorystoreMemcachedUpdateInstanceOperator(*, update_mask: Union[Dict, cloud_memcache.field_mask.FieldMask], instance: Union[Dict, cloud_memcache.Instance], location: Optional[str] = None, instance_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates the metadata and configuration of a specific Memcached instance.

   :param update_mask: Required. Mask of fields to update. At least one path must be supplied in this field.
       The elements of the repeated paths field may only include these fields from ``Instance``:

       -  ``displayName``

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.field_mask.FieldMas`

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreMemcachedUpdateInstanceOperator`

   :type update_mask: Union[Dict, google.cloud.memcache_v1beta2.types.cloud_memcache.field_mask.FieldMask]
   :param instance: Required. Update description. Only fields specified in update_mask are updated.

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.Instance`
   :type instance: Union[Dict, google.cloud.memcache_v1beta2.types.cloud_memcache.Instance]
   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance_id: The logical name of the Memcached instance in the customer project.
   :type instance_id: str
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
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
      :annotation: = ['update_mask', 'instance', 'location', 'instance_id', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: Dict)




.. py:class:: CloudMemorystoreMemcachedUpdateParametersOperator(*, update_mask: Union[Dict, cloud_memcache.field_mask.FieldMask], parameters: Union[Dict, cloud_memcache.MemcacheParameters], location: str, instance_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates the defined Memcached Parameters for an existing Instance. This method only stages the
       parameters, it must be followed by apply_parameters to apply the parameters to nodes of
       the Memcached Instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudMemorystoreMemcachedApplyParametersOperator`

   :param update_mask: Required. Mask of fields to update.
       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.field_mask.FieldMask`
   :type update_mask:
       Union[Dict, google.cloud.memcache_v1beta2.types.cloud_memcache.field_mask.FieldMask]
   :param parameters: The parameters to apply to the instance.
       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.MemcacheParameters`
   :type parameters: Union[Dict, google.cloud.memcache_v1beta2.types.cloud_memcache.MemcacheParameters]
   :param location: The location of the Cloud Memorystore instance (for example europe-west1)
   :type location: str
   :param instance_id: The logical name of the Memcached instance in the customer project.
   :type instance_id: str
   :param project_id: Project ID of the project that contains the instance. If set
       to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]

   .. attribute:: template_fields
      :annotation: = ['update_mask', 'parameters', 'location', 'instance_id', 'project_id', 'retry', 'timeout', 'metadata', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: Dict)




