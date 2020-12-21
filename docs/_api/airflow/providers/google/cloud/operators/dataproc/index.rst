:mod:`airflow.providers.google.cloud.operators.dataproc`
========================================================

.. py:module:: airflow.providers.google.cloud.operators.dataproc

.. autoapi-nested-parse::

   This module contains Google Dataproc operators.



Module Contents
---------------

.. py:class:: ClusterGenerator(project_id: str, num_workers: Optional[int] = None, zone: Optional[str] = None, network_uri: Optional[str] = None, subnetwork_uri: Optional[str] = None, internal_ip_only: Optional[bool] = None, tags: Optional[List[str]] = None, storage_bucket: Optional[str] = None, init_actions_uris: Optional[List[str]] = None, init_action_timeout: str = '10m', metadata: Optional[Dict] = None, custom_image: Optional[str] = None, custom_image_project_id: Optional[str] = None, image_version: Optional[str] = None, autoscaling_policy: Optional[str] = None, properties: Optional[Dict] = None, optional_components: Optional[List[str]] = None, num_masters: int = 1, master_machine_type: str = 'n1-standard-4', master_disk_type: str = 'pd-standard', master_disk_size: int = 1024, worker_machine_type: str = 'n1-standard-4', worker_disk_type: str = 'pd-standard', worker_disk_size: int = 1024, num_preemptible_workers: int = 0, service_account: Optional[str] = None, service_account_scopes: Optional[List[str]] = None, idle_delete_ttl: Optional[int] = None, auto_delete_time: Optional[datetime] = None, auto_delete_ttl: Optional[int] = None, customer_managed_key: Optional[str] = None, **kwargs)

   Create a new Dataproc Cluster.

   :param cluster_name: The name of the DataProc cluster to create. (templated)
   :type cluster_name: str
   :param project_id: The ID of the google cloud project in which
       to create the cluster. (templated)
   :type project_id: str
   :param num_workers: The # of workers to spin up. If set to zero will
       spin up cluster in a single node mode
   :type num_workers: int
   :param storage_bucket: The storage bucket to use, setting to None lets dataproc
       generate a custom one for you
   :type storage_bucket: str
   :param init_actions_uris: List of GCS uri's containing
       dataproc initialization scripts
   :type init_actions_uris: list[str]
   :param init_action_timeout: Amount of time executable scripts in
       init_actions_uris has to complete
   :type init_action_timeout: str
   :param metadata: dict of key-value google compute engine metadata entries
       to add to all instances
   :type metadata: dict
   :param image_version: the version of software inside the Dataproc cluster
   :type image_version: str
   :param custom_image: custom Dataproc image for more info see
       https://cloud.google.com/dataproc/docs/guides/dataproc-images
   :type custom_image: str
   :param custom_image_project_id: project id for the custom Dataproc image, for more info see
       https://cloud.google.com/dataproc/docs/guides/dataproc-images
   :type custom_image_project_id: str
   :param autoscaling_policy: The autoscaling policy used by the cluster. Only resource names
       including projectid and location (region) are valid. Example:
       ``projects/[projectId]/locations/[dataproc_region]/autoscalingPolicies/[policy_id]``
   :type autoscaling_policy: str
   :param properties: dict of properties to set on
       config files (e.g. spark-defaults.conf), see
       https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#SoftwareConfig
   :type properties: dict
   :param optional_components: List of optional cluster components, for more info see
       https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig#Component
   :type optional_components: list[str]
   :param num_masters: The # of master nodes to spin up
   :type num_masters: int
   :param master_machine_type: Compute engine machine type to use for the master node
   :type master_machine_type: str
   :param master_disk_type: Type of the boot disk for the master node
       (default is ``pd-standard``).
       Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
       ``pd-standard`` (Persistent Disk Hard Disk Drive).
   :type master_disk_type: str
   :param master_disk_size: Disk size for the master node
   :type master_disk_size: int
   :param worker_machine_type: Compute engine machine type to use for the worker nodes
   :type worker_machine_type: str
   :param worker_disk_type: Type of the boot disk for the worker node
       (default is ``pd-standard``).
       Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
       ``pd-standard`` (Persistent Disk Hard Disk Drive).
   :type worker_disk_type: str
   :param worker_disk_size: Disk size for the worker nodes
   :type worker_disk_size: int
   :param num_preemptible_workers: The # of preemptible worker nodes to spin up
   :type num_preemptible_workers: int
   :param labels: dict of labels to add to the cluster
   :type labels: dict
   :param zone: The zone where the cluster will be located. Set to None to auto-zone. (templated)
   :type zone: str
   :param network_uri: The network uri to be used for machine communication, cannot be
       specified with subnetwork_uri
   :type network_uri: str
   :param subnetwork_uri: The subnetwork uri to be used for machine communication,
       cannot be specified with network_uri
   :type subnetwork_uri: str
   :param internal_ip_only: If true, all instances in the cluster will only
       have internal IP addresses. This can only be enabled for subnetwork
       enabled networks
   :type internal_ip_only: bool
   :param tags: The GCE tags to add to all instances
   :type tags: list[str]
   :param region: The specified region where the dataproc cluster is created.
   :type region: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param service_account: The service account of the dataproc instances.
   :type service_account: str
   :param service_account_scopes: The URIs of service account scopes to be included.
   :type service_account_scopes: list[str]
   :param idle_delete_ttl: The longest duration that cluster would keep alive while
       staying idle. Passing this threshold will cause cluster to be auto-deleted.
       A duration in seconds.
   :type idle_delete_ttl: int
   :param auto_delete_time:  The time when cluster will be auto-deleted.
   :type auto_delete_time: datetime.datetime
   :param auto_delete_ttl: The life duration of cluster, the cluster will be
       auto-deleted at the end of this duration.
       A duration in seconds. (If auto_delete_time is set this parameter will be ignored)
   :type auto_delete_ttl: int
   :param customer_managed_key: The customer-managed key used for disk encryption
       ``projects/[PROJECT_STORING_KEYS]/locations/[LOCATION]/keyRings/[KEY_RING_NAME]/cryptoKeys/[KEY_NAME]`` # noqa # pylint: disable=line-too-long
   :type customer_managed_key: str

   
   .. method:: _get_init_action_timeout(self)



   
   .. method:: _build_gce_cluster_config(self, cluster_data)



   
   .. method:: _build_lifecycle_config(self, cluster_data)



   
   .. method:: _build_cluster_data(self)



   
   .. method:: make(self)

      Helper method for easier migration.
      :return: Dict representing Dataproc cluster.




.. py:class:: DataprocCreateClusterOperator(*, cluster_name: str, region: Optional[str] = None, project_id: Optional[str] = None, cluster_config: Optional[Dict] = None, labels: Optional[Dict] = None, request_id: Optional[str] = None, delete_on_error: bool = True, use_if_exists: bool = True, retry: Optional[Retry] = None, timeout: float = 1 * 60 * 60, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Create a new cluster on Google Cloud Dataproc. The operator will wait until the
   creation is successful or an error occurs in the creation process. If the cluster
   already exists and ``use_if_exists`` is True then the operator will:

   - if cluster state is ERROR then delete it if specified and raise error
   - if cluster state is CREATING wait for it and then check for ERROR state
   - if cluster state is DELETING wait for it and then create new cluster

   Please refer to

   https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters

   for a detailed explanation on the different parameters. Most of the configuration
   parameters detailed in the link are available as a parameter to this operator.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:DataprocCreateClusterOperator`

   :param project_id: The ID of the google cloud project in which
       to create the cluster. (templated)
   :type project_id: str
   :param cluster_name: Name of the cluster to create
   :type cluster_name: str
   :param labels: Labels that will be assigned to created cluster
   :type labels: Dict[str, str]
   :param cluster_config: Required. The cluster config to create.
       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.dataproc_v1.types.ClusterConfig`
   :type cluster_config: Union[Dict, google.cloud.dataproc_v1.types.ClusterConfig]
   :param region: The specified region where the dataproc cluster is created.
   :type region: str
   :parm delete_on_error: If true the cluster will be deleted if created with ERROR state. Default
       value is true.
   :type delete_on_error: bool
   :parm use_if_exists: If true use existing cluster
   :type use_if_exists: bool
   :param request_id: Optional. A unique id used to identify the request. If the server receives two
       ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and the
       first ``google.longrunning.Operation`` created and stored in the backend is returned.
   :type request_id: str
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
      :annotation: = ['project_id', 'region', 'cluster_config', 'cluster_name', 'labels', 'impersonation_chain']

      

   .. attribute:: template_fields_renderers
      

      

   
   .. method:: _create_cluster(self, hook: DataprocHook)



   
   .. method:: _delete_cluster(self, hook)



   
   .. method:: _get_cluster(self, hook: DataprocHook)



   
   .. method:: _handle_error_state(self, hook: DataprocHook, cluster: Cluster)



   
   .. method:: _wait_for_cluster_in_deleting_state(self, hook: DataprocHook)



   
   .. method:: _wait_for_cluster_in_creating_state(self, hook: DataprocHook)



   
   .. method:: execute(self, context)




.. py:class:: DataprocScaleClusterOperator(*, cluster_name: str, project_id: Optional[str] = None, region: str = 'global', num_workers: int = 2, num_preemptible_workers: int = 0, graceful_decommission_timeout: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Scale, up or down, a cluster on Google Cloud Dataproc.
   The operator will wait until the cluster is re-scaled.

   **Example**: ::

       t1 = DataprocClusterScaleOperator(
               task_id='dataproc_scale',
               project_id='my-project',
               cluster_name='cluster-1',
               num_workers=10,
               num_preemptible_workers=10,
               graceful_decommission_timeout='1h',
               dag=dag)

   .. seealso::
       For more detail on about scaling clusters have a look at the reference:
       https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/scaling-clusters

   :param cluster_name: The name of the cluster to scale. (templated)
   :type cluster_name: str
   :param project_id: The ID of the google cloud project in which
       the cluster runs. (templated)
   :type project_id: str
   :param region: The region for the dataproc cluster. (templated)
   :type region: str
   :param num_workers: The new number of workers
   :type num_workers: int
   :param num_preemptible_workers: The new number of preemptible workers
   :type num_preemptible_workers: int
   :param graceful_decommission_timeout: Timeout for graceful YARN decommissioning.
       Maximum value is 1d
   :type graceful_decommission_timeout: str
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
      :annotation: = ['cluster_name', 'project_id', 'region', 'impersonation_chain']

      

   .. attribute:: _graceful_decommission_timeout_object
      

      

   
   .. method:: _build_scale_cluster_data(self)



   
   .. method:: execute(self, context)

      Scale, up or down, a cluster on Google Cloud Dataproc.




.. py:class:: DataprocDeleteClusterOperator(*, project_id: str, region: str, cluster_name: str, cluster_uuid: Optional[str] = None, request_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a cluster in a project.

   :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
   :type project_id: str
   :param region: Required. The Cloud Dataproc region in which to handle the request.
   :type region: str
   :param cluster_name: Required. The cluster name.
   :type cluster_name: str
   :param cluster_uuid: Optional. Specifying the ``cluster_uuid`` means the RPC should fail
       if cluster with specified UUID does not exist.
   :type cluster_uuid: str
   :param request_id: Optional. A unique id used to identify the request. If the server receives two
       ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and the
       first ``google.longrunning.Operation`` created and stored in the backend is returned.
   :type request_id: str
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
      :annotation: = ['impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: DataprocJobBaseOperator(*, job_name: str = '{{task.task_id}}_{{ds_nodash}}', cluster_name: str = 'cluster-1', dataproc_properties: Optional[Dict] = None, dataproc_jars: Optional[List[str]] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, labels: Optional[Dict] = None, region: Optional[str] = None, job_error_states: Optional[Set[str]] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, asynchronous: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   The base class for operators that launch job on DataProc.

   :param job_name: The job name used in the DataProc cluster. This name by default
       is the task_id appended with the execution data, but can be templated. The
       name will always be appended with a random number to avoid name clashes.
   :type job_name: str
   :param cluster_name: The name of the DataProc cluster.
   :type cluster_name: str
   :param dataproc_properties: Map for the Hive properties. Ideal to put in
       default arguments (templated)
   :type dataproc_properties: dict
   :param dataproc_jars: HCFS URIs of jar files to add to the CLASSPATH of the Hive server and Hadoop
       MapReduce (MR) tasks. Can contain Hive SerDes and UDFs. (templated)
   :type dataproc_jars: list
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param labels: The labels to associate with this job. Label keys must contain 1 to 63 characters,
       and must conform to RFC 1035. Label values may be empty, but, if present, must contain 1 to 63
       characters, and must conform to RFC 1035. No more than 32 labels can be associated with a job.
   :type labels: dict
   :param region: The specified region where the dataproc cluster is created.
   :type region: str
   :param job_error_states: Job states that should be considered error states.
       Any states in this set will result in an error being raised and failure of the
       task. Eg, if the ``CANCELLED`` state should also be considered a task failure,
       pass in ``{'ERROR', 'CANCELLED'}``. Possible values are currently only
       ``'ERROR'`` and ``'CANCELLED'``, but could change in the future. Defaults to
       ``{'ERROR'}``.
   :type job_error_states: set
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]
   :param asynchronous: Flag to return after submitting the job to the Dataproc API.
       This is useful for submitting long running jobs and
       waiting on them asynchronously using the DataprocJobSensor
   :type asynchronous: bool

   :var dataproc_job_id: The actual "jobId" as submitted to the Dataproc API.
       This is useful for identifying or linking to the job in the Google Cloud Console
       Dataproc UI, as the actual "jobId" submitted to the Dataproc API is appended with
       an 8 character random string.
   :vartype dataproc_job_id: str

   .. attribute:: job_type
      :annotation: = 

      

   
   .. method:: create_job_template(self)

      Initialize `self.job_template` with default values



   
   .. method:: _generate_job_template(self)



   
   .. method:: execute(self, context)



   
   .. method:: on_kill(self)

      Callback called when the operator is killed.
      Cancel any running job.




.. py:class:: DataprocSubmitPigJobOperator(*, query: Optional[str] = None, query_uri: Optional[str] = None, variables: Optional[Dict] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator`

   Start a Pig query Job on a Cloud DataProc cluster. The parameters of the operation
   will be passed to the cluster.

   It's a good practice to define dataproc_* parameters in the default_args of the dag
   like the cluster name and UDFs.

   .. code-block:: python

       default_args = {
           'cluster_name': 'cluster-1',
           'dataproc_pig_jars': [
               'gs://example/udf/jar/datafu/1.2.0/datafu.jar',
               'gs://example/udf/jar/gpig/1.2/gpig.jar'
           ]
       }

   You can pass a pig script as string or file reference. Use variables to pass on
   variables for the pig script to be resolved on the cluster or use the parameters to
   be resolved in the script as template parameters.

   **Example**: ::

       t1 = DataProcPigOperator(
               task_id='dataproc_pig',
               query='a_pig_script.pig',
               variables={'out': 'gs://example/output/{{ds}}'},
               dag=dag)

   .. seealso::
       For more detail on about job submission have a look at the reference:
       https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs

   :param query: The query or reference to the query
       file (pg or pig extension). (templated)
   :type query: str
   :param query_uri: The HCFS URI of the script that contains the Pig queries.
   :type query_uri: str
   :param variables: Map of named parameters for the query. (templated)
   :type variables: dict

   .. attribute:: template_fields
      :annotation: = ['query', 'variables', 'job_name', 'cluster_name', 'region', 'dataproc_jars', 'dataproc_properties', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.pg', '.pig']

      

   .. attribute:: ui_color
      :annotation: = #0273d4

      

   .. attribute:: job_type
      :annotation: = pig_job

      

   
   .. method:: generate_job(self)

      Helper method for easier migration to `DataprocSubmitJobOperator`.
      :return: Dict representing Dataproc job



   
   .. method:: execute(self, context)




.. py:class:: DataprocSubmitHiveJobOperator(*, query: Optional[str] = None, query_uri: Optional[str] = None, variables: Optional[Dict] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator`

   Start a Hive query Job on a Cloud DataProc cluster.

   :param query: The query or reference to the query file (q extension).
   :type query: str
   :param query_uri: The HCFS URI of the script that contains the Hive queries.
   :type query_uri: str
   :param variables: Map of named parameters for the query.
   :type variables: dict

   .. attribute:: template_fields
      :annotation: = ['query', 'variables', 'job_name', 'cluster_name', 'region', 'dataproc_jars', 'dataproc_properties', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.q', '.hql']

      

   .. attribute:: ui_color
      :annotation: = #0273d4

      

   .. attribute:: job_type
      :annotation: = hive_job

      

   
   .. method:: generate_job(self)

      Helper method for easier migration to `DataprocSubmitJobOperator`.
      :return: Dict representing Dataproc job



   
   .. method:: execute(self, context)




.. py:class:: DataprocSubmitSparkSqlJobOperator(*, query: Optional[str] = None, query_uri: Optional[str] = None, variables: Optional[Dict] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator`

   Start a Spark SQL query Job on a Cloud DataProc cluster.

   :param query: The query or reference to the query file (q extension). (templated)
   :type query: str
   :param query_uri: The HCFS URI of the script that contains the SQL queries.
   :type query_uri: str
   :param variables: Map of named parameters for the query. (templated)
   :type variables: dict

   .. attribute:: template_fields
      :annotation: = ['query', 'variables', 'job_name', 'cluster_name', 'region', 'dataproc_jars', 'dataproc_properties', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.q']

      

   .. attribute:: ui_color
      :annotation: = #0273d4

      

   .. attribute:: job_type
      :annotation: = spark_sql_job

      

   
   .. method:: generate_job(self)

      Helper method for easier migration to `DataprocSubmitJobOperator`.
      :return: Dict representing Dataproc job



   
   .. method:: execute(self, context)




.. py:class:: DataprocSubmitSparkJobOperator(*, main_jar: Optional[str] = None, main_class: Optional[str] = None, arguments: Optional[List] = None, archives: Optional[List] = None, files: Optional[List] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator`

   Start a Spark Job on a Cloud DataProc cluster.

   :param main_jar: The HCFS URI of the jar file that contains the main class
       (use this or the main_class, not both together).
   :type main_jar: str
   :param main_class: Name of the job class. (use this or the main_jar, not both
       together).
   :type main_class: str
   :param arguments: Arguments for the job. (templated)
   :type arguments: list
   :param archives: List of archived files that will be unpacked in the work
       directory. Should be stored in Cloud Storage.
   :type archives: list
   :param files: List of files to be copied to the working directory
   :type files: list

   .. attribute:: template_fields
      :annotation: = ['arguments', 'job_name', 'cluster_name', 'region', 'dataproc_jars', 'dataproc_properties', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #0273d4

      

   .. attribute:: job_type
      :annotation: = spark_job

      

   
   .. method:: generate_job(self)

      Helper method for easier migration to `DataprocSubmitJobOperator`.
      :return: Dict representing Dataproc job



   
   .. method:: execute(self, context)




.. py:class:: DataprocSubmitHadoopJobOperator(*, main_jar: Optional[str] = None, main_class: Optional[str] = None, arguments: Optional[List] = None, archives: Optional[List] = None, files: Optional[List] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator`

   Start a Hadoop Job on a Cloud DataProc cluster.

   :param main_jar: The HCFS URI of the jar file containing the main class
       (use this or the main_class, not both together).
   :type main_jar: str
   :param main_class: Name of the job class. (use this or the main_jar, not both
       together).
   :type main_class: str
   :param arguments: Arguments for the job. (templated)
   :type arguments: list
   :param archives: List of archived files that will be unpacked in the work
       directory. Should be stored in Cloud Storage.
   :type archives: list
   :param files: List of files to be copied to the working directory
   :type files: list

   .. attribute:: template_fields
      :annotation: = ['arguments', 'job_name', 'cluster_name', 'region', 'dataproc_jars', 'dataproc_properties', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #0273d4

      

   .. attribute:: job_type
      :annotation: = hadoop_job

      

   
   .. method:: generate_job(self)

      Helper method for easier migration to `DataprocSubmitJobOperator`.
      :return: Dict representing Dataproc job



   
   .. method:: execute(self, context)




.. py:class:: DataprocSubmitPySparkJobOperator(*, main: str, arguments: Optional[List] = None, archives: Optional[List] = None, pyfiles: Optional[List] = None, files: Optional[List] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator`

   Start a PySpark Job on a Cloud DataProc cluster.

   :param main: [Required] The Hadoop Compatible Filesystem (HCFS) URI of the main
           Python file to use as the driver. Must be a .py file. (templated)
   :type main: str
   :param arguments: Arguments for the job. (templated)
   :type arguments: list
   :param archives: List of archived files that will be unpacked in the work
       directory. Should be stored in Cloud Storage.
   :type archives: list
   :param files: List of files to be copied to the working directory
   :type files: list
   :param pyfiles: List of Python files to pass to the PySpark framework.
       Supported file types: .py, .egg, and .zip
   :type pyfiles: list

   .. attribute:: template_fields
      :annotation: = ['main', 'arguments', 'job_name', 'cluster_name', 'region', 'dataproc_jars', 'dataproc_properties', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #0273d4

      

   .. attribute:: job_type
      :annotation: = pyspark_job

      

   
   .. staticmethod:: _generate_temp_filename(filename)



   
   .. method:: _upload_file_temp(self, bucket, local_file)

      Upload a local file to a Google Cloud Storage bucket.



   
   .. method:: generate_job(self)

      Helper method for easier migration to `DataprocSubmitJobOperator`.
      :return: Dict representing Dataproc job



   
   .. method:: execute(self, context)




.. py:class:: DataprocInstantiateWorkflowTemplateOperator(*, template_id: str, region: str, project_id: Optional[str] = None, version: Optional[int] = None, request_id: Optional[str] = None, parameters: Optional[Dict[str, str]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Instantiate a WorkflowTemplate on Google Cloud Dataproc. The operator will wait
   until the WorkflowTemplate is finished executing.

   .. seealso::
       Please refer to:
       https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/projects.regions.workflowTemplates/instantiate

   :param template_id: The id of the template. (templated)
   :type template_id: str
   :param project_id: The ID of the google cloud project in which
       the template runs
   :type project_id: str
   :param region: The specified region where the dataproc cluster is created.
   :type region: str
   :param parameters: a map of parameters for Dataproc Template in key-value format:
       map (key: string, value: string)
       Example: { "date_from": "2019-08-01", "date_to": "2019-08-02"}.
       Values may not exceed 100 characters. Please refer to:
       https://cloud.google.com/dataproc/docs/concepts/workflows/workflow-parameters
   :type parameters: Dict[str, str]
   :param request_id: Optional. A unique id used to identify the request. If the server receives two
       ``SubmitJobRequest`` requests with the same id, then the second request will be ignored and the first
       ``Job`` created and stored in the backend is returned.
       It is recommended to always set this value to a UUID.
   :type request_id: str
   :param parameters: Optional. Map from parameter names to values that should be used for those
       parameters. Values may not exceed 100 characters.
   :type parameters: Dict[str, str]
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
      :annotation: = ['template_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: DataprocInstantiateInlineWorkflowTemplateOperator(*, template: Dict, region: str, project_id: Optional[str] = None, request_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Instantiate a WorkflowTemplate Inline on Google Cloud Dataproc. The operator will
   wait until the WorkflowTemplate is finished executing.

   .. seealso::
       Please refer to:
       https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/projects.regions.workflowTemplates/instantiateInline

   :param template: The template contents. (templated)
   :type template: dict
   :param project_id: The ID of the google cloud project in which
       the template runs
   :type project_id: str
   :param region: The specified region where the dataproc cluster is created.
   :type region: str
   :param parameters: a map of parameters for Dataproc Template in key-value format:
       map (key: string, value: string)
       Example: { "date_from": "2019-08-01", "date_to": "2019-08-02"}.
       Values may not exceed 100 characters. Please refer to:
       https://cloud.google.com/dataproc/docs/concepts/workflows/workflow-parameters
   :type parameters: Dict[str, str]
   :param request_id: Optional. A unique id used to identify the request. If the server receives two
       ``SubmitJobRequest`` requests with the same id, then the second request will be ignored and the first
       ``Job`` created and stored in the backend is returned.
       It is recommended to always set this value to a UUID.
   :type request_id: str
   :param parameters: Optional. Map from parameter names to values that should be used for those
       parameters. Values may not exceed 100 characters.
   :type parameters: Dict[str, str]
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
      :annotation: = ['template', 'impersonation_chain']

      

   .. attribute:: template_fields_renderers
      

      

   
   .. method:: execute(self, context)




.. py:class:: DataprocSubmitJobOperator(*, project_id: str, location: str, job: Dict, request_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, asynchronous: bool = False, cancel_on_kill: bool = True, wait_timeout: Optional[int] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Submits a job to a cluster.

   :param project_id: Required. The ID of the Google Cloud project that the job belongs to.
   :type project_id: str
   :param location: Required. The Cloud Dataproc region in which to handle the request.
   :type location: str
   :param job: Required. The job resource.
       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.dataproc_v1beta2.types.Job`
   :type job: Dict
   :param request_id: Optional. A unique id used to identify the request. If the server receives two
       ``SubmitJobRequest`` requests with the same id, then the second request will be ignored and the first
       ``Job`` created and stored in the backend is returned.
       It is recommended to always set this value to a UUID.
   :type request_id: str
   :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
       retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
       ``retry`` is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]
   :param gcp_conn_id:
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
   :param asynchronous: Flag to return after submitting the job to the Dataproc API.
       This is useful for submitting long running jobs and
       waiting on them asynchronously using the DataprocJobSensor
   :type asynchronous: bool
   :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
   :type cancel_on_kill: bool
   :param wait_timeout: How many seconds wait for job to be ready. Used only if ``asynchronous`` is False
   :type wait_timeout: int

   .. attribute:: template_fields
      :annotation: = ['project_id', 'location', 'job', 'impersonation_chain']

      

   .. attribute:: template_fields_renderers
      

      

   
   .. method:: execute(self, context: Dict)



   
   .. method:: on_kill(self)




.. py:class:: DataprocUpdateClusterOperator(*, location: str, cluster_name: str, cluster: Union[Dict, Cluster], update_mask: Union[Dict, FieldMask], graceful_decommission_timeout: Union[Dict, Duration], request_id: Optional[str] = None, project_id: Optional[str] = None, retry: Retry = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates a cluster in a project.

   :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
   :type project_id: str
   :param location: Required. The Cloud Dataproc region in which to handle the request.
   :type location: str
   :param cluster_name: Required. The cluster name.
   :type cluster_name: str
   :param cluster: Required. The changes to the cluster.

       If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.dataproc_v1beta2.types.Cluster`
   :type cluster: Union[Dict, google.cloud.dataproc_v1beta2.types.Cluster]
   :param update_mask: Required. Specifies the path, relative to ``Cluster``, of the field to update. For
       example, to change the number of workers in a cluster to 5, the ``update_mask`` parameter would be
       specified as ``config.worker_config.num_instances``, and the ``PATCH`` request body would specify the
       new value. If a dict is provided, it must be of the same form as the protobuf message
       :class:`~google.cloud.dataproc_v1beta2.types.FieldMask`
   :type update_mask: Union[Dict, google.cloud.dataproc_v1beta2.types.FieldMask]
   :param graceful_decommission_timeout: Optional. Timeout for graceful YARN decommissioning. Graceful
       decommissioning allows removing nodes from the cluster without interrupting jobs in progress. Timeout
       specifies how long to wait for jobs in progress to finish before forcefully removing nodes (and
       potentially interrupting jobs). Default timeout is 0 (for forceful decommission), and the maximum
       allowed timeout is 1 day.
   :type graceful_decommission_timeout: Union[Dict, google.cloud.dataproc_v1beta2.types.Duration]
   :param request_id: Optional. A unique id used to identify the request. If the server receives two
       ``UpdateClusterRequest`` requests with the same id, then the second request will be ignored and the
       first ``google.longrunning.Operation`` created and stored in the backend is returned.
   :type request_id: str
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
      :annotation: = ['impersonation_chain']

      

   
   .. method:: execute(self, context: Dict)




