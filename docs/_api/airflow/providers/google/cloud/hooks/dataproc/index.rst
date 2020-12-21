:mod:`airflow.providers.google.cloud.hooks.dataproc`
====================================================

.. py:module:: airflow.providers.google.cloud.hooks.dataproc

.. autoapi-nested-parse::

   This module contains a Google Cloud Dataproc hook.



Module Contents
---------------

.. py:class:: DataProcJobBuilder(project_id: str, task_id: str, cluster_name: str, job_type: str, properties: Optional[Dict[str, str]] = None)

   A helper class for building Dataproc job.

   
   .. method:: add_labels(self, labels: dict)

      Set labels for Dataproc job.

      :param labels: Labels for the job query.
      :type labels: dict



   
   .. method:: add_variables(self, variables: List[str])

      Set variables for Dataproc job.

      :param variables: Variables for the job query.
      :type variables: List[str]



   
   .. method:: add_args(self, args: List[str])

      Set args for Dataproc job.

      :param args: Args for the job query.
      :type args: List[str]



   
   .. method:: add_query(self, query: List[str])

      Set query uris for Dataproc job.

      :param query: URIs for the job queries.
      :type query: List[str]



   
   .. method:: add_query_uri(self, query_uri: str)

      Set query uri for Dataproc job.

      :param query_uri: URI for the job query.
      :type query_uri: str



   
   .. method:: add_jar_file_uris(self, jars: List[str])

      Set jars uris for Dataproc job.

      :param jars: List of jars URIs
      :type jars: List[str]



   
   .. method:: add_archive_uris(self, archives: List[str])

      Set archives uris for Dataproc job.

      :param archives: List of archives URIs
      :type archives: List[str]



   
   .. method:: add_file_uris(self, files: List[str])

      Set file uris for Dataproc job.

      :param files: List of files URIs
      :type files: List[str]



   
   .. method:: add_python_file_uris(self, pyfiles: List[str])

      Set python file uris for Dataproc job.

      :param pyfiles: List of python files URIs
      :type pyfiles: List[str]



   
   .. method:: set_main(self, main_jar: Optional[str], main_class: Optional[str])

      Set Dataproc main class.

      :param main_jar: URI for the main file.
      :type main_jar: str
      :param main_class: Name of the main class.
      :type main_class: str
      :raises: Exception



   
   .. method:: set_python_main(self, main: str)

      Set Dataproc main python file uri.

      :param main: URI for the python main file.
      :type main: str



   
   .. method:: set_job_name(self, name: str)

      Set Dataproc job name.

      :param name: Job name.
      :type name: str



   
   .. method:: build(self)

      Returns Dataproc job.

      :return: Dataproc job
      :rtype: dict




.. py:class:: DataprocHook

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud Dataproc APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. method:: get_cluster_client(self, location: Optional[str] = None)

      Returns ClusterControllerClient.



   
   .. method:: get_template_client(self)

      Returns WorkflowTemplateServiceClient.



   
   .. method:: get_job_client(self, location: Optional[str] = None)

      Returns JobControllerClient.



   
   .. method:: create_cluster(self, region: str, project_id: str, cluster_name: str, cluster_config: Union[Dict, Cluster], labels: Optional[Dict[str, str]] = None, request_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Creates a cluster in a project.

      :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
      :type project_id: str
      :param region: Required. The Cloud Dataproc region in which to handle the request.
      :type region: str
      :param cluster_name: Name of the cluster to create
      :type cluster_name: str
      :param labels: Labels that will be assigned to created cluster
      :type labels: Dict[str, str]
      :param cluster_config: Required. The cluster config to create.
          If a dict is provided, it must be of the same form as the protobuf message
          :class:`~google.cloud.dataproc_v1.types.ClusterConfig`
      :type cluster_config: Union[Dict, google.cloud.dataproc_v1.types.ClusterConfig]
      :param request_id: Optional. A unique id used to identify the request. If the server receives two
          ``CreateClusterRequest`` requests with the same id, then the second request will be ignored and
          the first ``google.longrunning.Operation`` created and stored in the backend is returned.
      :type request_id: str
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: delete_cluster(self, region: str, cluster_name: str, project_id: str, cluster_uuid: Optional[str] = None, request_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

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
          ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and
          the first ``google.longrunning.Operation`` created and stored in the backend is returned.
      :type request_id: str
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: diagnose_cluster(self, region: str, cluster_name: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets cluster diagnostic information. After the operation completes GCS uri to
      diagnose is returned

      :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
      :type project_id: str
      :param region: Required. The Cloud Dataproc region in which to handle the request.
      :type region: str
      :param cluster_name: Required. The cluster name.
      :type cluster_name: str
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: get_cluster(self, region: str, cluster_name: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets the resource representation for a cluster in a project.

      :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
      :type project_id: str
      :param region: Required. The Cloud Dataproc region in which to handle the request.
      :type region: str
      :param cluster_name: Required. The cluster name.
      :type cluster_name: str
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: list_clusters(self, region: str, filter_: str, project_id: str, page_size: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists all regions/{region}/clusters in a project.

      :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
      :type project_id: str
      :param region: Required. The Cloud Dataproc region in which to handle the request.
      :type region: str
      :param filter_: Optional. A filter constraining the clusters to list. Filters are case-sensitive.
      :type filter_: str
      :param page_size: The maximum number of resources contained in the underlying API response. If page
          streaming is performed per- resource, this parameter does not affect the return value. If page
          streaming is performed per-page, this determines the maximum number of resources in a page.
      :type page_size: int
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: update_cluster(self, location: str, cluster_name: str, cluster: Union[Dict, Cluster], update_mask: Union[Dict, FieldMask], project_id: str, graceful_decommission_timeout: Optional[Union[Dict, Duration]] = None, request_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Updates a cluster in a project.

      :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
      :type project_id: str
      :param location: Required. The Cloud Dataproc region in which to handle the request.
      :type location: str
      :param cluster_name: Required. The cluster name.
      :type cluster_name: str
      :param cluster: Required. The changes to the cluster.

          If a dict is provided, it must be of the same form as the protobuf message
          :class:`~google.cloud.dataproc_v1.types.Cluster`
      :type cluster: Union[Dict, google.cloud.dataproc_v1.types.Cluster]
      :param update_mask: Required. Specifies the path, relative to ``Cluster``, of the field to update. For
          example, to change the number of workers in a cluster to 5, the ``update_mask`` parameter would be
          specified as ``config.worker_config.num_instances``, and the ``PATCH`` request body would specify
          the new value, as follows:

          ::

               { "config":{ "workerConfig":{ "numInstances":"5" } } }

          Similarly, to change the number of preemptible workers in a cluster to 5, the ``update_mask``
          parameter would be ``config.secondary_worker_config.num_instances``, and the ``PATCH`` request
          body would be set as follows:

          ::

               { "config":{ "secondaryWorkerConfig":{ "numInstances":"5" } } }

          If a dict is provided, it must be of the same form as the protobuf message
          :class:`~google.cloud.dataproc_v1.types.FieldMask`
      :type update_mask: Union[Dict, google.cloud.dataproc_v1.types.FieldMask]
      :param graceful_decommission_timeout: Optional. Timeout for graceful YARN decommissioning. Graceful
          decommissioning allows removing nodes from the cluster without interrupting jobs in progress.
          Timeout specifies how long to wait for jobs in progress to finish before forcefully removing nodes
          (and potentially interrupting jobs). Default timeout is 0 (for forceful decommission), and the
          maximum allowed timeout is 1 day.

          Only supported on Dataproc image versions 1.2 and higher.

          If a dict is provided, it must be of the same form as the protobuf message
          :class:`~google.cloud.dataproc_v1.types.Duration`
      :type graceful_decommission_timeout: Union[Dict, google.cloud.dataproc_v1.types.Duration]
      :param request_id: Optional. A unique id used to identify the request. If the server receives two
          ``UpdateClusterRequest`` requests with the same id, then the second request will be ignored and
          the first ``google.longrunning.Operation`` created and stored in the backend is returned.
      :type request_id: str
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: create_workflow_template(self, location: str, template: Union[Dict, WorkflowTemplate], project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Creates new workflow template.

      :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
      :type project_id: str
      :param location: Required. The Cloud Dataproc region in which to handle the request.
      :type location: str
      :param template: The Dataproc workflow template to create. If a dict is provided,
          it must be of the same form as the protobuf message WorkflowTemplate.
      :type template: Union[dict, WorkflowTemplate]
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: instantiate_workflow_template(self, location: str, template_name: str, project_id: str, version: Optional[int] = None, request_id: Optional[str] = None, parameters: Optional[Dict[str, str]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Instantiates a template and begins execution.

      :param template_name: Name of template to instantiate.
      :type template_name: str
      :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
      :type project_id: str
      :param location: Required. The Cloud Dataproc region in which to handle the request.
      :type location: str
      :param version: Optional. The version of workflow template to instantiate. If specified,
          the workflow will be instantiated only if the current version of
          the workflow template has the supplied version.
          This option cannot be used to instantiate a previous version of
          workflow template.
      :type version: int
      :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
          with the same tag from running. This mitigates risk of concurrent
          instances started due to retries.
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



   
   .. method:: instantiate_inline_workflow_template(self, location: str, template: Union[Dict, WorkflowTemplate], project_id: str, request_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Instantiates a template and begins execution.

      :param template: The workflow template to instantiate. If a dict is provided,
          it must be of the same form as the protobuf message WorkflowTemplate
      :type template: Union[Dict, WorkflowTemplate]
      :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
      :type project_id: str
      :param location: Required. The Cloud Dataproc region in which to handle the request.
      :type location: str
      :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
          with the same tag from running. This mitigates risk of concurrent
          instances started due to retries.
      :type request_id: str
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: wait_for_job(self, job_id: str, location: str, project_id: str, wait_time: int = 10, timeout: Optional[int] = None)

      Helper method which polls a job to check if it finishes.

      :param job_id: Id of the Dataproc job
      :type job_id: str
      :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
      :type project_id: str
      :param location: Required. The Cloud Dataproc region in which to handle the request.
      :type location: str
      :param wait_time: Number of seconds between checks
      :type wait_time: int
      :param timeout: How many seconds wait for job to be ready. Used only if ``asynchronous`` is False
      :type timeout: int



   
   .. method:: get_job(self, location: str, job_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets the resource representation for a job in a project.

      :param job_id: Id of the Dataproc job
      :type job_id: str
      :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
      :type project_id: str
      :param location: Required. The Cloud Dataproc region in which to handle the request.
      :type location: str
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: submit_job(self, location: str, job: Union[dict, Job], project_id: str, request_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Submits a job to a cluster.

      :param job: The job resource. If a dict is provided,
          it must be of the same form as the protobuf message Job
      :type job: Union[Dict, Job]
      :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
      :type project_id: str
      :param location: Required. The Cloud Dataproc region in which to handle the request.
      :type location: str
      :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
          with the same tag from running. This mitigates risk of concurrent
          instances started due to retries.
      :type request_id: str
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: submit(self, project_id: str, job: dict, region: str = 'global', job_error_states: Optional[Iterable[str]] = None)

      Submits Google Cloud Dataproc job.

      :param project_id: The id of Google Cloud Dataproc project.
      :type project_id: str
      :param job: The job to be submitted
      :type job: dict
      :param region: The region of Google Dataproc cluster.
      :type region: str
      :param job_error_states: Job states that should be considered error states.
      :type job_error_states: List[str]



   
   .. method:: cancel_job(self, job_id: str, project_id: str, location: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Starts a job cancellation request.

      :param project_id: Required. The ID of the Google Cloud project that the job belongs to.
      :type project_id: str
      :param location: Required. The Cloud Dataproc region in which to handle the request.
      :type location: str
      :param job_id: Required. The job ID.
      :type job_id: str
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]




