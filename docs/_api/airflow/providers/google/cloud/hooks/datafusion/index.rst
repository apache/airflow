:mod:`airflow.providers.google.cloud.hooks.datafusion`
======================================================

.. py:module:: airflow.providers.google.cloud.hooks.datafusion

.. autoapi-nested-parse::

   This module contains Google DataFusion hook.



Module Contents
---------------

.. data:: Operation
   

   

.. py:class:: PipelineStates

   Data Fusion pipeline states

   .. attribute:: PENDING
      :annotation: = PENDING

      

   .. attribute:: STARTING
      :annotation: = STARTING

      

   .. attribute:: RUNNING
      :annotation: = RUNNING

      

   .. attribute:: SUSPENDED
      :annotation: = SUSPENDED

      

   .. attribute:: RESUMING
      :annotation: = RESUMING

      

   .. attribute:: COMPLETED
      :annotation: = COMPLETED

      

   .. attribute:: FAILED
      :annotation: = FAILED

      

   .. attribute:: KILLED
      :annotation: = KILLED

      

   .. attribute:: REJECTED
      :annotation: = REJECTED

      


.. data:: FAILURE_STATES
   

   

.. data:: SUCCESS_STATES
   

   

.. py:class:: DataFusionHook(api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google DataFusion.

   .. attribute:: _conn
      :annotation: :Optional[Resource]

      

   
   .. method:: wait_for_operation(self, operation: Dict[str, Any])

      Waits for long-lasting operation to complete.



   
   .. method:: wait_for_pipeline_state(self, pipeline_name: str, pipeline_id: str, instance_url: str, namespace: str = 'default', success_states: Optional[List[str]] = None, failure_states: Optional[List[str]] = None, timeout: int = 5 * 60)

      Polls pipeline state and raises an exception if the state is one of
      `failure_states` or the operation timed_out.



   
   .. staticmethod:: _name(project_id: str, location: str, instance_name: str)



   
   .. staticmethod:: _parent(project_id: str, location: str)



   
   .. staticmethod:: _base_url(instance_url: str, namespace: str)



   
   .. method:: _cdap_request(self, url: str, method: str, body: Optional[Union[List, Dict]] = None)



   
   .. method:: get_conn(self)

      Retrieves connection to DataFusion.



   
   .. method:: restart_instance(self, instance_name: str, location: str, project_id: str)

      Restart a single Data Fusion instance.
      At the end of an operation instance is fully restarted.

      :param instance_name: The name of the instance to restart.
      :type instance_name: str
      :param location: The Cloud Data Fusion location in which to handle the request.
      :type location: str
      :param project_id: The ID of the Google Cloud project that the instance belongs to.
      :type project_id: str



   
   .. method:: delete_instance(self, instance_name: str, location: str, project_id: str)

      Deletes a single Date Fusion instance.

      :param instance_name: The name of the instance to delete.
      :type instance_name: str
      :param location: The Cloud Data Fusion location in which to handle the request.
      :type location: str
      :param project_id: The ID of the Google Cloud project that the instance belongs to.
      :type project_id: str



   
   .. method:: create_instance(self, instance_name: str, instance: Dict[str, Any], location: str, project_id: str)

      Creates a new Data Fusion instance in the specified project and location.

      :param instance_name: The name of the instance to create.
      :type instance_name: str
      :param instance: An instance of Instance.
          https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
      :type instance: Dict[str, Any]
      :param location: The Cloud Data Fusion location in which to handle the request.
      :type location: str
      :param project_id: The ID of the Google Cloud project that the instance belongs to.
      :type project_id: str



   
   .. method:: get_instance(self, instance_name: str, location: str, project_id: str)

      Gets details of a single Data Fusion instance.

      :param instance_name: The name of the instance.
      :type instance_name: str
      :param location: The Cloud Data Fusion location in which to handle the request.
      :type location: str
      :param project_id: The ID of the Google Cloud project that the instance belongs to.
      :type project_id: str



   
   .. method:: patch_instance(self, instance_name: str, instance: Dict[str, Any], update_mask: str, location: str, project_id: str)

      Updates a single Data Fusion instance.

      :param instance_name: The name of the instance to create.
      :type instance_name: str
      :param instance: An instance of Instance.
          https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
      :type instance: Dict[str, Any]
      :param update_mask: Field mask is used to specify the fields that the update will overwrite
          in an instance resource. The fields specified in the updateMask are relative to the resource,
          not the full request. A field will be overwritten if it is in the mask. If the user does not
          provide a mask, all the supported fields (labels and options currently) will be overwritten.
          A comma-separated list of fully qualified names of fields. Example: "user.displayName,photo".
          https://developers.google.com/protocol-buffers/docs/reference/google.protobuf?_ga=2.205612571.-968688242.1573564810#google.protobuf.FieldMask
      :type update_mask: str
      :param location: The Cloud Data Fusion location in which to handle the request.
      :type location: str
      :param project_id: The ID of the Google Cloud project that the instance belongs to.
      :type project_id: str



   
   .. method:: create_pipeline(self, pipeline_name: str, pipeline: Dict[str, Any], instance_url: str, namespace: str = 'default')

      Creates a Cloud Data Fusion pipeline.

      :param pipeline_name: Your pipeline name.
      :type pipeline_name: str
      :param pipeline: The pipeline definition. For more information check:
          https://docs.cdap.io/cdap/current/en/developer-manual/pipelines/developing-pipelines.html#pipeline-configuration-file-format
      :type pipeline: Dict[str, Any]
      :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
      :type instance_url: str
      :param namespace: if your pipeline belongs to a Basic edition instance, the namespace ID
          is always default. If your pipeline belongs to an Enterprise edition instance, you
          can create a namespace.
      :type namespace: str



   
   .. method:: delete_pipeline(self, pipeline_name: str, instance_url: str, version_id: Optional[str] = None, namespace: str = 'default')

      Deletes a Cloud Data Fusion pipeline.

      :param pipeline_name: Your pipeline name.
      :type pipeline_name: str
      :param version_id: Version of pipeline to delete
      :type version_id: Optional[str]
      :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
      :type instance_url: str
      :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
          is always default. If your pipeline belongs to an Enterprise edition instance, you
          can create a namespace.
      :type namespace: str



   
   .. method:: list_pipelines(self, instance_url: str, artifact_name: Optional[str] = None, artifact_version: Optional[str] = None, namespace: str = 'default')

      Lists Cloud Data Fusion pipelines.

      :param artifact_version: Artifact version to filter instances
      :type artifact_version: Optional[str]
      :param artifact_name: Artifact name to filter instances
      :type artifact_name: Optional[str]
      :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
      :type instance_url: str
      :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
          is always default. If your pipeline belongs to an Enterprise edition instance, you
          can create a namespace.
      :type namespace: str



   
   .. method:: _get_workflow_state(self, pipeline_name: str, instance_url: str, pipeline_id: str, namespace: str = 'default')



   
   .. method:: start_pipeline(self, pipeline_name: str, instance_url: str, namespace: str = 'default', runtime_args: Optional[Dict[str, Any]] = None)

      Starts a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

      :param pipeline_name: Your pipeline name.
      :type pipeline_name: str
      :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
      :type instance_url: str
      :param runtime_args: Optional runtime JSON args to be passed to the pipeline
      :type runtime_args: Optional[Dict[str, Any]]
      :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
          is always default. If your pipeline belongs to an Enterprise edition instance, you
          can create a namespace.
      :type namespace: str



   
   .. method:: stop_pipeline(self, pipeline_name: str, instance_url: str, namespace: str = 'default')

      Stops a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

      :param pipeline_name: Your pipeline name.
      :type pipeline_name: str
      :param instance_url: Endpoint on which the REST APIs is accessible for the instance.
      :type instance_url: str
      :param namespace: f your pipeline belongs to a Basic edition instance, the namespace ID
          is always default. If your pipeline belongs to an Enterprise edition instance, you
          can create a namespace.
      :type namespace: str




