:mod:`airflow.providers.amazon.aws.hooks.emr`
=============================================

.. py:module:: airflow.providers.amazon.aws.hooks.emr


Module Contents
---------------

.. py:class:: EmrHook(emr_conn_id: Optional[str] = None, *args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS EMR. emr_conn_id is only necessary for using the
   create_job_flow method.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. method:: get_cluster_id_by_name(self, emr_cluster_name: str, cluster_states: List[str])

      Fetch id of EMR cluster with given name and (optional) states.
      Will return only if single id is found.

      :param emr_cluster_name: Name of a cluster to find
      :type emr_cluster_name: str
      :param cluster_states: State(s) of cluster to find
      :type cluster_states: list
      :return: id of the EMR cluster



   
   .. method:: create_job_flow(self, job_flow_overrides: Dict[str, Any])

      Creates a job flow using the config from the EMR connection.
      Keys of the json extra hash may have the arguments of the boto3
      run_job_flow method.
      Overrides for this config may be passed as the job_flow_overrides.




