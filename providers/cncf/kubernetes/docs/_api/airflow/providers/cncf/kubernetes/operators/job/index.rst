 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

airflow.providers.cncf.kubernetes.operators.job
===============================================

.. py:module:: airflow.providers.cncf.kubernetes.operators.job

.. autoapi-nested-parse::

   Executes a Kubernetes Job.



Attributes
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.operators.job.log


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.operators.job.KubernetesJobOperator
   airflow.providers.cncf.kubernetes.operators.job.KubernetesDeleteJobOperator
   airflow.providers.cncf.kubernetes.operators.job.KubernetesPatchJobOperator


Module Contents
---------------

.. py:data:: log

.. py:class:: KubernetesJobOperator(*, job_template_file = None, full_job_spec = None, backoff_limit = None, completion_mode = None, completions = None, manual_selector = None, parallelism = None, selector = None, suspend = None, ttl_seconds_after_finished = None, wait_until_job_complete = False, job_poll_interval = 10, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)

   Bases: :py:obj:`airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`


   Executes a Kubernetes Job.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:KubernetesJobOperator`

   .. note::
       If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__
       and Airflow is not running in the same cluster, consider using
       :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartJobOperator`, which
       simplifies the authorization process.

   :param job_template_file: path to job template file (templated)
   :param full_job_spec: The complete JodSpec
   :param backoff_limit: Specifies the number of retries before marking this job failed. Defaults to 6
   :param completion_mode: CompletionMode specifies how Pod completions are tracked. It can be ``NonIndexed`` (default) or ``Indexed``.
   :param completions: Specifies the desired number of successfully finished pods the job should be run with.
   :param manual_selector: manualSelector controls generation of pod labels and pod selectors.
   :param parallelism: Specifies the maximum desired number of pods the job should run at any given time.
   :param selector: The selector of this V1JobSpec.
   :param suspend: Suspend specifies whether the Job controller should create Pods or not.
   :param ttl_seconds_after_finished: ttlSecondsAfterFinished limits the lifetime of a Job that has finished execution (either Complete or Failed).
   :param wait_until_job_complete: Whether to wait until started job finished execution (either Complete or
       Failed). Default is False.
   :param job_poll_interval: Interval in seconds between polling the job status. Default is 10.
       Used if the parameter `wait_until_job_complete` set True.
   :param deferrable: Run operator in the deferrable mode. Note that the parameter
       `wait_until_job_complete` must be set True.


   .. py:attribute:: template_fields
      :type:  collections.abc.Sequence[str]


   .. py:attribute:: job_template_file
      :value: None



   .. py:attribute:: full_job_spec
      :value: None



   .. py:attribute:: job_request_obj
      :type:  kubernetes.client.models.V1Job | None
      :value: None



   .. py:attribute:: job
      :type:  kubernetes.client.models.V1Job | None
      :value: None



   .. py:attribute:: backoff_limit
      :value: None



   .. py:attribute:: completion_mode
      :value: None



   .. py:attribute:: completions
      :value: None



   .. py:attribute:: manual_selector
      :value: None



   .. py:attribute:: parallelism
      :value: None



   .. py:attribute:: selector
      :value: None



   .. py:attribute:: suspend
      :value: None



   .. py:attribute:: ttl_seconds_after_finished
      :value: None



   .. py:attribute:: wait_until_job_complete
      :value: False



   .. py:attribute:: job_poll_interval
      :value: 10



   .. py:attribute:: deferrable
      :value: True



   .. py:property:: hook
      :type: airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook



   .. py:property:: job_client
      :type: kubernetes.client.BatchV1Api



   .. py:method:: create_job(job_request_obj)


   .. py:method:: execute(context)

      Based on the deferrable parameter runs the pod asynchronously or synchronously.



   .. py:method:: execute_deferrable()


   .. py:method:: execute_complete(context, event, **kwargs)


   .. py:method:: deserialize_job_template_file(path)
      :staticmethod:


      Generate a Job from a file.

      Unfortunately we need access to the private method
      ``_ApiClient__deserialize_model`` from the kubernetes client.
      This issue is tracked here: https://github.com/kubernetes-client/python/issues/977.

      :param path: Path to the file
      :return: a kubernetes.client.models.V1Job



   .. py:method:: on_kill()

      Override this method to clean up subprocesses when a task instance gets killed.

      Any use of the threading, subprocess or multiprocessing module within an
      operator needs to be cleaned up, or it will leave ghost processes behind.



   .. py:method:: build_job_request_obj(context = None)

      Return V1Job object based on job template file, full job spec, and other operator parameters.

      The V1Job attributes are derived (in order of precedence) from operator params, full job spec, job
      template file.



   .. py:method:: reconcile_jobs(base_job, client_job)
      :staticmethod:


      Merge Kubernetes Job objects.

      :param base_job: has the base attributes which are overwritten if they exist
          in the client job and remain if they do not exist in the client_job
      :param client_job: the job that the client wants to create.
      :return: the merged jobs

      This can't be done recursively as certain fields are overwritten and some are concatenated.



   .. py:method:: reconcile_job_specs(base_spec, client_spec)
      :staticmethod:


      Merge Kubernetes JobSpec objects.

      :param base_spec: has the base attributes which are overwritten if they exist
          in the client_spec and remain if they do not exist in the client_spec
      :param client_spec: the spec that the client wants to create.
      :return: the merged specs



.. py:class:: KubernetesDeleteJobOperator(*, name, namespace, kubernetes_conn_id = KubernetesHook.default_conn_name, config_file = None, in_cluster = None, cluster_context = None, delete_on_status = None, wait_for_completion = False, poll_interval = 10.0, **kwargs)

   Bases: :py:obj:`airflow.models.BaseOperator`


   Delete a Kubernetes Job.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:KubernetesDeleteJobOperator`

   :param name: name of the Job.
   :param namespace: the namespace to run within kubernetes.
   :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
       for the Kubernetes cluster.
   :param config_file: The path to the Kubernetes config file. (templated)
       If not specified, default value is ``~/.kube/config``
   :param in_cluster: run kubernetes client with in_cluster configuration.
   :param cluster_context: context that points to kubernetes cluster.
       Ignored when in_cluster is True. If None, current-context is used. (templated)
   :param delete_on_status: Condition for performing delete operation depending on the job status. Values:
       ``None`` - delete the job regardless of its status, "Complete" - delete only successfully completed
       jobs, "Failed" - delete only failed jobs. (default: ``None``)
   :param wait_for_completion: Whether to wait for the job to complete. (default: ``False``)
   :param poll_interval: Interval in seconds between polling the job status. Used when the ``delete_on_status``
       parameter is set. (default: 10.0)


   .. py:attribute:: template_fields
      :type:  collections.abc.Sequence[str]
      :value: ('config_file', 'name', 'namespace', 'cluster_context')



   .. py:attribute:: name


   .. py:attribute:: namespace


   .. py:attribute:: kubernetes_conn_id
      :value: 'kubernetes_default'



   .. py:attribute:: config_file
      :value: None



   .. py:attribute:: in_cluster
      :value: None



   .. py:attribute:: cluster_context
      :value: None



   .. py:attribute:: delete_on_status
      :value: None



   .. py:attribute:: wait_for_completion
      :value: False



   .. py:attribute:: poll_interval
      :value: 10.0



   .. py:property:: hook
      :type: airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook



   .. py:property:: client
      :type: kubernetes.client.BatchV1Api



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: KubernetesPatchJobOperator(*, name, namespace, body, kubernetes_conn_id = KubernetesHook.default_conn_name, config_file = None, in_cluster = None, cluster_context = None, **kwargs)

   Bases: :py:obj:`airflow.models.BaseOperator`


   Update a Kubernetes Job.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:KubernetesPatchJobOperator`

   :param name: name of the Job
   :param namespace: the namespace to run within kubernetes
   :param body: Job json object with parameters for update
       https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.25/#job-v1-batch
       e.g. ``{"spec": {"suspend": True}}``
   :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
       for the Kubernetes cluster.
   :param config_file: The path to the Kubernetes config file. (templated)
       If not specified, default value is ``~/.kube/config``
   :param in_cluster: run kubernetes client with in_cluster configuration.
   :param cluster_context: context that points to kubernetes cluster.
       Ignored when in_cluster is True. If None, current-context is used. (templated)


   .. py:attribute:: template_fields
      :type:  collections.abc.Sequence[str]
      :value: ('config_file', 'name', 'namespace', 'body', 'cluster_context')



   .. py:attribute:: name


   .. py:attribute:: namespace


   .. py:attribute:: body


   .. py:attribute:: kubernetes_conn_id
      :value: 'kubernetes_default'



   .. py:attribute:: config_file
      :value: None



   .. py:attribute:: in_cluster
      :value: None



   .. py:attribute:: cluster_context
      :value: None



   .. py:property:: hook
      :type: airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
