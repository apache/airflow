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

airflow.providers.cncf.kubernetes.operators.pod
===============================================

.. py:module:: airflow.providers.cncf.kubernetes.operators.pod

.. autoapi-nested-parse::

   Executes task in a Kubernetes POD.



Attributes
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.operators.pod.alphanum_lower
   airflow.providers.cncf.kubernetes.operators.pod.KUBE_CONFIG_ENV_VAR


Exceptions
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.operators.pod.PodReattachFailure
   airflow.providers.cncf.kubernetes.operators.pod.PodCredentialsExpiredFailure


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.operators.pod.PodEventType
   airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator


Module Contents
---------------

.. py:data:: alphanum_lower
   :value: 'abcdefghijklmnopqrstuvwxyz0123456789'


.. py:data:: KUBE_CONFIG_ENV_VAR
   :value: 'KUBECONFIG'


.. py:class:: PodEventType

   Bases: :py:obj:`enum.Enum`


   Type of Events emitted by kubernetes pod.


   .. py:attribute:: WARNING
      :value: 'Warning'



   .. py:attribute:: NORMAL
      :value: 'Normal'



.. py:exception:: PodReattachFailure

   Bases: :py:obj:`airflow.exceptions.AirflowException`


   When we expect to be able to find a pod but cannot.


.. py:exception:: PodCredentialsExpiredFailure

   Bases: :py:obj:`airflow.exceptions.AirflowException`


   When pod fails to refresh credentials.


.. py:class:: KubernetesPodOperator(*, kubernetes_conn_id = KubernetesHook.default_conn_name, namespace = None, image = None, name = None, random_name_suffix = True, cmds = None, arguments = None, ports = None, volume_mounts = None, volumes = None, env_vars = None, env_from = None, secrets = None, in_cluster = None, cluster_context = None, labels = None, reattach_on_restart = True, startup_timeout_seconds = 120, startup_check_interval_seconds = 5, get_logs = True, base_container_name = None, init_container_logs = None, container_logs = None, image_pull_policy = None, annotations = None, container_resources = None, affinity = None, config_file = None, node_selector = None, image_pull_secrets = None, service_account_name = None, hostnetwork = False, host_aliases = None, tolerations = None, security_context = None, container_security_context = None, dnspolicy = None, dns_config = None, hostname = None, subdomain = None, schedulername = None, full_pod_spec = None, init_containers = None, log_events_on_failure = False, do_xcom_push = False, pod_template_file = None, pod_template_dict = None, priority_class_name = None, pod_runtime_info_envs = None, termination_grace_period = None, configmaps = None, skip_on_exit_code = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), poll_interval = 2, log_pod_spec_on_failure = True, on_finish_action = 'delete_pod', is_delete_operator_pod = None, termination_message_policy = 'File', active_deadline_seconds = None, callbacks = None, progress_callback = None, logging_interval = None, **kwargs)

   Bases: :py:obj:`airflow.models.BaseOperator`


   Execute a task in a Kubernetes Pod.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:KubernetesPodOperator`

   .. note::
       If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__
       and Airflow is not running in the same cluster, consider using
       :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`, which
       simplifies the authorization process.

   :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
       for the Kubernetes cluster.
   :param namespace: the namespace to run within kubernetes.
   :param image: Docker image you wish to launch. Defaults to hub.docker.com,
       but fully qualified URLS will point to custom repositories. (templated)
   :param name: name of the pod in which the task will run, will be used (plus a random
       suffix if random_name_suffix is True) to generate a pod id (DNS-1123 subdomain,
       containing only [a-z0-9.-]).
   :param random_name_suffix: if True, will generate a random suffix.
   :param cmds: entrypoint of the container. (templated)
       The docker images's entrypoint is used if this is not provided.
   :param arguments: arguments of the entrypoint. (templated)
       The docker image's CMD is used if this is not provided.
   :param ports: ports for the launched pod.
   :param volume_mounts: volumeMounts for the launched pod.
   :param volumes: volumes for the launched pod. Includes ConfigMaps and PersistentVolumes.
   :param env_vars: Environment variables initialized in the container. (templated)
   :param env_from: (Optional) List of sources to populate environment variables in the container.
   :param secrets: Kubernetes secrets to inject in the container.
       They can be exposed as environment vars or files in a volume.
   :param in_cluster: run kubernetes client with in_cluster configuration.
   :param cluster_context: context that points to kubernetes cluster.
       Ignored when in_cluster is True. If None, current-context is used. (templated)
   :param reattach_on_restart: if the worker dies while the pod is running, reattach and monitor
       during the next try. If False, always create a new pod for each try.
   :param labels: labels to apply to the Pod. (templated)
   :param startup_timeout_seconds: timeout in seconds to startup the pod.
   :param startup_check_interval_seconds: interval in seconds to check if the pod has already started
   :param get_logs: get the stdout of the base container as logs of the tasks.
   :param init_container_logs: list of init containers whose logs will be published to stdout
       Takes a sequence of containers, a single container name or True. If True,
       all the containers logs are published.
   :param container_logs: list of containers whose logs will be published to stdout
       Takes a sequence of containers, a single container name or True. If True,
       all the containers logs are published. Works in conjunction with get_logs param.
       The default value is the base container.
   :param image_pull_policy: Specify a policy to cache or always pull an image.
   :param annotations: non-identifying metadata you can attach to the Pod.
       Can be a large range of data, and can include characters
       that are not permitted by labels. (templated)
   :param container_resources: resources for the launched pod. (templated)
   :param affinity: affinity scheduling rules for the launched pod.
   :param config_file: The path to the Kubernetes config file. (templated)
       If not specified, default value is ``~/.kube/config``
   :param node_selector: A dict containing a group of scheduling rules. (templated)
   :param image_pull_secrets: Any image pull secrets to be given to the pod.
       If more than one secret is required, provide a
       comma separated list: secret_a,secret_b
   :param service_account_name: Name of the service account
   :param hostnetwork: If True enable host networking on the pod.
   :param host_aliases: A list of host aliases to apply to the containers in the pod.
   :param tolerations: A list of kubernetes tolerations.
   :param security_context: security options the pod should run with (PodSecurityContext).
   :param container_security_context: security options the container should run with.
   :param dnspolicy: dnspolicy for the pod.
   :param dns_config: dns configuration (ip addresses, searches, options) for the pod.
   :param hostname: hostname for the pod.
   :param subdomain: subdomain for the pod.
   :param schedulername: Specify a schedulername for the pod
   :param full_pod_spec: The complete podSpec
   :param init_containers: init container for the launched Pod
   :param log_events_on_failure: Log the pod's events if a failure occurs
   :param do_xcom_push: If True, the content of the file
       /airflow/xcom/return.json in the container will also be pushed to an
       XCom when the container completes.
   :param pod_template_file: path to pod template file (templated)
   :param pod_template_dict: pod template dictionary (templated)
   :param priority_class_name: priority class name for the launched Pod
   :param pod_runtime_info_envs: (Optional) A list of environment variables,
       to be set in the container.
   :param termination_grace_period: Termination grace period if task killed in UI,
       defaults to kubernetes default
   :param configmaps: (Optional) A list of names of config maps from which it collects ConfigMaps
       to populate the environment variables with. The contents of the target
       ConfigMap's Data field will represent the key-value pairs as environment variables.
       Extends env_from.
   :param skip_on_exit_code: If task exits with this exit code, leave the task
       in ``skipped`` state (default: None). If set to ``None``, any non-zero
       exit code will be treated as a failure.
   :param base_container_name: The name of the base container in the pod. This container's logs
       will appear as part of this task's logs if get_logs is True. Defaults to None. If None,
       will consult the class variable BASE_CONTAINER_NAME (which defaults to "base") for the base
       container name to use.
   :param deferrable: Run operator in the deferrable mode.
   :param poll_interval: Polling period in seconds to check for the status. Used only in deferrable mode.
   :param log_pod_spec_on_failure: Log the pod's specification if a failure occurs
   :param on_finish_action: What to do when the pod reaches its final state, or the execution is interrupted.
       If "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
       only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod.
   :param termination_message_policy: The termination message policy of the base container.
       Default value is "File"
   :param active_deadline_seconds: The active_deadline_seconds which translates to active_deadline_seconds
       in V1PodSpec.
   :param callbacks: KubernetesPodOperatorCallback instance contains the callbacks methods on different step
       of KubernetesPodOperator.
   :param logging_interval: max time in seconds that task should be in deferred state before
       resuming to fetch the latest logs. If ``None``, then the task will remain in deferred state until pod
       is done, and no logs will be visible until that time.


   .. py:attribute:: BASE_CONTAINER_NAME
      :value: 'base'



   .. py:attribute:: ISTIO_CONTAINER_NAME
      :value: 'istio-proxy'



   .. py:attribute:: KILL_ISTIO_PROXY_SUCCESS_MSG
      :value: 'HTTP/1.1 200'



   .. py:attribute:: POD_CHECKED_KEY
      :value: 'already_checked'



   .. py:attribute:: POST_TERMINATION_TIMEOUT
      :value: 120



   .. py:attribute:: template_fields
      :type:  collections.abc.Sequence[str]
      :value: ('image', 'cmds', 'annotations', 'arguments', 'env_vars', 'labels', 'config_file',...



   .. py:attribute:: template_fields_renderers


   .. py:attribute:: kubernetes_conn_id
      :value: 'kubernetes_default'



   .. py:attribute:: do_xcom_push
      :value: False



   .. py:attribute:: image
      :value: None



   .. py:attribute:: namespace
      :value: None



   .. py:attribute:: cmds
      :value: []



   .. py:attribute:: arguments
      :value: []



   .. py:attribute:: labels


   .. py:attribute:: startup_timeout_seconds
      :value: 120



   .. py:attribute:: startup_check_interval_seconds
      :value: 5



   .. py:attribute:: env_vars
      :value: []



   .. py:attribute:: pod_runtime_info_envs
      :value: []



   .. py:attribute:: env_from
      :value: []



   .. py:attribute:: ports
      :value: []



   .. py:attribute:: volume_mounts
      :value: []



   .. py:attribute:: volumes
      :value: []



   .. py:attribute:: secrets
      :value: []



   .. py:attribute:: in_cluster
      :value: None



   .. py:attribute:: cluster_context
      :value: None



   .. py:attribute:: reattach_on_restart
      :value: True



   .. py:attribute:: get_logs
      :value: True



   .. py:attribute:: base_container_name
      :value: 'base'



   .. py:attribute:: init_container_logs
      :value: None



   .. py:attribute:: container_logs
      :value: 'base'



   .. py:attribute:: image_pull_policy
      :value: None



   .. py:attribute:: node_selector


   .. py:attribute:: annotations


   .. py:attribute:: affinity


   .. py:attribute:: container_resources
      :value: None



   .. py:attribute:: config_file
      :value: None



   .. py:attribute:: image_pull_secrets
      :value: []



   .. py:attribute:: service_account_name
      :value: None



   .. py:attribute:: hostnetwork
      :value: False



   .. py:attribute:: host_aliases
      :value: None



   .. py:attribute:: tolerations
      :value: []



   .. py:attribute:: security_context


   .. py:attribute:: container_security_context
      :value: None



   .. py:attribute:: dnspolicy
      :value: None



   .. py:attribute:: dns_config
      :value: None



   .. py:attribute:: hostname
      :value: None



   .. py:attribute:: subdomain
      :value: None



   .. py:attribute:: schedulername
      :value: None



   .. py:attribute:: full_pod_spec
      :value: None



   .. py:attribute:: init_containers
      :value: []



   .. py:attribute:: log_events_on_failure
      :value: False



   .. py:attribute:: priority_class_name
      :value: None



   .. py:attribute:: pod_template_file
      :value: None



   .. py:attribute:: pod_template_dict
      :value: None



   .. py:attribute:: name


   .. py:attribute:: random_name_suffix
      :value: True



   .. py:attribute:: termination_grace_period
      :value: None



   .. py:attribute:: pod_request_obj
      :type:  kubernetes.client.models.V1Pod | None
      :value: None



   .. py:attribute:: pod
      :type:  kubernetes.client.models.V1Pod | None
      :value: None



   .. py:attribute:: skip_on_exit_code
      :value: None



   .. py:attribute:: deferrable
      :value: True



   .. py:attribute:: poll_interval
      :value: 2



   .. py:attribute:: remote_pod
      :type:  kubernetes.client.models.V1Pod | None
      :value: None



   .. py:attribute:: log_pod_spec_on_failure
      :value: True



   .. py:attribute:: on_finish_action


   .. py:attribute:: termination_message_policy
      :value: 'File'



   .. py:attribute:: active_deadline_seconds
      :value: None



   .. py:attribute:: logging_interval
      :value: None



   .. py:attribute:: callbacks
      :value: []



   .. py:property:: pod_manager
      :type: airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager



   .. py:property:: hook
      :type: airflow.providers.cncf.kubernetes.utils.pod_manager.PodOperatorHookProtocol



   .. py:property:: client
      :type: kubernetes.client.CoreV1Api



   .. py:method:: find_pod(namespace, context, *, exclude_checked = True)

      Return an already-running pod for this task instance if one exists.



   .. py:method:: log_matching_pod(pod, context)


   .. py:method:: get_or_create_pod(pod_request_obj, context)


   .. py:method:: await_pod_start(pod)


   .. py:method:: extract_xcom(pod)

      Retrieve xcom value and kill xcom sidecar container.



   .. py:method:: execute(context)

      Based on the deferrable parameter runs the pod asynchronously or synchronously.



   .. py:method:: execute_sync(context)


   .. py:method:: await_init_containers_completion(pod)


   .. py:method:: await_pod_completion(pod)


   .. py:method:: execute_async(context)


   .. py:method:: convert_config_file_to_dict()

      Convert passed config_file to dict representation.



   .. py:method:: invoke_defer_method(last_log_time = None)

      Redefine triggers which are being used in child classes.



   .. py:method:: trigger_reentry(context, event)

      Point of re-entry from trigger.

      If ``logging_interval`` is None, then at this point, the pod should be done, and we'll just fetch
      the logs and exit.

      If ``logging_interval`` is not None, it could be that the pod is still running, and we'll just
      grab the latest logs and defer back to the trigger again.



   .. py:method:: post_complete_action(*, pod, remote_pod, context, **kwargs)

      Actions that must be done after operator finishes logic of the deferrable_execution.



   .. py:method:: cleanup(pod, remote_pod)


   .. py:method:: is_istio_enabled(pod)

      Check if istio is enabled for the namespace of the pod by inspecting the namespace labels.



   .. py:method:: kill_istio_sidecar(pod)


   .. py:method:: process_pod_deletion(pod, *, reraise=True)


   .. py:method:: patch_already_checked(pod, *, reraise=True)

      Add an "already checked" label to ensure we don't reattach on retries.



   .. py:method:: on_kill()

      Override this method to clean up subprocesses when a task instance gets killed.

      Any use of the threading, subprocess or multiprocessing module within an
      operator needs to be cleaned up, or it will leave ghost processes behind.



   .. py:method:: build_pod_request_obj(context = None)

      Return V1Pod object based on pod template file, full pod spec, and other operator parameters.

      The V1Pod attributes are derived (in order of precedence) from operator params, full pod spec, pod
      template file.



   .. py:method:: dry_run()

      Print out the pod definition that would be created by this operator.

      Does not include labels specific to the task instance (since there isn't
      one in a dry_run) and excludes all empty elements.



   .. py:method:: process_duplicate_label_pods(pod_list)

      Patch or delete the existing pod with duplicate labels.

      This is to handle an edge case that can happen only if reattach_on_restart
      flag is False, and the previous run attempt has failed because the task
      process has been killed externally by the cluster or another process.

      If the task process is killed externally, it breaks the code execution and
      immediately exists the task. As a result the pod created in the previous attempt
      will not be properly deleted or patched by cleanup() method.

      Return the newly created pod to be used for the next run attempt.
