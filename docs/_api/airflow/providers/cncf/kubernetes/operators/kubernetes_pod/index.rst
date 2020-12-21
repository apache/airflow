:mod:`airflow.providers.cncf.kubernetes.operators.kubernetes_pod`
=================================================================

.. py:module:: airflow.providers.cncf.kubernetes.operators.kubernetes_pod

.. autoapi-nested-parse::

   Executes task in a Kubernetes POD



Module Contents
---------------

.. py:class:: KubernetesPodOperator(*, namespace: Optional[str] = None, image: Optional[str] = None, name: Optional[str] = None, cmds: Optional[List[str]] = None, arguments: Optional[List[str]] = None, ports: Optional[List[k8s.V1ContainerPort]] = None, volume_mounts: Optional[List[k8s.V1VolumeMount]] = None, volumes: Optional[List[k8s.V1Volume]] = None, env_vars: Optional[List[k8s.V1EnvVar]] = None, env_from: Optional[List[k8s.V1EnvFromSource]] = None, secrets: Optional[List[Secret]] = None, in_cluster: Optional[bool] = None, cluster_context: Optional[str] = None, labels: Optional[Dict] = None, reattach_on_restart: bool = True, startup_timeout_seconds: int = 120, get_logs: bool = True, image_pull_policy: str = 'IfNotPresent', annotations: Optional[Dict] = None, resources: Optional[k8s.V1ResourceRequirements] = None, affinity: Optional[Dict] = None, config_file: Optional[str] = None, node_selectors: Optional[Dict] = None, image_pull_secrets: Optional[List[k8s.V1LocalObjectReference]] = None, service_account_name: str = 'default', is_delete_operator_pod: bool = False, hostnetwork: bool = False, tolerations: Optional[List] = None, security_context: Optional[Dict] = None, dnspolicy: Optional[str] = None, schedulername: Optional[str] = None, full_pod_spec: Optional[k8s.V1Pod] = None, init_containers: Optional[List[k8s.V1Container]] = None, log_events_on_failure: bool = False, do_xcom_push: bool = False, pod_template_file: Optional[str] = None, priority_class_name: Optional[str] = None, pod_runtime_info_envs: List[PodRuntimeInfoEnv] = None, termination_grace_period: Optional[int] = None, configmaps: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Execute a task in a Kubernetes Pod

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:KubernetesPodOperator`

   .. note::
       If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__
       and Airflow is not running in the same cluster, consider using
       :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`, which
       simplifies the authorization process.

   :param namespace: the namespace to run within kubernetes.
   :type namespace: str
   :param image: Docker image you wish to launch. Defaults to hub.docker.com,
       but fully qualified URLS will point to custom repositories. (templated)
   :type image: str
   :param name: name of the pod in which the task will run, will be used (plus a random
       suffix) to generate a pod id (DNS-1123 subdomain, containing only [a-z0-9.-]).
   :type name: str
   :param cmds: entrypoint of the container. (templated)
       The docker images's entrypoint is used if this is not provided.
   :type cmds: list[str]
   :param arguments: arguments of the entrypoint. (templated)
       The docker image's CMD is used if this is not provided.
   :type arguments: list[str]
   :param ports: ports for launched pod.
   :type ports: list[k8s.V1ContainerPort]
   :param volume_mounts: volumeMounts for launched pod.
   :type volume_mounts: list[k8s.V1VolumeMount]
   :param volumes: volumes for launched pod. Includes ConfigMaps and PersistentVolumes.
   :type volumes: list[k8s.V1Volume]
   :param env_vars: Environment variables initialized in the container. (templated)
   :type env_vars: list[k8s.V1EnvVar]
   :param secrets: Kubernetes secrets to inject in the container.
       They can be exposed as environment vars or files in a volume.
   :type secrets: list[airflow.kubernetes.secret.Secret]
   :param in_cluster: run kubernetes client with in_cluster configuration.
   :type in_cluster: bool
   :param cluster_context: context that points to kubernetes cluster.
       Ignored when in_cluster is True. If None, current-context is used.
   :type cluster_context: str
   :param reattach_on_restart: if the scheduler dies while the pod is running, reattach and monitor
   :type reattach_on_restart: bool
   :param labels: labels to apply to the Pod. (templated)
   :type labels: dict
   :param startup_timeout_seconds: timeout in seconds to startup the pod.
   :type startup_timeout_seconds: int
   :param get_logs: get the stdout of the container as logs of the tasks.
   :type get_logs: bool
   :param image_pull_policy: Specify a policy to cache or always pull an image.
   :type image_pull_policy: str
   :param annotations: non-identifying metadata you can attach to the Pod.
       Can be a large range of data, and can include characters
       that are not permitted by labels.
   :type annotations: dict
   :param resources: A dict containing resources requests and limits.
       Possible keys are request_memory, request_cpu, limit_memory, limit_cpu,
       and limit_gpu, which will be used to generate airflow.kubernetes.pod.Resources.
       See also kubernetes.io/docs/concepts/configuration/manage-compute-resources-container
   :type resources: k8s.V1ResourceRequirements
   :param affinity: A dict containing a group of affinity scheduling rules.
   :type affinity: dict
   :param config_file: The path to the Kubernetes config file. (templated)
       If not specified, default value is ``~/.kube/config``
   :type config_file: str
   :param node_selectors: A dict containing a group of scheduling rules.
   :type node_selectors: dict
   :param image_pull_secrets: Any image pull secrets to be given to the pod.
       If more than one secret is required, provide a
       comma separated list: secret_a,secret_b
   :type image_pull_secrets: List[k8s.V1LocalObjectReference]
   :param service_account_name: Name of the service account
   :type service_account_name: str
   :param is_delete_operator_pod: What to do when the pod reaches its final
       state, or the execution is interrupted.
       If False (default): do nothing, If True: delete the pod
   :type is_delete_operator_pod: bool
   :param hostnetwork: If True enable host networking on the pod.
   :type hostnetwork: bool
   :param tolerations: A list of kubernetes tolerations.
   :type tolerations: list tolerations
   :param security_context: security options the pod should run with (PodSecurityContext).
   :type security_context: dict
   :param dnspolicy: dnspolicy for the pod.
   :type dnspolicy: str
   :param schedulername: Specify a schedulername for the pod
   :type schedulername: str
   :param full_pod_spec: The complete podSpec
   :type full_pod_spec: kubernetes.client.models.V1Pod
   :param init_containers: init container for the launched Pod
   :type init_containers: list[kubernetes.client.models.V1Container]
   :param log_events_on_failure: Log the pod's events if a failure occurs
   :type log_events_on_failure: bool
   :param do_xcom_push: If True, the content of the file
       /airflow/xcom/return.json in the container will also be pushed to an
       XCom when the container completes.
   :type do_xcom_push: bool
   :param pod_template_file: path to pod template file (templated)
   :type pod_template_file: str
   :param priority_class_name: priority class name for the launched Pod
   :type priority_class_name: str
   :param termination_grace_period: Termination grace period if task killed in UI,
       defaults to kubernetes default
   :type termination_grace_period: int

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['image', 'cmds', 'arguments', 'env_vars', 'labels', 'config_file', 'pod_template_file']

      

   
   .. staticmethod:: create_labels_for_pod(context)

      Generate labels for the pod to track the pod in case of Operator crash

      :param context: task context provided by airflow DAG
      :return: dict



   
   .. method:: execute(self, context)



   
   .. method:: handle_pod_overlap(self, labels: dict, try_numbers_match: bool, launcher: Any, pod: k8s.V1Pod)

      In cases where the Scheduler restarts while a KubernetesPodOperator task is running,
      this function will either continue to monitor the existing pod or launch a new pod
      based on the `reattach_on_restart` parameter.

      :param labels: labels used to determine if a pod is repeated
      :type labels: dict
      :param try_numbers_match: do the try numbers match? Only needed for logging purposes
      :type try_numbers_match: bool
      :param launcher: PodLauncher
      :param pod_list: list of pods found



   
   .. staticmethod:: _get_pod_identifying_label_string(labels)



   
   .. staticmethod:: _try_numbers_match(context, pod)



   
   .. method:: _set_name(self, name)



   
   .. method:: create_pod_request_obj(self)

      Creates a V1Pod based on user parameters. Note that a `pod` or `pod_template_file`
      will supersede all other values.



   
   .. method:: create_new_pod_for_operator(self, labels, launcher)

      Creates a new pod and monitors for duration of task

      :param labels: labels used to track pod
      :param launcher: pod launcher that will manage launching and monitoring pods
      :return:



   
   .. method:: patch_already_checked(self, pod: k8s.V1Pod)

      Add an "already tried annotation to ensure we only retry once



   
   .. method:: monitor_launched_pod(self, launcher, pod)

      Monitors a pod to completion that was created by a previous KubernetesPodOperator

      :param launcher: pod launcher that will manage launching and monitoring pods
      :param pod: podspec used to find pod using k8s API
      :return:



   
   .. method:: on_kill(self)




