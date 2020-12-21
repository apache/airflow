:mod:`airflow.kubernetes.pod_generator_deprecated`
==================================================

.. py:module:: airflow.kubernetes.pod_generator_deprecated

.. autoapi-nested-parse::

   This module provides an interface between the previous Pod
   API and outputs a kubernetes.client.models.V1Pod.
   The advantage being that the full Kubernetes API
   is supported and no serialization need be written.



Module Contents
---------------

.. data:: MAX_POD_ID_LEN
   :annotation: = 253

   

.. data:: MAX_LABEL_LEN
   :annotation: = 63

   

.. py:class:: PodDefaults

   Static defaults for Pods

   .. attribute:: XCOM_MOUNT_PATH
      :annotation: = /airflow/xcom

      

   .. attribute:: SIDECAR_CONTAINER_NAME
      :annotation: = airflow-xcom-sidecar

      

   .. attribute:: XCOM_CMD
      :annotation: = trap "exit 0" INT; while true; do sleep 30; done;

      

   .. attribute:: VOLUME_MOUNT
      

      

   .. attribute:: VOLUME
      

      

   .. attribute:: SIDECAR_CONTAINER
      

      


.. function:: make_safe_label_value(string)
   Valid label values must be 63 characters or less and must be empty or begin and
   end with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),
   dots (.), and alphanumerics between.

   If the label value is greater than 63 chars once made safe, or differs in any
   way from the original value sent to this function, then we need to truncate to
   53 chars, and append it with a unique hash.


.. py:class:: PodGenerator(image: Optional[str] = None, name: Optional[str] = None, namespace: Optional[str] = None, volume_mounts: Optional[List[Union[k8s.V1VolumeMount, dict]]] = None, envs: Optional[Dict[str, str]] = None, cmds: Optional[List[str]] = None, args: Optional[List[str]] = None, labels: Optional[Dict[str, str]] = None, node_selectors: Optional[Dict[str, str]] = None, ports: Optional[List[Union[k8s.V1ContainerPort, dict]]] = None, volumes: Optional[List[Union[k8s.V1Volume, dict]]] = None, image_pull_policy: Optional[str] = None, restart_policy: Optional[str] = None, image_pull_secrets: Optional[str] = None, init_containers: Optional[List[k8s.V1Container]] = None, service_account_name: Optional[str] = None, resources: Optional[Union[k8s.V1ResourceRequirements, dict]] = None, annotations: Optional[Dict[str, str]] = None, affinity: Optional[dict] = None, hostnetwork: bool = False, tolerations: Optional[list] = None, security_context: Optional[Union[k8s.V1PodSecurityContext, dict]] = None, configmaps: Optional[List[str]] = None, dnspolicy: Optional[str] = None, schedulername: Optional[str] = None, extract_xcom: bool = False, priority_class_name: Optional[str] = None)

   Contains Kubernetes Airflow Worker configuration logic

   Represents a kubernetes pod and manages execution of a single pod.
   Any configuration that is container specific gets applied to
   the first container in the list of containers.

   :param image: The docker image
   :type image: Optional[str]
   :param name: name in the metadata section (not the container name)
   :type name: Optional[str]
   :param namespace: pod namespace
   :type namespace: Optional[str]
   :param volume_mounts: list of kubernetes volumes mounts
   :type volume_mounts: Optional[List[Union[k8s.V1VolumeMount, dict]]]
   :param envs: A dict containing the environment variables
   :type envs: Optional[Dict[str, str]]
   :param cmds: The command to be run on the first container
   :type cmds: Optional[List[str]]
   :param args: The arguments to be run on the pod
   :type args: Optional[List[str]]
   :param labels: labels for the pod metadata
   :type labels: Optional[Dict[str, str]]
   :param node_selectors: node selectors for the pod
   :type node_selectors: Optional[Dict[str, str]]
   :param ports: list of ports. Applies to the first container.
   :type ports: Optional[List[Union[k8s.V1ContainerPort, dict]]]
   :param volumes: Volumes to be attached to the first container
   :type volumes: Optional[List[Union[k8s.V1Volume, dict]]]
   :param image_pull_policy: Specify a policy to cache or always pull an image
   :type image_pull_policy: str
   :param restart_policy: The restart policy of the pod
   :type restart_policy: str
   :param image_pull_secrets: Any image pull secrets to be given to the pod.
       If more than one secret is required, provide a comma separated list:
       secret_a,secret_b
   :type image_pull_secrets: str
   :param init_containers: A list of init containers
   :type init_containers: Optional[List[k8s.V1Container]]
   :param service_account_name: Identity for processes that run in a Pod
   :type service_account_name: Optional[str]
   :param resources: Resource requirements for the first containers
   :type resources: Optional[Union[k8s.V1ResourceRequirements, dict]]
   :param annotations: annotations for the pod
   :type annotations: Optional[Dict[str, str]]
   :param affinity: A dict containing a group of affinity scheduling rules
   :type affinity: Optional[dict]
   :param hostnetwork: If True enable host networking on the pod
   :type hostnetwork: bool
   :param tolerations: A list of kubernetes tolerations
   :type tolerations: Optional[list]
   :param security_context: A dict containing the security context for the pod
   :type security_context: Optional[Union[k8s.V1PodSecurityContext, dict]]
   :param configmaps: Any configmap refs to envfrom.
       If more than one configmap is required, provide a comma separated list
       configmap_a,configmap_b
   :type configmaps: List[str]
   :param dnspolicy: Specify a dnspolicy for the pod
   :type dnspolicy: Optional[str]
   :param schedulername: Specify a schedulername for the pod
   :type schedulername: Optional[str]
   :param pod: The fully specified pod. Mutually exclusive with `path_or_string`
   :type pod: Optional[kubernetes.client.models.V1Pod]
   :param extract_xcom: Whether to bring up a container for xcom
   :type extract_xcom: bool
   :param priority_class_name: priority class name for the launched Pod
   :type priority_class_name: str

   
   .. method:: gen_pod(self)

      Generates pod



   
   .. staticmethod:: add_sidecar(pod: k8s.V1Pod)

      Adds sidecar



   
   .. staticmethod:: from_obj(obj)

      Converts to pod from obj



   
   .. staticmethod:: make_unique_pod_id(dag_id)

      Kubernetes pod names must be <= 253 chars and must pass the following regex for
      validation
      ``^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$``

      :param dag_id: a dag_id with only alphanumeric characters
      :return: ``str`` valid Pod name of appropriate length




