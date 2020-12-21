:mod:`airflow.kubernetes.pod_generator`
=======================================

.. py:module:: airflow.kubernetes.pod_generator

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


.. function:: datetime_to_label_safe_datestring(datetime_obj: datetime.datetime) -> str
   Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but
   not "_" let's
   replace ":" with "_"

   :param datetime_obj: datetime.datetime object
   :return: ISO-like string representing the datetime


.. function:: label_safe_datestring_to_datetime(string: str) -> datetime.datetime
   Kubernetes doesn't permit ":" in labels. ISO datetime format uses ":" but not
   "_", let's
   replace ":" with "_"

   :param string: str
   :return: datetime.datetime object


.. py:class:: PodGenerator(pod: Optional[k8s.V1Pod] = None, pod_template_file: Optional[str] = None, extract_xcom: bool = True)

   Contains Kubernetes Airflow Worker configuration logic

   Represents a kubernetes pod and manages execution of a single pod.
   Any configuration that is container specific gets applied to
   the first container in the list of containers.

   :param pod: The fully specified pod. Mutually exclusive with `path_or_string`
   :type pod: Optional[kubernetes.client.models.V1Pod]
   :param pod_template_file: Path to YAML file. Mutually exclusive with `pod`
   :type pod_template_file: Optional[str]
   :param extract_xcom: Whether to bring up a container for xcom
   :type extract_xcom: bool

   
   .. method:: gen_pod(self)

      Generates pod



   
   .. staticmethod:: add_xcom_sidecar(pod: k8s.V1Pod)

      Adds sidecar



   
   .. staticmethod:: from_obj(obj)

      Converts to pod from obj



   
   .. staticmethod:: from_legacy_obj(obj)

      Converts to pod from obj



   
   .. staticmethod:: reconcile_pods(base_pod: k8s.V1Pod, client_pod: Optional[k8s.V1Pod])

      :param base_pod: has the base attributes which are overwritten if they exist
          in the client pod and remain if they do not exist in the client_pod
      :type base_pod: k8s.V1Pod
      :param client_pod: the pod that the client wants to create.
      :type client_pod: k8s.V1Pod
      :return: the merged pods

      This can't be done recursively as certain fields some overwritten, and some concatenated.



   
   .. staticmethod:: reconcile_metadata(base_meta, client_meta)

      Merge kubernetes Metadata objects
      :param base_meta: has the base attributes which are overwritten if they exist
          in the client_meta and remain if they do not exist in the client_meta
      :type base_meta: k8s.V1ObjectMeta
      :param client_meta: the spec that the client wants to create.
      :type client_meta: k8s.V1ObjectMeta
      :return: the merged specs



   
   .. staticmethod:: reconcile_specs(base_spec: Optional[k8s.V1PodSpec], client_spec: Optional[k8s.V1PodSpec])

      :param base_spec: has the base attributes which are overwritten if they exist
          in the client_spec and remain if they do not exist in the client_spec
      :type base_spec: k8s.V1PodSpec
      :param client_spec: the spec that the client wants to create.
      :type client_spec: k8s.V1PodSpec
      :return: the merged specs



   
   .. staticmethod:: reconcile_containers(base_containers: List[k8s.V1Container], client_containers: List[k8s.V1Container])

      :param base_containers: has the base attributes which are overwritten if they exist
          in the client_containers and remain if they do not exist in the client_containers
      :type base_containers: List[k8s.V1Container]
      :param client_containers: the containers that the client wants to create.
      :type client_containers: List[k8s.V1Container]
      :return: the merged containers

      The runs recursively over the list of containers.



   
   .. staticmethod:: construct_pod(dag_id: str, task_id: str, pod_id: str, try_number: int, kube_image: str, date: datetime.datetime, command: List[str], pod_override_object: Optional[k8s.V1Pod], base_worker_pod: k8s.V1Pod, namespace: str, scheduler_job_id: str)

      Construct a pod by gathering and consolidating the configuration from 3 places:
      - airflow.cfg
      - executor_config
      - dynamic arguments



   
   .. staticmethod:: serialize_pod(pod: k8s.V1Pod)

      Converts a k8s.V1Pod into a jsonified object

      @param pod:
      @return:



   
   .. staticmethod:: deserialize_model_file(path: str)

      :param path: Path to the file
      :return: a kubernetes.client.models.V1Pod

      Unfortunately we need access to the private method
      ``_ApiClient__deserialize_model`` from the kubernetes client.
      This issue is tracked here; https://github.com/kubernetes-client/python/issues/977.



   
   .. staticmethod:: deserialize_model_dict(pod_dict: dict)

      Deserializes python dictionary to k8s.V1Pod
      @param pod_dict:
      @return:



   
   .. staticmethod:: make_unique_pod_id(pod_id)

      Kubernetes pod names must be <= 253 chars and must pass the following regex for
      validation
      ``^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$``

      :param pod_id: a dag_id with only alphanumeric characters
      :return: ``str`` valid Pod name of appropriate length




.. function:: merge_objects(base_obj, client_obj)
   :param base_obj: has the base attributes which are overwritten if they exist
       in the client_obj and remain if they do not exist in the client_obj
   :param client_obj: the object that the client wants to create.
   :return: the merged objects


.. function:: extend_object_field(base_obj, client_obj, field_name)
   :param base_obj: an object which has a property `field_name` that is a list
   :param client_obj: an object which has a property `field_name` that is a list.
       A copy of this object is returned with `field_name` modified
   :param field_name: the name of the list field
   :type field_name: str
   :return: the client_obj with the property `field_name` being the two properties appended


