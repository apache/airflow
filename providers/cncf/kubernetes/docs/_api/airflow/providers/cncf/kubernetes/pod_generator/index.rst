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

airflow.providers.cncf.kubernetes.pod_generator
===============================================

.. py:module:: airflow.providers.cncf.kubernetes.pod_generator

.. autoapi-nested-parse::

   Pod generator.

   This module provides an interface between the previous Pod
   API and outputs a kubernetes.client.models.V1Pod.
   The advantage being that the full Kubernetes API
   is supported and no serialization need be written.



Attributes
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.pod_generator.log
   airflow.providers.cncf.kubernetes.pod_generator.MAX_LABEL_LEN


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.pod_generator.PodGenerator


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.pod_generator.make_safe_label_value
   airflow.providers.cncf.kubernetes.pod_generator.datetime_to_label_safe_datestring
   airflow.providers.cncf.kubernetes.pod_generator.label_safe_datestring_to_datetime
   airflow.providers.cncf.kubernetes.pod_generator.merge_objects
   airflow.providers.cncf.kubernetes.pod_generator.extend_object_field


Module Contents
---------------

.. py:data:: log

.. py:data:: MAX_LABEL_LEN
   :value: 63


.. py:function:: make_safe_label_value(string)

   Normalize a provided label to be of valid length and characters.

   Valid label values must be 63 characters or less and must be empty or begin and
   end with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),
   dots (.), and alphanumerics between.

   If the label value is greater than 63 chars once made safe, or differs in any
   way from the original value sent to this function, then we need to truncate to
   53 chars, and append it with a unique hash.


.. py:function:: datetime_to_label_safe_datestring(datetime_obj)

   Transform a datetime string to use as a label.

   Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but
   not "_" let's
   replace ":" with "_"

   :param datetime_obj: datetime.datetime object
   :return: ISO-like string representing the datetime


.. py:function:: label_safe_datestring_to_datetime(string)

   Transform a label back to a datetime object.

   Kubernetes doesn't permit ":" in labels. ISO datetime format uses ":" but not
   "_", let's
   replace ":" with "_"

   :param string: str
   :return: datetime.datetime object


.. py:class:: PodGenerator(pod = None, pod_template_file = None, extract_xcom = True)

   Contains Kubernetes Airflow Worker configuration logic.

   Represents a kubernetes pod and manages execution of a single pod.
   Any configuration that is container specific gets applied to
   the first container in the list of containers.

   :param pod: The fully specified pod. Mutually exclusive with ``pod_template_file``
   :param pod_template_file: Path to YAML file. Mutually exclusive with ``pod``
   :param extract_xcom: Whether to bring up a container for xcom


   .. py:attribute:: extract_xcom
      :value: True



   .. py:method:: from_obj(obj)
      :staticmethod:


      Convert to pod from obj.



   .. py:method:: reconcile_pods(base_pod, client_pod)
      :staticmethod:


      Merge Kubernetes Pod objects.

      :param base_pod: has the base attributes which are overwritten if they exist
          in the client pod and remain if they do not exist in the client_pod
      :param client_pod: the pod that the client wants to create.
      :return: the merged pods

      This can't be done recursively as certain fields are overwritten and some are concatenated.



   .. py:method:: reconcile_metadata(base_meta, client_meta)
      :staticmethod:


      Merge Kubernetes Metadata objects.

      :param base_meta: has the base attributes which are overwritten if they exist
          in the client_meta and remain if they do not exist in the client_meta
      :param client_meta: the spec that the client wants to create.
      :return: the merged specs



   .. py:method:: reconcile_specs(base_spec, client_spec)
      :staticmethod:


      Merge Kubernetes PodSpec objects.

      :param base_spec: has the base attributes which are overwritten if they exist
          in the client_spec and remain if they do not exist in the client_spec
      :param client_spec: the spec that the client wants to create.
      :return: the merged specs



   .. py:method:: reconcile_containers(base_containers, client_containers)
      :staticmethod:


      Merge Kubernetes Container objects.

      :param base_containers: has the base attributes which are overwritten if they exist
          in the client_containers and remain if they do not exist in the client_containers
      :param client_containers: the containers that the client wants to create.
      :return: the merged containers

      The runs recursively over the list of containers.



   .. py:method:: construct_pod(dag_id, task_id, pod_id, try_number, kube_image, date, args, pod_override_object, base_worker_pod, namespace, scheduler_job_id, run_id = None, map_index = -1, *, with_mutation_hook = False)
      :classmethod:


      Create a Pod.

      Construct a pod by gathering and consolidating the configuration from 3 places:
          - airflow.cfg
          - executor_config
          - dynamic arguments



   .. py:method:: serialize_pod(pod)
      :staticmethod:


      Convert a k8s.V1Pod into a json serializable dictionary.

      :param pod: k8s.V1Pod object
      :return: Serialized version of the pod returned as dict



   .. py:method:: deserialize_model_file(path)
      :staticmethod:


      Generate a Pod from a file.

      :param path: Path to the file
      :return: a kubernetes.client.models.V1Pod



   .. py:method:: deserialize_model_dict(pod_dict)
      :staticmethod:


      Deserializes a Python dictionary to k8s.V1Pod.

      Unfortunately we need access to the private method
      ``_ApiClient__deserialize_model`` from the kubernetes client.
      This issue is tracked here; https://github.com/kubernetes-client/python/issues/977.

      :param pod_dict: Serialized dict of k8s.V1Pod object
      :return: De-serialized k8s.V1Pod



.. py:function:: merge_objects(base_obj, client_obj)

   Merge objects.

   :param base_obj: has the base attributes which are overwritten if they exist
       in the client_obj and remain if they do not exist in the client_obj
   :param client_obj: the object that the client wants to create.
   :return: the merged objects


.. py:function:: extend_object_field(base_obj, client_obj, field_name)

   Add field values to existing objects.

   :param base_obj: an object which has a property ``field_name`` that is a list
   :param client_obj: an object which has a property ``field_name`` that is a list.
       A copy of this object is returned with ``field_name`` modified
   :param field_name: the name of the list field
   :return: the client_obj with the property ``field_name`` being the two properties appended
