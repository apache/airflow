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

airflow.providers.cncf.kubernetes.pod_generator_deprecated
==========================================================

.. py:module:: airflow.providers.cncf.kubernetes.pod_generator_deprecated

.. autoapi-nested-parse::

   Backwards compatibility for Pod generation.

   This module provides an interface between the previous Pod
   API and outputs a kubernetes.client.models.V1Pod.
   The advantage being that the full Kubernetes API
   is supported and no serialization need be written.



Attributes
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.pod_generator_deprecated.MAX_POD_ID_LEN
   airflow.providers.cncf.kubernetes.pod_generator_deprecated.MAX_LABEL_LEN


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.pod_generator_deprecated.PodDefaults
   airflow.providers.cncf.kubernetes.pod_generator_deprecated.PodGenerator


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.pod_generator_deprecated.make_safe_label_value


Module Contents
---------------

.. py:data:: MAX_POD_ID_LEN
   :value: 253


.. py:data:: MAX_LABEL_LEN
   :value: 63


.. py:class:: PodDefaults

   Static defaults for Pods.


   .. py:attribute:: XCOM_MOUNT_PATH
      :value: '/airflow/xcom'



   .. py:attribute:: SIDECAR_CONTAINER_NAME
      :value: 'airflow-xcom-sidecar'



   .. py:attribute:: XCOM_CMD
      :value: 'trap "exit 0" INT; while true; do sleep 30; done;'



   .. py:attribute:: VOLUME_MOUNT


   .. py:attribute:: VOLUME


   .. py:attribute:: SIDECAR_CONTAINER


.. py:function:: make_safe_label_value(string)

   Normalize a provided label to be of valid length and characters.

   Valid label values must be 63 characters or less and must be empty or begin and
   end with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),
   dots (.), and alphanumerics between.

   If the label value is greater than 63 chars once made safe, or differs in any
   way from the original value sent to this function, then we need to truncate to
   53 chars, and append it with a unique hash.


.. py:class:: PodGenerator(image = None, name = None, namespace = None, volume_mounts = None, envs = None, cmds = None, args = None, labels = None, node_selectors = None, ports = None, volumes = None, image_pull_policy = None, restart_policy = None, image_pull_secrets = None, init_containers = None, service_account_name = None, resources = None, annotations = None, affinity = None, hostnetwork = False, tolerations = None, security_context = None, configmaps = None, dnspolicy = None, schedulername = None, extract_xcom = False, priority_class_name = None)

   Contains Kubernetes Airflow Worker configuration logic.

   Represents a kubernetes pod and manages execution of a single pod.
   Any configuration that is container specific gets applied to
   the first container in the list of containers.

   :param image: The docker image
   :param name: name in the metadata section (not the container name)
   :param namespace: pod namespace
   :param volume_mounts: list of kubernetes volumes mounts
   :param envs: A dict containing the environment variables
   :param cmds: The command to be run on the first container
   :param args: The arguments to be run on the pod
   :param labels: labels for the pod metadata
   :param node_selectors: node selectors for the pod
   :param ports: list of ports. Applies to the first container.
   :param volumes: Volumes to be attached to the first container
   :param image_pull_policy: Specify a policy to cache or always pull an image
   :param restart_policy: The restart policy of the pod
   :param image_pull_secrets: Any image pull secrets to be given to the pod.
       If more than one secret is required, provide a comma separated list:
       secret_a,secret_b
   :param init_containers: A list of init containers
   :param service_account_name: Identity for processes that run in a Pod
   :param resources: Resource requirements for the first containers
   :param annotations: annotations for the pod
   :param affinity: A dict containing a group of affinity scheduling rules
   :param hostnetwork: If True enable host networking on the pod
   :param tolerations: A list of kubernetes tolerations
   :param security_context: A dict containing the security context for the pod
   :param configmaps: Any configmap refs to read ``configmaps`` for environments from.
       If more than one configmap is required, provide a comma separated list
       configmap_a,configmap_b
   :param dnspolicy: Specify a dnspolicy for the pod
   :param schedulername: Specify a schedulername for the pod
   :param pod: The fully specified pod. Mutually exclusive with ``path_or_string``
   :param extract_xcom: Whether to bring up a container for xcom
   :param priority_class_name: priority class name for the launched Pod


   .. py:attribute:: pod


   .. py:attribute:: metadata


   .. py:attribute:: container


   .. py:attribute:: spec


   .. py:attribute:: extract_xcom
      :value: False



   .. py:method:: gen_pod()

      Generate pod.



   .. py:method:: add_sidecar(pod)
      :staticmethod:


      Add sidecar.



   .. py:method:: from_obj(obj)
      :staticmethod:


      Convert to pod from obj.



   .. py:method:: make_unique_pod_id(dag_id)
      :staticmethod:


      Generate a unique Pod name.

      Kubernetes pod names must be <= 253 chars and must pass the following regex for
      validation
      ``^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$``

      :param dag_id: a dag_id with only alphanumeric characters
      :return: ``str`` valid Pod name of appropriate length
