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

airflow.providers.cncf.kubernetes.operators.resource
====================================================

.. py:module:: airflow.providers.cncf.kubernetes.operators.resource

.. autoapi-nested-parse::

   Manage a Kubernetes Resource.



Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.operators.resource.KubernetesCreateResourceOperator
   airflow.providers.cncf.kubernetes.operators.resource.KubernetesDeleteResourceOperator


Module Contents
---------------

.. py:class:: KubernetesCreateResourceOperator(*, yaml_conf = None, yaml_conf_file = None, namespace = None, kubernetes_conn_id = KubernetesHook.default_conn_name, custom_resource_definition = False, namespaced = True, config_file = None, **kwargs)

   Bases: :py:obj:`KubernetesResourceBaseOperator`


   Create a resource in a kubernetes.


   .. py:method:: create_custom_from_yaml_object(body)


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: KubernetesDeleteResourceOperator(*, yaml_conf = None, yaml_conf_file = None, namespace = None, kubernetes_conn_id = KubernetesHook.default_conn_name, custom_resource_definition = False, namespaced = True, config_file = None, **kwargs)

   Bases: :py:obj:`KubernetesResourceBaseOperator`


   Delete a resource in a kubernetes.


   .. py:method:: delete_custom_from_yaml_object(body)


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
