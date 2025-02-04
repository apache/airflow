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

airflow.providers.cncf.kubernetes.operators.kueue
=================================================

.. py:module:: airflow.providers.cncf.kubernetes.operators.kueue

.. autoapi-nested-parse::

   Manage a Kubernetes Kueue.



Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.operators.kueue.KubernetesInstallKueueOperator
   airflow.providers.cncf.kubernetes.operators.kueue.KubernetesStartKueueJobOperator


Module Contents
---------------

.. py:class:: KubernetesInstallKueueOperator(kueue_version, kubernetes_conn_id = 'kubernetes_default', *args, **kwargs)

   Bases: :py:obj:`airflow.models.BaseOperator`


   Installs a Kubernetes Kueue.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:KubernetesInstallKueueOperator`

   :param kueue_version: The Kubernetes Kueue version to install.
   :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
       for the Kubernetes cluster.


   .. py:attribute:: template_fields
      :type:  collections.abc.Sequence[str]
      :value: ('kueue_version', 'kubernetes_conn_id')



   .. py:attribute:: kubernetes_conn_id
      :value: 'kubernetes_default'



   .. py:attribute:: kueue_version


   .. py:property:: hook
      :type: airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: KubernetesStartKueueJobOperator(queue_name, *args, **kwargs)

   Bases: :py:obj:`airflow.providers.cncf.kubernetes.operators.job.KubernetesJobOperator`


   Executes a Kubernetes Job in Kueue.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:KubernetesStartKueueJobOperator`

   :param queue_name: The name of the Queue in the cluster


   .. py:attribute:: template_fields


   .. py:attribute:: queue_name
