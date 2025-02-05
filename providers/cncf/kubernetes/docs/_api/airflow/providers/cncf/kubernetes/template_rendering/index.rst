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

airflow.providers.cncf.kubernetes.template_rendering
====================================================

.. py:module:: airflow.providers.cncf.kubernetes.template_rendering


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.template_rendering.render_k8s_pod_yaml
   airflow.providers.cncf.kubernetes.template_rendering.get_rendered_k8s_spec


Module Contents
---------------

.. py:function:: render_k8s_pod_yaml(task_instance)

   Render k8s pod yaml.


.. py:function:: get_rendered_k8s_spec(task_instance, session=NEW_SESSION)

   Fetch rendered template fields from DB.
