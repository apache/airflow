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

Setting resources for containers
================================

It is possible to set `resources <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/>`__ for the containers managed by the chart.
You can define different resources for various Airflow containers. By default the resources are not set.

.. note::

   The Kubernetes scheduler can use resources to decide which node to place the pod on. Since a pod resource request/limit is the sum of
   the resource requests/limits for each container in the pod, it is advised to specify resources for each container in the pod.

Possible containers where resources can be configured include:

.. jinja:: params_ctx

   {% for section in sections %}
      {% for param in section["params"] %}
         {% if param["name"].endswith("resources") %}
   * ``{{ param["name"] }}``
         {% endif %}
      {% endfor %}
   {% endfor %}

For example, specifying resources for scheduler container:

.. code-block:: yaml
   :caption: values.yaml

   scheduler:
      resources:
      limits:
         cpu: 1
         memory: 1Gi
      requests:
         cpu: 500m
         memory: 512Gi
