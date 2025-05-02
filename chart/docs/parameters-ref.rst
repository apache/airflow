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

Parameters reference
====================

The following tables lists the configurable parameters of the Airflow chart and their default values.

.. jinja:: params_ctx

    {% for section in sections %}

    .. _parameters:{{ section["name"] }}:

    {{ section["name"] }}
    {{ "=" * (section["name"]|length + 2) }}

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Parameter
         - Description
         - Default

      {% for param in section["params"] %}
       * - ``{{ param["name"] }}``
         - {{ param["description"] }}
         - ``{{ param["default"] }}``
         {% if param["examples"] %}
           Examples:

           .. code-block:: yaml

              {{ param["examples"] | indent(width=10) }}

         {% endif %}
      {% endfor %}

    {% endfor %}


Specify each parameter using the ``--set key=value[,key=value]`` argument to ``helm install``. For example,

.. code-block:: bash

  helm install my-release apache-airflow/airflow \
    --set executor=CeleryExecutor \
    --set enablePodLaunching=false .
