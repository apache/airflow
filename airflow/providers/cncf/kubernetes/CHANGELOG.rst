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


Changelog
---------

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Features
~~~~~~~~

* ``Add 'KubernetesPodOperat' 'pod-template-file' jinja template support (#15942)``
* ``Save pod name to xcom for KubernetesPodOperator (#15755)``

Bug Fixes
~~~~~~~~~

* ``Bug Fix Pod-Template Affinity Ignored due to empty Affinity K8S Object (#15787)``
* ``Bug Pod Template File Values Ignored (#16095)``
* ``Fix issue with parsing error logs in the KPO (#15638)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
   * ``Updated documentation for June 2021 provider release (#16294)``

   * ``More documentation update for June providers release (#16405)``

1.2.0
.....

Features
~~~~~~~~

* ``Require 'name' with KubernetesPodOperator (#15373)``
* ``Change KPO node_selectors warning to proper deprecationwarning (#15507)``

Bug Fixes
~~~~~~~~~

* ``Fix timeout when using XCom with KubernetesPodOperator (#15388)``
* ``Fix labels on the pod created by ''KubernetsPodOperator'' (#15492)``

1.1.0
.....

Features
~~~~~~~~

* ``Separate Kubernetes pod_launcher from core airflow (#15165)``
* ``Add ability to specify api group and version for Spark operators (#14898)``
* ``Use libyaml C library when available. (#14577)``

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``Allow pod name override in KubernetesPodOperator if pod_template is used. (#14186)``
* ``Allow users of the KPO to *actually* template environment variables (#14083)``

1.0.1
.....

Updated documentation and readme files.

Bug fixes
~~~~~~~~~

* ``Pass image_pull_policy in KubernetesPodOperator correctly (#13289)``

1.0.0
.....

Initial version of the provider.
