
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

``apache-airflow-providers-cncf-kubernetes``
============================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/kubernetes>
    Operators <operators>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/cncf/kubernetes/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/master/airflow/providers/cncf/kubernetes/example_dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-cncf-kubernetes/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-cncf-kubernetes
------------------------------------------------------

`Kubernetes <https://kubernetes.io/>`__


Release: 1.2.0

Provider package
----------------

This is a provider package for ``cncf.kubernetes`` provider. All classes for this provider package
are in ``airflow.providers.cncf.kubernetes`` python package.

Installation
------------

You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-cncf-kubernetes``

PIP requirements
----------------

================  ====================
PIP package       Version required
================  ====================
``cryptography``  ``>=2.0.0``
``kubernetes``    ``>=3.0.0, <12.0.0``
================  ====================

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
