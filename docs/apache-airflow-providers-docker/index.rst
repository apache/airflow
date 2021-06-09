
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

``apache-airflow-providers-docker``
===================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: References

    Connection types <connections/docker>
    Python API <_api/airflow/providers/docker/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/main/airflow/providers/docker/example_dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-docker/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-docker
------------------------------------------------------

`Docker <https://docs.docker.com/install/>`__


Release: 2.0.0

Provider package
----------------

This is a provider package for ``docker`` provider. All classes for this provider package
are in ``airflow.providers.docker`` python package.

Installation
------------

You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-docker``

PIP requirements
----------------

=============  ==================
PIP package    Version required
=============  ==================
``docker``
=============  ==================

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
* ``Replace DockerOperator's 'volumes' arg for 'mounts' (#15843)``

The ``volumes`` parameter in
:class:`~airflow.providers.docker.operators.docker.DockerOperator` and
:class:`~airflow.providers.docker.operators.docker_swarm.DockerSwarmOperator`
was replaced by the ``mounts`` parameter, which uses the newer
`mount syntax <https://docs.docker.com/storage/>`__ instead of ``--bind``.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.2.0
.....

Features
~~~~~~~~

* ``Entrypoint support in docker operator (#14642)``
* ``Add PythonVirtualenvDecorator to Taskflow API (#14761)``
* ``Support all terminus task states in Docker Swarm Operator (#14960)``


1.1.0
.....

Features
~~~~~~~~

* ``Add privileged option in DockerOperator (#14157)``

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``

1.0.1
.....

Updated documentation and readme files.

Bug fixes
~~~~~~~~~

* ``Remove failed DockerOperator tasks with auto_remove=True (#13532) (#13993)``
* ``Fix error on DockerSwarmOperator with auto_remove True (#13532) (#13852)``


1.0.0
.....

Initial version of the provider.
