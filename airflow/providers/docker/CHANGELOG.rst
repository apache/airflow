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

Change in ``DockerOperator`` and ``DockerSwarmOperator``
````````````````````````````````````````````````````````

The ``volumes`` parameter in
:class:`~airflow.providers.docker.operators.docker.DockerOperator` and
:class:`~airflow.providers.docker.operators.docker_swarm.DockerSwarmOperator`
was replaced by the ``mounts`` parameter, which uses the newer
`mount syntax <https://docs.docker.com/storage/>`__ instead of ``--bind``.


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
