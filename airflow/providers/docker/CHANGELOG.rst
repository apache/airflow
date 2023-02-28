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


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

Changelog
---------

3.5.0
.....

Features
~~~~~~~~

* ``Add correct widgets in Docker Hook (#28700)``
* ``Make docker operators always use 'DockerHook' for API calls (#28363)``
* ``Skip DockerOperator task when it returns a provided exit code (#28996)``

Bug Fixes
~~~~~~~~~

* ``Fix label name for 'reauth' field in Docker Connection (#28974)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Jan 2023 mid-month wave of Providers (#28929)``

3.4.0
.....

Features
~~~~~~~~

* ``add hostname argument to DockerOperator (#27822)``
* ``Move min airflow version down for Docker Provider to 2.3.0 (#28648)``

3.3.0
.....

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``Add ipc_mode for DockerOperator (#27553)``
* ``Add env-file parameter to Docker Operator (#26951)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

3.2.0
.....

Features
~~~~~~~~

* ``Add logging options to docker operator (#26653)``
* ``Add pre-commit hook for custom_operator_name (#25786)``
* ``Implement ExternalPythonOperator (#25780)``

Bug Fixes
~~~~~~~~~

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

3.1.0
.....

Features
~~~~~~~~

* ``Force-remove container after DockerOperator execution (#23160)``

Bug Fixes
~~~~~~~~~

* ``'DockerOperator' fix cli.logs giving character array instead of string (#24726)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``
   * ``Clean up task decorator type hints and docstrings (#24667)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

Misc
~~~~

* ``Remove 'xcom_push' from 'DockerOperator' (#23981)``
* ``docker new system test (#23167)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.7.0
.....

Features
~~~~~~~~

* ``Add 'device_requests' parameter to 'DockerOperator' (#23554)``

Bug Fixes
~~~~~~~~~

* ``Fix new MyPy errors in main (#22884)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``

2.6.0
.....

Features
~~~~~~~~

* ``Add timeout parameter to 'DockerOperator' (#22502)``

2.5.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

Misc
~~~~

* ``Correct 'multiple_outputs' param descriptions mentioning lists/tuples (#22371)``

2.5.1
.....

Bug Fixes
~~~~~~~~~

* ``Avoid trying to kill container when it did not succeed for Docker (#22145)``

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.5.0
.....

Features
~~~~~~~~

* ``added docker network_mode options (#21986)``

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Change default python executable to python3 for docker decorator (#21973)``
   * ``Switch to Debian 11 (bullseye) as base for our dockerfiles (#21378) (#21875)``
   * ``Revert "Switch to Debian 11 (bullseye) as base for our dockerfiles (#21378)" (#21874)``
   * ``Switch to Debian 11 (bullseye) as base for our dockerfiles (#21378)``

2.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Fixes Docker xcom functionality (#21175)``
* ``Fix docker behaviour with byte lines returned (#21429)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add optional features in providers. (#21074)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Rewrite the task decorator as a composition (#20868)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.4.0
.....

Features
~~~~~~~~

* ``Allow DockerOperator's image to be templated (#19997)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix mypy docker provider (#20235)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Remove remaining 'pylint: disable' comments (#19541)``
   * ``Fix MyPy errors for Airflow decorators (#20034)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.3.0
.....

Features
~~~~~~~~

* ``Add support of placement in the DockerSwarmOperator (#18990)``

Bug Fixes
~~~~~~~~~

* ``Fixup string concatenations (#19099)``
* ``Remove the docker timeout workaround (#18872)``


Other
~~~~~

   * ``Move docker decorator example dag to docker provider (#18739)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.2.0
.....

Features
~~~~~~~~

* ``Add a Docker Taskflow decorator (#15330)``

This version of Docker Provider has a new feature - TaskFlow decorator that only works in Airflow 2.2.
If you try to use the decorator in pre-Airflow 2.2 version you will get an error:

.. code-block:: text

    AttributeError: '_TaskDecorator' object has no attribute 'docker'

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
   * ``Cope with '@task.docker' decorated function not returning anything (#18463)``

2.1.1
.....

Features
~~~~~~~~

* ``Add support for configs, secrets, networks and replicas for DockerSwarmOperator (#17474)``

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

2.1.0
.....

Features
~~~~~~~~

* ``Adds option to disable mounting temporary folder in DockerOperator (#16932)``

Bug Fixes
~~~~~~~~~

* ``[FIX] Docker provider - retry docker in docker (#17061)``
* ``fix string encoding when using xcom / json (#13536)``
* if ``xcom_all`` is set to ``False``, only the last line of the log (separated by ``\n``) will be
  included in the XCom value

The ``DockerOperator`` in version 2.0.0 did not work for remote Docker Engine or Docker-In-Docker case.
That was an unintended side effect of #15843 that has been fixed in #16932. There is a fallback mode
which will make Docker Operator works with warning and you will be able to remove the warning by
using the new parameter to disable mounting the folder.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Removes pylint from our toolchain (#16682)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
   * ``Prepares documentation for RC2 release of Docker Provider (#17066)``
   * ``Updating Docker example DAGs to use XComArgs (#16871)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

* ``Replace DockerOperator's 'volumes' arg for 'mounts' (#15843)``

The ``volumes`` parameter in
``airflow.providers.docker.operators.docker.DockerOperator`` and
``airflow.providers.docker.operators.docker_swarm.DockerSwarmOperator``
was replaced by the ``mounts`` parameter, which uses the newer
`mount syntax <https://docs.docker.com/storage/>`__ instead of ``--bind``.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Remove class references in changelogs (#16454)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

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
