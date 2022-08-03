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

3.2.0
.....

Features
~~~~~~~~

* ``Move all SQL classes to common-sql provider (#24836)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Align Black and blacken-docs configs (#24785)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.1.0
.....

Features
~~~~~~~~~

* ``Update Oracle library to latest version (#24311)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

Features
~~~~~~~~

* ``Add 'parameters' to templated fields in 'OracleOperator' (#22857)``

Misc
~~~~

* ``Make numpy effectively an optional dependency for Oracle provider (#24272)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare provider documentation 2022.05.11 (#23631)``
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.2.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.2.2
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix oracle test connection (#21699)``

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.2.0
.....

Features
~~~~~~~~

* ``Add more SQL template fields renderers (#21237)``
* ``[Oracle] Oracle Hook - automatically set current_schema when defined in Connection (#19084)``
* ``improved backwards compatibility (#21524)``

Bug Fixes
~~~~~~~~~

* ``Oracle Provider: Fix handling of bindvars with no parameters (#20720)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``

2.1.0
.....

Features
~~~~~~~~

* ``Add method 'callproc' on Oracle hook (#20072)``
* ``Add ''autocommit'' to ''OracleHook''  (#20085)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix mypy for providers: elasticsearch, oracle, yandex (#20344)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.0.1
.....

Bug Fixes
~~~~~~~~~

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix type annotations in OracleOperator,  JdbcOperator, SqliteOperator (#17406)``
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Removes pylint from our toolchain (#16682)``

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

* ``Add optional result handler to database hooks (#15581)``
* ``[Oracle] Add port to DSN (#15589)``
* ``add oracle  connection link (#15632)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Update Docstrings of Modules with Missing Params (#15391)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.1.0
.....

Features
~~~~~~~~

* ``A bunch of template_fields_renderers additions (#15130)``

1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
