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

``apache-airflow-providers-sqlite``


Changelog
---------

3.7.1
.....

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D401 Support in Providers (simple) (#37258)``
   * ``Add docs for RC2 wave of providers for 2nd round of Jan 2024 (#37019)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``

3.7.0
.....

Features
~~~~~~~~

* ``Make "placeholder" of ODBC configurable in UI (#36000)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.6.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for provider packages (#35693)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``

3.5.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``

3.4.3
.....

Misc
~~~~

* ``Add deprecation info to the providers modules and classes docstring (#32536)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``Improve provider documentation and README structure (#32125)``

3.4.2
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Add note about dropping Python 3.7 for providers (#32015)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check - Providers (#31427)``

3.4.1
.....

Misc
~~~~

* ``Bring back min-airflow-version for preinstalled providers (#31469)``

3.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``

3.3.2
.....

Misc
~~~~

* ``Use connection URI in SqliteHook (#28721)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add mechanism to suspend providers (#30422)``

3.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

3.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``Add SQLExecuteQueryOperator (#25717)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

3.2.1
.....

Features
~~~~~~~~

* ``Add common-sql lower bound for common-sql (#25789)``

Bug Fixes
~~~~~~~~~

* ``Fix placeholders in 'TrinoHook', 'PrestoHook', 'SqliteHook' (#25939)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.2.0
.....

Features
~~~~~~~~

* ``Unify DbApiHook.run() method with the methods which override it (#23971)``

3.1.0
.....

Features
~~~~~~~~

* ``Move all SQL classes to common-sql provider (#24836)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Fix ''SqliteHook'' compatibility with SQLAlchemy engine (#23790)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate SQLite example DAGs to new design #22461 (#24150)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.1.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.1.2
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.1.1
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.0
.....

Features
~~~~~~~~

* ``Add more SQL template fields renderers (#21237)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Misc. documentation typos and language improvements (#19599)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``Update documentation for September providers release (#18613)``
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
   * ``Updating miscellaneous provider DAGs to use TaskFlow API where applicable (#18278)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.0.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

Bug Fixes
~~~~~~~~~

* ``Fix sqlite hook - insert and replace functions (#17695)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Fix type annotations in OracleOperator,  JdbcOperator, SqliteOperator (#17406)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
   * ``Remove/refactor default_args pattern for miscellaneous providers (#16872)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Adds interactivity when generating provider documentation. (#15518)``
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Remove Backport Providers (#14886)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``


1.0.1
.....

Updated documentation and readme files.

* ``Add example DAG & how-to guide for sqlite (#13196)``

1.0.0
.....

Initial version of the provider.
