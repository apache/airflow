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
   Please, only add notes to the Changelog just below the "Changelog" header when there
   are some breaking changes and you want to add an explanation to the users on how they are supposed
   to deal with them. The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-teradata``

Changelog
---------

3.3.1
.....

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.3.0
.....

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``Add DdlOperator to execute Data Definition Language (DDL) statements on Teradata databases using TTU tbuild utility (#56675)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.2.3
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable ruff PLW1509 rule (#57659)``
   * ``Fix documentation/provider.yaml consistencies (#57283)``

3.2.2
.....

Misc
~~~~

* ``Fix mypy type errors in Teradata provider for SQLAlchemy 2 upgrade (#56861)``
* ``Improve error messages in Teradata provider (#56367)``
* ``Build correct SQLAlchemy URI in TeradataHook (#56305)``
* ``Migrate tableau, telegram, trino, teradata providers to ''common.compat'' (#57013)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare release for Sep 2025 1st wave of providers (#55203)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``
   * ``Make term Dag consistent in providers docs (#55101)``
   * ``Switch pre-commit to prek (#54258)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``
   * ``Prepare release for Sep 2025 2nd wave of providers (#55688)``

3.2.1
.....

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Cleanup type ignores in teradata provider where possible (#53254)``
* ``Make teradata provider compatible with mypy 1.16.1 (#53128)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Move all BaseHook usages to version_compat in teradata provider (#52858)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.2.0
.....

Features
~~~~~~~~

* ``Add BteqOperator for Executing Teradata BTEQ Scripts in Airflow (#51807)``

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Replace 'models.BaseOperator' to Task SDK one for Tableau, Telegram, and Teradata (#52642)``
* ``Disable UP038 ruff rule and revert mandatory 'X | Y' in insintance checks (#52644)``
* ``Drop support for Python 3.9 (#52072)``
* ``Enhanced the BTEQ operator to ensure platform independence. (#52252)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.1.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Bump min Airflow version in providers to 2.10 (#49843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

3.0.3
.....

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Apr 2nd wave of providers (#49051)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``
   * ``Prepare docs for Apr 1st wave of providers (#48828)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Upgrade ruff to latest version (#48553)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

3.0.2
.....

Bug Fixes
~~~~~~~~~

* ``fIx deprecation warnings in common.sql (#47169)``

Misc
~~~~

* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

3.0.1
.....

Misc
~~~~

* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Move TERADATA provider to new structure (#46060)``

3.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  All deprecated classes, parameters and features have been removed from the teradata provider package.
  The following breaking changes were introduced:

  * Removed deprecated ``bulk_insert_rows`` method from ``hooks``. Use ``insert_rows`` instead.

* ``Remove deprecations from Teradata Provider (#44746)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``

2.6.1
.....

Misc
~~~~

* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Start porting DAG definition code to the Task SDK (#43076)``
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

2.6.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.5.0
.....

Features
~~~~~~~~

* ``Implemented Query Band Support for the Teradata provider (#40716)``

Misc
~~~~

* ``Clean up remaining getattr connection DbApiHook (#40665)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.4.0
.....

Features
~~~~~~~~

* ``Added support of Teradata Compute Cluster Provision, Decommission, Suspend and Resume operations (#40509)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

2.3.0
.....

Features
~~~~~~~~

* ``Updates to Teradata Provider (#40378)``

2.2.0
.....

Features
~~~~~~~~

.. note::
  This release contains several new features including:
  • Introduction of Stored Procedure Support in Teradata Hook
  • Inclusion of the TeradataStoredProcedureOperator for executing stored procedures
  • Integration of Azure Blob Storage to Teradata Transfer Operator
  • Integration of Amazon S3 to Teradata Transfer Operator
  • Provision of necessary documentation, along with unit and system tests, for the Teradata Provider modifications.

* ``Updates to Teradata Provider (#39217)``

2.1.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

2.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``
* ``Always use the executemany method when inserting rows in DbApiHook as it's way much faster (#38715)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave (RC1) April 2024 (#38863)``
   * ``Bump ruff to 0.3.3 (#38240)``

2.0.0
.....

``Initial version of the provider. (#36953)``

Breaking changes
~~~~~~~~~~~~~~~~

Previous versions of this package were owned by ``Felipe Lolas`` under
https://github.com/flolas/apache-airflow-providers-teradata
These versions were not maintained by Apache-Airflow
If you are migrating from older version, it's recommended to read the docs and refactor your code
