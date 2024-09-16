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

``apache-airflow-providers-trino``


Changelog
---------

5.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.7.2
.....

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``
* ``Update pandas minimum requirement for Python 3.12 (#40272)``

5.7.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

5.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave (RC1) April 2024 (#38863)``
   * ``Bump ruff to 0.3.3 (#38240)``

5.6.3
.....

Misc
~~~~

* ``Limit 'pandas' to '<2.2' (#37748)``
* ``Implement AIP-60 Dataset URI formats (#37005)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix remaining D401 checks (#37434)``
   * ``Add comment about versions updated by release manager (#37488)``

5.6.2
.....

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``


5.6.1
.....

Misc
~~~~

* ``Set min pandas dependency to 1.2.5 for all providers and airflow (#36698)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Run mypy checks for full packages in CI (#36638)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

5.6.0
.....

Features
~~~~~~~~

* ``Make "placeholder" of ODBC configurable in UI (#36000)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.5.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add timezone parameter to TrinoHook (#35963)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

5.4.1
.....

Misc
~~~~

* ``Remove backcompat inheritance for DbApiHook (#35754)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Use reproducible builds for provider packages (#35693)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Work around typing issue in examples and providers (#35494)``
   * ``Switch from Black to Ruff formatter (#35287)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``

5.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``

5.3.1
.....

Misc
~~~~

* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``

5.3.0
.....

Features
~~~~~~~~

* ``Add OpenLineage support for Trino. (#32910)``

Misc
~~~~

* ``Consolidate import and usage of pandas (#33480)``

5.2.1
.....

Misc
~~~~

* ``Add more accurate typing for DbApiHook.run method (#31846)``
* ``Add deprecation info to the providers modules and classes docstring (#32536)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D205 Support - Providers: Snowflake to Zendesk (inclusive) (#32359)``

5.2.0
.....

Features
~~~~~~~~

* ``Trino Hook: Add ability to read JWT from file (#31950)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improve provider documentation and README structure (#32125)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``

5.1.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Add note about dropping Python 3.7 for providers (#32015)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check - Providers (#31427)``

5.1.0
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

5.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  In this version of the provider, deprecated GCS hook's param ``delegate_to`` is removed from ``GCSToPrestoOperator``.
  Impersonation can be achieved instead by utilizing the ``impersonation_chain`` param.

* ``remove delegate_to from GCP operators and hooks (#30748)``

.. Review and move the new changes to one of the sections above:
   * ``Add mechanism to suspend providers (#30422)``

4.3.2
.....

Misc
~~~~
* ``Deprecate 'delegate_to' param in GCP operators and update docs (#29088)``

4.3.1
.....

Misc
~~~~

* ``Remove outdated compat imports/code from providers (#28507)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.3.0
.....

Features
~~~~~~~~

* ``Add _serialize_cell method to TrinoHook and PrestoHook (#27724)``

Bug Fixes
~~~~~~~~~

* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

4.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``
* ``Bump Trino version to fix non-working DML queries (#27168)``

Features
~~~~~~~~

* ``Add SQLExecuteQueryOperator (#25717)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Allow setting client tags for trino connection (#27213)``
   * ``Use DbApiHook.run for DbApiHook.get_records and DbApiHook.get_first (#26944)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Allow session properties for trino connection (#27095)``

4.1.0
.....

Features
~~~~~~~~

* ``trino: Support CertificateAuthentication in the trino hook (#26246)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

4.0.1
.....

Features
~~~~~~~~

* ``Add common-sql lower bound for common-sql (#25789)``

Bug Fixes
~~~~~~~~~

* ``Fix placeholders in 'TrinoHook', 'PrestoHook', 'SqliteHook' (#25939)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Deprecated ``hql`` parameter has been removed in ``get_records``, ``get_first``, ``get_pandas_df`` and ``run``
methods of the ``TrinoHook``.

* ``Deprecate hql parameters and synchronize DBApiHook method APIs (#25299)``

Features
~~~~~~~~

* ``Unify DbApiHook.run() method with the methods which override it (#23971)``

3.1.0
.....

Features
~~~~~~~~

* ``Move all SQL classes to common-sql provider (#24836)``
* ``Add test_connection method to Trino hook (#24583)``
* ``Add 'on_kill()' to kill Trino query if the task is killed (#24559)``
* ``Add TrinoOperator (#24415)``

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

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``AIP-47 | Migrate Trino example DAGs to new design (#24118)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.3.0
.....

Features
~~~~~~~~

* ``TrinoHook add authentication via JWT token and Impersonation  (#23116)``
* ``Make presto and trino compatible with airflow 2.1 (#23061)``

Bug Fixes
~~~~~~~~~


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``
   * ``Fix new MyPy errors in main (#22884)``

2.2.0
.....

Features
~~~~~~~~

* ``Pass X-Trino-Client-Info in trino hook (#22535)``

2.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.1.1
.....

Misc
~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.1.0
.....

Features
~~~~~~~~

* ``Add GCSToTrinoOperator (#21704)``

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix mypy providers (#20190)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Replaced hql references to sql in TrinoHook and PrestoHook (#21630)``
   * ``Pass Trino hook params to DbApiHook (#21479)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Properly handle verify parameter in TrinoHook (#18791)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.0.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
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

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.0
.....

Initial version of the provider.
