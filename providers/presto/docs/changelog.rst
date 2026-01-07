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

``apache-airflow-providers-presto``


Changelog
---------

5.10.2
......

Misc
~~~~

* ``Remove top-level SDK reference in Core (#59817)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.10.1
......

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.10.0
......

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

5.9.4
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Replace the "postgres" extra on pandas from Presto provider with direct psycopg2-binary. (#57813)``
* ``Fix pandas extra in Presto provider (#57141)``
* ``Migrate presto provider to common.compat (#57136)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to 19 files in providers (airbyte, alibaba, atlassian, papermill, presto, redis, singularity, sqlite, tableau, vertica, weaviate, elasticsearch, exasol) (#57986)``
   * ``Fix documentation/provider.yaml consistencies (#57283)``

5.9.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix: Handle SQLAlchemy_URI for PrestoHook (#56298)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare release for Sep 2025 2nd wave of providers (#55688)``
   * ``Prepare release for Sep 2025 1st wave of providers (#55203)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``
   * ``Make term Dag consistent in providers docs (#55101)``
   * ``Switch pre-commit to prek (#54258)``

5.9.2
.....

Misc
~~~~

* ``Add sql extras to pandas in the providers where sqlalchemy is needed (#53535)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores in presto provider where possible (#53274)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.9.1
.....

Misc
~~~~

* ``Provider Migration: Update presto for Airflow 3.0 compatibility (#52608)``
* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.9.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Migrate 'PrestoHook' and 'TrinoHook' to use 'get_df' (#50123)``
* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

5.8.4
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

5.8.3
.....

Misc
~~~~

* ``Setting Airflow context Environment variables for operators (#47644)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

5.8.2
.....

Misc
~~~~

* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

5.8.1
.....

Misc
~~~~

* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``
* ``Remove obsolete pandas specfication for pre-python 3.9 (#45399)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``Revert "Improve example docs around SQLExecuteQueryOperator in Postgres/Oracle/Presto/Vertica/ODBC (#46352)" (#46368)``
   * ``Improve example docs around SQLExecuteQueryOperator in Postgres/Oracle/Presto/Vertica/ODBC (#46352)``
   * ``Move Presto provider to new structure (#46115)``

5.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Remove references to AIRFLOW_V_2_9_PLUS (#44987)``
* ``Consistent way of checking Airflow version in providers (#44686)``
* ``Update DAG example links in multiple providers documents (#44034)``
* ``Rename execution_date to logical_date across codebase (#43902)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Update path of example dags in docs (#45069)``

5.7.0
.....

Features
~~~~~~~~

* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

5.6.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.5.2
.....

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``
* ``Update pandas minimum requirement for Python 3.12 (#40272)``

5.5.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

5.5.0
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

5.4.2
.....

Misc
~~~~

* ``Limit 'pandas' to '<2.2' (#37748)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix remaining D401 checks (#37434)``
   * ``Add comment about versions updated by release manager (#37488)``

5.4.1
.....

Misc
~~~~

 * ``Set min pandas dependency to 1.2.5 for all providers and airflow (#36698)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

5.4.0
.....

Features
~~~~~~~~

* ``Make "placeholder" of ODBC configurable in UI (#36000)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.3.0
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
   * ``Update information about links into the provider.yaml files (#35837)``
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for providers (#35693)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Work around typing issue in examples and providers (#35494)``

5.2.1
.....

Misc
~~~~

* ``Bump min version of presto client to bring better dbapi compatibility (#35132)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``

5.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``

5.1.4
.....

Misc
~~~~

* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``

5.1.3
.....

Misc
~~~~

* ``Consolidate import and usage of pandas (#33480)``

5.1.2
.....

Misc
~~~~

* ``Add more accurate typing for DbApiHook.run method (#31846)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``D205 Support - Providers: Pagerduty to SMTP (inclusive) (#32358)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``Improve provider documentation and README structure (#32125)``

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

5.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  In this version of the provider, deprecated GCS hook's param ``delegate_to`` is removed from ``GCSToPrestoOperator``.
  Impersonation can be achieved instead by utilizing the ``impersonation_chain`` param.

* ``remove delegate_to from GCP operators and hooks (#30748)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add mechanism to suspend providers (#30422)``

4.2.2
.....

Misc
~~~~

* ``Deprecate 'delegate_to' param in GCP operators and update docs (#29088)``

4.2.1
.....

Misc
~~~~
* ``Remove outdated compat imports/code from providers (#28507)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.2.0
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

4.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use DbApiHook.run for DbApiHook.get_records and DbApiHook.get_first (#26944)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
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
methods of the ``PrestoHook``.

Remove ``PrestoToSlackOperator`` in favor of Slack provider ``SqlToSlackOperator``.

* ``Remove 'PrestoToSlackOperator' (#25425)``

Breaking changes
~~~~~~~~~~~~~~~~

* ``Deprecate hql parameters and synchronize DBApiHook method APIs (#25299)``

Features
~~~~~~~~~

* ``Unify DbApiHook.run() method with the methods which override it (#23971)``


3.1.0
.....

Features
~~~~~~~~

* ``Adding generic 'SqlToSlackOperator' (#24663)``
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

Features
~~~~~~~~

* ``Add 'PrestoToSlackOperator' (#23979)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Presto example DAGs to new design #22459 (#24145)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Make presto and trino compatible with airflow 2.1 (#23061)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``

2.2.0
.....

Features
~~~~~~~~

* ``Pass X-Presto-Client-Info in presto hook (#22416)``

2.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.1.1
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.1.0
.....

Features
~~~~~~~~

* ``Add GCSToPrestoOperator (#21084)``
* ``Quick Update GCS Presto (#21855)``
* ``Replaced hql references to sql in TrinoHook and PrestoHook (#21630)``

Bug Fixes
~~~~~~~~~

* ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
* ``Fix K8S changelog to be PyPI-compatible (#20614)``
* ``Fix MyPy Errors for Presto provider (#20244)``

Misc
~~~~

* ``Support for Python 3.10``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``

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

1.0.0
.....

Initial version of the provider.
