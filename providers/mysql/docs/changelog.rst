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

``apache-airflow-providers-mysql``


Changelog
---------

6.4.0
.....

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

6.3.5
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Migrate MySQL provider to 'common.compat' (#57165)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable ruff PLW2101,PLW2901,PLW3301 rule (#57700)``
   * ``Enable PT006 rule to mysql Provider test (#57937)``
   * ``Prepare release for Oct 2025 wave of providers (#57029)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``
   * ``Enable PT011 rule to prvoider tests (#56021)``

6.3.4
.....


Misc
~~~~

* ``Bump mysql-connector-python>=9.1.0 to support SQLAlchemy 2.0 (#52233)``
* ``Add optional SQLA logging/diagnostics (#54315)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch pre-commit to prek (#54258)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

6.3.3
.....

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores in mysql provider where possible (#53288)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.3.2
.....

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Provider Migration: Update mysql for Airflow 3.0 compatibility (#52500)``
* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Introducing fixture to create 'Connections' without DB in provider tests (#51930)``

6.3.1
.....

Misc
~~~~

* ``Bump problematic dependencies to avoid backtracking (#51702)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.3.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Bump min Airflow version in providers to 2.10 (#49843)``
* ``Help 'pip' with avoiding resolution too deep issues in Python 3.12 (#49853)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

6.2.2
.....

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove limitation for mysql-connector-python (#49321)``
   * ``Exclude 9.3.0 release of mysql-connector-python (#49300)``

6.2.1
.....

Bug Fixes
~~~~~~~~~

* ``fix: overwrite 'get-uri' for 'MySQL' (#48706)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``
   * ``Prepare docs for Apr 1st wave of providers (#48828)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

6.2.0
.....

Features
~~~~~~~~

* ``Add regex validation for table names in 'bulk_load' (#47254)``
* ``Add regex validation for table names in 'bulk_dump' (#47255)``

Bug Fixes
~~~~~~~~~

* ``Adding ticks to user provided table names (#47262)``

Misc
~~~~

* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

6.1.0
.....

Features
~~~~~~~~

* ``Introduce notion of dialects in DbApiHook (#41327)``

Bug Fixes
~~~~~~~~~

* ``Fix escaping of special characters or reserved words as column names in dialects of common sql provider (#45640)``

Misc
~~~~

* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``Move mysql, odbc, jenkins, pagerduty to new provider structure.  (#46102)``

6.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  All deprecated classes, parameters and features have been removed from the MySQL provider package.
  The following breaking changes were introduced:

  * Operators
     * Remove ``airflow.providers.mysql.operators.mysql.MySqlOperator``. Please use ``airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator``.
       Also, you can provide `hook_params={'schema': <database>}`.

* ``Remove Provider Deprecations in MySQL (#44665)``

Bug Fixes
~~~~~~~~~

* ``Fix error file not found. tmp file is deleted before inserting rows to DB in VerticaToMySQLOperator bulk  (#44028)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Update DAG example links in multiple providers documents (#44034)``
* ``Add basic asyncio support (#43944)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update path of example dags in docs (#45069)``

5.7.4
.....

Misc
~~~~

* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``
* ``Explain how to use uv with airflow virtualenv and make it works (#43604)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Start porting DAG definition code to the Task SDK (#43076)``

5.7.3
.....

Misc
~~~~

* ``Improve the cursor type definition of mysql (#43376)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

5.7.2
.....

Misc
~~~~

* ``Rename dataset related python variable names to asset (#41348)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.7.1
.....

Misc
~~~~

* ``Generalize caching of connection in DbApiHook to improve performance (#40751)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.6.3
.....

Misc
~~~~

* ``Clean up remaining getattr connection DbApiHook (#40665)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.6.2
.....

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``
* ``Resolving mysql deprecated operator warnings (#39725)``

5.6.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

5.6.0
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

5.5.4
.....

Misc
~~~~

* ``Unify 'aws_conn_id' type to always be 'str | None' (#37768)``
* ``Implement AIP-60 Dataset URI formats (#37005)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix remaining D401 checks (#37434)``
   * ``Add comment about versions updated by release manager (#37488)``

5.5.3
.....

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

5.5.2
.....

Misc
~~~~

* ``Bump min version of mysql-connector-python (#36668)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Standardize airflow build process and switch to Hatchling build backend (#36537)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

5.5.1
.....

Bug Fixes
~~~~~~~~~

* ``Sanitize filenames in MySQLHook (#33328)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Add documentation for 3rd wave of providers in Deember (#36464)``
   * ``Re-apply updated version numbers to 2nd wave of providers in December (#36380)``

5.5.0
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
   * ``Update information about links into the provider.yaml files (#35837)``
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for providers (#35693)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
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

5.3.1
.....

Misc
~~~~

* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
* ``Use a single  statement with multiple contexts instead of nested  statements in providers (#33768)``

5.3.0
.....

Features
~~~~~~~~

* ``Add init_command parameter to MySqlHook (#33359)``
* ``Add 'local_infile' parameter to 'S3ToMySqlOperator' (#33459)``

5.2.1
.....

This release brings back mysql-connector-python as required dependency of the provider - since 8.1.0
version has been released with Protobuf 4 support, removing dependency conflicts with other providers.

Misc
~~~~

* ``Bring back mysql-connector-python as required depednency (#32989)``


5.2.0
.....

Features
~~~~~~~~

* ``Add OpenLineage support for MySQL. (#31609)``

Misc
~~~~

* ``Add deprecation info to the providers modules and classes docstring (#32536)``
* ``Add default port to Openlineage authority method. (#32828)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: GRPC to Oracle (inclusive) (#32357)``
   * ``Improve provider documentation and README structure (#32125)``

5.1.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Replace unicodecsv with standard csv library (#31693)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check - Providers (#31427)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

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
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  If you are using mysql-connector-python please note that starting this release it is an extra option.
  The library will not be installed by default.

* ``Remove mysql-connector-python (#30487)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add mechanism to suspend providers (#30422)``

4.0.2
.....

Misc
~~~~

* ``Use MariaDB client binaries in arm64 image for support MySQL backend (#29519)``

4.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Fixed MyPy errors introduced by new mysql-connector-python (#28995)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Remove conn.close() ignores (#29005)" (#29010)``
   * ``Remove conn.close() ignores (#29005)``

4.0.0
.....

Breaking Changes
~~~~~~~~~~~~~~~~

You can no longer pass "local_infile" as extra in the connection. You should pass it instead as
hook's "local_infile" parameter when you create the MySqlHook (either directly or via hook_params).

* ``Move local_infile option from extra to hook parameter (#28811)``

3.4.0
.....

Features
~~~~~~~~

* ``Allow SSL mode in MySQL provider (#27717)``

Bug fixes
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
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

3.2.1
.....

Misc
~~~~

* ``Add common-sql lower bound for common-sql (#25789)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``D400 first line should end with period batch02 (#25268)``

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

Bug Fixes
~~~~~~~~~

* ``Close the MySQL connections once operations are done. (#24508)``


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
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Migrate MySQL example DAGs to new design #22453 (#24142)``
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

Misc
~~~~

* ``Support for Python 3.10``
* ``Make DbApiHook use get_uri from Connection (#21764)``
* ``Update MySqlOperator example dag (#21434)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.2.0
.....

* ``Add more SQL template fields renderers (#21237)``
* ``Add conditional 'template_fields_renderers' check for new SQL lexers (#21403)``

Misc
~~~~

* ``Refactor vertica_to_mysql to make it more 'mypy' friendly (#20618)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``More f-strings (#18855)``
   * ``Update documentation for September providers release (#18613)``
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.1.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``refactor: fixed type annotation for 'sql' in MySqlOperator (#17388)``
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

2.1.0
.....

Features
~~~~~~~~

* ``Added template_fields_renderers for MySQL Operator (#16914)``
* ``Extended template_fields_renderers for MySQL provider (#16987)``
* ``Parse template parameters field for MySQL operator (#17080)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Removes pylint from our toolchain (#16682)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
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

Bug Fixes
~~~~~~~~~

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Make Airflow code Pylint 2.8 compatible (#15534)``
   * ``Update Docstrings of Modules with Missing Params (#15391)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add Connection Documentation for Providers (#15499)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.1.0
.....

Features
~~~~~~~~

* ``Adds 'Trino' provider (with lower memory footprint for tests) (#15187)``
* ``A bunch of template_fields_renderers additions (#15130)``

Bug fixes
~~~~~~~~~

* ``Fix autocommit calls for mysql-connector-python (#14869)``

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``MySQL hook respects conn_name_attr (#14240)``

1.0.1
.....

Updated documentation and readme files.


1.0.0
.....

Initial version of the provider.
