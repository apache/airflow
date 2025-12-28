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

``apache-airflow-providers-postgres``


Changelog
---------

6.5.1
.....

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.5.0
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
   * ``Remove SDK reference for NOTSET in Airflow Core (#58258)``

6.4.1
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``better error handling in SnowflakeHook and PostgresHook when old version of AzureBaseHook (#57184)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable ruff PLW2101,PLW2901,PLW3301 rule (#57700)``
   * ``Enable PT006 rule to postgres Provider test (#57934)``
   * ``Fix code formatting via ruff preview (#57641)``

6.4.0
.....

Features
~~~~~~~~

* ``Add Azure IAM/Entra ID support for PostgresHook (#55729)``

Misc
~~~~

* ``fix mypy type errors in common/sql provider for sqlalchemy 2 upgrade (#56824)``
* ``Migrate postgres provider to ''common.compat'' (#57022)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable PT011 rule to prvoider tests (#55980)``

6.3.0
.....


Features
~~~~~~~~

* ``Added specialized insert_rows in PostgresHook which uses faster psycopg execute_batch method (#54988)``
* ``Add rudimentary support for psycopg3 (#52976)``
* ``Implemented native get_column_names in PostgresDialect to become SQLAlchemy independent (#54437)``

Bug Fixes
~~~~~~~~~

* ``PostgresDialect should use index instead of name in get_column_names and get_primary_keys (#54832)``

Misc
~~~~

* ``Add CI support for SQLAlchemy 2.0 (#52233)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch pre-commit to prek (#54258)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

6.2.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix the PostgresHook ignoring custom adapters registered (#53707)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.2.2
.....

Bug Fixes
~~~~~~~~~

* ``fix(postgres/hooks): ensure get_df uses SQLAlchemy engine to avoid pandas warning (#52224)``

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores in postgres provider where possible (#53275)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.2.1
.....

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Disable UP038 ruff rule and revert mandatory 'X | Y' in insintance checks (#52644)``
* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.2.0
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

6.1.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix PostgresHook Json serialization (#49120)``

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Apr 2nd wave of providers (#49051)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``

6.1.2
.....

Misc
~~~~

* ``Update minimum version of common.sql provider to 1.23.0 (#48416)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

6.1.1
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

* ``Bump psycopg2 to 2.9.9 to possibly avoid crash on Python 3.12 (#46431)``
* ``Added ADR document describing why the notion of dialects is introduced (#45456)``
* ``Bump minimum version of psycopg2-binary to 2.9.7 (#45635)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``Revert "Improve example docs around SQLExecuteQueryOperator in Postgres/Oracle/Presto/Vertica/ODBC (#46352)" (#46368)``
   * ``Improve example docs around SQLExecuteQueryOperator in Postgres/Oracle/Presto/Vertica/ODBC (#46352)``
   * ``Move PGVECTOR provider to new structure (#46051)``

6.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  All deprecated classes, parameters and features have been removed from the Postgres provider package.
  The following breaking changes were introduced:

  * Hooks
     * The ``schema`` arg has been renamed to ``database`` as it contained the database name. Deprecated parameters, getters and setters have been removed. Please use ``database`` to set the database name.
  * Operators
     * Remove ``airflow.providers.postgres.operators.postgres.PostgresOperator``. Please use ``airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`` instead.

* ``Remove Provider Deprecations in Postgres (#44705)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Update DAG example links in multiple providers documents (#44034)``
* ``Add basic asyncio support (#43944)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Update path of example dags in docs (#45069)``
   * ``Allow configuration of sqlalchemy query parameter for JdbcHook and PostgresHook through extras (#44910)``

5.14.0
......

Features
~~~~~~~~

* ``Add AWS Redshift Serverless support to PostgresHook (#43669)``

Bug Fixes
~~~~~~~~~

* ``Fix PostgresHook bug when getting AWS Redshift Serverless credentials (#43807)``

Misc
~~~~

* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Start porting DAG definition code to the Task SDK (#43076)``
   * ``Add docs about 'cursor' extra param in Postgres Connection (#43134)``
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

5.13.1
......

Misc
~~~~

* ``Rename dataset related python variable names to asset (#41348)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.13.0
......

Features
~~~~~~~~

* ``feat: log client db messages for provider postgres (#40171)``

Misc
~~~~

* ``Generalize caching of connection in DbApiHook to improve performance (#40751)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.12.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.11.3
......

Misc
~~~~

* ``Clean up remaining getattr connection DbApiHook (#40665)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

5.11.2
......

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``

5.11.1
......

Bug Fixes
~~~~~~~~~

* ``fix: use 'sqlalchemy_url' property in 'get_uri' for postgresql provider (#38831)``

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

5.11.0
......

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Fix schema assigment in PostgresOperator (#39264)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``
* ``Always use the executemany method when inserting rows in DbApiHook as it's way much faster (#38715)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave (RC1) April 2024 (#38863)``
   * ``Update yanked versions in providers changelogs (#38262)``

5.10.2
......

Misc
~~~~

* ``Implement AIP-60 Dataset URI formats (#37005)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix remaining D401 checks (#37434)``
   * ``Add comment about versions updated by release manager (#37488)``

5.10.1
......

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add docs for RC2 wave of providers for 2nd round of Jan 2024 (#37019)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``
   * ``Standardize airflow build process and switch to Hatchling build backend (#36537)``
   * ``Run mypy checks for full packages in CI (#36638)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``

5.10.0
......

Features
~~~~~~~~

* ``Make "placeholder" of ODBC configurable in UI (#36000)``


Bug Fixes
~~~~~~~~~

* ``Follow BaseHook connection fields method signature in child classes (#36086)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.9.0
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
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for providers (#35693)``

5.8.0
.....

Features
~~~~~~~~

* ``Refactor cursor retrieval in PostgresHook. (#35498)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``

5.7.1
.....

Bug Fixes
~~~~~~~~~

* ``'PostgresOperator' should not overwrite 'SQLExecuteQueryOperator.template_fields' (#34969)``

5.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add 'get_ui_field_behaviour()' method to PostgresHook (#34811)``

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``

5.6.1
.....

Misc
~~~~

* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
* ``Use a single  statement with multiple contexts instead of nested  statements in providers (#33768)``
* ``Use literal dict instead of calling dict() in providers (#33761)``

5.6.0
.....

Features
~~~~~~~~

* ``openlineage, postgres: add OpenLineage support for Postgres (#31617)``

Misc
~~~~

* ``Add deprecation info to the providers modules and classes docstring (#32536)``

5.5.2
.....

Misc
~~~~

* ``Deprecate 'runtime_parameters' in favor of options in 'hook_params' (#32345)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``Improve provider documentation and README structure (#32125)``

5.5.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Add note about dropping Python 3.7 for providers (#32015)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improve docstrings in providers (#31681)``
   * ``Add D400 pydocstyle check - Providers (#31427)``

5.5.0
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
   * ``Add mechanism to suspend providers (#30422)``
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

5.4.0
.....

Features
~~~~~~~~
* ``Bring back psycopg2-binary as dependency instead of psycopg (#28316)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

5.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``PostgresHook: Added ON CONFLICT DO NOTHING statement when all target fields are primary keys (#26661)``
* ``Add SQLExecuteQueryOperator (#25717)``
* ``Rename schema to database in PostgresHook (#26744)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

5.2.2
.....

Misc
~~~~

* ``Add common-sql lower bound for common-sql (#25789)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Rename schema to database in 'PostgresHook' (#26436)``
   * ``Revert "Rename schema to database in 'PostgresHook' (#26436)" (#26734)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

5.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Bump dep on common-sql to fix issue with SQLTableCheckOperator (#26143)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``postgres provider: use non-binary psycopg2 (#25710)``

5.2.0
.....

Features
~~~~~~~~

* ``Use only public AwsHook's methods during IAM authorization (#25424)``
* ``Unify DbApiHook.run() method with the methods which override it (#23971)``


5.1.0
.....

Features
~~~~~~~~

* ``Move all SQL classes to common-sql provider (#24836)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Postgres example DAGs to new design #22458 (#24148)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

4.1.0
.....

Features
~~~~~~~~

* ``adds ability to pass config params to postgres operator (#21551)``

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

4.0.1
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

4.0.0
.....

The URIs returned by Postgres ``get_uri()`` returns ``postgresql://`` instead
of ``postgres://`` prefix which is the only supported prefix for the
SQLAlchemy 1.4.0+. Any usage of ``get_uri()`` where ``postgres://`` prefix
should be updated to reflect it.

Breaking changes
~~~~~~~~~~~~~~~~

* ``Replaces the usage of postgres:// with postgresql:// (#21205)``

Features
~~~~~~~~

* ``Add more SQL template fields renderers (#21237)``
* ``Add conditional 'template_fields_renderers' check for new SQL lexers (#21403)``

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Fix mypy errors in postgres/hooks and postgres/operators (#20600)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix mypy providers (#20190)``
   * ``Add documentation for January 2021 providers release (#21257)``


3.0.1
.....

Misc
~~~~

* ``Make DbApiHook use get_uri from Connection (#21764)``

2.4.0
.....

Features
~~~~~~~~

* ``19489 - Pass client_encoding for postgres connections (#19827)``
* ``Amazon provider remove deprecation, second try (#19815)``


Bug Fixes
~~~~~~~~~

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Adjust built-in base_aws methods to avoid Deprecation warnings (#19725)``
   * ``Revert 'Adjust built-in base_aws methods to avoid Deprecation warnings (#19725)' (#19791)``
   * ``Misc. documentation typos and language improvements (#19599)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``More f-strings (#18855)``

2.3.0
.....

Features
~~~~~~~~

* ``Added upsert method on S3ToRedshift operator (#18027)``

Bug Fixes
~~~~~~~~~

* ``Fix example dag of PostgresOperator (#18236)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

2.2.0
.....

Features
~~~~~~~~

* ``Make schema in DBApiHook private (#17423)``

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``refactor: fixed type annotation for 'sql' param in PostgresOperator (#17331)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Improve postgres provider logging (#17214)``

2.1.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``The postgres operator seem to conflict with earlier versions of Airflow``

Features
~~~~~~~~

* ``Add schema as DbApiHook instance attribute (#16521)``

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

Features
~~~~~~~~

* ``PostgresHook: deepcopy connection to avoid mutating connection obj (#15412)``
* ``postgres_hook_aws_conn_id (#16100)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Fix spelling (#15699)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.2
.....

* ``Do not forward cluster-identifier to psycopg2 (#15360)``


1.0.1
.....

Updated documentation and readme files. Added HowTo guide for Postgres Operator.

1.0.0
.....

Initial version of the provider.
