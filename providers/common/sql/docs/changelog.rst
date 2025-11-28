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

``apache-airflow-providers-common-sql``

Changelog
---------

1.30.0
......

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Move out some exceptions to TaskSDK (#54505)``
* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

1.29.0
......

Features
~~~~~~~~

* ``Allow a list of SQL statements in GenericTransfer (#56041)``

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Migrate 'common/sql' provider to 'common.compat' (#57135)``
* ``Migrate 'common.sql' provider to 'common.compat' (#57075)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to 19 files in providers (cncf,common) (#57995)``
   * ``Fix code formatting via ruff preview (#57641)``
   * ``Enable ruff PLW0127 rule (#57298)``

1.28.2
......

Misc
~~~~

* ``fix mypy type errors in common/sql provider for sqlalchemy 2 upgrade (#56824)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable PT011 rule to prvoider tests (#56495)``

1.28.1
......


Bug Fixes
~~~~~~~~~

* ``fix: rows_processor in SQLInsertRowsOperator (#55290)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch all airflow logging to structlog (#52651)``

1.28.0
......


Features
~~~~~~~~

* ``Add SQLInsertRowsOperator in common.sql provider (#55031)``
* ``Added paginated_sql_statement_clause parameter to GenericTransfer (#48866)``
* ``Add rudimentary support for psycopg3 (#52976)``

Bug Fixes
~~~~~~~~~

* ``Fixed resolving of dialect name when host of JdbcHook is an JDBC URL (#54446)``

Misc
~~~~

* ``Remove unneeded isort skip comments (#54979)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove airflow.models.DAG (#54383)``
   * ``Replace API server's direct Connection access workaround in BaseHook (#54083)``
   * ``Switch pre-commit to prek (#54258)``
   * ``make bundle_name not nullable (#47592)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

1.27.5
......

Bug Fixes
~~~~~~~~~

* ``Fix SnowflakeCheckOperator and SnowflakeValueCheckOperator to use parameters arg correctly (#53837)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.27.4
......

Misc
~~~~

* ``Fix unreachable code mypy warnings in teradata provider (#53488)``
* ``Add sql extras to pandas in the providers where sqlalchemy is needed (#53535)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores (#53297)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Move all BaseHook usages to version_compat in Common SQL (#52800)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add run_on_latest_version support for backfill and clear operations (#52177)``
   * ``Make dag_version_id in TI non-nullable (#50825)``

1.27.3
......

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Replace models.BaseOperator to Task SDK one for Common Providers (#52443)``
* ``Drop support for Python 3.9 (#52072)``
* ``Use BaseSensorOperator from task sdk in providers (#52296)``
* ``Bump upper binding on pandas in all providers (#52060)``

Doc-only
~~~~~~~~

* ``Minor pre-commit fixes (#51769)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Clean up messy default connection overrides in provider tests (#52137)``
   * ``Remove pytest db markers from common sql provider (#52105)``

1.27.2
......

Bug Fixes
~~~~~~~~~

* ``fix: Use source hook instead of destination hook when reading records in non-paginated mode in GenericTransfer (#50598)``

Misc
~~~~

* ``Port ''ti.run'' to Task SDK execution path (#50141)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.27.1
......

Misc
~~~~

* ``Update deprecated functions to use stable functions (#50275)``
* ``Update type to follow newest tpying spec (#50229)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Add test for  overload (#50200)``

1.27.0
......

.. note::
  This release of provider is only available for Airflow 2.10+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Fix SADeprecationWarning when using inspector with SQLAlchemy in DbApiHook (#48938)``
* ``Fix: SQLExecuteQueryOperator does not pass extra_dejson values to hook_params (#49282)``

Misc
~~~~

* ``Refine type overload for 'common-sql' (#50161)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``
* ``refactor: Log generated SQL-statement and passed parameters as ERROR message when an exception occurs during insert_rows (#48932)``

Doc-only
~~~~~~~~

* ``Update standard provider doc operators in core operators-and-hooks-ref.rst (#49401)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Avoid committing history for providers (#49907)``

1.26.0
......

Features
~~~~~~~~

* ``feat: integrate 'polars' in 'get_df', 'get_df_by_chunks' (#48875)``

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Apr 2nd wave of providers (#49051)``
   * ``feat(secrets_masker): add minimum secret length and skip masking for common terms (#48791)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``

1.25.0
......

Features
~~~~~~~~

* ``Add requires_result_fetch Configuration to SQLExecuteQueryOperator (#46997)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Upgrade ruff to latest version (#48553)``
   * ``Move 'BaseSensorOperator' to TaskSDK definitions (#48244)``

1.24.1
......

Bug Fixes
~~~~~~~~~

* ``Fix get_hook() got an unexpected keyword argument 'hook_params' in SQLExecuteQueryTrigger (#47728)``
* ``Do not allow semicolon in partition_clause (#48098)``

Misc
~~~~

* ``AIP-72: Handle Custom XCom Backend on Task SDK (#47339)``
* ``AIP-72: Implement short circuit and branch operators (#46584)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Bump various providers in preparation for Airflow 3.0.0b4 (#48013)``
   * ``Remove links to x/twitter.com (#47801)``

1.24.0
......

Features
~~~~~~~~

* ``Add deferred pagination mode to GenericTransfer (#44809)``

Bug Fixes
~~~~~~~~~

* ``fIx deprecation warnings in common.sql (#47169)``
* ``Add  missing 'methodtools' dependency of the 'common-sql' provider (#47148)``

Misc
~~~~

* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

1.23.0
......

.. note::
  This version has no code changes. It's released due to yank of previous version due to packaging issues.

1.22.0
......

Features
~~~~~~~~

* ``Introduce notion of dialects in DbApiHook (#41327)``

Bug Fixes
~~~~~~~~~

* ``Fix DbApiHook.insert_rows logging incorrect number of rows commited (#45789)``
* ``Fix escaping of special characters or reserved words as column names in dialects of common sql provider (#45640)``

Misc
~~~~

* ``Add run_after column to DagRun model (#45732)``
* ``Adding type ignore to sql hook (#46163)``
* ``Added ADR document describing why the notion of dialects is introduced (#45456)``
* ``Bring back mypy checks for new-structure providers (#45815)``
* ``Remove obsolete pandas specfication for pre-python 3.9 (#45399)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Moving EmptyOperator to standard provider (#46231)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``move standard, alibaba and common.sql provider to the new structure (#45964)``

1.21.0
......

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. warning::
  All deprecated classes, parameters and features have been removed from the Common SQL provider package.
  It could be that very old providers versions were depended on private functions of common.sql and as a result
  users may have created custom code around them without realizing using private functions. This has long been fixed
  and might affect users of very old provider versions. We are not considering this a breaking change yet we are listing
  the changes explicitly for visibility.
  The following breaking changes were introduced:

  * Hooks
      * Remove ``_make_serializable`` method from ``DbApiHook``. Use ``_make_common_data_structure`` instead.

Features
~~~~~~~~

* ``Added output_processor parameter to SQLQueryOperator and fixed bug with return_single_query_results handler when None is passed as split_statements (#44781)``
* ``Moved common SQL handler methods of common-sql-provider into dedicated module (#43747)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Fix static checks in common SQL hooks (#44930)``
* ``Added documentation for the SqlThresholdCheckOperator (#44787)``
* ``Remove Provider Deprecations in Common SQL (#44645)``
* ``Update DAG example links in multiple providers documents (#44034)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Update path of example dags in docs (#45069)``

1.20.0
......

Features
~~~~~~~~

* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Start porting DAG definition code to the Task SDK (#43076)``

1.19.0
......

Features
~~~~~~~~

* ``Feature: Added fast_executemany parameter to insert_rows of DbApiHook (#43357)``
* ``Make conn id parameters templated in GenericTransfer and also allow passing hook parameters like in BaseSQLOperator (#42891)``
* ``Allow capture of replication lag in SqlSensor (#43107)``

Misc
~~~~

* ``fix mypy failure (#43206)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

1.18.0
......

Features
~~~~~~~~

* ``feat(providers/common/sql): add warning to connection setter (#42736)``

Bug Fixes
~~~~~~~~~

* ``FIX: Only pass connection to sqlalchemy engine in JdbcHook (#42705)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.17.1
......

Bug Fixes
~~~~~~~~~

* ``fix(providers/common/sql): add dummy connection setter for backward compatibility (#42490)``
* ``Changed type hinting for handler function (#42275)``

1.17.0
......

Features
~~~~~~~~

.. note::
  Connection in DB Hook is now cached to avoid multiple lookups when properties
  from extras have to be resolved.

* ``Generalize caching of connection in DbApiHook to improve performance (#40751)``

Misc
~~~~

* ``feat: log client db messages for provider postgres (#40171)``
* ``remove deprecated soft_fail from providers (#41710)``


1.16.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``fix: rm deprecated import (#41461)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``

1.15.0
......

Features
~~~~~~~~

* ``Create SQLAlchemy engine from connection in DB Hook and added autocommit param to insert_rows method (#40669)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.14.2
......

Bug Fixes
~~~~~~~~~

* ``FIX: DbApiHook.insert_rows unnecessarily restarting connections (#40615)``

Misc
~~~~

* ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Resolve postgres deprecations in tests (#40392)``

1.14.1
......

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``
* ``Update pandas minimum requirement for Python 3.12 (#40272)``
* ``standardizes template fields for 'BaseSQLOperator' and adds 'database' as a templated field (#39826)``

1.14.0
......

Features
~~~~~~~~

* ``Add 'parameters' as template field for SqlSensor (#39588)``

Bug Fixes
~~~~~~~~~

* ``DbAPiHook: Don't log a warning message if placeholder is None and make sure warning message is formatted correctly (#39690)``

Misc
~~~~

* ``refactor: The executemany parameter of insert_rows should not be deprecated as for some hooks we don't want to enable a system-wide supports_executemany parameter, that way we can also keep using it in dedicated situations (#39630)``
* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``Add typing for SqlSensor (#39773)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

1.13.0
......

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.



Features
~~~~~~~~

* ``Add 'sqlalchemy_url' property to 'DbApiHook' class (#38871)``
* ``Always use the executemany method when inserting rows in DbApiHook as it's way much faster (#38715)``

Bug Fixes
~~~~~~~~~

* ``Fix 'DbApiHook.insert_rows' when 'rows' is a generator (#38972)``
* ``Fix 'update-common-sql-api-stubs' pre-commit check (#38915)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``
* ``openlineage, snowflake: do not run external queries for Snowflake (#39113)``

1.12.0
......

Features
~~~~~~~~

* ``Add hook_params to template_fields for BaseSQLOperator-related Operators (#38724)``
* ``Make 'placeholder' of DbApiHook configurable in UI (#38528)``

Misc
~~~~

* ``Undeprecating 'DBApiHookForTests._make_common_data_structure' (#38573)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update yanked versions in providers changelogs (#38262)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``fix: try002 for provider common sql (#38800)``

1.11.1
......

Bug Fixes
~~~~~~~~~

* ``Make 'executemany' keyword arguments only in 'DbApiHook.insert_rows' (#37840)``
* ``Limit 'pandas' to '<2.2' (#37748)``

1.11.0
......

Features
~~~~~~~~

* ``Enhancement: Performance enhancement for insert_rows method DbApiHook with fast executemany + SAP Hana support (#37246)``

Bug Fixes
~~~~~~~~~

* ``Fix SQLThresholdCheckOperator error on falsey vals (#37150)``

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``
* ``Add more-itertools as dependency of common-sql (#37359)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave of Providers February 2024 (#37326)``

1.10.1
......

Misc
~~~~

* ``Set min pandas dependency to 1.2.5 for all providers and airflow (#36698)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

1.10.0
......

* ``Make "placeholder" of ODBC configurable in UI (#36000)``


Bug Fixes
~~~~~~~~~

* ``Return common data structure in DBApi derived classes``
* ``SQLCheckOperator fails if returns dict with any False values (#36273)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.9.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``
* ``Add Architecture Decision Record for common.sql introduction (#36015)``


1.8.1
.....

Misc
~~~~

* ``Add '_make_serializable' method which other SQL operators can overrides when result from cursor is not JSON-serializable (#32319)``
* ``Remove backcompat inheritance for DbApiHook (#35754)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use reproducible builds for providers (#35693)``
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Work around typing issue in examples and providers (#35494)``
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Upgrade pre-commits (#35033)``
   * ``D401 Support - A thru Common (Inclusive) (#34934)``

1.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``fix(providers/sql): respect soft_fail argument when exception is raised (#34199)``

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add missing header into 'common.sql' changelog (#34910)``
   * ``Refactor usage of str() in providers (#34320)``

1.7.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix BigQueryValueCheckOperator deferrable mode optimisation (#34018)``

Misc
~~~~

* ``Refactor regex in providers (#33898)``

1.7.1
.....

Misc
~~~~

* ``Refactor: Better percentage formatting (#33595)``
* ``Refactor: Simplify code in smaller providers (#33234)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix typos (double words and it's/its) (#33623)``

1.7.0
.....

Features
~~~~~~~~

* ``Add a new parameter to SQL operators to specify conn id field (#30784)``

1.6.2
.....

Misc
~~~~

* ``Make SQLExecute Query signature consistent with other SQL operators (#32974)``
* ``Get rid of Python2 numeric relics (#33050)``

1.6.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix local OpenLineage import in 'SQLExecuteQueryOperator'. (#32400)``

Misc
~~~~

* ``Add default port to Openlineage authority method. (#32828)``
* ``Add more accurate typing for DbApiHook.run method (#31846)``

1.6.0
.....

Features
~~~~~~~~

* ``openlineage, common.sql:  provide OL SQL parser as internal OpenLineage provider API (#31398)``

Misc
~~~~
* ``Pass SQLAlchemy engine to construct information schema query. (#32371)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
   * ``Improve provider documentation and README structure (#32125)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``

1.5.2
.....

Misc
~~~~

* ``Remove Python 3.7 support (#30963)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improve docstrings in providers (#31681)``
   * ``Add D400 pydocstyle check - Providers (#31427)``

1.5.1
.....

Misc
~~~~

* ``Bring back min-airflow-version for preinstalled providers (#31469)``

1.5.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``This version might cause unconstrained installation of old airflow version lead to Runtime Error.``

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add conditional output processing in SQL operators (#31136)``

Misc
~~~~

* ``Remove noisy log from SQL table check (#31037)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add mechanism to suspend providers (#30422)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

1.4.0
.....

Features
~~~~~~~~

* ``Add option to show output of 'SQLExecuteQueryOperator' in the log (#29954)``

Misc
~~~~

* ``Fix Python API docs formatting for Common SQL provider (#29863)``

1.3.4
.....

Bug Fixes
~~~~~~~~~

* ``Do not process output when do_xcom_push=False  (#29599)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Make the S3-to-SQL system test self-contained (#29204)``
   * ``Make static checks generated file  more stable across the board (#29080)``

1.3.3
.....

Bug Fixes
~~~~~~~~~

* ``Handle non-compliant behaviour of Exasol cursor (#28744)``

1.3.2
.....

Bug Fixes
~~~~~~~~~

* ``fIx isort problems introduced by recent isort release (#28434)``
* ``Fix template rendering for Common SQL operators (#28202)``
* ``Defer to hook setting for split_statements in SQLExecuteQueryOperator (#28635)``

Misc
~~~~

* ``Clarify docstrings for updated DbApiHook (#27966)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add pre-commits preventing accidental API changes in common.sql (#27962)``

1.3.1
.....

This release fixes a few errors that were introduced in common.sql operator while refactoring common parts:

* ``_process_output`` method in ``SQLExecuteQueryOperator`` has now consistent semantics and typing, it
  can also modify the returned (and stored in XCom) values in the operators that derive from the
  ``SQLExecuteQueryOperator``).
* descriptions of all returned results are stored as descriptions property in the DBApiHook
* last description of the cursor whether to return single query results values are now exposed in
  DBApiHook via last_description property.

Lack of consistency in the operator caused ``1.3.0`` to be yanked - the ``1.3.0`` should not be used - if
you have ``1.3.0`` installed, upgrade to ``1.3.1``.

Bug Fixes
~~~~~~~~~

* ``Restore removed (but used) methods in common.sql (#27843)``
* ``Fix errors in Databricks SQL operator introduced when refactoring (#27854)``
* ``Bump common.sql provider to 1.3.1 (#27888)``
* ``Fixing the behaviours of SQL Hooks and Operators finally (#27912)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

1.3.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Breaks Google 8.4.0 provider for SQLExecute``

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``Add SQLExecuteQueryOperator (#25717)``
* ``Use DbApiHook.run for DbApiHook.get_records and DbApiHook.get_first (#26944)``
* ``DbApiHook consistent insert_rows logging (#26758)``

Bug Fixes
~~~~~~~~~

* ``Common sql bugfixes and improvements (#26761)``
* ``Use unused SQLCheckOperator.parameters in SQLCheckOperator.execute. (#27599)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

1.2.0
.....

Features
~~~~~~~~

* ``Make placeholder style configurable (#25939)``
* ``Better error message for pre-common-sql providers (#26051)``

Bug Fixes
~~~~~~~~~

* ``Fix (and test) SQLTableCheckOperator on postgresql (#25821)``
* ``Don't use Pandas for SQLTableCheckOperator (#25822)``
* ``Discard semicolon stripping in SQL hook (#25855)``

1.1.0
.....

Features
~~~~~~~~

* ``Improve taskflow type hints with ParamSpec (#25173)``
* ``Move all "old" SQL operators to common.sql providers (#25350)``
* ``Deprecate hql parameters and synchronize DBApiHook method APIs (#25299)``
* ``Unify DbApiHook.run() method with the methods which override it (#23971)``
* ``Common SQLCheckOperators Various Functionality Update (#25164)``

Bug Fixes
~~~~~~~~~

* ``Allow Legacy SqlSensor to use the common.sql providers (#25293)``
* ``Fix fetch_all_handler & db-api tests for it (#25430)``
* ``Align Common SQL provider logo location (#25538)``
* ``Fix SQL split string to include ';-less' statements (#25713)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix CHANGELOG for common.sql provider and add amazon commit (#25636)``

1.0.0
.....

Initial version of the provider.
Adds ``SQLColumnCheckOperator`` and ``SQLTableCheckOperator``.
Moves ``DBApiHook``, ``SQLSensor`` and ``ConnectorProtocol`` to the provider.
