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

``apache-airflow-providers-snowflake``


Changelog
---------

6.8.0
.....

Features
~~~~~~~~

* ``Support optional scope in OAuth token request (#58871)``

Misc
~~~~

* ``chore: use OL macros instead of building OL ids from scratch (#59197)``
* ``Add backcompat for exceptions in providers (#58727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.7.0
.....

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``Add support for cancelling running queries via SQL API (#56164)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

6.6.1
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``better error handling in SnowflakeHook and PostgresHook when old version of AzureBaseHook (#57184)``
* ``Bump snowflake-connector-python>=3.16.0 (#57420)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable pt006 rule and fix new generate errors (#58238)``
   * ``Enable PT006 rule to 9 files in providers (snowflake,smtp/tests) (#57845)``
   * ``Attempt to resolve pip "ResolutionTooDeep" on cffi conflict (#57697)``
   * ``Enable ruff PLW1641 rule (#57679)``
   * ``Attempt to limit setuptools for Snowflake snowpark (#57581)``

6.6.0
.....

Features
~~~~~~~~

* ``Add Azure IAM/Entra ID support for SnowflakeHook (#55874)``

Bug Fixes
~~~~~~~~~

* ``Add 'snowflake-snowpark-python' pip resolver hint for Python 3.13 (#56606)``

Misc
~~~~

* ``Migrate snowflake provider to ''common.compat'' (#57003)``

Doc-only
~~~~~~~~

* ``Fix path of how-to-guide docs for copy_into_snowflake.rst(#56527)``
* ``Update Snowflake docs with breaking change (#56516)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable PT001 rule to prvoider tests (#55935)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``

6.5.4
.....


Misc
~~~~

* ``Switch all airflow logging to structlog (#52651)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.5.3
.....


Bug Fixes
~~~~~~~~~

* ``fix(snowflake): enhance error message formatting for SQL errors (#54063)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove airflow.models.DAG (#54383)``
   * ``Switch pre-commit to prek (#54258)``
   * ``make bundle_name not nullable (#47592)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

6.5.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix SnowflakeCheckOperator and SnowflakeValueCheckOperator to use parameters arg correctly (#53837)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.5.1
.....

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``another magic pip resolver hint (#53329)``
* ``fix: Improve logging and timeouts in OL helpers (#53139)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Cleanup type ignores in snowflake provider where possible (#53258)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Make snowpark optional for snowflake provider and disable it for Python 3.13 (#53489)``
* ``Deprecate decorators from Core (#53629)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Make dag_version_id in TI non-nullable (#50825)``
   * ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
   * ``Replace 'mock.patch("utcnow")' with time_machine. (#53642)``

6.5.0
.....

Features
~~~~~~~~

* ``feat: Add explicit support for SnowflakeSqlApiHook to Openlineage helper (#52161)``
* ``feat: Add new query related methods to SnowflakeSqlApiHook (#52157)``
* ``feat: Add SnowflakeSqlApiHook Retry Logic (#51463)``


Misc
~~~~

* ``Provider Migration: Update Snowflake provider for Airflow 3.0 compatibility (#52629)``
* ``Disable UP038 ruff rule and revert mandatory 'X | Y' in insintance checks (#52644)``
* ``Bump pyarrow to 16.1.0 minimum version for several providers (#52635)``
* ``Replace models.BaseOperator to Task SDK one for Common Providers (#52443)``
* ``Relax snowflake-snowpark-python for Python>=3.12 (#52356)``
* ``Drop support for Python 3.9 (#52072)``
* ``Replace 'models.BaseOperator' to Task SDK one for Standard Provider (#52292)``
* ``Bump upper binding on pandas in all providers (#52060)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix StopIteration in snowflake sql tests (#52394)``
   * ``Make sure all test version imports come from test_common (#52425)``
   * ``Add tests to test whether snowflake sql API handles invalid JSON (#52118)``

6.4.0
.....

Features
~~~~~~~~

* ``Extend SnowflakeHook OAuth implementation to support external IDPs and client_credentials grant (#51620)``

Bug Fixes
~~~~~~~~~

* ``fix: make query_ids in SnowflakeSqlApiOperator in deferrable mode consistent (#51542)``
* ``fix: Duplicate region in Snowflake URI no longer breaks OpenLineage (#50831)``
* ``Do not allow semicolons in CopyFromExternalStageToSnowflakeOperator fieldS (#51734)``

Misc
~~~~

* ``Port ''ti.run'' to Task SDK execution path (#50141)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.3.1
.....

Bug Fixes
~~~~~~~~~

* ``fix: Adjust OpenLineage task state check for Airflow 3 (#50380)``

Misc
~~~~

* ``nit: Switch to emitting OL events with adapter and not client directly (#50398)``

Doc-only
~~~~~~~~

* ``docs: remove stale warning about SnowflakeOperator (#50450)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix Breeze unit test (#50395)``
   * ``Use non-deprecated context in tests for Airflow 3 (#50391)``

6.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.10+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. note::
   ``private_key_content`` in Snowflake connection should now be base64 encoded. To encode your private key, you can use the following Python snippet:

   .. code-block:: python

         import base64

         with open("path/to/your/private_key.pem", "rb") as key_file:
             encoded_key = base64.b64encode(key_file.read()).decode("utf-8")
             print(encoded_key)

Features
~~~~~~~~

* ``Adding OAuth support for SnowflakeHook  (#47191)``

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  Existing connections using key pairs break as a result of changing the connection string to be base64 encoded.

  * ``make 'private_key_content' in snowflake connection to be a base64 encoded string (#49467)``

Bug Fixes
~~~~~~~~~

* ``Fix SnowflakeSqlApiHook backwards compatibility for get_oauth_token method (#49482)``
* ``Fix mypy for get_oauth_token signature in SnowflakeSqlApiHook (#49449)``
* ``Fix infinite recursive call of _get_conn_params while getting oauth token from snowflake (#50344)``
* ``Fix: adjust dag_run extraction for Airflow 3 in OL utils (#50346)``

Misc
~~~~

* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``
* ``enhance: logs SQL before execution in 'snowflake' and 'databricks_sql' (#48942)``
* ``chore: import paths use the stable functions (#49460)``
* ``add root parent information to OpenLineage events (#49237)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Avoid committing history for providers (#49907)``
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Prepare ad hoc release for providers May 2025 (#50166)``

6.2.2
.....

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.2.1
.....

Misc
~~~~

* ``Make '@task' import from airflow.sdk (#48896)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``

6.2.0
.....

Features
~~~~~~~~

* ``feat: Send separate OpenLineage event for each Snowflake query_id (#47736)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Upgrade ruff to latest version (#48553)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

6.1.1
.....

Bug Fixes
~~~~~~~~~

* ``fix mark task as completed before Snowflake job completes in SnowflakeSqlApiOperator when deferrable is False (#46672)``
* ``[OpenLineage] fixed inputs in OL implementation of CopyFromExternalStageToSnowflakeOperator (#47168)``
* ``fix deprecation warnings in common.sql (#47169)``

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

* ``SnowflakeSqlApiOperator snowflake_conn_id add to template_fields (#46422)``

Bug Fixes
~~~~~~~~~

* ``snowflake: pass through the ocsp_fail_open setting (#46476)``

Misc
~~~~

* ``AIP-83 amendment: Add logic for generating run_id when logical date is None. (#46616)``
* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``
* ``Remove obsolete pandas specfication for pre-python 3.9 (#45399)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``Move SNOWFLAKE provider to new structure (#46059)``
   * ``move standard, alibaba and common.sql provider to the new structure (#45964)``

6.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  All deprecated classes, parameters and features have been removed from the snowflake provider package.
  The following breaking changes were introduced:

  * Removed deprecated ``SnowflakeOperator``. Use ``SQLExecuteQueryOperator`` instead.

* ``Remove Provider Deprecations in Snowflake (#44756)``

Features
~~~~~~~~

* ``enable client_store_temporary_credential for snowflake provider (#44431)``
* ``Allow 'json_result_force_utf8_encoding' specification in 'providers.snowflake.hooks.SnowflakeHook' extra dict (#44264)``
* ``make host/port configurable for Snowflake connections (#44079)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Update DAG example links in multiple providers documents (#44034)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Update path of example dags in docs (#45069)``

5.8.1
.....

Misc
~~~~

* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``
* ``Move python operator to Standard provider (#42081)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Start porting DAG definition code to the Task SDK (#43076)``
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

5.8.0
.....

Features
~~~~~~~~

* ``Add Snowpark operator and decorator (#42457)``

Bug Fixes
~~~~~~~~~

* ``fix: SnowflakeSqlApiOperator not resolving parameters in SQL (#42719)``
* ``Make 'private_key_content' a sensitive field  in Snowflake connection (#42649)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.7.1
.....

Misc
~~~~

* ``Update snowflake naming for account names and locators for openlineage (#41775)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Fix: Pass hook parameters to SnowflakeSqlApiHook and prep them for API call (#41150)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.6.1
.....

Misc
~~~~

* ``openlineage: migrate OpenLineage provider to V2 facets. (#39530)``
* ``Clean up remaining getattr connection DbApiHook (#40665)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.6.0
.....

Features
~~~~~~~~

* ``chore: Add param support for client_request_mfa_token in SnowflakeHook (#40394)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

5.5.2
.....

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``
* ``openlineage: add some debug logging around sql parser call sites (#40200)``
* ``Update pandas minimum requirement for Python 3.12 (#40272)``
* ``Bump Snowflake client driver versions to minimum 2.7.11 per support policy (#39886)``

5.5.1
.....

Misc
~~~~

* ``Remove 'openlineage.common' dependencies in Google and Snowflake providers. (#39614)``
* ``Remove unused 'copy_into_postifx' param from docstring (#39454)``
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

Bug Fixes
~~~~~~~~~

* ``openlineage, snowflake: do not run external queries for Snowflake (#39113)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Activate RUF019 that checks for unnecessary key check (#38950)``

5.4.0
.....

Features
~~~~~~~~

* ``feat: update SnowflakeSqlApiHook to support OAuth (#37922)``

Misc
~~~~

* ``Remove reference to execution_info in snowflake hook docstring (#37804)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update yanked versions in providers changelogs (#38262)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``Avoid to use too broad 'noqa' (#37862)``

5.3.1
.....

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add d401 support to snowflake provider (#37299)``

5.3.0
.....

Features
~~~~~~~~

* ``feat: Add openlineage support for CopyFromExternalStageToSnowflakeOperator (#36535)``

Bug Fixes
~~~~~~~~~

* ``Fix stacklevel in warnings.warn into the providers (#36831)``

Misc
~~~~

* ``Optimize 'SnowflakeSqlApiOperator' execution in deferrable mode (#36850)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Run mypy checks for full packages in CI (#36638)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

5.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Return common data structure in DBApi derived classes``
* ``Follow BaseHook connection fields method signature in child classes (#36086)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

5.1.2
.....

Bug Fixes
~~~~~~~~~

* ``OpenLineage integration tried to use non-existed method in SnowflakeHook (#35752)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Use reproducible builds for providers (#35693)``

5.1.1
.....

Misc
~~~~

* ``Make schema filter uppercase in 'create_filter_clauses' (#35428)``
* ``Bump min 'snowflake-connector-python' version (#35440)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Switch from Black to Ruff formatter (#35287)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``

5.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Decode response in f-string (#34518)``


Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor: Simplify comparisons (#34181)``

5.0.1
.....

Misc
~~~~

* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
* ``Use a single  statement with multiple contexts instead of nested  statements in providers (#33768)``
* ``Use literal dict instead of calling dict() in providers (#33761)``

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  Removed deprecated ``S3ToSnowflakeOperator`` in favor of ``CopyFromExternalStageToSnowflakeOperator``.
  The parameter that was passed as ``s3_keys`` needs to be changed to ``files``, and the behavior should stay the same.

  Removed deprecated ``SnowflakeToSlackOperator`` in favor of ``SqlToSlackOperator`` from Slack Provider.
  Parameters that were passed as ``schema``, ``role``, ``database``, ``warehouse`` need to be included into
  ``sql_hook_params`` parameter, and the behavior should stay the same.


* ``Remove deprecated 'S3ToSnowflake' and 'SnowflakeToSlack' operators (#33558)``

Bug Fixes
~~~~~~~~~

* ``Set snowflake_conn_id on Snowflake Operators to avoid error (#33681)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump apache-airflow-providers-snowflake due to breaking changes (#33615)``


4.4.2
.....

Misc
~~~~

* ``Add a new parameter to SQL operators to specify conn id field (#30784)``

4.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix connection parameters of 'SnowflakeValueCheckOperator' (#32605)``

4.4.0
.....

Features
~~~~~~~~

* ``openlineage, snowflake: add OpenLineage support for Snowflake (#31696)``

Misc
~~~~

* ``Add more accurate typing for DbApiHook.run method (#31846)``
* ``Add deprecation info to the providers modules and classes docstring (#32536)``

4.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix an issue that crashes Airflow Webserver when passed invalid private key path to Snowflake (#32016)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``build(pre-commit): check deferrable default value (#32370)``
   * ``D205 Support - Providers: Snowflake to Zendesk (inclusive) (#32359)``

4.3.0
.....

Features
~~~~~~~~

* ``Add Deferrable switch to SnowflakeSqlApiOperator (#31596)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``Improve provider documentation and README structure (#32125)``

4.2.0
.....

.. note::
  This release dropped support for Python 3.7

Features
~~~~~~~~

* ``Add SnowflakeSqlApiOperator operator (#30698)``

Misc
~~~~

* ``SnowflakeSqlApiOperator - Change the base class (#31751)``
* ``Moved sql_api_generate_jwt out of hooks folder (#31586)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check (#31742)``
   * ``Add D400 pydocstyle check - Providers (#31427)``
   * ``Improve docstrings in providers (#31681)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

4.1.0
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

4.0.5
.....

Misc
~~~~

* ``Update documentation for snowflake provider 4.0 breaking change (#30020)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add mechanism to suspend providers (#30422)``

4.0.4
.....

Bug Fixes
~~~~~~~~~

* ``Fix missing parens for files parameter (#29437)``

4.0.3
.....

Bug Fixes
~~~~~~~~~

* ``provide missing connection to the parent class operator (#29211)``
* ``Snowflake Provider - hide host from UI (#29208)``


4.0.2
.....

Breaking changes
~~~~~~~~~~~~~~~~


.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

The ``SnowflakeHook`` is now conforming to the same semantics as all the other ``DBApiHook``
implementations and returns the same kind of response in its ``run`` method. Previously (pre 4.* versions
of the provider, the Hook returned Dictionary of ``{ "column": "value" ... }`` which was not compatible
with other DBApiHooks that return just sequence of sequences. After this change (and dependency
on common.sql >= 1.3.1),the ``SnowflakeHook`` returns now python DbApi-compatible "results" by default.

The ``description`` (i.e. among others names and types of columns returned) can be retrieved
via ``descriptions`` and ``last_description`` fields of the hook after ``run`` method completes.

That makes the ``DatabricksSqlHook`` suitable for generic SQL operator and detailed lineage analysis.

If you had custom hooks or used the Hook in your TaskFlow code or custom operators that relied on this
behaviour, you need to adapt your DAGs or you can switch back the ``SnowflakeHook`` to return dictionaries
by passing ``return_dictionaries=True`` to the run method of the hook.

The ``SnowflakeOperator`` is also more standard and derives from common
``SQLExecuteQueryOperator`` and uses more consistent approach to process output when SQL queries are run.
However in this case the result returned by ``execute`` method is unchanged (it still returns Dictionaries
rather than sequences and those dictionaries are pushed to XCom, so your DAGs relying on this behaviour
should continue working without any change.

UPDATE: One of the unmentioned, breaking changes in the operator in 4.0 line was to switch autocommit to
False by default. While not very friendly to the users, it was a side effect of unifying the interface
with other SQL operators and we released it to the users, so switching it back again would cause even more
confusion. You should manually add autocommit=True to your SnowflakeOperator if you want to continue using
it and expect autocommit to work, but even better, you should switch to SQLExecuteQueryOperator.

In SnowflakeHook, if both ``extra__snowflake__foo`` and ``foo`` existed in connection extra
dict, the prefixed version would be used; now, the non-prefixed version will be preferred.

The ``4.0.0`` and ``4.0.1`` versions have been broken and yanked, so the 4.0.2 is the first change from the
``4.*`` line that should be used.

* ``Fix wrapping of run() method result of exasol and snowflake DB hooks (#27997)``
* ``Make Snowflake Hook conform to semantics of DBApi (#28006)``

4.0.1 (YANKED)
..............

.. warning::

    This version is yanked, as it contained problems when interacting with common.sql provider. Please install
    a version released afterwards.

Bug Fixes
~~~~~~~~~

* ``Fix errors in Databricks SQL operator introduced when refactoring (#27854)``
* ``Bump common.sql provider to 1.3.1 (#27888)``
* ``Fixing the behaviours of SQL Hooks and Operators finally (#27912)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

4.0.0 (YANKED)
..............

.. warning::

    This version is yanked, as it contained problems when interacting with common.sql provider. Please install
    a version released afterwards.

* ``Update snowflake hook to not use extra prefix (#26764)``

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``Add SQLExecuteQueryOperator (#25717)``

Bug fixes
~~~~~~~~~

* ``Use unused SQLCheckOperator.parameters in SQLCheckOperator.execute. (#27599)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable string normalization in python formatting - providers (#27205)``

3.3.0
.....

Features
~~~~~~~~

* ``Add custom handler param in SnowflakeOperator (#25983)``

Bug Fixes
~~~~~~~~~

* ``Fix wrong deprecation warning for 'S3ToSnowflakeOperator' (#26047)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``copy into snowflake from external stage (#25541)``

3.2.0
.....

Features
~~~~~~~~

* ``Move all "old" SQL operators to common.sql providers (#25350)``
* ``Unify DbApiHook.run() method with the methods which override it (#23971)``


3.1.0
.....

Features
~~~~~~~~

* ``Adding generic 'SqlToSlackOperator' (#24663)``
* ``Move all SQL classes to common-sql provider (#24836)``
* ``Pattern parameter in S3ToSnowflakeOperator (#24571)``

Bug Fixes
~~~~~~~~~

* ``S3ToSnowflakeOperator: escape single quote in s3_keys (#24607)``

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

* ``Fix error when SnowflakeHook take empty list in 'sql' param (#23767)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Snowflake system tests to new design #22434 (#24151)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.7.0
.....

Features
~~~~~~~~

* ``Allow multiline text in private key field for Snowflake (#23066)``

2.6.0
.....

Features
~~~~~~~~

* ``Add support for private key in connection for Snowflake (#22266)``

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.5.2
.....

Misc
~~~~

* ``Remove Snowflake limits (#22181)``

2.5.1
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.5.0
.....

Features
~~~~~~~~

* ``Add more SQL template fields renderers (#21237)``

Bug Fixes
~~~~~~~~~

* ``Fix #21096: Support boolean in extra__snowflake__insecure_mode (#21155)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add optional features in providers. (#21074)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Snowflake Provider: Improve tests for Snowflake Hook (#20745)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.4.0
.....

Features
~~~~~~~~

* ``Support insecure mode in SnowflakeHook (#20106)``
* ``Remove unused code in SnowflakeHook (#20107)``
* ``Improvements for 'SnowflakeHook.get_sqlalchemy_engine'  (#20509)``
* ``Exclude snowflake-sqlalchemy v1.2.5 (#20245)``
* ``Limit Snowflake connector to <2.7.2 (#20395)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix MyPy Errors for Snowflake provider. (#20212)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Remove duplicate get_connection in SnowflakeHook (#19543)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.3.0
.....

Features
~~~~~~~~

* ``Add test_connection method for Snowflake Hook (#19041)``
* ``Add region to Snowflake URI. (#18650)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Moving the example tag a little bit up to include the part where you specify the snowflake_conn_id (#19180)``

2.2.0
.....

Features
~~~~~~~~

* ``Add Snowflake operators based on SQL Checks  (#17741)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

2.1.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Fix messed-up changelog in 3 providers (#17380)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

2.1.0
.....

Features
~~~~~~~~

* ``Adding: Snowflake Role in snowflake provider hook (#16735)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Logging and returning info about query execution SnowflakeHook (#15736)``
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

* ``Add 'template_fields' to 'S3ToSnowflake' operator (#15926)``
* ``Allow S3ToSnowflakeOperator to omit schema (#15817)``
* ``Added ability for Snowflake to attribute usage to Airflow by adding an application parameter (#16420)``

Bug Fixes
~~~~~~~~~

* ``fix: restore parameters support when sql passed to SnowflakeHook as str (#16102)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Fix formatting and missing import (#16455)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.3.0
.....

Features
~~~~~~~~

* ``Expose snowflake query_id in snowflake hook and operator (#15533)``

1.2.0
.....

Features
~~~~~~~~

* ``Add dynamic fields to snowflake connection (#14724)``

1.1.1
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``
* ``Prepare to release the next wave of providers: (#14487)``

1.1.0
.....

Updated documentation and readme files.

Features
~~~~~~~~

* ``Fix S3ToSnowflakeOperator to support uploading all files in the specified stage (#12505)``
* ``Add connection arguments in S3ToSnowflakeOperator (#12564)``

1.0.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Snowflake breaks openssl when used``

Initial version of the provider.
