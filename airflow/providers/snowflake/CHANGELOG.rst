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
   * ``Use reproducible builds for provider packages (#35693)``

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

.. Review and move the new changes to one of the sections above:
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
