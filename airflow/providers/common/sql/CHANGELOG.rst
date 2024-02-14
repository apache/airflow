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

1.11.0
......

Features
~~~~~~~~

* ``Enhancement: Performance enhancement for insert_rows method DbApiHook with fast executemany + SAP Hana support (#37246)``

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

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

.. Review and move the new changes to one of the sections above:
   * ``Use reproducible builds for provider packages (#35693)``
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

1.5.0
.....

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
   * ``Make static checks generated file  more stable accross the board (#29080)``

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

1.3.0
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
