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

``apache-airflow-providers-databricks``

Changelog
---------

6.12.0
......

Features
~~~~~~~~

* ``Add TimeoutError to be a retryable error in databricks provider (#43137)``
* ``Add ClientConnectorError to be a retryable error in databricks provider (#43091)``

Bug Fixes
~~~~~~~~~

* ``DatabricksHook: fix status property to work with ClientResponse used in async mode (#43333)``
* ``[DatabricksHook] Respect connection settings (#42618)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

6.11.0
......

Features
~~~~~~~~

* ``Add 'on_kill' to Databricks Workflow Operator (#42115)``

Misc
~~~~

* ``add warning log when task_key>100 (#42813)``
* ``Add debug logs to print Request/Response data in  Databricks provider (#42662)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.10.0
......

Features
~~~~~~~~

* ``(feat) databricks repair run with reason match and appropriate new settings (#41412)``

Misc
~~~~

* ``Removed deprecated method referance airflow.www.auth.has_access when min airflow version >= 2.8.0 (#41747)``
* ``remove deprecated soft_fail from providers (#41710)``

6.9.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.8.0
.....

.. note::
  This release reverts some of the functionality added in 6.7.0
  around json parameter compatible with XComs, Jinja expression values

Features
~~~~~~~~

* ``Add DatabricksWorkflowPlugin (#40724)``

Bug Fixes
~~~~~~~~~

* ``DatabricksPlugin - Fix dag view redirect URL by using url_for redirect (#41040)``
* ``Fix named parameters templating in Databricks operators (#40864)``

Misc
~~~~

* ``[Databricks Provider] Revert PRs #40864 and #40471 (#41050)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Add DatabricksWorkflowPlugin (#40153)" (#40714)``
   * ``Add DatabricksWorkflowPlugin (#40153)``

6.7.0
.....

Features
~~~~~~~~

* ``Make Databricks operators' json parameter compatible with XComs, Jinja expression values (#40471)``

Bug Fixes
~~~~~~~~~

* ``Bug/fix support azure managed identities in Databricks operator (#40332)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

6.6.0
.....

Features
~~~~~~~~

* ``Add DatabricksTaskOperator (#40013)``
* ``Add DatabricksWorkflowTaskGroup (#39771)``

Bug Fixes
~~~~~~~~~

* ``Databricks: optional include of user names in 'list_jobs' (#40178)``

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``
* ``Lower log levels from INFO to DEBUG to reduce log verbosity - Databricks provider auth (#39941)``
* ``Update pandas minimum requirement for Python 3.12 (#40272)``

6.5.0
.....

Features
~~~~~~~~

* ``add deferrable support to 'DatabricksNotebookOperator' (#39295)``

Bug Fixes
~~~~~~~~~

* ``get all failed tasks errors in when exception raised in DatabricksCreateJobsOperator (#39354)``

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``Better typing for BaseOperator 'defer' (#39742)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

6.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add DatabricksNotebookOperator (#39178)``
* ``Add notification settings parameters (#39175)``
* ``[FEAT] raise exception with main notebook error in DatabricksRunNowDeferrableOperator (#39110)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

6.3.0
.....

Features
~~~~~~~~

* ``Add cancel_previous_run to DatabricksRunNowOperator (#38702)``
* ``add repair_run support to DatabricksRunNowOperator in deferrable mode (#38619)``
* ``Adds job_id as path param in update permission (#38962)``

Bug Fixes
~~~~~~~~~

* ``Fix remaining D401 checks (#37434)``
* ``Update ACL during job reset (#38741)``
* ``Remove extra slash from update permission endpoint (#38918)``
* ``DatabricksRunNowOperator: fix typo in latest_repair_id (#39050)``

Misc
~~~~

* ``refactor(databricks): remove redundant else block (#38397)``
* ``Rename 'DatabricksSqlOperator''s fields' names to comply with templated fields validation (#38052)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update yanked versions in providers changelogs (#38262)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Restore Python 3.12 support for Databricks (#38207)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Exclude Python 3.12 for Databricks provider (#38070)``
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``update pre-commit (#37665)``
   * ``Prepare docs 1st wave (RC1) April 2024 (#38863)``
   * ``Prepare docs 1st wave (RC2) April 2024 (#38995)``

6.2.0
.....

Features
~~~~~~~~

* ``Update DatabricksSqlOperator to work with namedtuple (#37025)``

Misc
~~~~

* ``Bump aiohttp min version to avoid CVE-2024-23829 and CVE-2024-23334 (#37110)``
* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``
* ``Get rid of pytest-httpx as dependency (#37334)``

6.1.0
.....

Features
~~~~~~~~

* ``[FEAT] adds repair run functionality for databricks (#36601)``

Bug Fixes
~~~~~~~~~

* ``Fix databricks_sql hook query failing on empty result for return_tuple (#36827)``
* ``Rename columns to valid namedtuple attributes + ensure Row.fields are retrieved as tuple (#36949)``
* ``check status before DatabricksSubmitRunOperator & DatabricksSubmitRunOperator executes in deferrable mode (#36862)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Standardize airflow build process and switch to Hatchling build backend (#36537)``
   * ``Run mypy checks for full packages in CI (#36638)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

6.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

* ``Return common data structure in DBApi derived classes (#36205)``

Bug Fixes
~~~~~~~~~

* ``Fix: Implement support for 'fetchone()' in the ODBCHook and the Databricks SQL Hook (#36161)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``
* ``fix typos in DatabricksSubmitRunOperator (#36248)``
* ``Add code snippet formatting in docstrings via Ruff (#36262)``

.. Review and move the new changes to one of the sections above:
   * ``Prepare docs 1st wave of Providers December 2023 (#36112)``
   * ``Prepare docs 1st wave of Providers December 2023 RC2 (#36190)``

.. Review and move the new changes to one of the sections above:
   * ``Re-apply updated version numbers to 2nd wave of providers in December (#36380)``
   * ``Prepare 2nd wave of providers in December (#36373)``

5.0.1 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``The provider DBApiHook output returned broken output.``

Misc
~~~~

* ``Make pyodbc.Row and databricks.Row JSON-serializable via new 'make_serializable' method (#32319)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use reproducible builds for provider packages (#35693)``
   * ``Fix and reapply templates for provider documentation (#35686)``

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

The ``offset`` parameter has been deprecated from ``list_jobs`` in favor of faster pagination with ``page_token`` similarly to `Databricks API <https://docs.databricks.com/api/workspace/jobs/list>`_.

* ``Remove offset-based pagination from 'list_jobs' function in 'DatabricksHook' (#34926)``

4.7.0
.....

Features
~~~~~~~~

* ``Add operator to create jobs in Databricks (#35156)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``D401 Support - Providers: DaskExecutor to Github (Inclusive) (#34935)``

4.6.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add 'DatabricksHook' ClusterState (#34643)``

Bug Fixes
~~~~~~~~~

* ``Respect 'soft_fail' parameter in 'DatabricksSqlSensor' (#34544)``
* ``Respect 'soft_fail' argument when running DatabricksPartitionSensor (#34517)``
* ``Decode response in f-string (#34518)``

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Use aiohttp.BasicAuth instead of HTTPBasicAuth for aiohttp session in databricks hook (#34590)``
* ``Update 'list_jobs' function in 'DatabricksHook' to token-based pagination  (#33472)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor usage of str() in providers (#34320)``


4.5.0
.....

Features
~~~~~~~~

* ``Add "QUEUED" to RUN_LIFE_CYCLE_STATES following deployement of … (#33886)``
* ``allow DatabricksSubmitRunOperator to accept a pipeline name for a pipeline_task (#32903)``

Misc
~~~~

* ``Replace sequence concatenation by unpacking in Airflow providers (#33933)``
* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
* ``Use literal dict instead of calling dict() in providers (#33761)``
* ``Use f-string instead of  in Airflow providers (#33752)``

4.4.0
.....

.. note::
  This release excluded databricks-sql-connector version 2.9.0 due to a bug that it does not properly declare urllib3
  for more information please see https://github.com/databricks/databricks-sql-python/issues/190

Features
~~~~~~~~

* ``Add Service Principal OAuth for Databricks. (#33005)``

Misc
~~~~

* ``Update docs in databricks.py - we use 2.1 now (#32340)``
* ``Do not create lists we don't need (#33519)``
* ``Refactor: Improve detection of duplicates and list sorting (#33675)``
* ``Simplify conditions on len() in other providers (#33569)``
* ``Refactor: Simplify code in smaller providers (#33234)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Ecldude databrick connector 2.9.0 due to a bug (#33311)``

4.3.3
.....

Misc
~~~~

* ``Add a new parameter to SQL operators to specify conn id field (#30784)``

4.3.2
.....

Bug Fixes
~~~~~~~~~

* ``fix(providers/databricks): remove the execute method from to-be-deprecated DatabricksRunNowDeferrableOperator (#32806)``

Misc
~~~~

* ``Add missing execute_complete method for 'DatabricksRunNowOperator' (#32689)``
* ``Add more accurate typing for DbApiHook.run method (#31846)``

4.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Modify 'template_fields' of 'DatabricksSqlOperator' to support parent class fields (#32253)``

Misc
~~~~

* ``Add default_deferrable config (#31712)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D205 Support - Providers: Databricks to Github (inclusive) (#32243)``
   * ``Improve provider documentation and README structure (#32125)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``

4.3.0
.....

.. note::
  This release dropped support for Python 3.7

Features
~~~~~~~~

* ``add a return when the event is yielded in a loop to stop the execution (#31985)``

Bug Fixes
~~~~~~~~~

* ``Fix type annotation (#31888)``
* ``Fix Databricks SQL operator serialization (#31780)``
* ``Making Databricks run related multi-query string in one session again (#31898) (#31899)``

Misc
~~~~
* ``Remove return statement after yield from triggers class (#31703)``
* ``Remove Python 3.7 support (#30963)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improve docstrings in providers (#31681)``
   * ``Add discoverability for triggers in provider.yaml (#31576)``
   * ``Add D400 pydocstyle check - Providers (#31427)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

4.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add conditional output processing in SQL operators (#31136)``
* ``Add cancel all runs functionality to Databricks hook (#31038)``
* ``Add retry param in databrics async operator (#30744)``
* ``Add repair job functionality to databricks hook (#30786)``
* ``Add 'DatabricksPartitionSensor' (#30980)``

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``
* ``Deprecate databricks async operator (#30761)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move TaskInstanceKey to a separate file (#31033)``
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add cli cmd to list the provider trigger info (#30822)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

4.1.0
.....

Features
~~~~~~~~

* ``Add delete inactive run functionality to databricks provider (#30646)``
* ``Databricks SQL sensor (#30477)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add mechanism to suspend providers (#30422)``

4.0.1
.....

Bug Fixes
~~~~~~~~~

* ``DatabricksSubmitRunOperator to support taskflow (#29840)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``adding trigger info to provider yaml (#29950)``

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

The ``DatabricksSqlHook`` is now conforming to the same semantics as all the other ``DBApiHook``
implementations and returns the same kind of response in its ``run`` method. Previously (pre 4.* versions
of the provider, the Hook returned Tuple of ("cursor description", "results") which was not compatible
with other DBApiHooks that return just "results". After this change (and dependency on common.sql >= 1.3.1),
The ``DatabricksSqlHook`` returns now "results" only. The ``description`` can be retrieved via
``last_description`` field of the hook after ``run`` method completes.

That makes the ``DatabricksSqlHook`` suitable for generic SQL operator and detailed lineage analysis.

If you had custom hooks or used the Hook in your TaskFlow code or custom operators that relied on this
behaviour, you need to adapt your DAGs.

The Databricks ``DatabricksSQLOperator`` is also more standard and derives from common
``SQLExecuteQueryOperator`` and uses more consistent approach to process output when SQL queries are run.
However in this case the result returned by ``execute`` method is unchanged (it still returns Tuple of
("description", "results") and this Tuple is pushed to XCom, so your DAGs relying on this behaviour
should continue working without any change.

* ``Fix errors in Databricks SQL operator introduced when refactoring (#27854)``
* ``Bump common.sql provider to 1.3.1 (#27888)``

Bug Fixes
~~~~~~~~~

* ``Fix templating fields and do_xcom_push in DatabricksSQLOperator (#27868)``
* ``Fixing the behaviours of SQL Hooks and Operators finally (#27912)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

3.4.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``There is a bug in DatabricsksSQLOperator``

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``
* ``Replace urlparse with urlsplit (#27389)``

Features
~~~~~~~~

* ``Add SQLExecuteQueryOperator (#25717)``
* ``Use new job search API for triggering Databricks job by name (#27446)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

3.3.0
.....

Features
~~~~~~~~

* ``DatabricksSubmitRunOperator dbt task support (#25623)``

Misc
~~~~

* ``Add common-sql lower bound for common-sql (#25789)``
* ``Remove duplicated connection-type within the provider (#26628)``

Bug Fixes
~~~~~~~~~

* ``Databricks: fix provider name in the User-Agent string (#25873)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``D400 first line should end with period batch02 (#25268)``

3.2.0
.....

Features
~~~~~~~~

* ``Databricks: update user-agent string (#25578)``
* ``More improvements in the Databricks operators (#25260)``
* ``Improved telemetry for Databricks provider (#25115)``
* ``Unify DbApiHook.run() method with the methods which override it (#23971)``

Bug Fixes
~~~~~~~~~

* ``Databricks: fix test_connection implementation (#25114)``
* ``Do not convert boolean values to string in deep_string_coerce function (#25394)``
* ``Correctly handle output of the failed tasks (#25427)``
* ``Databricks: Fix provider for Airflow 2.2.x (#25674)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``updated documentation for databricks operator (#24599)``
   * ``Prepare docs for new providers release (August 2022) (#25618)``

3.1.0
.....

Features
~~~~~~~~

* ``Added databricks_conn_id as templated field (#24945)``
* ``Add 'test_connection' method to Databricks hook (#24617)``
* ``Move all SQL classes to common-sql provider (#24836)``

Bug Fixes
~~~~~~~~~

* ``Update providers to use functools compat for ''cached_property'' (#24582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Automatically detect if non-lazy logging interpolation is used (#24910)``
   * ``Remove "bad characters" from our codebase (#24841)``
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

* ``Add Deferrable Databricks operators (#19736)``
* ``Add git_source to DatabricksSubmitRunOperator (#23620)``

Bug Fixes
~~~~~~~~~

* ``fix: DatabricksSubmitRunOperator and DatabricksRunNowOperator cannot define .json as template_ext (#23622) (#23641)``
* ``Fix UnboundLocalError when sql is empty list in DatabricksSqlHook (#23815)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``AIP-47 - Migrate databricks DAGs to new design #22442 (#24203)``
   * ``Introduce 'flake8-implicit-str-concat' plugin to static checks (#23873)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.7.0
.....

Features
~~~~~~~~

* ``Update to the released version of DBSQL connector``
* ``DatabricksSqlOperator - switch to databricks-sql-connector 2.x``
* ``Further improvement of Databricks Jobs operators (#23199)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Address review comments``
   * ``Clean up in-line f-string concatenation (#23591)``
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``

2.6.0
.....

Features
~~~~~~~~

* ``More operators for Databricks Repos (#22422)``
* ``Add a link to Databricks Job Run (#22541)``
* ``Databricks SQL operators are now Python 3.10 compatible (#22886)``

Bug Fixes
~~~~~~~~~

* ``Databricks: Correctly handle HTTP exception (#22885)``

Misc
~~~~

* ``Refactor 'DatabricksJobRunLink' to not create ad hoc TaskInstances (#22571)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update black precommit (#22521)``
   * ``Fix new MyPy errors in main (#22884)``
   * ``Prepare mid-April provider documentation. (#22819)``

   * ``Prepare for RC2 release of March Databricks provider (#22979)``

2.5.0
.....

Features
~~~~~~~~

* ``Operator for updating Databricks Repos (#22278)``

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.4.0
.....

Features
~~~~~~~~

* ``Add new options to DatabricksCopyIntoOperator (#22076)``
* ``Databricks hook - retry on HTTP Status 429 as well (#21852)``

Misc
~~~~

* ``Skip some tests for Databricks from running on Python 3.10 (#22221)``

2.3.0
.....

Features
~~~~~~~~

* ``Add-showing-runtime-error-feature-to-DatabricksSubmitRunOperator (#21709)``
* ``Databricks: add support for triggering jobs by name (#21663)``
* ``Added template_ext = ('.json') to databricks operators #18925 (#21530)``
* ``Databricks SQL operators (#21363)``

Bug Fixes
~~~~~~~~~

* ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``

Misc
~~~~

* ``Support for Python 3.10``
* ``Updated Databricks docs for correct jobs 2.1 API and links (#21494)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``

2.2.0
.....

Features
~~~~~~~~

* ``Add 'wait_for_termination' argument for Databricks Operators (#20536)``
* ``Update connection object to ''cached_property'' in ''DatabricksHook'' (#20526)``
* ``Remove 'host' as an instance attr in 'DatabricksHook' (#20540)``
* ``Databricks: fix verification of Managed Identity (#20550)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix MyPy Errors for Databricks provider. (#20265)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Fix mypy databricks operator (#20598)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.1.0
.....

Features
~~~~~~~~

* ``Databricks: add more methods to represent run state information (#19723)``
* ``Databricks - allow Azure SP authentication on other Azure clouds (#19722)``
* ``Databricks: allow to specify PAT in Password field (#19585)``
* ``Databricks jobs 2.1 (#19544)``
* ``Update Databricks API from 2.0 to 2.1 (#19412)``
* ``Authentication with AAD tokens in Databricks provider (#19335)``
* ``Update Databricks operators to match latest version of API 2.0 (#19443)``
* ``Remove db call from DatabricksHook.__init__() (#20180)``

Bug Fixes
~~~~~~~~~

* ``Fixup string concatenations (#19099)``
* ``Databricks hook: fix expiration time check (#20036)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``Refactor DatabricksHook (#19835)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Unhide changelog entry for databricks (#20128)``
   * ``Update documentation for RC2 release of November Databricks Provider (#20086)``

2.0.2
.....

Bug Fixes
~~~~~~~~~
   * ``Move DB call out of DatabricksHook.__init__ (#18339)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

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
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``An initial rework of the 'Concepts' docs (#15444)``
   * ``Remove Backport Providers (#14886)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add documentation for Databricks connection (#15410)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
