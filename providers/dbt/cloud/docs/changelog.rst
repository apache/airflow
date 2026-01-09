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


``apache-airflow-providers-dbt-cloud``


Changelog
---------

4.6.2
.....

Misc
~~~~

* ``Remove top-level SDK reference in Core (#59817)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``TaskInstance unused method cleanup (#59835)``

4.6.1
.....

Misc
~~~~

* ``chore: use OL macros instead of building OL ids from scratch (#59197)``
* ``Add backcompat for exceptions in providers (#58727)``

Doc-only
~~~~~~~~

* ``CHG: fix address (#59193)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.6.0
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
   * ``Bump min version of openlineage libraries to 1.40.0 to fix compat issues (#58302)``

4.5.0
.....

Features
~~~~~~~~

* ``Fixes inconsistency where other dbt operators already supported hook_params (#57242)``

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to 14 files in providers (databricks,dbt,docker) (#57994)``

4.4.4
.....

Bug Fixes
~~~~~~~~~

* ``Add retry mechanism and error handling to DBT Hook (#56651)``

Misc
~~~~

* ``Migrate dbt.cloud provider to ''common.compat'' (#56999)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable PT011 rule to prvoider tests (#56320)``
   * ``nit: Bump required OL client for Openlineage provider (#56302)``

4.4.3
.....


Bug Fixes
~~~~~~~~~

* ``Change operator DbtCloudRunJobOperator to send job_run_id to XCom (#55184)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch pre-commit to prek (#54258)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

4.4.2
.....

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup mypy ignore in dbt provider where possible (#53270)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``mocking definition order FIX (#52905)``

4.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Converting int account IDs to str in DBT Cloud connections (#51957)``

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Disable UP038 ruff rule and revert mandatory 'X | Y' in insintance checks (#52644)``
* ``Replace 'models.BaseOperator' to Task SDK one for DBT & Databricks (#52377)``
* ``Drop support for Python 3.9 (#52072)``
* ``Use BaseSensorOperator from task sdk in providers (#52296)``
* ``Add deprecation to 'airflow/sensors/base.py' (#52249)``
* ``Adding 'invocation_id' to run-results as expected by Openlineage (#51916)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Make sure all test version imports come from test_common (#52425)``
   * ``removed usage of pytest.mark.db_test from dbt tests (#52031)``
   * ``Introducing fixture to create 'Connections' without DB in provider tests (#51930)``
   * ``Switch the Supervisor/task process from line-based to length-prefixed (#51699)``

4.4.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``add root parent information to OpenLineage events (#49237)``

Misc
~~~~

* ``nit: Switch to emitting OL events with adapter and not client directly (#50398)``
* ``fix: adjust dag_run extraction for Airflow 3 in OL utils (#50346)``
* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``
* ``Use Label class from task sdk in providers (#49398)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

4.3.3
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

4.3.2
.....

Bug Fixes
~~~~~~~~~

* ``fix: add explicit requirement for OpenLineage version on DBT function (#47999)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Upgrade ruff to latest version (#48553)``

4.3.1
.....

Features
~~~~~~~~

* ``feat: Adjust DBT OpenLineage to Airflow 3 and improve logging (#47500)``

Misc
~~~~

* ``AIP-72: Handle Custom XCom Backend on Task SDK (#47339)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

4.2.1
.....

Misc
~~~~

* ``AIP-72: Moving BaseOperatorLink to task sdk (#47008)``
* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

4.2.0
.....

.. note::
  This version has no code changes. It's released due to yank of previous version due to packaging issues.

4.1.0
.....

Features
~~~~~~~~

* ``New Optional dbt Cloud Job Operator Params (#45634)``

Misc
~~~~

* ``AIP-72: Improving Operator Links Interface to Prevent User Code Execution in Webserver (#46613)``
* ``Add missing newline on conn string example (#45603)``
* ``Remove classes from 'typing_compat' that can be imported directly (#45589)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Moving EmptyOperator to standard provider (#46231)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``refactor(providers/dbt.cloud): move dbt cloud provider to new structure (#46208)``

4.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
   All deprecated classes, parameters and features have been removed from the DBT provider package.
   The following breaking changes were introduced:

   * Sensors
      * Remove ``airflow.providers.dbt.cloud.sensors.dbt.DbtCloudJobRunAsyncSensor``. Use ``airflow.providers.dbt.cloud.sensors.dbt.DbtCloudJobRunSensor`` with ``deferrable`` set to ``True`` instead.
      * Removed ``polling_interval`` parameter from ``DbtCloudJobRunSensor``. Use ``poke_interval`` instead.

* ``Remove Provider Deprecations in DBT (#44638)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Fix yoda-conditions (#44466)``
* ``utilize more information to deterministically generate OpenLineage run_id (#43936)``
* ``Remove commented breakpoint in dbt provider (#44163)``
* ``Rename execution_date to logical_date across codebase (#43902)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``

3.11.2
......

Bug Fixes
~~~~~~~~~

* ``Added condition to check if it is a scheduled save or rerun (#43453)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.11.1
......

Misc
~~~~

* ``Set lower bound to asgiref>=2.3.0 (#43001)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

3.11.0
......

Features
~~~~~~~~

* ``Add ability to provide proxy for dbt Cloud connection (#42737)``

Misc
~~~~

* ``Simplify code for recent dbt provider change (#42840)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.10.1
......

Misc
~~~~

* ``remove deprecated soft_fail from providers (#41710)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.10.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Aug 1st wave of providers (#41230)``
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

3.9.0
.....

Features
~~~~~~~~

* ``Add 'retry_from_failure' parameter to DbtCloudRunJobOperator (#38868)``

Bug Fixes
~~~~~~~~~

* ``Fix 'DbtCloudRunJobOperator' to Use Correct Status Parameters for 'reuse_existing_run' (#40048)``

3.8.1
.....

.. warning::
  You need to take action on this note only if you are running Airflow>=2.10.0
  In Airflow 2.10.0, we fix the way try_number works, so that it no longer returns different values depending
  on task instance state.  Importantly, after the task is done, it no longer shows current_try + 1.
  Thus we patch this provider to fix try_number references so they no longer adjust for the old, bad behavior.

Bug Fixes
~~~~~~~~~

* ``Scheduler to handle incrementing of try_number (#39336)``
* ``Validate dbt 'cause' field to be less than 255 characters (#38896)``

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``Add (optional) dependency between dbt-cloud and openlineage providers (#39366)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``



3.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``fix(dbt): fix wrong payload set when reuse_existing_run set to True in DbtCloudRunJobOperator (#39271)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``
* ``migrate to dbt v3 api for project endpoints (#39214)``

3.7.1
.....

Bug Fixes
~~~~~~~~~

* ``fix: disabled_for_operators now stops whole event emission (#38033)``
* ``fix(dbt): add return statement to yield within a while loop in triggers (#38395)``

3.7.0
.....

Features
~~~~~~~~

* ``feat(providers/dbt): add reuse_existing_run for allowing DbtCloudRunJobOperator to reuse existing run (#37474)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add comment about versions updated by release manager (#37488)``

3.6.1
.....

Misc
~~~~

* ``Bump aiohttp min version to avoid CVE-2024-23829 and CVE-2024-23334 (#37110)``
* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

3.6.0
.....

Features
~~~~~~~~

* ``feat: Add dag_id when generating OpenLineage run_id for task instance. (#36659)``

Bug Fixes
~~~~~~~~~

* ``Fix stacklevel in warnings.warn into the providers (#36831)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

3.5.1
.....

Bug Fixes
~~~~~~~~~

* ``Follow BaseHook connection fields method signature in child classes (#36086)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.5.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update information about links into the provider.yaml files (#35837)``

3.4.1
.....

Bug Fixes
~~~~~~~~~

* ``added cancelled handling in DbtCloudRunJobOperator deferred (#35597)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use reproducible builds for providers (#35693)``
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``D401 Support - Providers: DaskExecutor to Github (Inclusive) (#34935)``

3.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Remove useless print from dbt operator (#34322)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor usage of str() in providers (#34320)``


3.3.0
.....

Features
~~~~~~~~

* ``Add OpenLineage support for DBT Cloud. (#33959)``

Bug Fixes
~~~~~~~~~

* ``fix(providers/redis): respect soft_fail argument when exception is raised (#34164)``
* ``dbt, openlineage: set run_id after defer, do not log error if operator has no run_id set (#34270)``

Misc
~~~~

* ``Remove some useless try/except from providers code (#33967)``
* ``Use a single  statement with multiple contexts instead of nested  statements in providers (#33768)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for 09 2023 - 1st wave of Providers (#34201)``

3.2.3
.....

Misc
~~~~

* ``Refactor: Remove useless str() calls (#33629)``
* ``Refactor: Simplify code in smaller providers (#33234)``

3.2.2
.....

Misc
~~~~

* ``Add default_deferrable config (#31712)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: Databricks to Github (inclusive) (#32243)``
   * ``Improve provider documentation and README structure (#32125)``

3.2.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Remove Python 3.7 support (#30963)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improve docstrings in providers (#31681)``
   * ``Add discoverability for triggers in provider.yaml (#31576)``
   * ``Add D400 pydocstyle check - Providers (#31427)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

3.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``
* ``Optimize deferred execution mode in DbtCloudJobRunSensor (#30968)``
* ``Optimize deferred execution mode for DbtCloudRunJobOperator (#31188)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add cli cmd to list the provider trigger info (#30822)``
   * ``Upgrade ruff to 0.0.262 (#30809)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

3.1.1
.....

Misc
~~~~

* ``Merge DbtCloudJobRunAsyncSensor logic to DbtCloudJobRunSensor (#30227)``
* ``Move typing imports behind TYPE_CHECKING in DbtCloudHook (#29989)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add mechanism to suspend providers (#30422)``
   * ``adding trigger info to provider yaml (#29950)``

3.1.0
.....

Features
~~~~~~~~

* ``Add 'DbtCloudJobRunAsyncSensor' (#29695)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Beginning with version 2.0.0, users could specify single-tenant dbt Cloud domains via the ``schema`` parameter
in an Airflow connection. Subsequently in version 2.3.1, users could also connect to the dbt Cloud instances
outside of the US region as well as private instances by using the ``host`` parameter of their Airflow
connection to specify the entire tenant domain. Backwards compatibility for using ``schema`` was left in
place. Version 3.0.0 removes support for using ``schema`` to specify the tenant domain of a dbt Cloud
instance. If you wish to connect to a single-tenant, instance outside of the US, or a private instance, you
must use the ``host`` parameter to specify the _entire_ tenant domain name in your Airflow connection.

* ``Drop Connection.schema use in DbtCloudHook  (#29166)``

Features
~~~~~~~~

* ``Allow downloading of dbt Cloud artifacts to non-existent paths (#29048)``
* ``Add deferrable mode to 'DbtCloudRunJobOperator' (#29014)``

Misc
~~~~

* ``Provide more context for 'trigger_reason' in DbtCloudRunJobOperator (#28994)``


2.3.1
.....

Bug Fixes
~~~~~~~~~
* ``Use entire tenant domain name in dbt Cloud connection (#28890)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable string normalization in python formatting - providers (#27205)``

2.2.0
.....

Features
~~~~~~~~

* ``Add 'DbtCloudListJobsOperator' (#26475)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

2.1.0
.....

Features
~~~~~~~~

* ``Improve taskflow type hints with ParamSpec (#25173)``

2.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Update providers to use functools compat for ''cached_property'' (#24582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Enable dbt Cloud provider to interact with single tenant instances (#24264)``

Bug Fixes
~~~~~~~~~

* ``Fix typo in dbt Cloud provider description (#23179)``
* ``Fix new MyPy errors in main (#22884)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``AIP-47 - Migrate dbt DAGs to new design #22472 (#24202)``
   * ``Prepare provider documentation 2022.05.11 (#23631)``
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``
   * ``Replace usage of 'DummyOperator' with 'EmptyOperator' (#22974)``
   * ``Update dbt.py (#24218)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

1.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

1.0.1
.....

Initial version of the provider.
