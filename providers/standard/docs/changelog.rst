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

``apache-airflow-providers-standard``


Changelog
---------

1.0.0
.....

.. note::
  Stable release of the provider.

Bug Fixes
~~~~~~~~~

* ``Remove dag_version as a create_dagrun argument (#49148)``
* ``Fix ExternalTaskSensor task_group_id check condition (#49027)``
* ``Ensure that TI.id is unique per try. (#48749)``
* ``Conditionally add session related imports in standard provider (#49218)``

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.4.0
.....

Features
~~~~~~~~

* ``Make default as time.now() for TriggerDagRunOperator (#48969)``

Bug Fixes
~~~~~~~~~

* ``Fix WorkflowTrigger to work with TaskSDK (#48819)``
* ``Get 'LatestOnlyOperator' working with Task SDK (#48945)``
* ``Fix dagstate trigger to work with TaskSDK (#48747)``

Misc
~~~~

* ``Make '@task' import from airflow.sdk (#48896)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix default base value (#49013)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``

0.3.0
.....

* ``Make 'ExternalTaskSensor' work with Task SDK (#48651)``
* ``Make datetime objects in Context as Pendulum objects (#48592)``
* ``Fix _get_count in sensor_helper.py (#40795)``
* ``Fix logical_date error in BranchDateTimeOperator and BranchDayOfWeekOperator (#48486)``
* ``Move 'BaseSensorOperator' to TaskSDK definitions (#48244)``
* ``Migrate standard decorators to standard provider (#48683)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Upgrade ruff to latest version (#48553)``
   * ``Bump standard provider to 0.3.0 (#48763)``

0.2.0
.....

Features
~~~~~~~~

* ``AIP-72: Implement short circuit and branch operators (#46584)``

Bug Fixes
~~~~~~~~~

* ``Handle null logical date in TimeDeltaSensorAsync (#47652)``
* ``Fix deprecation warning for 'BranchMixIn' (#47856)``
* ``Fix DayOfWeekSensor use_task_logical_date condition (#47825)``
* ``Fix python operators errors when initialising plugins in virtualenv jinja script (#48035)``

Misc
~~~~

* ``AIP-72: Get 'TriggerDagRunOperator' working with Task SDK (#47882)``
* ``Relocate utils/weekday.py to standard provider (#47892)``
* ``AIP-72: Handle Custom XCom Backend on Task SDK (#47339)``
* ``Rewrite asset event registration (#47677)``
* ``Implement pre- and post-execute hooks in sdk (#48230)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Support '@task.bash' with Task SDK (#48060)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Bump various providers in preparation for Airflow 3.0.0b4 (#48013)``
   * ``Remove fixed comments (#47823)``
   * ``Remove links to x/twitter.com (#47801)``
   * ``Fix comment typo in PythonOperator (#47558)``

0.1.1
.....

Misc
~~~~

* ``Relocate SmoothOperator to standard provider and add tests (#47530)``
* ``AIP-72: Moving BaseOperatorLink to task sdk (#47008)``
* ``Move tests_common package to devel-common project (#47281)``
* ``Remove old UI and webserver (#46942)``
* ``Add deferred pagination mode to GenericTransfer (#44809)``
* ``Replace 'external_trigger' check with DagRunType (#45961)``
* ``Runtime context shouldn't have start_date as a key (#46961)``
* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix codespell issues detected by new codespell (#47259)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

0.1.0
.....

Features
~~~~~~~~

* ``AIP-82 Introduce 'BaseEventTrigger' as base class for triggers used with event driven scheduling (#46391)``
* ``AIP-83 amendment: Add logic for generating run_id when logical date is None. (#46616)``

Bug Fixes
~~~~~~~~~

* ``TriggerDagRunOperator by defaults set logical date as null (#46633)``
* ``Use run_id for ExternalDag and TriggerDagRun links (#46546)``

Misc
~~~~

* ``change listener API, add basic support for task instance listeners in TaskSDK, make OpenLineage provider support Airflow 3's listener interface (#45294)``
* ``Remove AirflowContextDeprecationWarning as all context should be clean for Airflow 3 (#46601)``
* ``refactor(utils/decorators): rewrite remove task decorator to use cst (#43383)``
* ``Add dynamic task mapping into TaskSDK runtime (#46032)``
* ``Moving EmptyOperator to standard provider (#46231)``
* ``Add run_after column to DagRun model (#45732)``
* ``Removing feature: send context in venv operators (using 'use_airflow_context') (#46306)``
* ``Remove import from MySQL provider tests in generic transfer test (#46274)``
* ``Fix failures on main related to DagRun validation (#45917)``
* ``Start porting mapped task to SDK (#45627)``
* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``
* ``Remove code for deprecation of Context keys (#45585)``
* ``AIP-72: Move non-user facing code to '_internal' (#45515)``
* ``AIP-72: Add support for 'get_current_context' in Task SDK (#45486)``
* ``Move Literal alias into TYPE_CHECKING block (#45345)``
* ``AIP-72: Add Taskflow API support & template rendering in Task SDK (#45444)``
* ``Remove tuple_in_condition helpers (#45201)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``move standard, alibaba and common.sql provider to the new structure (#45964)``

0.0.3
.....

.. note::
  Provider is still WIP. It can be used with production but we may introduce breaking changes without following semver until version 1.0.0

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  All deprecated classes, parameters and features have been removed from the SSH provider package.
  The following breaking changes were introduced:

  * operators
     * ``The deprecated parameter use_dill was removed in PythonOperator and all virtualenv and branching derivates. Please use serializer='dill' instead.``
     * ``The deprecated parameter use_dill was removed in all Python task decorators and virtualenv and branching derivates. Please use serializer='dill' instead.``

* ``Remove Provider Deprecations in Standard (#44541)``

Bug Fixes
~~~~~~~~~

* ``Add backward compatibility check for StartTriggerArgs import in filesystem sensor (#44458)``

Misc
~~~~

* ``Remove references to AIRFLOW_V_2_9_PLUS (#44987)``
* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Remove Pydanitc models introduced for AIP-44 (#44552)``
* ``Consistent way of checking Airflow version in providers (#44686)``
* ``Deferrable sensors can implement sensor timeout (#33718)``
* ``Remove AIP-44 code from renderedtifields.py (#44546)``
* ``Remove AIP-44 from taskinstance (#44540)``
* ``Move 'LatestOnlyOperator' operator to standard provider. (#44309)``
* ``Remove AIP-44 configuration from the code (#44454)``
* ``Move external task sensor to standard provider (#44288)``
* ``Move triggers to standard provider (#43608)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Correct new changelog breaking changes header (#44659)``
   * ``Add missing changelog to breaking change for Standard provider breaking changes (#44581)``

0.0.2
.....

.. note::
  Provider is still WIP. It can be used with production but we may introduce breaking changes without following semver until version 1.0.0

Bug Fixes
~~~~~~~~~

* ``Fix TriggerDagRunOperator extra_link when trigger_dag_id is templated (#42810)``

Misc
~~~~

* ``Move 'TriggerDagRunOperator' to standard provider (#44053)``
* ``Move filesystem sensor to standard provider (#43890)``
* ``Rename execution_date to logical_date across codebase (#43902)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``update standard provider CHANGELOG.rst (#44110)``

0.0.1
.....

.. note::
  Provider is still WIP. It can be used with production but we may introduce breaking changes without following semver until version 1.0.0

.. note::
   This provider created by migrating operators/sensors/hooks from Airflow 2 core.

Breaking changes
~~~~~~~~~~~~~~~~

* ``In BranchDayOfWeekOperator, DayOfWeekSensor, BranchDateTimeOperator parameter use_task_execution_date has been removed. Please use use_task_logical_date.``
* ``PythonVirtualenvOperator uses built-in venv instead of virtualenv package.``
* ``is_venv_installed method has been removed from PythonVirtualenvOperator as venv is built-in.``

* ``Initial version of the provider. (#41564)``
