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

1.10.1
......

Bug Fixes
~~~~~~~~~

* ``fix uv venv fail without direct internet access (#59046)``

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``
* ``Implement timetables in SDK (#58669)``
* ``nit: rename TriggerDagRunOperator._defer to deferrable (#58925)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.10.0
......

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``Auto-inject OpenLineage parent info into TriggerDagRunOperator conf (#58672)``
* ``Add few attrs from external_task sensor to OpenLineage events (#58719)``
* ``Allow virtualenv code to access connections/variables and send logs (#58148)``
* ``Add source to Param (#58615)``

Bug Fixes
~~~~~~~~~

* ``TriggerDagRunOperator deferral mode not working for Airflow 3 (#58497)``

Misc
~~~~

* ``Move out some exceptions to TaskSDK (#54505)``
* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``
* ``Remove SDK reference for NOTSET in Airflow Core (#58258)``
* ``Fix lower bound dependency to common-compat provider (#58833)``
* ``Remove global from task instance session (#58601)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``
   * ``Prepare release for 2025-11-27 wave of providers (#58697)``

1.9.2
.....

Bug Fixes
~~~~~~~~~

* ``fix: HITL params not validating (#57547)``
* ``Fix: Handle string formatted conf param in TriggerDagRunOperator (#57214)``
* ``Fix walking through wildcarded directory in FileTrigger (#57155)``

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Move subprocess utility closer to usage in python venv operators (#57189)``

Doc-only
~~~~~~~~

* ``Add caution on using Airflow packages in virtualenv operator (#57599)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable pt006 rule and fix new generate errors (#58238)``
   * ``fix MyPy type errors in datamodels/hitl.py (#57808)``
   * ``Enable PT006 rule to standard Provider test(ssensor, trigge, util) 9 files (#58022)``
   * ``Enable PT006 rule to standard Provider test(decorator, hook) 8 files (#58019)``
   * ``PT006 modify standard (operator) (#58020)``
   * ``Enable ruff PLW1509 rule (#57659)``
   * ``Fix mypy static errors in standard provider (#57762)``
   * ``Fix mypy type errors in providers/standard/ in external_task.py for SQLAlchemy 2 migration (#57369)``
   * ``Fix code formatting via ruff preview (#57641)``
   * ``Enable ruff PLW0602 rule (#57588)``
   * ``Revert virtualenv connections/variables access and logging as test are failing``
   * ``Enable PT011 rule to prvoider tests (#56929)``
   * ``Allow virtualenv code to access connections/variables and send logs (#57213)``
   * ``Fix mypy error in main (#57351)``
   * ``fix mypy errors in providers/standard/ (#57266)``


1.9.1
.....

Misc
~~~~

* ``Simplify version-specific imports in the Standard provider (#56867)``
* ``Throw NotImplementedError error when fail_when_dag_is_paused is used in TriggerDagRunOperator with Airflow 3.x (#56965)``

Doc-only
~~~~~~~~

* ``Correct 'Dag' to 'DAG' for code snippets in provider docs (#56727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.9.0
.....

Features
~~~~~~~~

* ``Add a '@task.stub' to allow tasks in other languages to be defined in dags (#56055)``

Bug Fixes
~~~~~~~~~

* ``Fix DagBag imports in Airflow 3.2+ (#56109)``

Misc
~~~~

* ``Move DagBag to airflow/dag_processing (#55139)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix 'example_bash_decorator' DAG (#56020)``
   * ``Enable pt011 rule 2 (#55749)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``
   * ``Prepare release for Sep 2025 3rd ad-hoc wave of providers (#56007)``

1.8.0
.....


Features
~~~~~~~~

* ``feat(hitl): get rid off "Fallback to defaults" in HITL (#55536)``
* ``feat(hitl): add fail_on_reject to ApprovalOperator (#55255)``

Bug Fixes
~~~~~~~~~

* ``fix(hitl): make the user model in HITLDetail consistent with airflow user model (#55463)``
* ``fix(hitl): Resolve Conflict 409 in API server when user actions at nearly timeout (#55243)``
* ``fix(hitl): fix HITL timeout error handling (#55760)``

Misc
~~~~

* ``refactor(hitl): rename response_at to responded_at (#55535)``
* ``refactor(hitl): remove AirflowException from HITLTriggerEventError inheritance (#55763)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove SDK dependency from SerializedDAG (#55538)``
   * ``Introduce e2e testing with testcontainers (#54072)``
   * ``Switch all airflow logging to structlog (#52651)``

1.7.0
.....


Features
~~~~~~~~

* ``Add options_mapping support to HITLBranchOperator (#55093)``
* ``feat(hitl): update url generating utility (#55022)``
* ``feat(hitl): add utility functions for generating the url to required actions page  (#54827)``
* ``Display a more friendly error when invalid branches are provided to branch operators (#54273)``
* ``Add owners/actors/respondents to HITLOperators (#54308)``

Bug Fixes
~~~~~~~~~

* ``Fix ''BranchPythonOperator'' failure when callable returns None (#54991)``
* ``Fix external_python task failure when ''expect_airflow=False'' (#54809)``
* ``Fix typos in HITL-related code and comments (#54670)``

Misc
~~~~

* ``refactor(hitl): rename HITLDetail.user_id as HITLDetail.responded_user_id and add HITLDetail.responded_user_name (#55019)``
* ``Revert "Fix rendering of template fields with start from trigger" (#55037)``
* ``Change StartTriggerArgs imports (#54856)``
* ``Do not use HITLDetailResponse from core in sdk (#54358)``
* ``Move DagBag to SDK and make it return SDK DAG objects (#53918)``
* ``Remove MappedOperator inheritance (#53696)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove airflow.models.DAG (#54383)``
   * ``Fix test_external_python tests setup (#55145)``
   * ``Move trigger_rule utils from 'airflow/utils'  to 'airflow.task'and integrate with Execution API spec (#53389)``
   * ``Import documentation with screenshots for HITL (#54618)``
   * ``Move filesystem sensor tests to standard provider (#54635)``
   * ``Switch pre-commit to prek (#54258)``
   * ``docs(hitl): fix typo in example_hitl_operator (#54537)``
   * ``make bundle_name not nullable (#47592)``
   * ``Remove SDK BaseOperator in TaskInstance (#53223)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

1.6.0
.....

Features
~~~~~~~~

* ``feat(HITL): add 'notifiers' to HITLOperator (#54128)``
* ``feat(HITL): add HITLBranchOperator (#53960)``
* ``feat(HITL): improve hitl trigger logging message (#53850)``
* ``feat(HITL): add "timedout" column to HITLTriggerEventSuccessPayload (#53852)``

Bug Fixes
~~~~~~~~~

* ``Restore 'execute_complete' functionality 'TimeSensor' when 'deferrable=True' (#53669)``
* ``Fix several deprecation warnings related to airflow.sdk (#53791)``
* ``Fix pycache_cleanup path handling in PythonVirtualenvOperator (#54214)``
* ``fix(HITL): guard empty options or chosen_options when writing response (#54355)``

Misc
~~~~

* ``refactor(HITL): replace timezone usage with airflow.sdk.timezone (#53962)``
* ``refactor(HITL): make default options class variables to avoid typo (#53849)``
* ``Add a warning about python interpreter using with uv (#54262)``
* ``Introduce 'StdoutCaptureManager' to isolate stdout from 'logging' logs (#54065)``
* ``Move some items in 'airflow.utils.context' to appropriate places (#53600)``

Doc-only
~~~~~~~~

* ``Fix BranchPythonOperator doc (#54205)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Documentation for Human-in-the-loop operator (#53694)``
   * ``Correct HITL version warnings to avoid confusion (#53876)``
   * ``Move functions in 'airflow.utils.decorator' to more appropriate places (#53420)``
   * ``Prepare release for Aug 2025 1st wave of providers (#54193)``

1.5.0
.....

Features
~~~~~~~~

* ``Add venv pycache clean up for the PythonVirtualenvOperator (#53390)``
* ``Add Human-in-the-loop logic to core Airflow and implement 'HITLOperator', 'ApprovalOperator', 'HITLEntryOperator' in standard provider (#52868)``

Bug Fixes
~~~~~~~~~

* ``Fix key error in _handle_execution_date_fn for ExternalTaskSensor (#53728)``
* ``fix: Type mismatch for DateInterval in latest only operator (#53541)``
* ``fix(HITL): Fix HITLEntryOperator "options" and "defaults" handling (#53184)``
* ``fix(HITL): handle hitl details when task instance is retried (#53824)``

Misc
~~~~

* ``Fix unreachable code mypy warnings in standard provider (#53431)``
* ``Align main branch after standard provider 1.4.1 release (#53511)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup mypy ignore in standard provider where possible (#53308)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove direct scheduler BaseOperator refs (#52234)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Move 'BaseHook' imports to version_compat for standard provider (#52766)``
* ``Deprecate and move 'airflow.utils.task_group' to SDK (#53450)``
* ``Deprecate decorators from Core (#53629)``
* ``Replace usages of XCOM_RETURN_KEY in providers to not be from utils (#53170)``
* ``Remove 'set_current_context' from 'airflow.models.taskinstance' (#53036)``
* ``Replace direct BaseOperator import with version_compat import (#53847)``
* ``Fix typo in serialized_params (#53848)``

Doc-only
~~~~~~~~

* ``docs: Correct TaskFlow capitalization in documentation (#51794)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Set up process for sharing code between different components (#53149)``
   * ``Replace 'mock.patch("utcnow")' with time_machine. (#53642)``
   * ``Add run_on_latest_version support for backfill and clear operations (#52177)``
   * ``docs(hitl): add example dag for all HITLOperator (#53360)``
   * ``Prepare release for Standard Provider 1.4.1``
   * ``Make dag_version_id in TI non-nullable (#50825)``
   * ``Fix example dag example_external_task_parent_deferrable.py imports (#52956)``

1.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix sensor skipping in Airflow 3.x branching operators (#53455)``

1.4.0
.....

Features
~~~~~~~~

* ``Add support for 'PackageIndex' connections in 'PythonVirtualenvOperator' (#52288)``
* ``Honor 'index_urls' when venv is created with 'uv' in 'PythonVirtualenvOperator' (#52287)``

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Disable UP038 ruff rule and revert mandatory 'X | Y' in insintance checks (#52644)``
* ``Upgrade ruff to latest version (0.12.1) (#52562)``
* ``Move compat shim in Standard Provider to 'version_compat.py' (#52567)``
* ``Add a bunch of no-redef ignores so Mypy is happy (#52507)``
* ``Drop support for Python 3.9 (#52072)``
* ``Replace 'models.BaseOperator' to Task SDK one for Standard Provider (#52292)``
* ``Add deprecation to 'airflow/sensors/base.py' (#52249)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``One more redef needing ignore (#52525)``
   * ``Make sure all test version imports come from test_common (#52425)``

1.3.0
.....

Features
~~~~~~~~

* ``feat: merge TimeDeltaSensorAsync to TimeDeltaSensor (#51133)``
* ``Add timezone support for date calculation in TimeSensor (#51043)``
* ``Merging 'TimeSensorAsync' with 'TimeSensor' (#50864)``

Bug Fixes
~~~~~~~~~

* ``Fix Airflow V2 incompatibility in ExternalTaskSensor (#51479)``
* ``bug fix: DateTimeSensor can't render jinja template if use native obj (#50744)``
* ``Fix backward compatibility for timeout in defer() with Airflow 2.11 (#50869)``

Misc
~~~~

* ``Port ''ti.run'' to Task SDK execution path (#50141)``

Doc-only
~~~~~~~~

* ``Move example_dags in standard provider to example_dags in sources (#51260)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Replace .parent.parent constructs (#51501)``
   * ``Improve testing for context serialization (#50566)``

1.2.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Bug Fixes
~~~~~~~~~

* ``Flattening the 'requirements' input for python venv operators (#50521)``
* ``Preserve all context keys during serialization (#50446)``
* ``Use latest bundle version when clearing / re-running dag (#50040)``
* ``Update 'WorkflowTrigger' to forward failed_stat (#50487)``

Misc
~~~~

* ``Adding deprecation notice for get_current_context in std provider (#50301)``
* ``Refactor Branch Operators to use 'BaseBranchOperator' (#48979)``
* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``
* ``refactor: Removed duplicate test_generic_transfer from wrong standard provider (#49786)``

Doc-only
~~~~~~~~

* ``Add back missing '[sources]' link in generated documentation's includes (#49978)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

1.1.0
.....

Features
~~~~~~~~

* ``feat: Add fail_when_dag_is_paused param to TriggerDagRunOperator (#48214)``

Bug Fixes
~~~~~~~~~

* ``Make LatestOnlyOperator work for default data-interval-less DAGs (#49554)``

Misc
~~~~

* ``Move DagIsPaused exception to standard provider (#49500)``
* ``Fix static check re removing unnecessary else condition (#49415)``

Doc-only
~~~~~~~~

* ``Update standard provider doc operators in core operators-and-hooks-ref.rst (#49401)``
* ``Update standard provider docs with correct imports (#49395)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix wrong link in standard provider yaml file (#49660)``
   * ``Add extra-links section to standard provider (#49447)``
   * ``Use unused pytest params in standard provider tests (#49422)``
   * ``Move test_sensor_helper.py to standard provider tests (#49396)``

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
* ``AIP-72: Add TaskFlow API support & template rendering in Task SDK (#45444)``
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
