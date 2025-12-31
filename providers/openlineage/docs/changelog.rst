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

``apache-airflow-providers-openlineage``


Changelog
---------

2.9.2
.....

Misc
~~~~

* ``Move MappedOperator to serialization (#59628)``
* ``Split SerializedBaseOperator from serde logic (#59627)``
* ``Minor cleanups removing SDK references from Core (#59491)``
* ``Refactor deprecated SQLA query openlineage provider (#59448)``
* ``nit: bump OL client dependency to 1.41 (#59321)``
* ``Extract shared "module_loading" distribution (#59139)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``TaskInstance unused method cleanup (#59835)``
   * ``Split SDK and serialized asset classes (#58993)``

2.9.1
.....

Misc
~~~~

* ``chore: Adjust AirflowRunFacet and docs after moving OL methods to BaseSQLOperator (#58903)``
* ``Move the traces and metrics code under a common observability package (#56187)``
* ``Implement timetables in SDK (#58669)``
* ``Remove global from openlineage provider (#58868)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``tests: Add OL system tests for deferrable TriggerDagRunOperator (#58933)``

2.9.0
.....

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``Add few attrs from external_task sensor to OpenLineage events (#58719)``
* ``Auto-inject OpenLineage parent info into TriggerDagRunOperator conf (#58672)``

Bug Fixes
~~~~~~~~~

* ``Fix OL root macros should reflect root from dagrun conf parent (#58428)``
* ``Fix root in parentRunFacet is not always sourced from dag run (#58407)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``
* ``Bump min version of openlineage libraries to 1.40.0 to fix compat issues (#58302)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``
   * ``Remove global from lineage.hook (#58285)``

2.8.0
.....

Features
~~~~~~~~

* ``feat(openlineage): Add parentRunFacet for DAG events (#57809)``
* ``nit: Use new taskinstance method to determine if task will emit OL event (#57446)``

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Refactor import statement for Session to use sqlalchemy.orm (#57586)``
* ``Migrate openlineage provider to common.compat (#57124)``

Doc-only
~~~~~~~~

* ``[Doc] Fixing 404 errors for OpenLineage & Oracle providers (#57469)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable ruff PLW2101,PLW2901,PLW3301 rule (#57700)``
   * ``Enable PT006 rule to openlineage Provider test (#57940)``
   * ``Fix MyPy type errors in providers openlineage (#57435)``
   * ``Fix MyPy type errors in providers utils/sql.py (#57448)``

2.7.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix openlineage dag state event emit on timed out dag (#56542)``
* ``Only import OpenLineage plugin listeners/hooks if plugin is enabled (#56266)``

Misc
~~~~

* ``nit: Bump required OL client for Openlineage provider (#56302)``
* ``chore: safeguard external call in OL sqlparser (#55692)``

Doc-only
~~~~~~~~

* ``Correct 'Dag' to 'DAG' for code snippets in provider docs (#56727)``
* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``chore: add sleep in ol test operator (#54807)``
   * ``Fix DagBag imports in 3.2+ (#56109)``

2.7.2
.....

Misc
~~~~

* ``Adjust OpenLineage utils to be compatible with Airflow 3.1.0 (#56040)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable PT011 rule to prvoider tests (#56021)``
   * ``Move DagBag to airflow/dag_processing (#55139)``

2.7.1
.....


Bug Fixes
~~~~~~~~~

* ``fix: prevent user facets from overriding OL facets (#55765)``

Misc
~~~~

* ``Remove SDK dependency from SerializedDAG (#55538)``
* ``Decouple secrets_masker project from airflow configuration (#55259)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.7.0
.....


Features
~~~~~~~~

* ``add tasksDuration to state run facet (#53644)``

Bug Fixes
~~~~~~~~~

* ``Fix setproctitle usage on macos (#53122)``
* ``Fix mypy no-redef errors for timeout imports in providers (#54471)``

Misc
~~~~

* ``Decouple serialization and deserialization code for tasks (#54569)``
* ``Move secrets_masker over to airflow_shared distribution (#54449)``
* ``Remove MappedOperator inheritance (#53696)``
* ``Update usage of timeout contextmanager from SDK where possible (#54183)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``
* ``docs: fix broken link on OpenLineage developer section (#54356)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove airflow.models.DAG (#54383)``
   * ``chore: add more test cases to OpenLineage system tests (#55138)``
   * ``chore: update ol system tests after bug fix in AF3 (#54629)``
   * ``Switch pre-commit to prek (#54258)``
   * ``Remove SDK BaseOperator in TaskInstance (#53223)``
   * ``chore: Adjust OL system tests to latest changes (#54352)``
   * ``Move email notifications from scheduler to DAG processor (#55238)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

2.6.1
.....

Bug Fixes
~~~~~~~~~

* ``Allow secrets redact function to have different redaction than '***' (#53977)``
* ``Fix several deprecation warnings related to airflow.sdk (#53791)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.6.0
.....

Features
~~~~~~~~

* ``Add OpenLineage support for EmptyOperator (#53730)``
* ``feat: Add new documentation facet to all OL events (#52343)``

Bug Fixes
~~~~~~~~~

* ``fix: Adjust emits_ol_events to account for inlets check since AF3.0.2 (#53449)``
* ``fix: Check dynamic transport env vars in is_disabled() (#53370)``
* ``Remove direct scheduler BaseOperator refs (#52234)``
* ``Fix Task Group Deprecation error from plugin (#53813)``

Misc
~~~~

* ``Updating openlineage-integration-common and openlineage-python dependencies for apache-airflow-providers-openlineage provider. (#53671)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores in openlineage provider where possible (#53284)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Make openlineage compatible with mypy 1.16.1 (#53119)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Restore ''get_previous_dagrun'' functionality for task context (#53655)``
   * ``Deprecate decorators from Core (#53629)``
   * ``Replace 'mock.patch("utcnow")' with time_machine. (#53642)``
   * ``Update main with Airflow 3.0.3 release details (#53349)``
   * ``Cleanup mypy ignores in openlineage test_listener (#53326)``
   * ``Make dag_version_id in TI non-nullable (#50825)``
   * ``fix: Adjust OL system test to latest changes (#52971)``

2.5.0
.....

Features
~~~~~~~~

* ``[OpenLineage] Added operator_provider_version to task event (#52468)``

Bug Fixes
~~~~~~~~~

* ``fix non existing 'ti.dag_run' access in openlineage provider (#51932)``
* ``Fix type import to AbstractOperator (#51773)``

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Disable UP038 ruff rule and revert mandatory 'X | Y' in insintance checks (#52644)``
* ``Add a bunch of no-redef ignores so Mypy is happy (#52507)``
* ``chore: use task_instance as source for all airflow identifiers used in listener (#52339)``
* ``Drop support for Python 3.9 (#52072)``
* ``Replace 'models.BaseOperator' to Task SDK one for Standard Provider (#52292)``
* ``Use BaseSensorOperator from task sdk in providers (#52296)``
* ``nit: bump openlineage libraries requirement to 1.34 (#52075)``
* ``Fixing ruff static check failures on main (#51937)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Make sure all test version imports come from test_common (#52425)``
   * ``Remove db_tests from openlineage provider (#52239)``
   * ``Fix compatibility test for Open Lineage (#51931)``
   * ``Fix failing openlineage test (#51928)``

2.4.0
.....

Features
~~~~~~~~

* ``feat: Add NominalTimeRunFacet to all OL events (#51404)``
* ``feat: Add TagsJobFacet to DAGRun OpenLineage events (#51303)``
* ``feat: Add Airflow-specific OL system tests validation and more tests (#51084)``
* ``feat: merge TimeDeltaSensorAsync to TimeDeltaSensor (#51133)``
* ``expose OpenLineage's lineage_root_* macros in plugin (#50532)``

Bug Fixes
~~~~~~~~~

* ``fix: Use task owner for TASK level Ownership facet (#51305)``
* ``Fix openlineage doc error (#51356)``
* ``Fix OpenLineage macro _get_logical_date (#51210)``
* ``Fix failing static checks (#51197)``
* ``Fix simple grammar mistakes in doc (#51138)``
* ``Fixes issue RuntimeTaskInstance does not contain log_url | added during taskrunner startup (#50376)``

Misc
~~~~

* ``nit: task-level facets should not overwrite integration-level facets (#51690)``
* ``Make duration in 'List Dag Run' page sortable (#51495)``
* ``import MappedOperator from airflow.sdk.definitions.mappedoperator (#51492)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``tests: Adjust OL system test after ownership facet changes (#51394)``

2.3.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``add root parent information to OpenLineage events (#49237)``
* ``feat: Add DAG versioning information to OpenLineage events (#48741)``
* ``Improve execution time messages for DAG or Task not found (#49352)``

Misc
~~~~

* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``
* ``nit: Remove duplicate warning when no OL metadata returned (#50350)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``tests: Fix clearing Variables for OpenLineage system tests (#50234)``
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Bump openlineage provider (#50230)``
   * ``Avoid committing history for providers (#49907)``
   * ``tests: Fix OpenLineage VariableTransport's initialization (#49550)``
   * ``Delete duplicate 'mock_supervisor_comms' pytest fixtures from OL provider (#49520)``
   * ``Remove redundant fixtures in OL provider (#49357)``

2.2.0
.....

Features
~~~~~~~~

* ``feat: Add support for task's manual state change notification in OpenLineage listener (#49040)``
* ``feat: Explicitly propagate airflow logging level to OL client (#49108)``
* ``feat: Add owner_links in DAG object in airflow facet (#49085)``

Misc
~~~~

* ``gate import behind Airflow 2 path (#49209)``
* ``remove superfluous else block (#49199)``
* ``chore: Update requirement for openlineage client to >=1.31.0 (#49083)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.3
.....

Misc
~~~~

* ``Move ObjectStoragePath and attach to Task SDK (#48906)``
* ``Make '@task' import from airflow.sdk (#48896)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``tests: verify openlineage airflow models serialization (#47915)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``

2.1.2
.....

Bug Fixes
~~~~~~~~~

* ``fix: Adjust OpenLineage DefaultExtractor for RuntimeTaskInstance in Airflow 3 (#47673)``
* ``Stop trying to reconfigure the ORM in the OpenLineage workers (#47580)``
* ``fix: Re-add configuring orm for OpenLineage's listener on scheduler (#48049)``
* ``fix: remove use of get_inlet_defs and get_outlet_defs from OpenLineage (#48792)``
* ``Make datetime objects in Context as Pendulum objects (#48592)``
* ``fix: OpenLineage BaseExtractor's on_failure should call on_complete by default (#48456)``
* ``Fix OL tests due to decorators move to standard provider (#48808)``

Misc
~~~~
* ``add OpenLineage configuration injection to SparkSubmitOperator (#47508)``
* ``feat: Add dagrun's end_date and duration to OL facet (#47901)``
* ``Use TaskInstance ID as FK in TaskReschedule instead of the multiple fields (#47459)``
* ``serialize http transports contained in composite transport (#47444)``
* ``Implement task-level "on" callbacks in sdk (#48002)``
* ``Calculate retry eligibility before task runs (#47996)``
* ``Implement triggering_asset_events in task sdk (#48650)``
* ``nit: log more details about OpenLineage exceptions being caught (#48459)``
* ``Add backcompat to openlineage provider method (#48406)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Bump OL provider for Airflow 3.0.0b4 release (#48011)``
   * ``Remove links to x/twitter.com (#47801)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``docs: Update OL docs after BaseExtractor changes (#48585)``
   * ``Remove auto lineage from Airflow (#48421)``
   * ``Upgrade ruff to latest version (#48553)``
   * ``Move BaseOperator to 'airflow/sdk/bases/operator.py' (#48529)``
   * ``Move bases classes to 'airflow.sdk.bases' (#48487)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``

2.1.1
.....

Bug Fixes
~~~~~~~~~

* ``fix: OpenLineage serialization of dataset timetables for Airflow 2.9 (#47150)``

Misc
~~~~

* ``chore: Update description of 'execution_timeout' in OpenLineage provider.yaml (#47448)``
* ``Remove the old 'task run' commands and LocalTaskJob (#47453)``
* ``Move task_sdk to a standalone task-sdk distribution (#47451)``
* ``Move uuid6 to be devel dependency of openlineage (#47464)``
* ``revert removing 'external_trigger' from OpenLineage provider (#47383)``
* ``Implement stale dag bundle cleanup (#46503)``
* ``Replace 'external_trigger' check with DagRunType (#45961)``
* ``Runtime context shouldn't have start_date as a key (#46961)``
* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

2.1.0
.....

Features
~~~~~~~~

* ``change listener API, add basic support for task instance listeners in TaskSDK, make OpenLineage provider support Airflow 3's listener interface (#45294)``
* ``feat: Add ProcessingEngineRunFacet to all OL events (#46283)``
* ``feat: automatically inject OL transport info into spark jobs (#45326)``
* ``feat: Add OpenLineage support for some SQL to GCS operators (#45242)``
* ``feat: automatically inject OL info into spark job in DataprocCreateBatchOperator (#44612)``

Bug Fixes
~~~~~~~~~

* ``Update OpenLineage emmiter to cope with nullable logical_date (#46722)``
* ``fix: OL sql parsing add try-except for sqlalchemy engine (#46366)``
* ``OpenLineage: Include 'AirflowDagRunFacet' in complete/failed events (#45615)``

Misc
~~~~

* ``Adding uuid6 as a dependency for openlineage (#46653)``
* ``Remove AirflowContextDeprecationWarning as all context should be clean for Airflow 3 (#46601)``
* ``Remove Airflow 3 Deprecation Warning dependency in OTel Provoder (#46600)``
* ``AIP-72: Move Secrets Masker to task SDK (#46375)``
* ``Add run_after column to DagRun model (#45732)``
* ``Remove old lineage stuff (#45260)``
* ``Start porting mapped task to SDK (#45627)``
* ``chore: Update docstring for DatabaseInfo in OpenLineage provider (#45638)``
* ``Remove classes from 'typing_compat' that can be imported directly (#45589)``
* ``udpated 404 hyperlink to gcstogcsoperator (#45311)``
* ``pass error for on_task_instance_failed in task sdk (#46941)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove remnants of old provider's structure (#46829)``
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``tests: Add more information to check in OL system test (#46379)``
   * ``Move Google provider to new provider structure (#46344)``
   * ``Moving EmptyOperator to standard provider (#46231)``
   * ``Fix example import tests after move of providers to new structure (#46217)``
   * ``Fixing OPENLINEAGE system tests import failure after new structure changes (#46204)``
   * ``Move OPENLINEAGE provider to new structure provider (#46068)``
   * ``update outdated hyperlinks referencing provider package files (#45332)``
   * ``Prepare docs for Feb 1st wave of providers (#46893)``

2.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
   All deprecated classes, parameters and features have been removed from the OpenLineage provider package.
   The following breaking changes were introduced:

   * Utils

      * Removed ``normalize_sql`` function from ``openlineage.utils`` module.

* ``Remove Provider Deprecations in OpenLineage (#44636)``

Features
~~~~~~~~

* ``add clear_number to OpenLineage's dagrun-level event run id generation (#44617)``
* ``utilize more information to deterministically generate OpenLineage run_id (#43936)``
* ``feat: automatically inject OL info into spark job in DataprocSubmitJobOperator (#44477)``

Misc
~~~~

* ``Remove references to AIRFLOW_V_2_9_PLUS (#44987)``
* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Consistent way of checking Airflow version in providers (#44686)``
* ``add basic system tests for OpenLineage (#43643)``
* ``Move Asset user facing components to task_sdk (#43773)``
* ``Rename execution_date to logical_date across codebase (#43902)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``

1.14.0
......

Features
~~~~~~~~

* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``
* ``Add ProcessingEngineRunFacet to OL DAG Start event (#43213)``

Bug Fixes
~~~~~~~~~

* ``serialize asset/dataset timetable conditions in OpenLineage info also for Airflow 2 (#43434)``
* ``OpenLineage: accept whole config when instantiating OpenLineageClient. (#43740)``

Misc
~~~~

* ``Temporarily limit openlineage to <1.24.0 (#43732)``
* ``Move python operator to Standard provider (#42081)``

1.13.0
......

Features
~~~~~~~~

* ``feat(providers/openlineage): Use asset in common provider (#43111)``

Misc
~~~~

* ``Ignore attr-defined for compat import (#43301)``
* ``nit: remove taskgroup's tooltip from OL's AirflowJobFacet (#43152)``
* ``require 1.2.1 common.compat for openlineage provider (#43039)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

1.12.2
......

Misc
~~~~

* ``Change imports to use Standard provider for BashOperator (#42252)``
* ``Drop python3.8 support core and providers (#42766)``
* ``Rename dataset related python variable names to asset (#41348)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.12.1
......

Bug Fixes
~~~~~~~~~

* ``fix: OpenLineage dag start event not being emitted (#42448)``
* ``fix: typo in error stack trace formatting for clearer output (#42017)``

1.12.0
......

Features
~~~~~~~~

* ``feat: notify about potential serialization failures when sending DagRun, don't serialize unnecessary params, guard listener for exceptions (#41690)``

Bug Fixes
~~~~~~~~~

* ``fix: cast list to flattened string in openlineage InfoJsonEncodable (#41786)``

Misc
~~~~

* ``chore: bump OL provider dependencies versions (#42059)``
* ``move to dag_run.logical_date from execution date in OpenLineage provider (#41889)``
* ``Unify DAG schedule args and change default to None (#41453)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.11.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``feat: add debug facet to all OpenLineage events (#41217)``
* ``feat: add fileloc to DAG info in AirflowRunFacet (#41311)``
* ``feat: remove openlineage client deprecated from_environment() method (#41310)``
* ``feat: openlineage listener captures hook-level lineage (#41482)``

Bug Fixes
~~~~~~~~~

* ``fix: get task dependencies without serializing task tree to string (#41494)``
* ``fix: return empty data instead of None when OpenLineage on_start method is missing (#41268)``
* ``fix: replace dagTree with downstream_task_ids (#41587)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``
* ``chore: remove openlineage deprecation warnings (#41284)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Aug 2nd wave of providers (#41559)``

1.10.0
......

Features
~~~~~~~~

* ``Add AirflowRun on COMPLETE/FAIL events (#40996)``
* ``openlineage: extend custom_run_facets to also be executed on complete and fail (#40953)``
* ``openlineage: migrate OpenLineage provider to V2 facets. (#39530)``
* ``openlineage: Add AirflowRunFacet for dag runEvents (#40854)``
* ``[AIP-62] Translate AIP-60 URI to OpenLineage (#40173)``
* ``Ability to add custom facet in OpenLineage events (#38982)``
* ``openlineage: add method to common.compat to not force hooks to try/except every 2.10 hook lineage call (#40812)``
* ``openlineage: use airflow provided getters from conf (#40790)``
* ``openlineage: add config to include 'full' task info based on conf setting (#40589)``
* ``Add TaskInstance log_url to OpenLineage facet (#40797)``
* ``openlineage: add deferrable information to task info in airflow run facet (#40682)``

Bug Fixes
~~~~~~~~~

* ``Adjust default extractor's on_failure detection for airflow 2.10 fix (#41094)``
* ``openlineage: make value of slots in attrs.define consistent across all OL usages (#40992)``
* ``Set 'slots' to True for facets used in DagRun (#40972)``
* ``openlineage: fix / add some task attributes in AirflowRunFacet (#40725)``

Misc
~~~~

* ``openlineage: replace dt.now with airflow.utils.timezone.utcnow (#40887)``
* ``openlineage: remove deprecated parentRun facet key (#40681)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.9.1
.....

Bug Fixes
~~~~~~~~~

* ``fix openlineage parsing dag tree with MappedOperator (#40621)``

1.9.0
.....

Features
~~~~~~~~

* ``local task job: add timeout, to not kill on_task_instance_success listener prematurely (#39890)``
* ``openlineage: add some debug logging around sql parser call sites (#40200)``
* ``Add task SLA and queued datetime information to AirflowRunFacet (#40091)``
* ``Add error stacktrace to OpenLineage task event (#39813)``
* ``Introduce AirflowJobFacet and AirflowStateRunFacet (#39520)``
* ``Use UUIDv7 for OpenLineage runIds (#39889)``
* ``openlineage: execute extraction and message sending in separate process (#40078)``
* ``Add few removed Task properties in AirflowRunFacet (#40371)``

Bug Fixes
~~~~~~~~~

* ``openlineage, redshift: do not call DB for schemas below Airflow 2.10 (#40197)``
* ``fix: scheduler crashing with OL provider on airflow standalone (#40459)``
* ``nit: fix logging level (#40461)``
* ``fix: provide stack trace under proper key in OL facet (#40372)``

Misc
~~~~

* ``fix: sqa deprecations for airflow providers (#39293)``
* ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 2nd wave June 2024 (#40273)``
   * ``fix: scheduler crashing with OL provider on airflow standalone (#40353)``
   * ``Revert "fix: scheduler crashing with OL provider on airflow standalone (#40353)" (#40402)``

1.8.0
.....

.. warning::
  In Airflow 2.10.0, we fix the way try_number works.
  For Airflow >= 2.10.0, use ``apache-airflow-providers-openlineage >= 1.8.0``.
  Older versions of Airflow are not affected, In case you run an incompatible version
  an exception will be raised asking you to upgrade provider version.

Features
~~~~~~~~

* ``Scheduler to handle incrementing of try_number (#39336)``

Bug Fixes
~~~~~~~~~

* ``fix: Prevent error when extractor can't be imported (#39736)``
* ``Re-configure ORM in spawned OpenLineage process in scheduler. (#39735)``

Misc
~~~~

* ``chore: Update conf retrieval docstring and adjust pool_size (#39721)``
* ``Remove 'openlineage.common' dependencies in Google and Snowflake providers. (#39614)``
* ``Use 'ProcessPoolExecutor' over 'ThreadPoolExecutor'. (#39235)``
* ``misc: Add custom provider runtime checks (#39609)``
* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``openlineage: notify that logged exception was caught (#39493)``
* ``chore: Add more OpenLineage logs to facilitate debugging (#39136)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add missing 'dag_state_change_process_pool_size' in 'provider.yaml'. (#39674)``
   * ``Run unit tests for Providers with airflow installed as package. (#39513)``
   * ``Reapply templates for all providers (#39554)``


1.7.1
.....

Misc
~~~~

* ``openlineage, snowflake: do not run external queries for Snowflake (#39113)``

1.7.0
.....

Features
~~~~~~~~

* ``Add lineage_job_namespace and lineage_job_name OpenLineage macros (#38829)``
* ``openlineage: add 'opt-in' option (#37725)``

Bug Fixes
~~~~~~~~~

* ``fix: Remove redundant operator information from facets (#38264)``
* ``fix: disabled_for_operators now stops whole event emission (#38033)``
* ``fix: Add fallbacks when retrieving Airflow configuration to avoid errors being raised (#37994)``
* ``fix: Fix parent id macro and remove unused utils (#37877)``

Misc
~~~~

* ``Avoid use of 'assert' outside of the tests (#37718)``
* ``Add default for 'task' on TaskInstance / fix attrs on TaskInstancePydantic (#37854)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Brings back mypy-checks (#38597)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``
   * ``fix: try002 for provider openlineage (#38806)``

1.6.0
.....

Features
~~~~~~~~

* ``feat: Add OpenLineage metrics for event_size and extraction time (#37797)``
* ``feat: Add OpenLineage support for File and User Airflow's lineage entities (#37744)``
* ``[OpenLineage] Add support for JobTypeJobFacet properties. (#37255)``
* ``chore: Update comments and logging in OpenLineage ExtractorManager (#37622)``

Bug Fixes
~~~~~~~~~

* ``fix: Check if operator is disabled in DefaultExtractor.extract_on_complete (#37392)``

Misc
~~~~

* ``docs: Update whole OpenLineage Provider docs. (#37620)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``tests: Add OpenLineage test cases for File to Dataset conversion (#37791)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Follow D401 style in openlineage, slack, and tableau providers (#37312)``

1.5.0
.....

Features
~~~~~~~~

* ``feat: Add dag_id when generating OpenLineage run_id for task instance. (#36659)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

1.4.0
.....

Features
~~~~~~~~

* ``Add OpenLineage support for Redshift SQL. (#35794)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``

1.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix typo. (#36362)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.3.0
.....

Features
~~~~~~~~

* ``feat: Add parent_run_id for COMPLETE and FAIL events (#36067)``
* ``Add basic metrics to stats collector. (#35368)``

Bug Fixes
~~~~~~~~~

* ``fix: Repair run_id for OpenLineage FAIL events (#36051)``
* ``Fix and reapply templates for provider documentation (#35686)``

Misc
~~~~

* ``Remove ClassVar annotations. (#36084)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for providers (#35693)``

1.2.1
.....

Misc
~~~~

* ``Make schema filter uppercase in 'create_filter_clauses' (#35428)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix bad regexp in mypy-providers specification in pre-commits (#35465)``
   * ``Switch from Black to Ruff formatter (#35287)``

1.2.0
.....

Features
~~~~~~~~

* ``Send column lineage from SQL operators. (#34843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``

1.1.1
.....

Misc
~~~~

* ``Adjust log levels in OpenLineage provider (#34801)``

1.1.0
.....

Features
~~~~~~~~

* ``Allow to disable openlineage at operator level (#33685)``


Bug Fixes
~~~~~~~~~

* ``Fix import in 'get_custom_facets'. (#34122)``

Misc
~~~~

* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
* ``Add OpenLineage support for DBT Cloud. (#33959)``
* ``Refactor unneeded  jumps in providers (#33833)``
* ``Refactor: Replace lambdas with comprehensions in providers (#33771)``

1.0.2
.....

Bug Fixes
~~~~~~~~~

* ``openlineage: don't run task instance listener in executor (#33366)``
* ``openlineage: do not try to redact Proxy objects from deprecated config (#33393)``
* ``openlineage: defensively check for provided datetimes in listener (#33343)``

Misc
~~~~

* ``Add OpenLineage support for Trino. (#32910)``
* ``Simplify conditions on len() in other providers (#33569)``
* ``Replace repr() with proper formatting (#33520)``

1.0.1
.....

Bug Fixes
~~~~~~~~~

* ``openlineage: disable running listener if not configured (#33120)``
* ``Don't use database as fallback when no schema parsed. (#32959)``

Misc
~~~~

* ``openlineage, bigquery: add openlineage method support for BigQueryExecuteQueryOperator (#31293)``
* ``Move openlineage configuration to provider (#33124)``

1.0.0
.....

Initial version of the provider.
