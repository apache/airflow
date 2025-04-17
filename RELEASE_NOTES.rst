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

.. contents:: Apache Airflow Releases
   :local:
   :depth: 1

.. towncrier release notes start

Airflow 3.0.0 (2025-04-22)
--------------------------
We are proud to announce the General Availability of **Apache Airflow® 3.0**, the most significant release in the
project's history. Airflow 3.0 builds on the foundation of Airflow 2 and introduces a new service-oriented architecture,
a modern React-based UI, enhanced security, and a host of long-requested features such as DAG versioning, improved
backfills, event-driven scheduling, and support for remote execution.

Highlights
^^^^^^^^^^

Major Architectural Advancements
""""""""""""""""""""""""""""""""

- **Task Execution API & Task SDK (AIP-72)**: Airflow 3.0 introduces a service-oriented architecture and a new API Server, enabling tasks to run anywhere, in any language, with improved isolation and security.
- **Edge Executor (AIP-69)**: A new experimental executor that enables edge compute patterns and event-driven orchestration scenarios.
- **Split CLI (AIP-81)**: The core CLI is now divided between local and remote functionality, with a new provider package (``airflowctl``) for API-based remote interactions.

UI Overhaul
"""""""""""

- **Modern React UI (AIP-38, AIP-84)**: Airflow's UI has been completely rewritten using React and FastAPI. This new UI supports a better UX across Grid, Graph, and Asset views.
- **DAG Versioning (AIP-65, AIP-66)**: DAG structure changes are now tracked natively. Users can inspect DAG history directly from the UI.

Expanded Scheduling and Execution
"""""""""""""""""""""""""""""""""

- **Data Assets & Asset-Driven DAGs (AIP-74, AIP-75)**: Data-aware scheduling has evolved into first-class Asset support, including a new ``@asset`` decorator syntax and asset-based execution.
- **External Event Scheduling (AIP-82)**: DAGs can now be triggered from external events via a pluggable message bus interface. Initial support for AWS SQS is included.
- **Scheduler-Managed Backfills (AIP-78)**: Backfills are now managed by the scheduler, with UI support and enhanced diagnostics.

ML & AI Use Cases
"""""""""""""""""

- **Support for Non-Data-Interval DAGs (AIP-83)**: Enables inference DAGs and hyperparameter tuning runs by removing the uniqueness constraint on execution dates.

Breaking Changes
^^^^^^^^^^^^^^^^

See the :doc:`Upgrade Guide <installation/upgrading_to_airflow3>` for a full list of changes and migration recommendations. Major breaking changes include:

Metadata Database Access
""""""""""""""""""""""""

- Direct access to the metadata DB from task code is no longer supported. Use the :doc:`REST API <stable-rest-api-ref>` instead.

Scheduling Changes
""""""""""""""""""

- New default: ``schedule=None``, ``catchup=False``
- ``schedule_interval`` and ``timetable`` are removed; use ``schedule`` exclusively.
- Raw cron strings now use ``CronTriggerTimetable`` instead of ``CronDataIntervalTimetable``.

Context and Parameters
""""""""""""""""""""""

- Several context variables removed (``conf``, ``execution_date``, ``dag_run.external_trigger``, etc)
- ``fail_stop`` renamed to ``fail_fast``
- ``.airflowignore`` now uses glob syntax by default

Deprecated and Removed Features
"""""""""""""""""""""""""""""""

+-----------------------------+----------------------------------------------------------+
| **Feature**                 | **Replacement**                                          |
+=============================+==========================================================+
| SubDAGs                     | Task Groups                                              |
+-----------------------------+----------------------------------------------------------+
| SLAs                        | Deadline Alerts (planned post-3.0)                       |
+-----------------------------+----------------------------------------------------------+
| ``EmailOperator`` (core)    | SMTP provider's ``EmailOperator``                        |
+-----------------------------+----------------------------------------------------------+
| ``dummy`` trigger rule      | ``always``                                               |
+-----------------------------+----------------------------------------------------------+
| ``none_failed_or_skipped`` | ``none_failed_min_one_success``                           |
+-----------------------------+----------------------------------------------------------+
| XCom pickling               | Use a custom XCom backend                                |
+-----------------------------+----------------------------------------------------------+

Upgrade Process
^^^^^^^^^^^^^^^

Airflow 3 was designed with migration in mind. Many Airflow 2 DAGs will work without changes. Use these tools:

- ``ruff check --preview --select AIR30 --fix``: Flag and auto-fix DAG-level changes
- ``airflow config lint``: Identify outdated or removed config options

**Minimum version required to upgrade**: Airflow 2.7

We recommend upgrading to the latest Airflow 2.10.x release before migrating to Airflow 3.0 to benefit from deprecation warnings. Check :doc:`Upgrade Guide <installation/upgrading_to_airflow3>` for more details.

Resources
^^^^^^^^^

- :doc:`Upgrade Guide <installation/upgrading_to_airflow3>`
- `Airflow AIPs <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals>`_

Airflow 2.10.5 (2025-02-10)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Ensure teardown tasks are executed when DAG run is set to failed (#45530)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously when a DAG run was manually set to "failed" or to "success" state the terminal state was set to all tasks.
But this was a gap for cases when setup- and teardown tasks were defined: If teardown was used to clean-up infrastructure
or other resources, they were also skipped and thus resources could stay allocated.

As of now when setup tasks had been executed before and the DAG is manually set to "failed" or "success" then teardown
tasks are executed. Teardown tasks are skipped if the setup was also skipped.

As a side effect this means if the DAG contains teardown tasks, then the manual marking of DAG as "failed" or "success"
will need to keep the DAG in running state to ensure that teardown tasks will be scheduled. They would not be scheduled
if the DAG is directly set to "failed" or "success".


Bug Fixes
"""""""""

- Prevent using ``trigger_rule=TriggerRule.ALWAYS`` in a task-generated mapping within bare tasks (#44751)
- Fix ShortCircuitOperator mapped tasks (#44912)
- Fix premature evaluation of tasks with certain trigger rules (e.g. ``ONE_DONE``) in a mapped task group (#44937)
- Fix task_id validation in BaseOperator (#44938) (#44938)
- Allow fetching XCom with forward slash from the API and escape it in the UI (#45134)
- Fix ``FileTaskHandler`` only read from default executor (#46000)
- Fix empty task instance for log (#45702) (#45703)
- Remove ``skip_if`` and ``run_if`` decorators before TaskFlow virtualenv tasks are run (#41832) (#45680)
- Fix request body for json requests in event log (#45546) (#45560)
- Ensure teardown tasks are executed when DAG run is set to failed (#45530) (#45581)
- Do not update DR on TI update after task execution (#45348)
- Fix object and array DAG params that have a None default (#45313) (#45315)
- Fix endless sensor rescheduling (#45224) (#45250)
- Evaluate None in SQLAlchemy's extended JSON type decorator (#45119) (#45120)
- Allow dynamic tasks to be filtered by ``rendered_map_index`` (#45109) (#45122)
- Handle relative paths when sanitizing URLs (#41995) (#45080)
- Set Autocomplete Off on Login Form (#44929) (#44940)
- Add Webserver parameters ``max_form_parts``, ``max_form_memory_size`` (#46243) (#45749)
- Fixed accessing thread local variable in BaseOperators ``execute`` safeguard mechanism (#44646) (#46280)
- Add map_index parameter to extra links API (#46337)


Miscellaneous
"""""""""""""

- Add traceback log output when SIGTERMs was sent (#44880) (#45077)
- Removed the ability for Operators to specify their own "scheduling deps" (#45713) (#45742)
- Deprecate ``conf`` from Task Context (#44993)


Airflow 2.10.4 (2024-12-16)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

TaskInstance ``priority_weight`` is capped in 32-bit signed integer ranges (#43611)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Some database engines are limited to 32-bit integer values. As some users reported errors in
weight rolled-over to negative values, we decided to cap the value to the 32-bit integer. Even
if internally in python smaller or larger values to 64 bit are supported, ``priority_weight`` is
capped and only storing values from -2147483648 to 2147483647.

Bug Fixes
^^^^^^^^^

- Fix stats of dynamic mapped tasks after automatic retries of failed tasks (#44300)
- Fix wrong display of multi-line messages in the log after filtering (#44457)
- Allow "/" in metrics validator (#42934) (#44515)
- Fix gantt flickering (#44488) (#44517)
- Fix problem with inability to remove fields from Connection form (#40421) (#44442)
- Check pool_slots on partial task import instead of execution (#39724) (#42693)
- Avoid grouping task instance stats by try_number for dynamic mapped tasks (#44300) (#44319)
- Re-queue task when they are stuck in queued (#43520) (#44158)
- Suppress the warnings where we check for sensitive values (#44148) (#44167)
- Fix get_task_instance_try_details to return appropriate schema (#43830) (#44133)
- Log message source details are grouped (#43681) (#44070)
- Fix duplication of Task tries in the UI (#43891) (#43950)
- Add correct mime-type in OpenAPI spec (#43879) (#43901)
- Disable extra links button if link is null or empty (#43844) (#43851)
- Disable XCom list ordering by execution_date (#43680) (#43696)
- Fix venv numpy example which needs to be 1.26 at least to be working in Python 3.12 (#43659)
- Fix Try Selector in Mapped Tasks also on Index 0 (#43590) (#43591)
- Prevent using ``trigger_rule="always"`` in a dynamic mapped task (#43810)
- Prevent using ``trigger_rule=TriggerRule.ALWAYS`` in a task-generated mapping within bare tasks (#44751)

Doc Only Changes
""""""""""""""""
- Update XCom docs around containers/helm (#44570) (#44573)

Miscellaneous
"""""""""""""
- Raise deprecation warning when accessing inlet or outlet events through str (#43922)


Airflow 2.10.3 (2024-11-05)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""
- Improves the handling of value masking when setting Airflow variables for enhanced security.  (#43123) (#43278)
- Adds support for task_instance_mutation_hook to handle mapped operators with index 0. (#42661) (#43089)
- Fixes executor cleanup to properly handle zombie tasks when task instances are terminated. (#43065)
- Adds retry logic for HTTP 502 and 504 errors in internal API calls to handle webserver startup issues. (#42994) (#43044)
- Restores the use of separate sessions for writing and deleting RTIF data to prevent StaleDataError. (#42928) (#43012)
- Fixes PythonOperator error by replacing hyphens with underscores in DAG names. (#42993)
- Improving validation of task retries to handle None values (#42532) (#42915)
- Fixes error handling in dataset managers when resolving dataset aliases into new datasets (#42733)
- Enables clicking on task names in the DAG Graph View to correctly select the corresponding task. (#38782) (#42697)
- Prevent redirect loop on /home with tags/last run filters (#42607) (#42609) (#42628)
- Support of host.name in OTEL metrics and usage of OTEL_RESOURCE_ATTRIBUTES in metrics (#42428) (#42604)
- Reduce eyestrain in dark mode with reduced contrast and saturation (#42567) (#42583)
- Handle ENTER key correctly in trigger form and allow manual JSON (#42525) (#42535)
- Ensure DAG trigger form submits with updated parameters upon keyboard submit (#42487) (#42499)
- Do not attempt to provide not ``stringified`` objects to UI via xcom if pickling is active (#42388) (#42486)
- Fix the span link of task instance to point to the correct span in the scheduler_job_loop (#42430) (#42480)
- Bugfix task execution from runner in Windows (#42426) (#42478)
- Allows overriding the hardcoded OTEL_SERVICE_NAME with an environment variable (#42242) (#42441)
- Improves trigger performance by using ``selectinload`` instead of ``joinedload`` (#40487) (#42351)
- Suppress warnings when masking sensitive configs (#43335) (#43337)
- Masking configuration values irrelevant to DAG author (#43040) (#43336)
- Execute templated bash script as file in BashOperator (#43191)
- Fixes schedule_downstream_tasks to include upstream tasks for one_success trigger rule (#42582) (#43299)
- Add retry logic in the scheduler for updating trigger timeouts in case of deadlocks. (#41429) (#42651)
- Mark all tasks as skipped when failing a dag_run manually (#43572)
- Fix ``TrySelector`` for Mapped Tasks in Logs and Details Grid Panel (#43566)
- Conditionally add OTEL events when processing executor events (#43558) (#43567)
- Fix broken stat ``scheduler_loop_duration`` (#42886) (#43544)
- Ensure total_entries in /api/v1/dags (#43377) (#43429)
- Include limit and offset in request body schema for List task instances (batch) endpoint (#43479)
- Don't raise a warning in ExecutorSafeguard when execute is called from an extended operator (#42849) (#43577)

Miscellaneous
"""""""""""""
- Deprecate session auth backend (#42911)
- Removed unicodecsv dependency for providers with Airflow version 2.8.0 and above (#42765) (#42970)
- Remove the referrer from Webserver to Scarf (#42901) (#42942)
- Bump ``dompurify`` from 2.2.9 to 2.5.6 in /airflow/www (#42263) (#42270)
- Correct docstring format in _get_template_context (#42244) (#42272)
- Backport: Bump Flask-AppBuilder to ``4.5.2`` (#43309) (#43318)
- Check python version that was used to install pre-commit venvs (#43282) (#43310)
- Resolve warning in Dataset Alias migration (#43425)

Doc Only Changes
""""""""""""""""
- Clarifying PLUGINS_FOLDER permissions by DAG authors (#43022) (#43029)
- Add templating info to TaskFlow tutorial (#42992)
- Airflow local settings no longer importable from dags folder (#42231) (#42603)
- Fix documentation for cpu and memory usage (#42147) (#42256)
- Fix instruction for docker compose (#43119) (#43321)
- Updates documentation to reflect that dag_warnings is returned instead of import_errors. (#42858) (#42888)


Airflow 2.10.2 (2024-09-18)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""
- Revert "Fix: DAGs are not marked as stale if the dags folder change" (#42220, #42217)
- Add missing open telemetry span and correct scheduled slots documentation (#41985)
- Fix require_confirmation_dag_change (#42063) (#42211)
- Only treat null/undefined as falsy when rendering XComEntry (#42199) (#42213)
- Add extra and ``renderedTemplates`` as keys to skip ``camelCasing`` (#42206) (#42208)
- Do not ``camelcase`` xcom entries (#42182) (#42187)
- Fix task_instance and dag_run links from list views (#42138) (#42143)
- Support multi-line input for Params of type string in trigger UI form (#40414) (#42139)
- Fix details tab log url detection (#42104) (#42114)
- Add new type of exception to catch timeout (#42064) (#42078)
- Rewrite how DAG to dataset / dataset alias are stored (#41987) (#42055)
- Allow dataset alias to add more than one dataset events (#42189) (#42247)

Miscellaneous
"""""""""""""
- Limit universal-pathlib below ``0.2.4`` as it breaks our integration (#42101)
- Auto-fix default deferrable with ``LibCST`` (#42089)
- Deprecate ``--tree`` flag for ``tasks list`` cli command (#41965)

Doc Only Changes
""""""""""""""""
- Update ``security_model.rst`` to clear unauthenticated endpoints exceptions (#42085)
- Add note about dataclasses and attrs to XComs page (#42056)
- Improve docs on markdown docs in DAGs (#42013)
- Add warning that listeners can be dangerous (#41968)


Airflow 2.10.1 (2024-09-05)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""
- Handle Example dags case when checking for missing files (#41874)
- Fix logout link in "no roles" error page (#41845)
- Set end_date and duration for triggers completed with end_from_trigger as True. (#41834)
- DAGs are not marked as stale if the dags folder change (#41829)
- Fix compatibility with FAB provider versions <1.3.0 (#41809)
- Don't Fail LocalTaskJob on heartbeat (#41810)
- Remove deprecation warning for cgitb in Plugins Manager (#41793)
- Fix log for notifier(instance) without ``__name__`` (#41699)
- Splitting syspath preparation into stages (#41694)
- Adding url sanitization for extra links (#41680)
- Fix InletEventsAccessors type stub (#41607)
- Fix UI rendering when XCom is INT, FLOAT, BOOL or NULL (#41605)
- Fix try selector refresh (#41503)
- Incorrect try number subtraction producing invalid span id for OTEL airflow (#41535)
- Add WebEncoder for trigger page rendering to avoid render failure (#41485)
- Adding ``tojson`` filter to example_inlet_event_extra example dag (#41890)
- Add backward compatibility check for executors that don't inherit BaseExecutor (#41927)

Miscellaneous
"""""""""""""
- Bump webpack from 5.76.0 to 5.94.0 in /airflow/www (#41879)
- Adding rel property to hyperlinks in logs (#41783)
- Field Deletion Warning when editing Connections (#41504)
- Make Scarf usage reporting in major+minor versions and counters in buckets (#41900)
- Lower down universal-pathlib minimum to 0.2.2 (#41943)
- Protect against None components of universal pathlib xcom backend (#41938)

Doc Only Changes
""""""""""""""""
- Remove Debian bullseye support (#41569)
- Add an example for auth with ``keycloak`` (#41791)


Airflow 2.10.0 (2024-08-15)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Scarf based telemetry: Airflow now collect telemetry data (#39510)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Airflow integrates Scarf to collect basic usage data during operation. Deployments can opt-out of data collection by
setting the ``[usage_data_collection]enabled`` option to ``False``, or the ``SCARF_ANALYTICS=false`` environment variable.

Datasets no longer trigger inactive DAGs (#38891)
"""""""""""""""""""""""""""""""""""""""""""""""""

Previously, when a DAG is paused or removed, incoming dataset events would still
trigger it, and the DAG would run when it is unpaused or added back in a DAG
file. This has been changed; a DAG's dataset schedule can now only be satisfied
by events that occur when the DAG is active. While this is a breaking change,
the previous behavior is considered a bug.

The behavior of time-based scheduling is unchanged, including the timetable part
of ``DatasetOrTimeSchedule``.

``try_number`` is no longer incremented during task execution (#39336)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously, the try number (``try_number``) was incremented at the beginning of task execution on the worker. This was problematic for many reasons.
For one it meant that the try number was incremented when it was not supposed to, namely when resuming from reschedule or deferral. And it also resulted in
the try number being "wrong" when the task had not yet started. The workarounds for these two issues caused a lot of confusion.

Now, instead, the try number for a task run is determined at the time the task is scheduled, and does not change in flight, and it is never decremented.
So after the task runs, the observed try number remains the same as it was when the task was running; only when there is a "new try" will the try number be incremented again.

One consequence of this change is, if users were "manually" running tasks (e.g. by calling ``ti.run()`` directly, or command line ``airflow tasks run``),
try number will no longer be incremented. Airflow assumes that tasks are always run after being scheduled by the scheduler, so we do not regard this as a breaking change.

``/logout`` endpoint in FAB Auth Manager is now CSRF protected (#40145)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``/logout`` endpoint's method in FAB Auth Manager has been changed from ``GET`` to ``POST`` in all existing
AuthViews (``AuthDBView``, ``AuthLDAPView``, ``AuthOAuthView``, ``AuthOIDView``, ``AuthRemoteUserView``), and
now includes CSRF protection to enhance security and prevent unauthorized logouts.

OpenTelemetry Traces for Apache Airflow (#37948).
"""""""""""""""""""""""""""""""""""""""""""""""""
This new feature adds capability for Apache Airflow to emit 1) airflow system traces of scheduler,
triggerer, executor, processor 2) DAG run traces for deployed DAG runs in OpenTelemetry format. Previously, only metrics were supported which emitted metrics in OpenTelemetry.
This new feature will add richer data for users to use OpenTelemetry standard to emit and send their trace data to OTLP compatible endpoints.

Decorator for Task Flow ``(@skip_if, @run_if)`` to make it simple to apply whether or not to skip a Task. (#41116)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
This feature adds a decorator to make it simple to skip a Task.

Using Multiple Executors Concurrently (#40701)
""""""""""""""""""""""""""""""""""""""""""""""
Previously known as hybrid executors, this new feature allows Airflow to use multiple executors concurrently. DAGs, or even individual tasks, can be configured
to use a specific executor that suits its needs best. A single DAG can contain tasks all using different executors. Please see the Airflow documentation for
more details. Note: This feature is still experimental. See `documentation on Executor <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html#using-multiple-executors-concurrently>`_ for a more detailed description.

New Features
""""""""""""
- AIP-61 Hybrid Execution (`AIP-61 <https://github.com/apache/airflow/pulls?q=is%3Apr+label%3Aarea%3Ahybrid-executors+is%3Aclosed+milestone%3A%22Airflow+2.10.0%22>`_)
- AIP-62 Getting Lineage from Hook Instrumentation (`AIP-62 <https://github.com/apache/airflow/pulls?q=is%3Apr+is%3Amerged+label%3AAIP-62+milestone%3A%22Airflow+2.10.0%22>`_)
- AIP-64 TaskInstance Try History (`AIP-64 <https://github.com/apache/airflow/pulls?q=is%3Apr+is%3Amerged+label%3AAIP-64+milestone%3A%22Airflow+2.10.0%22>`_)
- AIP-44 Internal API (`AIP-44 <https://github.com/apache/airflow/pulls?q=is%3Apr+label%3AAIP-44+milestone%3A%22Airflow+2.10.0%22+is%3Aclosed>`_)
- Enable ending the task directly from the triggerer without going into the worker. (#40084)
- Extend dataset dependencies (#40868)
- Feature/add token authentication to internal api (#40899)
- Add DatasetAlias to support dynamic Dataset Event Emission and Dataset Creation (#40478)
- Add example DAGs for inlet_events (#39893)
- Implement ``accessors`` to read dataset events defined as inlet (#39367)
- Decorator for Task Flow, to make it simple to apply whether or not to skip a Task. (#41116)
- Add start execution from triggerer support to dynamic task mapping (#39912)
- Add try_number to log table (#40739)
- Added ds_format_locale method in macros which allows localizing datetime formatting using Babel (#40746)
- Add DatasetAlias to support dynamic Dataset Event Emission and Dataset Creation (#40478, #40723, #40809, #41264, #40830, #40693, #41302)
- Use sentinel to mark dag as removed on re-serialization (#39825)
- Add parameter for the last number of queries to the DB in DAG file processing stats (#40323)
- Add prototype version dark mode for Airflow UI (#39355)
- Add ability to mark some tasks as successful in ``dag test``  (#40010)
- Allow use of callable for template_fields (#37028)
- Filter running/failed and active/paused dags on the home page(#39701)
- Add metrics about task CPU and memory usage (#39650)
- UI changes for DAG Re-parsing feature (#39636)
- Add Scarf based telemetry (#39510, #41318)
- Add dag re-parsing request endpoint (#39138)
- Redirect to new DAGRun after trigger from Grid view (#39569)
- Display ``endDate`` in task instance tooltip. (#39547)
- Implement ``accessors`` to read dataset events defined as inlet (#39367, #39893)
- Add color to log lines in UI for error and warnings based on keywords (#39006)
- Add Rendered k8s pod spec tab to ti details view (#39141)
- Make audit log before/after filterable (#39120)
- Consolidate grid collapse actions to a single full screen toggle (#39070)
- Implement Metadata to emit runtime extra (#38650)
- Add executor field to the DB and parameter to the operators (#38474)
- Implement context accessor for DatasetEvent extra (#38481)
- Add dataset event info to dag graph (#41012)
- Add button to toggle datasets on/off in dag graph (#41200)
- Add ``run_if`` & ``skip_if`` decorators (#41116)
- Add dag_stats rest api endpoint (#41017)
- Add listeners for Dag import errors (#39739)
- Allowing DateTimeSensorAsync, FileSensor and TimeSensorAsync to start execution from trigger during dynamic task mapping (#41182)


Improvements
""""""""""""
- Allow set Dag Run resource into Dag Level permission: extends Dag's access_control feature to allow Dag Run resource permissions. (#40703)
- Improve security and error handling for the internal API (#40999)
- Datasets UI Improvements (#40871)
- Change DAG Audit log tab to Event Log (#40967)
- Make standalone dag file processor works in DB isolation mode (#40916)
- Show only the source on the consumer DAG page and only triggered DAG run in the producer DAG page (#41300)
- Update metrics names to allow multiple executors to report metrics (#40778)
- Format DAG run count (#39684)
- Update styles for ``renderedjson`` component (#40964)
- Improve ATTRIBUTE_REMOVED sentinel to use class and more context (#40920)
- Make XCom display as react json (#40640)
- Replace usages of task context logger with the log table (#40867)
- Rollback for all retry exceptions (#40882) (#40883)
- Support rendering ObjectStoragePath value (#40638)
- Add try_number and map_index as params for log event endpoint (#40845)
- Rotate fernet key in batches to limit memory usage (#40786)
- Add gauge metric for 'last_num_of_db_queries' parameter (#40833)
- Set parallelism log messages to warning level for better visibility (#39298)
- Add error handling for encoding the dag runs (#40222)
- Use params instead of dag_run.conf in example DAG (#40759)
- Load Example Plugins with Example DAGs (#39999)
- Stop deferring TimeDeltaSensorAsync task when the target_dttm is in the past (#40719)
- Send important executor logs to task logs (#40468)
- Open external links in new tabs (#40635)
- Attempt to add ReactJSON view to rendered templates (#40639)
- Speeding up regex match time for custom warnings (#40513)
- Refactor DAG.dataset_triggers into the timetable class (#39321)
- add next_kwargs to StartTriggerArgs (#40376)
- Improve UI error handling (#40350)
- Remove double warning in CLI  when config value is deprecated (#40319)
- Implement XComArg concat() (#40172)
- Added ``get_extra_dejson`` method with nested parameter which allows you to specify if you want the nested json as string to be also deserialized (#39811)
- Add executor field to the task instance API (#40034)
- Support checking for db path absoluteness on Windows (#40069)
- Introduce StartTriggerArgs and prevent start trigger initialization in scheduler (#39585)
- Add task documentation to details tab in grid view (#39899)
- Allow executors to be specified with only the class name of the Executor (#40131)
- Remove obsolete conditional logic related to try_number (#40104)
- Allow Task Group Ids to be passed as branches in BranchMixIn (#38883)
- Javascript connection form will apply CodeMirror to all textarea's dynamically (#39812)
- Determine needs_expansion at time of serialization (#39604)
- Add indexes on dag_id column in referencing tables to speed up deletion of dag records (#39638)
- Add task failed dependencies to details page (#38449)
- Remove webserver try_number adjustment (#39623)
- Implement slicing in lazy sequence (#39483)
- Unify lazy db sequence implementations (#39426)
- Add ``__getattr__`` to task decorator stub (#39425)
- Allow passing labels to FAB Views registered via Plugins (#39444)
- Simpler error message when trying to offline migrate with sqlite (#39441)
- Add soft_fail to TriggerDagRunOperator (#39173)
- Rename "dataset event" in context to use "outlet" (#39397)
- Resolve ``RemovedIn20Warning`` in ``airflow task`` command (#39244)
- Determine fail_stop on client side when db isolated (#39258)
- Refactor cloudpickle support in Python operators/decorators (#39270)
- Update trigger kwargs migration to specify existing_nullable (#39361)
- Allowing tasks to start execution directly from triggerer without going to worker (#38674)
- Better ``db migrate`` error messages (#39268)
- Add stacklevel into the ``suppress_and_warn`` warning (#39263)
- Support searching by dag_display_name (#39008)
- Allow sort by on all fields in MappedInstances.tsx (#38090)
- Expose count of scheduled tasks in metrics (#38899)
- Use ``declarative_base`` from ``sqlalchemy.orm`` instead of ``sqlalchemy.ext.declarative`` (#39134)
- Add example DAG to demonstrate emitting approaches (#38821)
- Give ``on_task_instance_failed`` access to the error that caused the failure (#38155)
- Simplify dataset serialization (#38694)
- Add heartbeat recovery message to jobs (#34457)
- Remove select_column option in TaskInstance.get_task_instance (#38571)
- Don't create session in get_dag if not reading dags from database (#38553)
- Add a migration script for encrypted trigger kwargs (#38358)
- Implement render_templates on TaskInstancePydantic (#38559)
- Handle optional session in _refresh_from_db (#38572)
- Make type annotation less confusing in task_command.py (#38561)
- Use fetch_dagrun directly to avoid session creation (#38557)
- Added ``output_processor`` parameter to ``BashProcessor`` (#40843)
- Improve serialization for Database Isolation Mode (#41239)
- Only orphan non-orphaned Datasets (#40806)
- Adjust gantt width based on task history dates (#41192)
- Enable scrolling on legend with high number of elements. (#41187)

Bug Fixes
"""""""""
- Bugfix for get_parsing_context() when ran with LocalExecutor (#40738)
- Validating provider documentation urls before displaying in views (#40933)
- Move import to make PythonOperator working on Windows (#40424)
- Fix dataset_with_extra_from_classic_operator example DAG (#40747)
- Call listener on_task_instance_failed() after ti state is changed (#41053)
- Add ``never_fail`` in BaseSensor (#40915)
- Fix tasks API endpoint when DAG doesn't have ``start_date`` (#40878)
- Fix and adjust URL generation for UI grid and older runs (#40764)
- Rotate fernet key optimization (#40758)
- Fix class instance vs. class type in validate_database_executor_compatibility() call (#40626)
- Clean up dark mode (#40466)
- Validate expected types for args for DAG, BaseOperator and TaskGroup (#40269)
- Exponential Backoff Not Functioning in BaseSensorOperator Reschedule Mode (#39823)
- local task job: add timeout, to not kill on_task_instance_success listener prematurely (#39890)
- Move Post Execution Log Grouping behind Exception Print (#40146)
- Fix triggerer race condition in HA setting (#38666)
- Pass triggered or existing DAG Run logical date to DagStateTrigger (#39960)
- Passing ``external_task_group_id`` to ``WorkflowTrigger`` (#39617)
- ECS Executor: Set tasks to RUNNING state once active (#39212)
- Only heartbeat if necessary in backfill loop (#39399)
- Fix trigger kwarg encryption migration (#39246)
- Fix decryption of trigger kwargs when downgrading. (#38743)
- Fix wrong link in TriggeredDagRuns (#41166)
- Pass MapIndex to LogLink component for external log systems (#41125)
- Add NonCachingRotatingFileHandler for worker task (#41064)
- Add argument include_xcom in method resolve an optional value (#41062)
- Sanitizing file names in example_bash_decorator DAG (#40949)
- Show dataset aliases in dependency graphs (#41128)
- Render Dataset Conditions in DAG Graph view (#41137)
- Add task duration plot across dagruns (#40755)
- Add start execution from trigger support for existing core sensors (#41021)
- add example dag for dataset_alias (#41037)
- Add dataset alias unique constraint and remove wrong dataset alias removing logic (#41097)
- Set "has_outlet_datasets" to true if "dataset alias" exists (#41091)
- Make HookLineageCollector group datasets by (#41034)
- Enhance start_trigger_args serialization (#40993)
- Refactor ``BaseSensorOperator`` introduce ``skip_policy`` parameter (#40924)
- Fix viewing logs from triggerer when task is deferred (#41272)
- Refactor how triggered dag run url is replaced (#41259)
- Added support for additional sql alchemy session args (#41048)
- Allow empty list in TriggerDagRun failed_state (#41249)
- Clean up the exception handler when run_as_user is the airflow user  (#41241)
- Collapse docs when click and folded (#41214)
- Update updated_at when saving to db as session.merge does not trigger on-update (#40782)
- Fix query count statistics when parsing DAF file (#41149)
- Method Resolution Order in operators without ``__init__`` (#41086)
- Ensure try_number incremented for empty operator (#40426)

Miscellaneous
"""""""""""""
- Remove the Experimental flag from ``OTel`` Traces (#40874)
- Bump packaging version to 23.0 in order to fix issue with older otel (#40865)
- Simplify _auth_manager_is_authorized_map function (#40803)
- Use correct unknown executor exception in scheduler job (#40700)
- Add D1 ``pydocstyle`` rules to pyproject.toml (#40569)
- Enable enforcing ``pydocstyle`` rule D213 in ruff. (#40448, #40464)
- Update ``Dag.test()`` to run with an executor if desired (#40205)
- Update jest and babel minor versions (#40203)
- Refactor BashOperator and Bash decorator for consistency and simplicity (#39871)
- Add ``AirflowInternalRuntimeError`` for raise ``non catchable`` errors (#38778)
- ruff version bump 0.4.5 (#39849)
- Bump ``pytest`` to 8.0+ (#39450)
- Remove stale comment about TI index (#39470)
- Configure ``back_populates`` between ``DagScheduleDatasetReference.dag`` and ``DagModel.schedule_dataset_references`` (#39392)
- Remove deprecation warnings in endpoints.py (#39389)
- Fix SQLA deprecations in Airflow core (#39211)
- Use class-bound attribute directly in SA (#39198, #39195)
- Fix stacklevel for TaskContextLogger (#39142)
- Capture warnings during collect DAGs (#39109)
- Resolve ``B028`` (no-explicit-stacklevel) in core (#39123)
- Rename model ``ImportError`` to ``ParseImportError`` for avoid shadowing with builtin exception (#39116)
- Add option to support cloudpickle in PythonVenv/External Operator (#38531)
- Suppress ``SubDagOperator`` examples warnings (#39057)
- Add log for running callback (#38892)
- Use ``model_dump`` instead of ``dict`` for serialize Pydantic V2 model (#38933)
- Widen cheat sheet column to avoid wrapping commands (#38888)
- Update ``hatchling`` to latest version (1.22.5) (#38780)
- bump uv to 0.1.29 (#38758)
- Add missing serializations found during provider tests fixing (#41252)
- Bump ``ws`` from 7.5.5 to 7.5.10 in /airflow/www (#40288)
- Improve typing for allowed/failed_states in TriggerDagRunOperator (#39855)

Doc Only Changes
""""""""""""""""
- Add ``filesystems`` and ``dataset-uris`` to "how to create your own provider" page (#40801)
- Fix (TM) to (R) in Airflow repository (#40783)
- Set ``otel_on`` to True in example airflow.cfg (#40712)
- Add warning for _AIRFLOW_PATCH_GEVENT  (#40677)
- Update multi-team diagram proposal after Airflow 3 discussions (#40671)
- Add stronger warning that MSSQL is not supported and no longer functional (#40565)
- Fix misleading mac menu structure in howto (#40440)
- Update k8s supported version in docs (#39878)
- Add compatibility note for Listeners (#39544)
- Update edge label image in documentation example with the new graph view (#38802)
- Update UI doc screenshots (#38680)
- Add section "Manipulating queued dataset events through REST API" (#41022)
- Add information about lack of security guarantees for docker compose (#41072)
- Add links to example dags in use params section (#41031)
- Change ``task_id`` from ``send_email`` to ``send_email_notification`` in ``taskflow.rst`` (#41060)
- Remove unnecessary nginx redirect rule from reverse proxy documentation (#38953)



Airflow 2.9.3 (2024-07-15)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Time unit for ``scheduled_duration`` and ``queued_duration`` changed (#37936)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``scheduled_duration`` and ``queued_duration`` metrics are now emitted in milliseconds instead of seconds.

By convention all statsd metrics should be emitted in milliseconds, this is later expected in e.g. ``prometheus`` statsd-exporter.


Support for OpenTelemetry Metrics is no longer "Experimental" (#40286)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Experimental support for OpenTelemetry was added in 2.7.0 since then fixes and improvements were added and now we announce the feature as stable.



Bug Fixes
"""""""""
- Fix calendar view scroll (#40458)
- Validating provider description for urls in provider list view (#40475)
- Fix compatibility with old MySQL 8.0 (#40314)
- Fix dag (un)pausing won't work on environment where dag files are missing (#40345)
- Extra being passed to SQLalchemy (#40391)
- Handle unsupported operand int + str when value of tag is int (job_id) (#40407)
- Fix TriggeredDagRunOperator triggered link (#40336)
- Add ``[webserver]update_fab_perms`` to deprecated configs (#40317)
- Swap dag run link from legacy graph to grid with graph tab (#40241)
- Change ``httpx`` to ``requests`` in ``file_task_handler`` (#39799)
- Fix import future annotations in venv jinja template (#40208)
- Ensures DAG params order regardless of backend (#40156)
- Use a join for TI notes in TI batch API endpoint (#40028)
- Improve trigger UI for string array format validation (#39993)
- Disable jinja2 rendering for doc_md (#40522)
- Skip checking sub dags list if taskinstance state is skipped (#40578)
- Recognize quotes when parsing urls in logs (#40508)

Doc Only Changes
""""""""""""""""
- Add notes about passing secrets via environment variables (#40519)
- Revamp some confusing log messages (#40334)
- Add more precise description of masking sensitive field names (#40512)
- Add slightly more detailed guidance about upgrading to the docs (#40227)
- Metrics allow_list complete example (#40120)
- Add warning to deprecated api docs that access control isn't applied (#40129)
- Simpler command to check local scheduler is alive (#40074)
- Add a note and an example clarifying the usage of DAG-level params (#40541)
- Fix highlight of example code in dags.rst (#40114)
- Add warning about the PostgresOperator being deprecated (#40662)
- Updating airflow download links to CDN based links (#40618)
- Fix import statement for DatasetOrTimetable example (#40601)
- Further clarify triage process (#40536)
- Fix param order in PythonOperator docstring (#40122)
- Update serializers.rst to mention that bytes are not supported (#40597)

Miscellaneous
"""""""""""""
- Upgrade build installers and dependencies (#40177)
- Bump braces from 3.0.2 to 3.0.3 in /airflow/www (#40180)
- Upgrade to another version of trove-classifier (new CUDA classifiers) (#40564)
- Rename "try_number" increments that are unrelated to the airflow concept (#39317)
- Update trove classifiers to the latest version as build dependency (#40542)
- Upgrade to latest version of ``hatchling`` as build dependency (#40387)
- Fix bug in ``SchedulerJobRunner._process_executor_events`` (#40563)
- Remove logging for "blocked" events (#40446)



Airflow 2.9.2 (2024-06-10)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""
- Fix bug that makes ``AirflowSecurityManagerV2`` leave transactions in the ``idle in transaction`` state (#39935)
- Fix alembic auto-generation and rename mismatching constraints (#39032)
- Add the existing_nullable to the downgrade side of the migration (#39374)
- Fix Mark Instance state buttons stay disabled if user lacks permission (#37451). (#38732)
- Use SKIP LOCKED instead of NOWAIT in mini scheduler (#39745)
- Remove DAG Run Add option from FAB view (#39881)
- Add max_consecutive_failed_dag_runs in API spec (#39830)
- Fix example_branch_operator failing in python 3.12 (#39783)
- Fetch served logs also when task attempt is up for retry and no remote logs available (#39496)
- Change dataset URI validation to raise warning instead of error in Airflow 2.9 (#39670)
- Visible DAG RUN doesn't point to the same dag run id (#38365)
- Refactor ``SafeDogStatsdLogger`` to use ``get_validator`` to enable pattern matching (#39370)
- Fix custom actions in security manager ``has_access`` (#39421)
- Fix HTTP 500 Internal Server Error if DAG is triggered with bad params (#39409)
- Fix static file caching is disabled in Airflow Webserver. (#39345)
- Fix TaskHandlerWithCustomFormatter now adds prefix only once (#38502)
- Do not provide deprecated ``execution_date`` in ``@apply_lineage`` (#39327)
- Add missing conn_id to string representation of ObjectStoragePath (#39313)
- Fix ``sql_alchemy_engine_args`` config example (#38971)
- Add Cache-Control "no-store" to all dynamically generated content (#39550)

Miscellaneous
"""""""""""""
- Limit ``yandex`` provider to avoid ``mypy`` errors (#39990)
- Warn on mini scheduler failures instead of debug (#39760)
- Change type definition for ``provider_info_cache`` decorator (#39750)
- Better typing for BaseOperator ``defer`` (#39742)
- More typing in TimeSensor and TimeSensorAsync (#39696)
- Re-raise exception from strict dataset URI checks (#39719)
- Fix stacklevel for _log_state helper (#39596)
- Resolve SA warnings in migrations scripts (#39418)
- Remove unused index ``idx_last_scheduling_decision`` on ``dag_run`` table (#39275)

Doc Only Changes
""""""""""""""""
- Provide extra tip on labeling DynamicTaskMapping (#39977)
- Improve visibility of links / variables / other configs in Configuration Reference (#39916)
- Remove 'legacy' definition for ``CronDataIntervalTimetable`` (#39780)
- Update plugins.rst examples to use pyproject.toml over setup.py (#39665)
- Fix nit in pg set-up doc (#39628)
- Add Matomo to Tracking User Activity docs (#39611)
- Fix Connection.get -> Connection. get_connection_from_secrets (#39560)
- Adding note for provider dependencies (#39512)
- Update docker-compose command (#39504)
- Update note about restarting triggerer process (#39436)
- Updating S3LogLink with an invalid bucket link (#39424)
- Update testing_packages.rst (#38996)
- Add multi-team diagrams (#38861)



Airflow 2.9.1 (2024-05-03)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Stackdriver logging bugfix requires Google provider ``10.17.0`` or later (#38071)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If you use Stackdriver logging, you must use Google provider version ``10.17.0`` or later. Airflow ``2.9.1`` now passes ``gcp_log_name`` to the ``StackdriverTaskHandler`` instead of ``name``, and this will fail on earlier provider versions.

This fixes a bug where the log name configured in ``[logging] remove_base_log_folder`` was overridden when Airflow configured logging, resulting in task logs going to the wrong destination.



Bug Fixes
"""""""""
- Make task log messages include run_id (#39280)
- Copy menu_item ``href`` for nav bar (#39282)
- Fix trigger kwarg encryption migration (#39246, #39361, #39374)
- Add workaround for datetime-local input in ``firefox`` (#39261)
- Add Grid button to Task Instance view (#39223)
- Get served logs when remote or executor logs not available for non-running task try (#39177)
- Fixed side effect of menu filtering causing disappearing menus (#39229)
- Use grid view for Task Instance's ``log_url`` (#39183)
- Improve task filtering ``UX`` (#39119)
- Improve rendered_template ``ux`` in react dag page (#39122)
- Graph view improvements (#38940)
- Check that the dataset<>task exists before trying to render graph (#39069)
- Hostname was "redacted", not "redact"; remove it when there is no context (#39037)
- Check whether ``AUTH_ROLE_PUBLIC`` is set in ``check_authentication`` (#39012)
- Move rendering of ``map_index_template`` so it renders for failed tasks as long as it was defined before the point of failure (#38902)
- ``Undeprecate`` ``BaseXCom.get_one`` method for now (#38991)
- Add ``inherit_cache`` attribute for ``CreateTableAs`` custom SA Clause (#38985)
- Don't wait for DagRun lock in mini scheduler (#38914)
- Fix calendar view with no DAG Run (#38964)
- Changed the background color of external task in graph (#38969)
- Fix dag run selection (#38941)
- Fix ``SAWarning`` 'Coercing Subquery object into a select() for use in IN()' (#38926)
- Fix implicit ``cartesian`` product in AirflowSecurityManagerV2 (#38913)
- Fix problem that links in legacy log view can not be clicked (#38882)
- Fix dag run link params (#38873)
- Use async db calls in WorkflowTrigger (#38689)
- Fix audit log events filter (#38719)
- Use ``methodtools.lru_cache`` instead of ``functools.lru_cache`` in class methods (#37757)
- Raise deprecated warning in ``airflow dags backfill`` only if ``-I`` / ``--ignore-first-depends-on-past`` provided (#38676)

Miscellaneous
"""""""""""""
- ``TriggerDagRunOperator`` deprecate ``execution_date`` in favor of ``logical_date`` (#39285)
- Force to use Airflow Deprecation warnings categories on ``@deprecated`` decorator (#39205)
- Add warning about run/import Airflow under the Windows (#39196)
- Update ``is_authorized_custom_view`` from auth manager to handle custom actions (#39167)
- Add in Trove classifiers Python 3.12 support (#39004)
- Use debug level for ``minischeduler`` skip (#38976)
- Bump ``undici`` from ``5.28.3 to 5.28.4`` in ``/airflow/www`` (#38751)


Doc Only Changes
""""""""""""""""
- Fix supported k8s version in docs (#39172)
- Dynamic task mapping ``PythonOperator`` op_kwargs (#39242)
- Add link to ``user`` and ``role`` commands (#39224)
- Add ``k8s 1.29`` to supported version in docs (#39168)
- Data aware scheduling docs edits (#38687)
- Update ``DagBag`` class docstring to include all params (#38814)
- Correcting an example taskflow example (#39015)
- Remove decorator from rendering fields example (#38827)



Airflow 2.9.0 (2024-04-08)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Following Listener API methods are considered stable and can be used for production system (were experimental feature in older Airflow versions) (#36376):
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Lifecycle events:

- ``on_starting``
- ``before_stopping``

DagRun State Change Events:

- ``on_dag_run_running``
- ``on_dag_run_success``
- ``on_dag_run_failed``

TaskInstance State Change Events:

- ``on_task_instance_running``
- ``on_task_instance_success``
- ``on_task_instance_failed``

Support for Microsoft SQL-Server for Airflow Meta Database has been removed (#36514)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

After `discussion <https://lists.apache.org/thread/r06j306hldg03g2my1pd4nyjxg78b3h4>`__
and a `voting process <https://lists.apache.org/thread/pgcgmhf6560k8jbsmz8nlyoxosvltph2>`__,
the Airflow's PMC members and Committers have reached a resolution to no longer maintain MsSQL as a
supported Database Backend.

As of Airflow 2.9.0 support of MsSQL has been removed for Airflow Database Backend.

A migration script which can help migrating the database *before* upgrading to Airflow 2.9.0 is available in
`airflow-mssql-migration repo on Github <https://github.com/apache/airflow-mssql-migration>`_.
Note that the migration script is provided without support and warranty.

This does not affect the existing provider packages (operators and hooks), DAGs can still access and process data from MsSQL.

Dataset URIs are now validated on input (#37005)
""""""""""""""""""""""""""""""""""""""""""""""""

Datasets must use a URI that conform to rules laid down in AIP-60, and the value
will be automatically normalized when the DAG file is parsed. See
`documentation on Datasets <https://airflow.apache.org/docs/apache-airflow/2.9.0/authoring-and-scheduling/datasets.html>`_ for
a more detailed description on the rules.

You may need to change your Dataset identifiers if they look like a URI, but are
used in a less mainstream way, such as relying on the URI's auth section, or
have a case-sensitive protocol name.

The method ``get_permitted_menu_items`` in ``BaseAuthManager`` has been renamed ``filter_permitted_menu_items`` (#37627)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Add REST API actions to Audit Log events (#37734)
"""""""""""""""""""""""""""""""""""""""""""""""""

The Audit Log ``event`` name for REST API events will be prepended with ``api.`` or ``ui.``, depending on if it came from the Airflow UI or externally.

Official support for Python 3.12 (#38025)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
There are a few caveats though:

* Pendulum2 does not support Python 3.12. For Python 3.12 you need to use
  `Pendulum 3 <https://pendulum.eustace.io/blog/announcing-pendulum-3-0-0.html>`_

* Minimum SQLAlchemy version supported when Pandas is installed for Python 3.12 is ``1.4.36`` released in
  April 2022. Airflow 2.9.0 increases the minimum supported version of SQLAlchemy to ``1.4.36`` for all
  Python versions.

Not all Providers support Python 3.12. At the initial release of Airflow 2.9.0 the following providers
are released without support for Python 3.12:

  * ``apache.beam`` - pending on `Apache Beam support for 3.12 <https://github.com/apache/beam/issues/29149>`_
  * ``papermill`` - pending on Releasing Python 3.12 compatible papermill client version
    `including this merged issue <https://github.com/nteract/papermill/pull/771>`_

Prevent large string objects from being stored in the Rendered Template Fields (#38094)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
There's now a limit to the length of data that can be stored in the Rendered Template Fields.
The limit is set to 4096 characters. If the data exceeds this limit, it will be truncated. You can change this limit
by setting the ``[core]max_template_field_length`` configuration option in your airflow config.

Change xcom table column value type to longblob for MySQL backend (#38401)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Xcom table column ``value`` type has changed from ``blob`` to ``longblob``. This will allow you to store relatively big data in Xcom but process can take a significant amount of time if you have a lot of large data stored in Xcom.

To downgrade from revision: ``b4078ac230a1``, ensure that you don't have Xcom values larger than 65,535 bytes. Otherwise, you'll need to clean those rows or run ``airflow db clean xcom`` to clean the Xcom table.

Stronger validation for key parameter defaults in taskflow context variables (#38015)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

As for the taskflow implementation in conjunction with context variable defaults invalid parameter orders can be
generated, it is now not accepted anymore (and validated) that taskflow functions are defined with defaults
other than ``None``. If you have done this before you most likely will see a broken DAG and a error message like
``Error message: Context key parameter my_param can't have a default other than None``.

New Features
""""""""""""
- Allow users to write dag_id and task_id in their national characters, added display name for dag / task (v2) (#38446)
- Prevent large objects from being stored in the RTIF (#38094)
- Use current time to calculate duration when end date is not present. (#38375)
- Add average duration mark line in task and dagrun duration charts. (#38214, #38434)
- Add button to manually create dataset events (#38305)
- Add ``Matomo`` as an option for analytics_tool. (#38221)
- Experimental: Support custom weight_rule implementation to calculate the TI priority_weight (#38222)
- Adding ability to automatically set DAG to off after X times it failed sequentially (#36935)
- Add dataset conditions to next run datasets modal (#38123)
- Add task log grouping to UI (#38021)
- Add dataset_expression to grid dag details (#38121)
- Introduce mechanism to support multiple executor configuration (#37635)
- Add color formatting for ANSI chars in logs from task executions (#37985)
- Add the dataset_expression as part of DagModel and DAGDetailSchema (#37826)
- Allow longer rendered_map_index (#37798)
- Inherit the run_ordering from DatasetTriggeredTimetable for DatasetOrTimeSchedule (#37775)
- Implement AIP-60 Dataset URI formats (#37005)
- Introducing Logical Operators for dataset conditional logic (#37101)
- Add post endpoint for dataset events (#37570)
- Show custom instance names for a mapped task in UI (#36797)
- Add excluded/included events to get_event_logs api (#37641)
- Add datasets to dag graph (#37604)
- Show dataset events above task/run details in grid view (#37603)
- Introduce new config variable to control whether DAG processor outputs to stdout (#37439)
- Make Datasets ``hashable`` (#37465)
- Add conditional logic for dataset triggering (#37016)
- Implement task duration page in react. (#35863)
- Add ``queuedEvent`` endpoint to get/delete DatasetDagRunQueue (#37176)
- Support multiple XCom output in the BaseOperator (#37297)
- AIP-58: Add object storage backend for xcom (#37058)
- Introduce ``DatasetOrTimeSchedule`` (#36710)
- Add ``on_skipped_callback`` to ``BaseOperator`` (#36374)
- Allow override of hovered navbar colors (#36631)
- Create new Metrics with Tagging (#36528)
- Add support for openlineage to AFS and common.io (#36410)
- Introduce ``@task.bash`` TaskFlow decorator (#30176, #37875)

Improvements
""""""""""""
- More human friendly "show tables" output for db cleanup (#38654)
- Improve trigger assign_unassigned by merging alive_triggerer_ids and get_sorted_triggers queries (#38664)
- Add exclude/include events filters to audit log (#38506)
- Clean up unused triggers in a single query for all dialects except MySQL (#38663)
- Update Confirmation Logic for Config Changes on Sensitive Environments Like Production (#38299)
- Improve datasets graph UX (#38476)
- Only show latest dataset event timestamp after last run (#38340)
- Add button to clear only failed tasks in a dagrun. (#38217)
- Delete all old dag pages and redirect to grid view (#37988)
- Check task attribute before use in sentry.add_tagging() (#37143)
- Mysql change xcom value col type for MySQL backend (#38401)
- ``ExternalPythonOperator`` use version from ``sys.version_info`` (#38377)
- Replace too broad exceptions into the Core (#38344)
- Add CLI support for bulk pause and resume of DAGs (#38265)
- Implement methods on TaskInstancePydantic and DagRunPydantic (#38295, #38302, #38303, #38297)
- Made filters bar collapsible and add a full screen toggle (#38296)
- Encrypt all trigger attributes (#38233, #38358, #38743)
- Upgrade react-table package. Use with Audit Log table (#38092)
- Show if dag page filters are active (#38080)
- Add try number to mapped instance (#38097)
- Add retries to job heartbeat (#37541)
- Add REST API events to Audit Log (#37734)
- Make current working directory as templated field in BashOperator (#37968)
- Add calendar view to react (#37909)
- Add ``run_id`` column to log table (#37731)
- Add ``tryNumber`` to grid task instance tooltip (#37911)
- Session is not used in _do_render_template_fields (#37856)
- Improve MappedOperator property types (#37870)
- Remove provide_session decorator from TaskInstancePydantic methods (#37853)
- Ensure the "airflow.task" logger used for TaskInstancePydantic and TaskInstance (#37857)
- Better error message for internal api call error (#37852)
- Increase tooltip size of dag grid view (#37782) (#37805)
- Use named loggers instead of root logger (#37801)
- Add Run Duration in React (#37735)
- Avoid non-recommended usage of logging (#37792)
- Improve DateTimeTrigger typing (#37694)
- Make sure all unique run_ids render a task duration bar (#37717)
- Add Dag Audit Log to React (#37682)
- Add log event for auto pause (#38243)
- Better message for exception for templated base operator fields (#37668)
- Clean up webserver endpoints adding to audit log (#37580)
- Filter datasets graph by dag_id (#37464)
- Use new exception type inheriting BaseException for SIGTERMs (#37613)
- Refactor dataset class inheritance (#37590)
- Simplify checks for package versions (#37585)
- Filter Datasets by associated dag_ids (GET /datasets) (#37512)
- Enable "airflow tasks test" to run deferrable operator (#37542)
- Make datasets list/graph width adjustable (#37425)
- Speedup determine installed airflow version in ``ExternalPythonOperator`` (#37409)
- Add more task details from rest api (#37394)
- Add confirmation dialog box for DAG run actions (#35393)
- Added shutdown color to the STATE_COLORS (#37295)
- Remove legacy dag details page and redirect to grid (#37232)
- Order XCom entries by map index in API (#37086)
- Add data_interval_start and data_interval_end in dagrun create API endpoint (#36630)
- Making links in task logs as hyperlinks by preventing HTML injection (#36829)
- Improve ExternalTaskSensor Async Implementation (#36916)
- Make Datasets ``Pathlike`` (#36947)
- Simplify query for orphaned tasks (#36566)
- Add deferrable param in FileSensor (#36840)
- Run Trigger Page: Configurable number of recent configs (#36878)
- Merge ``nowait`` and skip_locked into with_row_locks (#36889)
- Return the specified field when get ``dag/dagRun`` in the REST API (#36641)
- Only iterate over the items if debug is enabled for DagFileProcessorManager (#36761)
- Add a fuzzy/regex pattern-matching for metric allow and block list (#36250)
- Allow custom columns in cli dags list (#35250)
- Make it possible to change the default cron timetable (#34851)
- Some improvements to Airflow IO code (#36259)
- Improve TaskInstance typing hints (#36487)
- Remove dependency of ``Connexion`` from auth manager interface (#36209)
- Refactor ExternalDagLink to not create ad hoc TaskInstances (#36135)

Bug Fixes
"""""""""
- Load providers configuration when gunicorn workers start (#38795)
- Fix grid header rendering (#38720)
- Add a task instance dependency for mapped dependencies (#37498)
- Improve stability of remove_task_decorator function (#38649)
- Mark more fields on API as dump-only (#38616)
- Fix ``total_entries`` count on the event logs endpoint (#38625)
- Add padding to bottom of log block. (#38610)
- Properly serialize nested attrs classes (#38591)
- Fixing the ``tz`` in next run ID info (#38482)
- Show abandoned tasks in Grid View (#38511)
- Apply task instance mutation hook consistently (#38440)
- Override ``chakra`` styles to keep ``dropdowns`` in filter bar (#38456)
- Store duration in seconds and scale to handle case when a value in the series has a larger unit than the preceding durations. (#38374)
- Don't allow defaults other than None in context parameters, and improve error message (#38015)
- Make postgresql default engine args comply with SA 2.0 (#38362)
- Add return statement to yield within a while loop in triggers (#38389)
- Ensure ``__exit__`` is called in decorator context managers (#38383)
- Make the method ``BaseAuthManager.is_authorized_custom_view`` abstract (#37915)
- Add upper limit to planned calendar events calculation (#38310)
- Fix Scheduler in daemon mode doesn't create PID at the specified location (#38117)
- Properly serialize TaskInstancePydantic and DagRunPydantic (#37855)
- Fix graph task state border color (#38084)
- Add back methods removed in security manager (#37997)
- Don't log "403" from worker serve-logs as "Unknown error". (#37933)
- Fix execution data validation error in ``/get_logs_with_metadata`` endpoint (#37756)
- Fix task duration selection (#37630)
- Refrain from passing ``encoding`` to the SQL engine in SQLAlchemy v2 (#37545)
- Fix 'implicitly coercing SELECT object to scalar subquery' in latest dag run statement (#37505)
- Clean up typing with max_execution_date query builder (#36958)
- Optimize max_execution_date query in single dag case (#33242)
- Fix list dags command for get_dagmodel is None (#36739)
- Load ``consuming_dags`` attr eagerly before dataset listener (#36247)

Miscellaneous
"""""""""""""
- Remove display of param from the UI (#38660)
- Update log level to debug from warning about scheduled_duration metric (#38180)
- Use ``importlib_metadata`` with compat to Python 3.10/3.12 ``stdlib`` (#38366)
- Refactored ``__new__`` magic method of BaseOperatorMeta to avoid bad mixing classic and decorated operators (#37937)
- Use ``sys.version_info`` for determine Python Major.Minor (#38372)
- Add missing deprecated Fab auth manager (#38376)
- Remove unused loop variable from airflow package (#38308)
- Adding max consecutive failed dag runs info in UI (#38229)
- Bump minimum version of ``blinker`` add where it requires (#38140)
- Bump follow-redirects from 1.15.4 to 1.15.6 in /airflow/www (#38156)
- Bump Cryptography to ``> 39.0.0`` (#38112)
- Add Python 3.12 support (#36755, #38025, #36595)
- Avoid use of ``assert`` outside of the tests (#37718)
- Update ObjectStoragePath for universal_pathlib>=v0.2.2 (#37930)
- Resolve G004: Logging statement uses f-string (#37873)
- Update build and install dependencies. (#37910)
- Bump sanitize-html from 2.11.0 to 2.12.1 in /airflow/www (#37833)
- Update to latest installer versions. (#37754)
- Deprecate smtp configs in airflow settings / local_settings (#37711)
- Deprecate PY* constants into the airflow module (#37575)
- Remove usage of deprecated ``flask._request_ctx_stack`` (#37522)
- Remove redundant ``login`` attribute in ``airflow.__init__.py`` (#37565)
- Upgrade to FAB 4.3.11 (#37233)
- Remove SCHEDULED_DEPS which is no longer used anywhere since 2.0.0 (#37140)
- Replace ``datetime.datetime.utcnow`` by ``airflow.utils.timezone.utcnow`` in core (#35448)
- Bump aiohttp min version to avoid CVE-2024-23829 and CVE-2024-23334 (#37110)
- Move config related to FAB auth manager to FAB provider (#36232)
- Remove MSSQL support form Airflow core (#36514)
- Remove ``is_authorized_cluster_activity`` from auth manager (#36175)
- Create FAB provider and move FAB auth manager in it (#35926)

Doc Only Changes
""""""""""""""""
- Improve timetable documentation (#38505)
- Reorder OpenAPI Spec tags alphabetically (#38717)
- Update UI screenshots in the documentation (#38680, #38403, #38438, #38435)
- Remove section as it's no longer true with dataset expressions PR (#38370)
- Refactor DatasetOrTimeSchedule timetable docs (#37771)
- Migrate executor docs to respective providers (#37728)
- Add directive to render a list of URI schemes (#37700)
- Add doc page with providers deprecations (#37075)
- Add a cross reference to security policy (#37004)
- Improve AIRFLOW__WEBSERVER__BASE_URL docs (#37003)
- Update faq.rst with (hopefully) clearer description of start_date (#36846)
- Update public interface doc re operators (#36767)
- Add ``exception`` to templates ref list (#36656)
- Add auth manager interface as public interface (#36312)
- Reference fab provider documentation in Airflow documentation (#36310)
- Create auth manager documentation (#36211)
- Update permission docs (#36120)
- Docstring improvement to _covers_every_hour (#36081)
- Add note that task instance, dag and lifecycle listeners are non-experimental (#36376)


Airflow 2.8.4 (2024-03-25)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""
- Fix incorrect serialization of ``FixedTimezone`` (#38139)
- Fix excessive permission changing for log task handler (#38164)
- Fix task instances list link (#38096)
- Fix a bug where scheduler heartrate parameter was not used (#37992)
- Add padding to prevent grid horizontal scroll overlapping tasks (#37942)
- Fix hash caching in ``ObjectStoragePath`` (#37769)

Miscellaneous
"""""""""""""
- Limit ``importlib_resources`` as it breaks ``pytest_rewrites`` (#38095, #38139)
- Limit ``pandas`` to ``<2.2`` (#37748)
- Bump ``croniter`` to fix an issue with 29 Feb cron expressions (#38198)

Doc Only Changes
""""""""""""""""
- Tell users what to do if their scanners find issues in the image (#37652)
- Add a section about debugging in Docker Compose with PyCharm (#37940)
- Update deferrable docs to clarify kwargs when trigger resumes operator (#38122)


Airflow 2.8.3 (2024-03-11)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

The smtp provider is now pre-installed when you install Airflow. (#37713)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Bug Fixes
"""""""""
- Add "MENU" permission in auth manager (#37881)
- Fix external_executor_id being overwritten (#37784)
- Make more MappedOperator members modifiable (#37828)
- Set parsing context dag_id in dag test command (#37606)

Miscellaneous
"""""""""""""
- Remove useless methods from security manager (#37889)
- Improve code coverage for TriggerRuleDep (#37680)
- The SMTP provider is now preinstalled when installing Airflow (#37713)
- Bump min versions of openapi validators (#37691)
- Properly include ``airflow_pre_installed_providers.txt`` artifact (#37679)

Doc Only Changes
""""""""""""""""
- Clarify lack of sync between workers and scheduler (#37913)
- Simplify some docs around airflow_local_settings (#37835)
- Add section about local settings configuration (#37829)
- Fix docs of ``BranchDayOfWeekOperator`` (#37813)
- Write to secrets store is not supported by design (#37814)
- ``ERD`` generating doc improvement (#37808)
- Update incorrect config value (#37706)
- Update security model to clarify Connection Editing user's capabilities (#37688)
- Fix ImportError on examples dags (#37571)


Airflow 2.8.2 (2024-02-26)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

The ``allowed_deserialization_classes`` flag now follows a glob pattern (#36147).
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

For example if one wants to add the class ``airflow.tests.custom_class`` to the
``allowed_deserialization_classes`` list, it can be done by writing the full class
name (``airflow.tests.custom_class``) or a pattern such as the ones used in glob
search (e.g., ``airflow.*``, ``airflow.tests.*``).

If you currently use a custom regexp path make sure to rewrite it as a glob pattern.

Alternatively, if you still wish to match it as a regexp pattern, add it under the new
list ``allowed_deserialization_classes_regexp`` instead.

The audit_logs permissions have been updated for heightened security (#37501).
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This was done under the policy that we do not want users like Viewer, Ops,
and other users apart from Admin to have access to audit_logs. The intention behind
this change is to restrict users with less permissions from viewing user details
like First Name, Email etc. from the audit_logs when they are not permitted to.

The impact of this change is that the existing users with non admin rights won't be able
to view or access the audit_logs, both from the Browse tab or from the DAG run.

``AirflowTimeoutError`` is no longer ``except`` by default through ``Exception`` (#35653).
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``AirflowTimeoutError`` is now inheriting ``BaseException`` instead of
``AirflowException``->``Exception``.
See https://docs.python.org/3/library/exceptions.html#exception-hierarchy

This prevents code catching ``Exception`` from accidentally
catching ``AirflowTimeoutError`` and continuing to run.
``AirflowTimeoutError`` is an explicit intent to cancel the task, and should not
be caught in attempts to handle the error and return some default value.

Catching ``AirflowTimeoutError`` is still possible by explicitly ``except``ing
``AirflowTimeoutError`` or ``BaseException``.
This is discouraged, as it may allow the code to continue running even after
such cancellation requests.
Code that previously depended on performing strict cleanup in every situation
after catching ``Exception`` is advised to use ``finally`` blocks or
context managers. To perform only the cleanup and then automatically
re-raise the exception.
See similar considerations about catching ``KeyboardInterrupt`` in
https://docs.python.org/3/library/exceptions.html#KeyboardInterrupt


Bug Fixes
"""""""""
- Sort dag processing stats by last_runtime (#37302)
- Allow pre-population of trigger form values via URL parameters (#37497)
- Base date for fetching dag grid view must include selected run_id (#34887)
- Check permissions for ImportError (#37468)
- Move ``IMPORT_ERROR`` from DAG related permissions to view related permissions (#37292)
- Change ``AirflowTaskTimeout`` to inherit ``BaseException`` (#35653)
- Revert "Fix future DagRun rarely triggered by race conditions when max_active_runs reached its upper limit. (#31414)" (#37596)
- Change margin to padding so first task can be selected (#37527)
- Fix Airflow serialization for ``namedtuple`` (#37168)
- Fix bug with clicking url-unsafe tags (#37395)
- Set deterministic and new getter for ``Treeview`` function (#37162)
- Fix permissions of parent folders for log file handler (#37310)
- Fix permission check on DAGs when ``access_entity`` is specified (#37290)
- Fix the value of ``dateTimeAttrFormat`` constant (#37285)
- Resolve handler close race condition at triggerer shutdown (#37206)
- Fixing status icon alignment for various views (#36804)
- Remove superfluous ``@Sentry.enrich_errors``  (#37002)
- Use execution_date= param as a backup to base date for grid view (#37018)
- Handle SystemExit raised in the task. (#36986)
- Revoking audit_log permission from all users except admin (#37501)
- Fix broken regex for allowed_deserialization_classes (#36147)
- Fix the bug that affected the DAG end date. (#36144)
- Adjust node width based on task name length (#37254)
- fix: PythonVirtualenvOperator crashes if any python_callable function is defined in the same source as DAG (#37165)
- Fix collapsed grid width, line up selected bar with gantt (#37205)
- Adjust graph node layout (#37207)
- Revert the sequence of initializing configuration defaults (#37155)
- Displaying "actual" try number in TaskInstance view (#34635)
- Bugfix Triggering DAG with parameters is mandatory when show_trigger_form_if_no_params is enabled (#37063)
- Secret masker ignores passwords with special chars (#36692)
- Fix DagRuns with UPSTREAM_FAILED tasks get stuck in the backfill. (#36954)
- Disable ``dryrun`` auto-fetch (#36941)
- Fix copy button on a DAG run's config (#36855)
- Fix bug introduced by replacing spaces by + in run_id (#36877)
- Fix webserver always redirecting to home page if user was not logged in (#36833)
- REST API set description on POST to ``/variables`` endpoint (#36820)
- Sanitize the conn_id to disallow potential script execution (#32867)
- Fix task id copy button copying wrong id (#34904)
- Fix security manager inheritance in fab provider (#36538)
- Avoid ``pendulum.from_timestamp`` usage (#37160)

Miscellaneous
"""""""""""""
- Install latest docker ``CLI`` instead of specific one (#37651)
- Bump ``undici`` from ``5.26.3`` to ``5.28.3`` in ``/airflow/www`` (#37493)
- Add Python ``3.12`` exclusions in ``providers/pyproject.toml`` (#37404)
- Remove ``markdown`` from core dependencies (#37396)
- Remove unused ``pageSize`` method. (#37319)
- Add more-itertools as dependency of common-sql (#37359)
- Replace other ``Python 3.11`` and ``3.12`` deprecations (#37478)
- Include ``airflow_pre_installed_providers.txt`` into ``sdist`` distribution (#37388)
- Turn Pydantic into an optional dependency (#37320)
- Limit ``universal-pathlib to < 0.2.0`` (#37311)
- Allow running airflow against sqlite in-memory DB for tests (#37144)
- Add description to ``queue_when`` (#36997)
- Updated ``config.yml`` for environment variable ``sql_alchemy_connect_args``  (#36526)
- Bump min version of ``Alembic to 1.13.1`` (#36928)
- Limit ``flask-session`` to ``<0.6`` (#36895)

Doc Only Changes
""""""""""""""""
- Fix upgrade docs to reflect true ``CLI`` flags available (#37231)
- Fix a bug in fundamentals doc (#37440)
- Add redirect for deprecated page (#37384)
- Fix the ``otel`` config descriptions (#37229)
- Update ``Objectstore`` tutorial with ``prereqs`` section (#36983)
- Add more precise description on avoiding generic ``package/module`` names (#36927)
- Add airflow version substitution into Docker Compose Howto (#37177)
- Add clarification about DAG author capabilities to security model (#37141)
- Move docs for cron basics to Authoring and Scheduling section (#37049)
- Link to release notes in the upgrade docs (#36923)
- Prevent templated field logic checks in ``__init__`` of operators automatically (#33786)


Airflow 2.8.1 (2024-01-19)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Target version for core dependency ``pendulum`` package set to 3 (#36281).
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Support for pendulum 2.1.2 will be saved for a while, presumably until the next feature version of Airflow.
It is advised to upgrade user code to use pendulum 3 as soon as possible.

Pendulum 3 introduced some subtle incompatibilities that you might rely on in your code - for example
default rendering of dates is missing ``T`` in the rendered date representation, which is not ISO8601
compliant. If you rely on the default rendering of dates, you might need to adjust your code to use
``isoformat()`` method to render dates in ISO8601 format.

Airflow packaging specification follows modern Python packaging standards (#36537).
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
We standardized Airflow dependency configuration to follow latest development in Python packaging by
using ``pyproject.toml``. Airflow is now compliant with those accepted PEPs:

* `PEP-440 Version Identification and Dependency Specification <https://www.python.org/dev/peps/pep-0440/>`__
* `PEP-517 A build-system independent format for source trees <https://www.python.org/dev/peps/pep-0517/>`__
* `PEP-518 Specifying Minimum Build System Requirements for Python Projects <https://www.python.org/dev/peps/pep-0518/>`__
* `PEP-561 Distributing and Packaging Type Information <https://www.python.org/dev/peps/pep-0561/>`__
* `PEP-621 Storing project metadata in pyproject.toml <https://www.python.org/dev/peps/pep-0621/>`__
* `PEP-660 Editable installs for pyproject.toml based builds (wheel based) <https://www.python.org/dev/peps/pep-0660/>`__
* `PEP-685 Comparison of extra names for optional distribution dependencies <https://www.python.org/dev/peps/pep-0685/>`__

Also we implement multiple license files support coming from Draft, not yet accepted (but supported by ``hatchling``) PEP:
* `PEP 639 Improving License Clarity with Better Package Metadata <https://peps.python.org/pep-0639/>`__

This has almost no noticeable impact on users if they are using modern Python packaging and development tools, generally
speaking Airflow should behave as it did before when installing it from PyPI and it should be much easier to install
it for development purposes using ``pip install -e ".[devel]"``.

The differences from the user side are:

* Airflow extras now get extras normalized to ``-`` (following PEP-685) instead of ``_`` and ``.``
  (as it was before in some extras). When you install airflow with such extras (for example ``dbt.core`` or
  ``all_dbs``) you should use ``-`` instead of ``_`` and ``.``.

In most modern tools this will work in backwards-compatible way, but in some old version of those tools you might need to
replace ``_`` and ``.`` with ``-``. You can also get warnings that the extra you are installing does not exist - but usually
this warning is harmless and the extra is installed anyway. It is, however, recommended to change to use ``-`` in extras in your dependency
specifications for all Airflow extras.

* Released airflow package does not contain ``devel``, ``devel-*``, ``doc`` and ``docs-gen`` extras.
  Those extras are only available when you install Airflow from sources in ``--editable`` mode. This is
  because those extras are only used for development and documentation building purposes and are not needed
  when you install Airflow for production use. Those dependencies had unspecified and varying behaviour for
  released packages anyway and you were not supposed to use them in released packages.

* The ``all`` and ``all-*`` extras were not always working correctly when installing Airflow using constraints
  because they were also considered as development-only dependencies. With this change, those dependencies are
  now properly handling constraints and they will install properly with constraints, pulling the right set
  of providers and dependencies when constraints are used.

Graphviz dependency is now an optional one, not required one (#36647).
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
The ``graphviz`` dependency has been problematic as Airflow required dependency - especially for
ARM-based installations. Graphviz packages require binary graphviz libraries - which is already a
limitation, but they also require to install graphviz Python bindings to be build and installed.
This does not work for older Linux installation but - more importantly - when you try to install
Graphviz libraries for Python 3.8, 3.9 for ARM M1 MacBooks, the packages fail to install because
Python bindings compilation for M1 can only work for Python 3.10+.

This is not a breaking change technically - the CLIs to render the DAGs is still there and IF you
already have graphviz installed, it will continue working as it did before. The only problem when it
does not work is where you do not have graphviz installed it will raise an error and inform that you need it.

Graphviz will remain to be installed for most users:

* the Airflow Image will still contain graphviz library, because
  it is added there as extra
* when previous version of Airflow has been installed already, then
  graphviz library is already installed there and Airflow will
  continue working as it did

The only change will be a new installation of new version of Airflow from the scratch, where graphviz will
need to be specified as extra or installed separately in order to enable DAG rendering option.

Bug Fixes
"""""""""
- Fix airflow-scheduler exiting with code 0 on exceptions (#36800)
- Fix Callback exception when a removed task is the last one in the ``taskinstance`` list (#36693)
- Allow anonymous user edit/show resource when set ``AUTH_ROLE_PUBLIC=admin`` (#36750)
- Better error message when sqlite URL uses relative path (#36774)
- Explicit string cast required to force integer-type run_ids to be passed as strings instead of integers (#36756)
- Add log lookup exception for empty ``op`` subtypes (#35536)
- Remove unused index on task instance (#36737)
- Fix check on subclass for ``typing.Union`` in ``_infer_multiple_outputs`` for Python 3.10+ (#36728)
- Make sure ``multiple_outputs`` is inferred correctly even when using ``TypedDict`` (#36652)
- Add back FAB constant in legacy security manager (#36719)
- Fix AttributeError when using ``Dagrun.update_state`` (#36712)
- Do not let ``EventsTimetable`` schedule past events if ``catchup=False`` (#36134)
- Support encryption for triggers parameters (#36492)
- Fix the type hint for ``tis_query`` in ``_process_executor_events`` (#36655)
- Redirect to index when user does not have permission to access a page (#36623)
- Avoid using dict as default value in ``call_regular_interval`` (#36608)
- Remove option to set a task instance to running state in UI (#36518)
- Fix details tab not showing when using dynamic task mapping (#36522)
- Raise error when ``DagRun`` fails while running ``dag test`` (#36517)
- Refactor ``_manage_executor_state`` by refreshing TIs in batch (#36502)
- Add flask config: ``MAX_CONTENT_LENGTH`` (#36401)
- Fix get_leaves calculation for teardown in nested group (#36456)
- Stop serializing timezone-naive datetime to timezone-aware datetime with UTC tz (#36379)
- Make ``kubernetes`` decorator type annotation consistent with operator (#36405)
- Fix Webserver returning 500 for POST requests to ``api/dag/*/dagrun`` from anonymous user (#36275)
- Fix the required access for get_variable endpoint (#36396)
- Fix datetime reference in ``DAG.is_fixed_time_schedule`` (#36370)
- Fix AirflowSkipException message raised by BashOperator (#36354)
- Allow PythonVirtualenvOperator.skip_on_exit_code to be zero (#36361)
- Increase width of execution_date input in trigger.html (#36278)
- Fix logging for pausing DAG (#36182)
- Stop deserializing pickle when enable_xcom_pickling is False (#36255)
- Check DAG read permission before accessing DAG code (#36257)
- Enable mark task as failed/success always (#36254)
- Create latest log dir symlink as relative link (#36019)
- Fix Python-based decorators templating (#36103)

Miscellaneous
"""""""""""""
- Rename concurrency label to max active tasks (#36691)
- Restore function scoped ``httpx`` import in file_task_handler for performance (#36753)
- Add support of Pendulum 3 (#36281)
- Standardize airflow build process and switch to ``hatchling`` build backend (#36537)
- Get rid of ``pyarrow-hotfix`` for ``CVE-2023-47248`` (#36697)
- Make ``graphviz`` dependency optional (#36647)
- Announce MSSQL support end in Airflow 2.9.0, add migration script hints (#36509)
- Set min ``pandas`` dependency to 1.2.5 for all providers and airflow (#36698)
- Bump follow-redirects from 1.15.3 to 1.15.4 in ``/airflow/www`` (#36700)
- Provide the logger_name param to base hook in order to override the logger name (#36674)
- Fix run type icon alignment with run type text (#36616)
- Follow BaseHook connection fields method signature in FSHook (#36444)
- Remove redundant ``docker`` decorator type annotations (#36406)
- Straighten typing in workday timetable (#36296)
- Use ``batch_is_authorized_dag`` to check if user has permission to read DAGs (#36279)
- Replace deprecated get_accessible_dag_ids and use get_readable_dags in get_dag_warnings (#36256)

Doc Only Changes
""""""""""""""""
- Metrics tagging documentation (#36627)
- In docs use logical_date instead of deprecated execution_date (#36654)
- Add section about live-upgrading Airflow (#36637)
- Replace ``numpy`` example with practical exercise demonstrating top-level code (#35097)
- Improve and add more complete description in the architecture diagrams (#36513)
- Improve the error message displayed when there is a webserver error (#36570)
- Update ``dags.rst`` with information on DAG pausing (#36540)
- Update installation prerequisites after upgrading to Debian Bookworm (#36521)
- Add description on the ways how users should approach DB monitoring (#36483)
- Add branching based on mapped task group example to dynamic-task-mapping.rst (#36480)
- Add further details to replacement documentation (#36485)
- Use cards when describing priority weighting methods (#36411)
- Update ``metrics.rst`` for param ``dagrun.schedule_delay`` (#36404)
- Update admonitions in Python operator doc to reflect sentiment (#36340)
- Improve audit_logs.rst (#36213)
- Remove Redshift mention from the list of managed Postgres backends (#36217)

Airflow 2.8.0 (2023-12-18)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Raw HTML code in DAG docs and DAG params descriptions is disabled by default (#35460)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
To ensure that no malicious javascript can be injected with DAG descriptions or trigger UI forms by DAG authors
a new parameter ``webserver.allow_raw_html_descriptions`` was added with default value of ``False``.
If you trust your DAG authors code and want to allow using raw HTML in DAG descriptions and params, you can restore the previous
behavior by setting the configuration value to ``True``.

To ensure Airflow is secure by default, the raw HTML support in trigger UI has been super-seeded by markdown support via
the ``description_md`` attribute. If you have been using ``description_html`` please migrate to ``description_md``.
The ``custom_html_form`` is now deprecated.

New Features
""""""""""""
- AIP-58: Add Airflow ObjectStore (AFS) (`AIP-58 <https://github.com/apache/airflow/pulls?q=is%3Apr+is%3Amerged+label%3AAIP-58+milestone%3A%22Airflow+2.8.0%22>`_)
- Add XCom tab to Grid (#35719)
- Add "literal" wrapper to disable field templating (#35017)
- Add task context logging feature to allow forwarding messages to task logs (#32646, #32693, #35857)
- Add Listener hooks for Datasets (#34418, #36247)
- Allow override of navbar text color (#35505)
- Add lightweight serialization for deltalake tables (#35462)
- Add support for serialization of iceberg tables (#35456)
- ``prev_end_date_success`` method access (#34528)
- Add task parameter to set custom logger name (#34964)
- Add pyspark decorator (#35247)
- Add trigger as a valid option for the db clean command (#34908)
- Add decorators for external and venv python branching operators (#35043)
- Allow PythonVenvOperator using other index url (#33017)
- Add Python Virtualenv Operator Caching (#33355)
- Introduce a generic export for containerized executor logging (#34903)
- Add ability to clear downstream tis in ``List Task Instances`` view  (#34529)
- Attribute ``clear_number`` to track DAG run being cleared (#34126)
- Add BranchPythonVirtualenvOperator (#33356)
- Allow PythonVenvOperator using other index url (#33017)
- Add CLI notification commands to providers (#33116)
- Use dropdown instead of buttons when there are more than 10 retries in log tab (#36025)

Improvements
""""""""""""
- Add ``multiselect`` to run state in grid view (#35403)
- Fix warning message in ``Connection.get_hook`` in case of ImportError (#36005)
- Add processor_subdir to import_error table to handle multiple dag processors (#35956)
- Consolidate the call of change_state to fail or success in the core executors (#35901)
- Relax mandatory requirement for start_date when schedule=None (#35356)
- Use ExitStack to manage mutation of secrets_backend_list in dag.test (#34620)
- improved visibility of tasks in ActionModal for ``taskinstance`` (#35810)
- Create directories based on ``AIRFLOW_CONFIG`` path (#35818)
- Implements ``JSON-string`` connection representation generator (#35723)
- Move ``BaseOperatorLink`` into the separate module (#35032)
- Set mark_end_on_close after set_context (#35761)
- Move external logs links to top of react logs page (#35668)
- Change terminal mode to ``cbreak`` in ``execute_interactive`` and handle ``SIGINT`` (#35602)
- Make raw HTML descriptions configurable (#35460)
- Allow email field to be templated (#35546)
- Hide logical date and run id in trigger UI form (#35284)
- Improved instructions for adding dependencies in TaskFlow (#35406)
- Add optional exit code to list import errors (#35378)
- Limit query result on DB rather than client in ``synchronize_log_template`` function (#35366)
- Allow description to be passed in when using variables CLI (#34791)
- Allow optional defaults in required fields with manual triggered dags (#31301)
- Permitting airflow kerberos to run in different modes (#35146)
- Refactor commands to unify daemon context handling (#34945)
- Add extra fields to plugins endpoint (#34913)
- Add description to pools view (#34862)
- Move cli's Connection export and Variable export command print logic to a separate function (#34647)
- Extract and reuse get_kerberos_principle func from get_kerberos_principle (#34936)
- Change type annotation for ``BaseOperatorLink.operators`` (#35003)
- Optimise and migrate to ``SA2-compatible`` syntax for TaskReschedule (#33720)
- Consolidate the permissions name in SlaMissModelView (#34949)
- Add debug log saying what's being run to ``EventScheduler`` (#34808)
- Increase log reader stream loop sleep duration to 1 second (#34789)
- Resolve pydantic deprecation warnings re ``update_forward_refs`` (#34657)
- Unify mapped task group lookup logic (#34637)
- Allow filtering event logs by attributes (#34417)
- Make connection login and password TEXT (#32815)
- Ban import ``Dataset`` from ``airflow`` package in codebase (#34610)
- Use ``airflow.datasets.Dataset`` in examples and tests (#34605)
- Enhance task status visibility (#34486)
- Simplify DAG trigger UI (#34567)
- Ban import AirflowException from airflow (#34512)
- Add descriptions for airflow resource config parameters (#34438)
- Simplify trigger name expression (#34356)
- Move definition of Pod*Exceptions to pod_generator (#34346)
- Add deferred tasks to the cluster_activity view Pools Slots (#34275)
- heartbeat failure log message fix (#34160)
- Rename variables for dag runs (#34049)
- Clarify new_state in OpenAPI spec (#34056)
- Remove ``version`` top-level element from docker compose files (#33831)
- Remove generic trigger cancelled error log (#33874)
- Use ``NOT EXISTS`` subquery instead of ``tuple_not_in_condition`` (#33527)
- Allow context key args to not provide a default (#33430)
- Order triggers by - TI priority_weight when assign unassigned triggers (#32318)
- Add metric ``triggerer_heartbeat`` (#33320)
- Allow ``airflow variables export`` to print to stdout (#33279)
- Workaround failing deadlock when running backfill (#32991)
- add dag_run_ids and task_ids filter for the batch task instance API endpoint (#32705)
- Configurable health check threshold for triggerer (#33089)
- Rework provider manager to treat Airflow core hooks like other provider hooks (#33051)
- Ensure DAG-level references are filled on unmap (#33083)
- Affix webserver access_denied warning to be configurable (#33022)
- Add support for arrays of different data types in the Trigger Form UI (#32734)
- Add a mechanism to warn if executors override existing CLI commands (#33423)

Bug Fixes
"""""""""
- Account for change in UTC offset when calculating next schedule (#35887)
- Add read access to pools for viewer role (#35352)
- Fix gantt chart queued duration when queued_dttm is greater than start_date for deferred tasks (#35984)
- Avoid crushing container when directory is not found on rm (#36050)
- Update ``reset_user_sessions`` to work from either CLI or web (#36056)
- Fix UI Grid error when DAG has been removed. (#36028)
- Change Trigger UI to use HTTP POST in web ui (#36026)
- Fix airflow db shell needing an extra key press to exit (#35982)
- Change dag grid ``overscroll`` behaviour to auto (#35717)
- Run triggers inline with dag test (#34642)
- Add ``borderWidthRight`` to grid for Firefox ``scrollbar`` (#35346)
- Fix for infinite recursion due to secrets_masker (#35048)
- Fix write ``processor_subdir`` in serialized_dag table (#35661)
- Reload configuration for standalone dag file processor (#35725)
- Long custom operator name overflows in graph view (#35382)
- Add try_number to extra links query (#35317)
- Prevent assignment of non JSON serializable values to DagRun.conf dict (#35096)
- Numeric values in DAG details are incorrectly rendered as timestamps (#35538)
- Fix Scheduler and triggerer crashes in daemon mode when statsd metrics are enabled (#35181)
- Infinite UI redirection loop after deactivating an active user (#35486)
- Bug fix fetch_callback of Partial Subset DAG (#35256)
- Fix DagRun data interval for DeltaDataIntervalTimetable (#35391)
- Fix query in ``get_dag_by_pickle`` util function (#35339)
- Fix TriggerDagRunOperator failing to trigger subsequent runs when reset_dag_run=True (#35429)
- Fix weight_rule property type in ``mappedoperator`` (#35257)
- Bugfix/prevent concurrency with cached venv (#35258)
- Fix dag serialization (#34042)
- Fix py/url-redirection by replacing request.referrer by get_redirect() (#34237)
- Fix updating variables during variable imports (#33932)
- Use Literal from airflow.typing_compat in Airflow core (#33821)
- Always use ``Literal`` from ``typing_extensions`` (#33794)

Miscellaneous
"""""""""""""
- Change default MySQL client to MariaDB (#36243)
- Mark daskexecutor provider as removed (#35965)
- Bump FAB to ``4.3.10`` (#35991)
- Mark daskexecutor provider as removed (#35965)
- Rename ``Connection.to_json_dict`` to ``Connection.to_dict`` (#35894)
- Upgrade to Pydantic v2 (#35551)
- Bump ``moto`` version to ``>= 4.2.9`` (#35687)
- Use ``pyarrow-hotfix`` to mitigate CVE-2023-47248 (#35650)
- Bump ``axios`` from ``0.26.0 to 1.6.0`` in ``/airflow/www/`` (#35624)
- Make docker decorator's type annotation consistent with operator (#35568)
- Add default to ``navbar_text_color`` and ``rm`` condition in style (#35553)
- Avoid initiating session twice in ``dag_next_execution`` (#35539)
- Work around typing issue in examples and providers (#35494)
- Enable ``TCH004`` and ``TCH005`` rules (#35475)
- Humanize log output about retrieved DAG(s) (#35338)
- Switch from Black to Ruff formatter (#35287)
- Upgrade to Flask Application Builder 4.3.9 (#35085)
- D401 Support (#34932, #34933)
- Use requires_access to check read permission on dag instead of checking it explicitly (#34940)
- Deprecate lazy import ``AirflowException`` from airflow (#34541)
- View util refactoring on mapped stuff use cases (#34638)
- Bump ``postcss`` from ``8.4.25 to 8.4.31`` in ``/airflow/www`` (#34770)
- Refactor Sqlalchemy queries to 2.0 style (#34763, #34665, #32883, #35120)
- Change to lazy loading of io in pandas serializer (#34684)
- Use ``airflow.models.dag.DAG`` in examples (#34617)
- Use airflow.exceptions.AirflowException in core (#34510)
- Check that dag_ids passed in request are consistent (#34366)
- Refactors to make code better (#34278, #34113, #34110, #33838, #34260, #34409, #34377, #34350)
- Suspend qubole provider (#33889)
- Generate Python API docs for Google ADS (#33814)
- Improve importing in modules (#33812, #33811, #33810, #33806, #33807, #33805, #33804, #33803,
  #33801, #33799, #33800, #33797, #33798, #34406, #33808)
- Upgrade Elasticsearch to 8 (#33135)

Doc Only Changes
""""""""""""""""
- Add support for tabs (and other UX components) to docs (#36041)
- Replace architecture diagram of Airflow with diagrams-generated one (#36035)
- Add the section describing the security model of DAG Author capabilities (#36022)
- Enhance docs for zombie tasks (#35825)
- Reflect drop/add support of DB Backends versions in documentation (#35785)
- More detail on mandatory task arguments (#35740)
- Indicate usage of the ``re2`` regex engine in the .airflowignore documentation. (#35663)
- Update ``best-practices.rst`` (#35692)
- Update ``dag-run.rst`` to mention Airflow's support for extended cron syntax through croniter (#35342)
- Update ``webserver.rst`` to include information of supported OAuth2 providers (#35237)
- Add back dag_run to docs (#35142)
- Fix ``rst`` code block format (#34708)
- Add typing to concrete taskflow examples (#33417)
- Add concrete examples for accessing context variables from TaskFlow tasks (#33296)
- Fix links in security docs (#33329)


Airflow 2.7.3 (2023-11-06)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""
- Fix pre-mature evaluation of tasks in mapped task group (#34337)
- Add TriggerRule missing value in rest API (#35194)
- Fix Scheduler crash looping when dagrun creation fails (#35135)
- Fix test connection with ``codemirror`` and extra (#35122)
- Fix usage of cron-descriptor since BC in v1.3.0 (#34836)
- Fix ``get_plugin_info`` for class based listeners. (#35022)
- Some improvements/fixes for dag_run and task_instance endpoints (#34942)
- Fix the dags count filter in webserver home page (#34944)
- Return only the TIs of the readable dags when ~ is provided as a dag_id (#34939)
- Fix triggerer thread crash in daemon mode (#34931)
- Fix wrong plugin schema (#34858)
- Use DAG timezone in TimeSensorAsync (#33406)
- Mark tasks with ``all_skipped`` trigger rule as ``skipped`` if any task is in ``upstream_failed`` state (#34392)
- Add read only validation to read only fields (#33413)

Misc/Internal
"""""""""""""
- Improve testing harness to separate DB and non-DB tests (#35160, #35333)
- Add pytest db_test markers to our tests (#35264)
- Add pip caching for faster build (#35026)
- Upper bound ``pendulum`` requirement to ``<3.0`` (#35336)
- Limit ``sentry_sdk`` to ``1.33.0`` (#35298)
- Fix subtle bug in mocking processor_agent in our tests (#35221)
- Bump ``@babel/traverse`` from ``7.16.0 to 7.23.2`` in ``/airflow/www`` (#34988)
- Bump ``undici`` from ``5.19.1 to 5.26.3`` in ``/airflow/www`` (#34971)
- Remove unused set from ``SchedulerJobRunner`` (#34810)
- Remove warning about ``max_tis per query > parallelism`` (#34742)
- Improve modules import in Airflow core by moving some of them into a type-checking block (#33755)
- Fix tests to respond to Python 3.12 handling of utcnow in sentry-sdk (#34946)
- Add ``connexion<3.0`` upper bound (#35218)
- Limit Airflow to ``< 3.12`` (#35123)
- update moto version (#34938)
- Limit WTForms to below ``3.1.0`` (#34943)

Doc Only Changes
""""""""""""""""
- Fix variables substitution in Airflow Documentation (#34462)
- Added example for defaults in ``conn.extras`` (#35165)
- Update datasets.rst issue with running example code (#35035)
- Remove ``mysql-connector-python`` from recommended MySQL driver (#34287)
- Fix syntax error in task dependency ``set_downstream`` example (#35075)
- Update documentation to enable test connection (#34905)
- Update docs errors.rst - Mention sentry "transport" configuration option (#34912)
- Update dags.rst to put SubDag deprecation note right after the SubDag section heading (#34925)
- Add info on getting variables and config in custom secrets backend (#34834)
- Document BaseExecutor interface in more detail to help users in writing custom executors (#34324)
- Fix broken link to ``airflow_local_settings.py`` template (#34826)
- Fixes python_callable function assignment context kwargs example in params.rst (#34759)
- Add missing multiple_outputs=True param in the TaskFlow example (#34812)
- Remove extraneous ``'>'`` in provider section name (#34813)
- Fix imports in extra link documentation (#34547)



Airflow 2.7.2 (2023-10-12)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes


Bug Fixes
"""""""""
- Check if the lower of provided values are sensitives in config endpoint (#34712)
- Add support for ZoneInfo and generic UTC to fix datetime serialization (#34683, #34804)
- Fix AttributeError: 'Select' object has no attribute 'count' during the airflow db migrate command (#34348)
- Make dry run optional for patch task instance  (#34568)
- Fix non deterministic datetime deserialization (#34492)
- Use iterative loop to look for mapped parent (#34622)
- Fix is_parent_mapped value by checking if any of the parent ``taskgroup`` is mapped (#34587)
- Avoid top-level airflow import to avoid circular dependency (#34586)
- Add more exemptions to lengthy metric list (#34531)
- Fix dag warning endpoint permissions (#34355)
- Fix task instance access issue in the batch endpoint (#34315)
- Correcting wrong time showing in grid view (#34179)
- Fix www ``cluster_activity`` view not loading due to ``standaloneDagProcessor`` templating (#34274)
- Set ``loglevel=DEBUG`` in 'Not syncing ``DAG-level`` permissions' (#34268)
- Make param validation consistent for DAG validation and triggering (#34248)
- Ensure details panel is shown when any tab is selected (#34136)
- Fix issues related to ``access_control={}`` (#34114)
- Fix not found ``ab_user`` table in the CLI session (#34120)
- Fix FAB-related logging format interpolation (#34139)
- Fix query bug in ``next_run_datasets_summary`` endpoint (#34143)
- Fix for TaskGroup toggles for duplicated labels (#34072)
- Fix the required permissions to clear a TI from the UI (#34123)
- Reuse ``_run_task_session`` in mapped ``render_template_fields`` (#33309)
- Fix scheduler logic to plan new dag runs by ignoring manual runs (#34027)
- Add missing audit logs for Flask actions add, edit and delete (#34090)
- Hide Irrelevant Dag Processor from Cluster Activity Page (#33611)
- Remove infinite animation for pinwheel, spin for 1.5s (#34020)
- Restore rendering of provider configuration with ``version_added`` (#34011)

Doc Only Changes
""""""""""""""""
- Clarify audit log permissions (#34815)
- Add explanation for Audit log users (#34814)
- Import ``AUTH_REMOTE_USER`` from FAB in WSGI middleware example (#34721)
- Add information about drop support MsSQL as DB Backend in the future (#34375)
- Document how to use the system's timezone database (#34667)
- Clarify what landing time means in doc (#34608)
- Fix screenshot in dynamic task mapping docs (#34566)
- Fix class reference in Public Interface documentation (#34454)
- Clarify var.value.get  and var.json.get usage (#34411)
- Schedule default value description (#34291)
- Docs for triggered_dataset_event (#34410)
- Add DagRun events (#34328)
- Provide tabular overview about trigger form param types (#34285)
- Add link to Amazon Provider Configuration in Core documentation (#34305)
- Add "security infrastructure" paragraph to security model (#34301)
- Change links to SQLAlchemy 1.4 (#34288)
- Add SBOM entry in security documentation (#34261)
- Added more example code for XCom push and pull (#34016)
- Add state utils to Public Airflow Interface (#34059)
- Replace markdown style link with rst style link (#33990)
- Fix broken link to the "UPDATING.md" file (#33583)

Misc/Internal
"""""""""""""
- Update min-sqlalchemy version to account for latest features used (#34293)
- Fix SesssionExemptMixin spelling (#34696)
- Restrict ``astroid`` version < 3 (#34658)
- Fail dag test if defer without triggerer (#34619)
- Fix connections exported output (#34640)
- Don't run isort when creating new alembic migrations (#34636)
- Deprecate numeric type python version in PythonVirtualEnvOperator (#34359)
- Refactor ``os.path.splitext`` to ``Path.*`` (#34352, #33669)
- Replace = by is for type comparison (#33983)
- Refactor integer division (#34180)
- Refactor: Simplify comparisons (#34181)
- Refactor: Simplify string generation (#34118)
- Replace unnecessary dict comprehension with dict() in core (#33858)
- Change "not all" to "any" for ease of readability (#34259)
- Replace assert by if...raise in code (#34250, #34249)
- Move default timezone to except block (#34245)
- Combine similar if logic in core (#33988)
- Refactor: Consolidate import and usage of random (#34108)
- Consolidate importing of os.path.* (#34060)
- Replace sequence concatenation by unpacking in Airflow core (#33934)
- Refactor unneeded 'continue' jumps around the repo (#33849, #33845, #33846, #33848, #33839, #33844, #33836, #33842)
- Remove [project] section from ``pyproject.toml`` (#34014)
- Move the try outside the loop when this is possible in Airflow core (#33975)
- Replace loop by any when looking for a positive value in core (#33985)
- Do not create lists we don't need (#33519)
- Remove useless string join from core (#33969)
- Add TCH001 and TCH002 rules to pre-commit to detect and move type checking modules (#33865)
- Add cancel_trigger_ids to to_cancel dequeue in batch (#33944)
- Avoid creating unnecessary list when parsing stats datadog tags (#33943)
- Replace dict.items by dict.values when key is not used in core (#33940)
- Replace lambdas with comprehensions (#33745)
- Improve modules import in Airflow core by some of them into a type-checking block (#33755)
- Refactor: remove unused state - SHUTDOWN (#33746, #34063, #33893)
- Refactor: Use in-place .sort() (#33743)
- Use literal dict instead of calling dict() in Airflow core (#33762)
- remove unnecessary map and rewrite it using list in Airflow core (#33764)
- Replace lambda by a def method in Airflow core (#33758)
- Replace type func by ``isinstance`` in fab_security manager (#33760)
- Replace single quotes by double quotes in all Airflow modules (#33766)
- Merge multiple ``isinstance`` calls for the same object in a single call (#33767)
- Use a single  statement with multiple contexts instead of nested  statements in core (#33769)
- Refactor: Use f-strings (#33734, #33455)
- Refactor: Use random.choices (#33631)
- Use ``str.splitlines()`` to split lines (#33592)
- Refactor: Remove useless str() calls (#33629)
- Refactor: Improve detection of duplicates and list sorting (#33675)
- Simplify conditions on ``len()`` (#33454)


Airflow 2.7.1 (2023-09-07)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

CronTriggerTimetable is now less aggressive when trying to skip a run (#33404)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

When setting ``catchup=False``, CronTriggerTimetable no longer skips a run if
the scheduler does not query the timetable immediately after the previous run
has been triggered.

This should not affect scheduling in most cases, but can change the behaviour if
a DAG is paused-unpaused to manually skip a run. Previously, the timetable (with
``catchup=False``) would only start a run after a DAG is unpaused, but with this
change, the scheduler would try to look at little bit back to schedule the
previous run that covers a part of the period when the DAG was paused. This
means you will need to keep a DAG paused longer (namely, for the entire cron
period to pass) to really skip a run.

Note that this is also the behaviour exhibited by various other cron-based
scheduling tools, such as ``anacron``.

``conf.set()`` becomes case insensitive to match ``conf.get()`` behavior (#33452)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Also, ``conf.get()`` will now break if used with non-string parameters.

``conf.set(section, key, value)`` used to be case sensitive, i.e. ``conf.set("SECTION", "KEY", value)``
and ``conf.set("section", "key", value)`` were stored as two distinct configurations.
This was inconsistent with the behavior of ``conf.get(section, key)``, which was always converting the section and key to lower case.

As a result, configuration options set with upper case characters in the section or key were unreachable.
That's why we are now converting section and key to lower case in ``conf.set`` too.

We also changed a bit the behavior of ``conf.get()``. It used to allow objects that are not strings in the section or key.
Doing this will now result in an exception. For instance, ``conf.get("section", 123)`` needs to be replaced with ``conf.get("section", "123")``.

Bug Fixes
"""""""""
- Ensure that tasks wait for running indirect setup (#33903)
- Respect "soft_fail" for core async sensors (#33403)
- Differentiate 0 and unset as a default param values (#33965)
- Raise 404 from Variable PATCH API if variable is not found (#33885)
- Fix ``MappedTaskGroup`` tasks not respecting upstream dependency (#33732)
- Add limit 1 if required first value from query result (#33672)
- Fix UI DAG counts including deleted DAGs (#33778)
- Fix cleaning zombie RESTARTING tasks (#33706)
- ``SECURITY_MANAGER_CLASS`` should be a reference to class, not a string (#33690)
- Add back ``get_url_for_login`` in security manager (#33660)
- Fix ``2.7.0 db`` migration job errors (#33652)
- Set context inside templates (#33645)
- Treat dag-defined access_control as authoritative if defined (#33632)
- Bind engine before attempting to drop archive tables (#33622)
- Add a fallback in case no first name and last name are set (#33617)
- Sort data before ``groupby`` in TIS duration calculation (#33535)
- Stop adding values to rendered templates UI when there is no dagrun (#33516)
- Set strict to True when parsing dates in webserver views (#33512)
- Use ``dialect.name`` in custom SA types (#33503)
- Do not return ongoing dagrun when a ``end_date`` is less than ``utcnow`` (#33488)
- Fix a bug in ``formatDuration`` method (#33486)
- Make ``conf.set`` case insensitive (#33452)
- Allow timetable to slightly miss catchup cutoff (#33404)
- Respect ``soft_fail`` argument when ``poke`` is called (#33401)
- Create a new method used to resume the task in order to implement specific logic for operators (#33424)
- Fix DagFileProcessor interfering with dags outside its ``processor_subdir`` (#33357)
- Remove the unnecessary ``<br>`` text in Provider's view (#33326)
- Respect ``soft_fail`` argument when ExternalTaskSensor runs in deferrable mode (#33196)
- Fix handling of default value and serialization of Param class (#33141)
- Check if the dynamically-added index is in the table schema before adding (#32731)
- Fix rendering the mapped parameters when using ``expand_kwargs`` method (#32272)
- Fix dependencies for celery and opentelemetry for Python 3.8 (#33579)

Misc/Internal
"""""""""""""
- Bring back ``Pydantic`` 1 compatibility (#34081, #33998)
- Use a trimmed version of README.md for PyPI (#33637)
- Upgrade to ``Pydantic`` 2 (#33956)
- Reorganize ``devel_only`` extra in Airflow's setup.py (#33907)
- Bumping ``FAB`` to ``4.3.4`` in order to fix issues with filters (#33931)
- Add minimum requirement for ``sqlalchemy to 1.4.24`` (#33892)
- Update version_added field for configs in config file (#33509)
- Replace ``OrderedDict`` with plain dict (#33508)
- Consolidate import and usage of itertools (#33479)
- Static check fixes (#33462)
- Import utc from datetime and normalize its import (#33450)
- D401 Support (#33352, #33339, #33337, #33336, #33335, #33333, #33338)
- Fix some missing type hints (#33334)
- D205 Support - Stragglers (#33301, #33298, #33297)
- Refactor: Simplify code (#33160, #33270, #33268, #33267, #33266, #33264, #33292, #33453, #33476, #33567,
  #33568, #33480, #33753, #33520, #33623)
- Fix ``Pydantic`` warning about ``orm_mode`` rename (#33220)
- Add MySQL 8.1 to supported versions. (#33576)
- Remove ``Pydantic`` limitation for version < 2 (#33507)

Doc only changes
"""""""""""""""""
- Add documentation explaining template_ext (and how to override it) (#33735)
- Explain how users can check if python code is top-level (#34006)
- Clarify that DAG authors can also run code in DAG File Processor (#33920)
- Fix broken link in Modules Management page (#33499)
- Fix secrets backend docs (#33471)
- Fix config description for base_log_folder (#33388)


Airflow 2.7.0 (2023-08-18)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Remove Python 3.7 support (#30963)
""""""""""""""""""""""""""""""""""
As of now, Python 3.7 is no longer supported by the Python community.
Therefore, to use Airflow 2.7.0, you must ensure your Python version is
either 3.8, 3.9, 3.10, or 3.11.

Old Graph View is removed (#32958)
""""""""""""""""""""""""""""""""""
The old Graph View is removed. The new Graph View is the default view now.

The trigger UI form is skipped in web UI if no parameters are defined in a DAG (#33351)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If you are using ``dag_run.conf`` dictionary and web UI JSON entry to run your DAG you should either:

* `Add params to your DAG <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html#use-params-to-provide-a-trigger-ui-form>`_
* Enable the new configuration ``show_trigger_form_if_no_params`` to bring back old behaviour

The "db init", "db upgrade" commands and "[database] load_default_connections" configuration options are deprecated (#33136).
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Instead, you should use "airflow db migrate" command to create or upgrade database. This command will not create default connections.
In order to create default connections you need to run "airflow connections create-default-connections" explicitly,
after running "airflow db migrate".

In case of SMTP SSL connection, the context now uses the "default" context (#33070)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
The "default" context is Python's ``default_ssl_contest`` instead of previously used "none". The
``default_ssl_context`` provides a balance between security and compatibility but in some cases,
when certificates are old, self-signed or misconfigured, it might not work. This can be configured
by setting "ssl_context" in "email" configuration of Airflow.

Setting it to "none" brings back the "none" setting that was used in Airflow 2.6 and before,
but it is not recommended due to security reasons ad this setting disables validation of certificates and allows MITM attacks.

Disable default allowing the testing of connections in UI, API and CLI(#32052)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
For security reasons, the test connection functionality is disabled by default across Airflow UI,
API and CLI. The availability of the functionality can be controlled by the
``test_connection`` flag in the ``core`` section of the Airflow
configuration (``airflow.cfg``). It can also be controlled by the
environment variable ``AIRFLOW__CORE__TEST_CONNECTION``.

The following values are accepted for this config param:
1. ``Disabled``: Disables the test connection functionality and
disables the Test Connection button in the UI.

This is also the default value set in the Airflow configuration.
2. ``Enabled``: Enables the test connection functionality and
activates the Test Connection button in the UI.

3. ``Hidden``: Disables the test connection functionality and
hides the Test Connection button in UI.

For more information on capabilities of users, see the documentation:
https://airflow.apache.org/docs/apache-airflow/stable/security/security_model.html#capabilities-of-authenticated-ui-users
It is strongly advised to **not** enable the feature until you make sure that only
highly trusted UI/API users have "edit connection" permissions.

The ``xcomEntries`` API disables support for the ``deserialize`` flag by default (#32176)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
For security reasons, the ``/dags/*/dagRuns/*/taskInstances/*/xcomEntries/*``
API endpoint now disables the ``deserialize`` option to deserialize arbitrary
XCom values in the webserver. For backward compatibility, server admins may set
the ``[api] enable_xcom_deserialize_support`` config to *True* to enable the
flag and restore backward compatibility.

However, it is strongly advised to **not** enable the feature, and perform
deserialization at the client side instead.

Change of the default Celery application name (#32526)
""""""""""""""""""""""""""""""""""""""""""""""""""""""
Default name of the Celery application changed from ``airflow.executors.celery_executor`` to ``airflow.providers.celery.executors.celery_executor``.

You should change both your configuration and Health check command to use the new name:
  * in configuration (``celery_app_name`` configuration in ``celery`` section) use ``airflow.providers.celery.executors.celery_executor``
  * in your Health check command use ``airflow.providers.celery.executors.celery_executor.app``


The default value for ``scheduler.max_tis_per_query`` is changed from 512 to 16 (#32572)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
This change is expected to make the Scheduler more responsive.

``scheduler.max_tis_per_query`` needs to be lower than ``core.parallelism``.
If both were left to their default value previously, the effective default value of ``scheduler.max_tis_per_query`` was 32
(because it was capped at ``core.parallelism``).

To keep the behavior as close as possible to the old config, one can set ``scheduler.max_tis_per_query = 0``,
in which case it'll always use the value of ``core.parallelism``.

Some executors have been moved to corresponding providers (#32767)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
In order to use the executors, you need to install the providers:

* for Celery executors you need to install ``apache-airflow-providers-celery`` package >= 3.3.0
* for Kubernetes executors you need to install ``apache-airflow-providers-cncf-kubernetes`` package >= 7.4.0
* For Dask executors you need to install ``apache-airflow-providers-daskexecutor`` package in any version

You can achieve it also by installing airflow with ``[celery]``, ``[cncf.kubernetes]``, ``[daskexecutor]`` extras respectively.

Users who base their images on the ``apache/airflow`` reference image (not slim) should be unaffected - the base
reference image comes with all the three providers installed.

Improvement Changes
^^^^^^^^^^^^^^^^^^^

PostgreSQL only improvement: Added index on taskinstance table (#30762)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
This index seems to have great positive effect in a setup with tens of millions such rows.

New Features
""""""""""""
- Add OpenTelemetry to Airflow (`AIP-49 <https://github.com/apache/airflow/pulls?q=is%3Apr+is%3Amerged+label%3AAIP-49+milestone%3A%22Airflow+2.7.0%22>`_)
- Trigger Button - Implement Part 2 of AIP-50 (#31583)
- Removing Executor Coupling from Core Airflow (`AIP-51 <https://github.com/apache/airflow/pulls?q=is%3Apr+is%3Amerged+label%3AAIP-51+milestone%3A%22Airflow+2.7.0%22>`_)
- Automatic setup and teardown tasks (`AIP-52 <https://github.com/apache/airflow/pulls?q=is%3Apr+is%3Amerged+label%3AAIP-52+milestone%3A%22Airflow+2.7.0%22>`_)
- OpenLineage in Airflow (`AIP-53 <https://github.com/apache/airflow/pulls?q=is%3Apr+is%3Amerged+milestone%3A%22Airflow+2.7.0%22+label%3Aprovider%3Aopenlineage>`_)
- Experimental: Add a cache to Variable and Connection when called at dag parsing time (#30259)
- Enable pools to consider deferred tasks (#32709)
- Allows to choose SSL context for SMTP connection (#33070)
- New gantt tab (#31806)
- Load plugins from providers (#32692)
- Add ``BranchExternalPythonOperator`` (#32787, #33360)
- Add option for storing configuration description in providers (#32629)
- Introduce Heartbeat Parameter to Allow ``Per-LocalTaskJob`` Configuration (#32313)
- Add Executors discovery and documentation (#32532)
- Add JobState for job state constants (#32549)
- Add config to disable the 'deserialize' XCom API flag (#32176)
- Show task instance in web UI by custom operator name (#31852)
- Add default_deferrable config (#31712)
- Introducing ``AirflowClusterPolicySkipDag`` exception (#32013)
- Use ``reactflow`` for datasets graph (#31775)
- Add an option to load the dags from db for command tasks run (#32038)
- Add version of ``chain`` which doesn't require matched lists (#31927)
- Use operator_name instead of task_type in UI (#31662)
- Add ``--retry`` and ``--retry-delay`` to ``airflow db check`` (#31836)
- Allow skipped task state task_instance_schema.py (#31421)
- Add a new config for celery result_backend engine options (#30426)
- UI Add Cluster Activity Page (#31123, #32446)
- Adding keyboard shortcuts to common actions (#30950)
- Adding more information to kubernetes executor logs (#29929)
- Add support for configuring custom alembic file (#31415)
- Add running and failed status tab for DAGs on the UI (#30429)
- Add multi-select, proposals and labels for trigger form (#31441)
- Making webserver config customizable (#29926)
- Render DAGCode in the Grid View as a tab (#31113)
- Add rest endpoint to get option of configuration (#31056)
- Add ``section`` query param in get config rest API (#30936)
- Create metrics to track ``Scheduled->Queued->Running`` task state transition times (#30612)
- Mark Task Groups as Success/Failure (#30478)
- Add CLI command to list the provider trigger info (#30822)
- Add Fail Fast feature for DAGs (#29406)

Improvements
""""""""""""
- Improve graph nesting logic (#33421)
- Configurable health check threshold for triggerer (#33089, #33084)
- add dag_run_ids and task_ids filter for the batch task instance API endpoint (#32705)
- Ensure DAG-level references are filled on unmap (#33083)
- Add support for arrays of different data types in the Trigger Form UI (#32734)
- Always show gantt and code tabs (#33029)
- Move listener success hook to after SQLAlchemy commit (#32988)
- Rename ``db upgrade`` to ``db migrate`` and add ``connections create-default-connections`` (#32810, #33136)
- Remove old gantt chart and redirect to grid views gantt tab (#32908)
- Adjust graph zoom based on selected task (#32792)
- Call listener on_task_instance_running after rendering templates (#32716)
- Display execution_date in graph view task instance tooltip. (#32527)
- Allow configuration to be contributed by providers (#32604, #32755, #32812)
- Reduce default for max TIs per query, enforce ``<=`` parallelism (#32572)
- Store config description in Airflow configuration object (#32669)
- Use ``isdisjoint`` instead of ``not intersection`` (#32616)
- Speed up calculation of leaves and roots for task groups (#32592)
- Kubernetes Executor Load Time Optimizations (#30727)
- Save DAG parsing time if dag is not schedulable (#30911)
- Updates health check endpoint to include ``dag_processor`` status. (#32382)
- Disable default allowing the testing of connections in UI, API and CLI (#32052, #33342)
- Fix config var types under the scheduler section (#32132)
- Allow to sort Grid View alphabetically (#32179)
- Add hostname to triggerer metric ``[triggers.running]`` (#32050)
- Improve DAG ORM cleanup code (#30614)
- ``TriggerDagRunOperator``: Add ``wait_for_completion`` to ``template_fields`` (#31122)
- Open links in new tab that take us away from Airflow UI (#32088)
- Only show code tab when a task is not selected (#31744)
- Add descriptions for celery and dask cert configs (#31822)
- ``PythonVirtualenvOperator`` termination log in alert (#31747)
- Migration of all DAG details to existing grid view dag details panel (#31690)
- Add a diagram to help visualize timer metrics (#30650)
- Celery Executor load time optimizations (#31001)
- Update code style for ``airflow db`` commands to SQLAlchemy 2.0 style (#31486)
- Mark uses of md5 as "not-used-for-security" in FIPS environments (#31171)
- Add pydantic support to serde (#31565)
- Enable search in note column in DagRun and TaskInstance (#31455)
- Save scheduler execution time by adding new Index idea for dag_run (#30827)
- Save scheduler execution time by caching dags (#30704)
- Support for sorting DAGs by Last Run Date in the web UI (#31234)
- Better typing for Job and JobRunners (#31240)
- Add sorting logic by created_date for fetching triggers (#31151)
- Remove DAGs.can_create on access control doc, adjust test fixture (#30862)
- Split Celery logs into stdout/stderr (#30485)
- Decouple metrics clients and ``validators`` into their own modules (#30802)
- Description added for pagination in ``get_log`` api (#30729)
- Optimize performance of scheduling mapped tasks (#30372)
- Add sentry transport configuration option (#30419)
- Better message on deserialization error (#30588)

Bug Fixes
"""""""""
- Remove user sessions when resetting password (#33347)
- ``Gantt chart:`` Use earliest/oldest ti dates if different than dag run start/end (#33215)
- Fix ``virtualenv`` detection for Python ``virtualenv`` operator (#33223)
- Correctly log when there are problems trying to ``chmod`` ``airflow.cfg`` (#33118)
- Pass app context to webserver_config.py (#32759)
- Skip served logs for non-running task try (#32561)
- Fix reload gunicorn workers (#32102)
- Fix future DagRun rarely triggered by race conditions when ``max_active_runs`` reached its upper limit. (#31414)
- Fix BaseOperator ``get_task_instances`` query (#33054)
- Fix issue with using the various state enum value in logs (#33065)
- Use string concatenation to prepend base URL for log_url (#33063)
- Update graph nodes with operator style attributes (#32822)
- Affix webserver access_denied warning to be configurable (#33022)
- Only load task action modal if user can edit (#32992)
- OpenAPI Spec fix nullable alongside ``$ref`` (#32887)
- Make the decorators of ``PythonOperator`` sub-classes extend its decorator (#32845)
- Fix check if ``virtualenv`` is installed in ``PythonVirtualenvOperator`` (#32939)
- Unwrap Proxy before checking ``__iter__`` in is_container() (#32850)
- Override base log folder by using task handler's base_log_folder (#32781)
- Catch arbitrary exception from run_job to prevent zombie scheduler (#32707)
- Fix depends_on_past work for dynamic tasks (#32397)
- Sort extra_links for predictable order in UI. (#32762)
- Fix prefix group false graph (#32764)
- Fix bad delete logic for dagruns (#32684)
- Fix bug in prune_dict where empty dict and list would be removed even in strict mode (#32573)
- Add explicit browsers list and correct rel for blank target links (#32633)
- Handle returned None when multiple_outputs is True (#32625)
- Fix returned value when ShortCircuitOperator condition is falsy and there is not downstream tasks (#32623)
- Fix returned value when ShortCircuitOperator condition is falsy (#32569)
- Fix rendering of ``dagRunTimeout`` (#32565)
- Fix permissions on ``/blocked`` endpoint (#32571)
- Bugfix, prevent force of unpause on trigger DAG (#32456)
- Fix data interval in ``cli.dags.trigger`` command output (#32548)
- Strip ``whitespaces`` from airflow connections form (#32292)
- Add timedelta support for applicable arguments of sensors (#32515)
- Fix incorrect default on ``readonly`` property in our API (#32510)
- Add xcom map_index as a filter to xcom endpoint (#32453)
- Fix CLI commands when custom timetable is used (#32118)
- Use WebEncoder to encode DagRun.conf in DagRun's list view (#32385)
- Fix logic of the skip_all_except method (#31153)
- Ensure dynamic tasks inside dynamic task group only marks the (#32354)
- Handle the cases that webserver.expose_config is set to non-sensitive-only instead of boolean value (#32261)
- Add retry functionality for handling process termination caused by database network issues (#31998)
- Adapt Notifier for sla_miss_callback (#31887)
- Fix XCOM view (#31807)
- Fix for "Filter dags by tag" flickering on initial load of dags.html (#31578)
- Fix where expanding ``resizer`` would not expanse grid view (#31581)
- Fix MappedOperator-BaseOperator attr sync check (#31520)
- Always pass named ``type_`` arg to drop_constraint (#31306)
- Fix bad ``drop_constraint`` call in migrations (#31302)
- Resolving problems with redesigned grid view (#31232)
- Support ``requirepass`` redis sentinel (#30352)
- Fix webserver crash when calling get ``/config`` (#31057)

Misc/Internal
"""""""""""""
- Modify pathspec version restriction (#33349)
- Refactor: Simplify code in ``dag_processing`` (#33161)
- For now limit ``Pydantic`` to ``< 2.0.0`` (#33235)
- Refactor: Simplify code in models (#33181)
- Add elasticsearch group to pre-2.7 defaults (#33166)
- Refactor: Simplify dict manipulation in airflow/cli (#33159)
- Remove redundant dict.keys() call (#33158)
- Upgrade ruff to latest 0.0.282 version in pre-commits (#33152)
- Move openlineage configuration to provider (#33124)
- Replace State by TaskInstanceState in Airflow executors (#32627)
- Get rid of Python 2 numeric relics (#33050)
- Remove legacy dag code (#33058)
- Remove legacy task instance modal (#33060)
- Remove old graph view (#32958)
- Move CeleryExecutor to the celery provider (#32526, #32628)
- Move all k8S classes to ``cncf.kubernetes`` provider (#32767, #32891)
- Refactor existence-checking SQL to helper (#32790)
- Extract Dask executor to new daskexecutor provider (#32772)
- Remove atlas configuration definition (#32776)
- Add Redis task handler (#31855)
- Move writing configuration for webserver to main (webserver limited) (#32766)
- Improve getting the query count in Airflow API endpoints (#32630)
- Remove click upper bound (#32634)
- Add D400 ``pydocstyle`` check - core Airflow only (#31297)
- D205 Support (#31742, #32575, #32213, #32212, #32591, #32449, #32450)
- Bump word-wrap from ``1.2.3 to 1.2.4`` in ``/airflow/www`` (#32680)
- Strong-type all single-state enum values (#32537)
- More strong typed state conversion (#32521)
- SQL query improvements in utils/db.py (#32518)
- Bump semver from ``6.3.0 to 6.3.1`` in ``/airflow/www`` (#32506)
- Bump jsonschema version to ``4.18.0`` (#32445)
- Bump ``stylelint`` from ``13.13.1 to 15.10.1`` in ``/airflow/www`` (#32435)
- Bump tough-cookie from ``4.0.0 to 4.1.3`` in ``/airflow/www`` (#32443)
- upgrade flask-appbuilder (#32054)
- Support ``Pydantic`` 2 (#32366)
- Limit click until we fix mypy issues (#32413)
- A couple of minor cleanups (#31890)
- Replace State usages with strong-typed ``enums`` (#31735)
- Upgrade ruff to ``0.272`` (#31966)
- Better error message when serializing callable without name (#31778)
- Improve the views module a bit (#31661)
- Remove ``asynctest`` (#31664)
- Refactor sqlalchemy queries to ``2.0`` style (#31569, #31772, #32350, #32339, #32474, #32645)
- Remove Python ``3.7`` support (#30963)
- Bring back min-airflow-version for preinstalled providers (#31469)
- Docstring improvements (#31375)
- Improve typing in SchedulerJobRunner (#31285)
- Upgrade ruff to ``0.0.262`` (#30809)
- Upgrade to MyPy ``1.2.0`` (#30687)

Docs only changes
"""""""""""""""""
- Clarify UI user types in security model (#33021)
- Add links to ``DAGRun / DAG / Task`` in templates-ref.rst (#33013)
- Add docs of how to test for DAG Import Errors (#32811)
- Clean-up of our new security page (#32951)
- Cleans up Extras reference page (#32954)
- Update Dag trigger API and command docs (#32696)
- Add deprecation info to the Airflow modules and classes docstring (#32635)
- Formatting installation doc to improve readability (#32502)
- Fix triggerer HA doc (#32454)
- Add type annotation to code examples (#32422)
- Document cron and delta timetables (#32392)
- Update index.rst doc to correct grammar (#32315)
- Fixing small typo in python.py (#31474)
- Separate out and clarify policies for providers (#30657)
- Fix docs: add an "apache" prefix to pip install (#30681)


Airflow 2.6.3 (2023-07-10)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default allowed pattern of a run_id has been changed to ``^[A-Za-z0-9_.~:+-]+$`` (#32293).
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Previously, there was no validation on the run_id string. There is now a validation regex that
can be set by configuring ``allowed_run_id_pattern`` in ``scheduler`` section.

Bug Fixes
"""""""""
- Use linear time regular expressions (#32303)
- Fix triggerers alive check and add a new conf for triggerer heartbeat rate (#32123)
- Catch the exception that triggerer initialization failed (#31999)
- Hide sensitive values from extra in connection edit form (#32309)
- Sanitize ``DagRun.run_id`` and allow flexibility (#32293)
- Add triggerer canceled log (#31757)
- Fix try number shown in the task view (#32361)
- Retry transactions on occasional deadlocks for rendered fields (#32341)
- Fix behaviour of LazyDictWithCache when import fails (#32248)
- Remove ``executor_class`` from Job - fixing backfill for custom executors (#32219)
- Fix bugged singleton implementation (#32218)
- Use ``mapIndex`` to display extra links per mapped task. (#32154)
- Ensure that main triggerer thread exits if the async thread fails (#32092)
- Use ``re2`` for matching untrusted regex (#32060)
- Render list items in rendered fields view (#32042)
- Fix hashing of ``dag_dependencies`` in serialized dag (#32037)
- Return ``None`` if an XComArg fails to resolve in a multiple_outputs Task (#32027)
- Check for DAG ID in query param from url as well as kwargs (#32014)
- Flash an error message instead of failure in ``rendered-templates`` when map index is not found (#32011)
- Fix ``ExternalTaskSensor`` when there is no task group TIs for the current execution date (#32009)
- Fix number param html type in trigger template (#31980, #31946)
- Fix masking nested variable fields (#31964)
- Fix ``operator_extra_links`` property serialization in mapped tasks (#31904)
- Decode old-style nested Xcom value (#31866)
- Add a check for trailing slash in webserver base_url (#31833)
- Fix connection uri parsing when the host includes a scheme (#31465)
- Fix database session closing with ``xcom_pull`` and ``inlets`` (#31128)
- Fix DAG's ``on_failure_callback`` is not invoked when task failed during testing dag. (#30965)
- Fix airflow module version check when using ``ExternalPythonOperator`` and debug logging level (#30367)

Misc/Internal
"""""""""""""
- Fix ``task.sensor`` annotation in type stub (#31954)
- Limit ``Pydantic`` to ``< 2.0.0`` until we solve ``2.0.0`` incompatibilities (#32312)
- Fix ``Pydantic`` 2 pickiness about model definition (#32307)

Doc only changes
""""""""""""""""
- Add explanation about tag creation and cleanup (#32406)
- Minor updates to docs (#32369, #32315, #32310, #31794)
- Clarify Listener API behavior (#32269)
- Add information for users who ask for requirements (#32262)
- Add links to DAGRun / DAG / Task in Templates Reference (#32245)
- Add comment to warn off a potential wrong fix (#32230)
- Add a note that we'll need to restart triggerer to reflect any trigger change (#32140)
- Adding missing hyperlink to the tutorial documentation (#32105)
- Added difference between Deferrable and Non-Deferrable Operators (#31840)
- Add comments explaining need for special "trigger end" log message (#31812)
- Documentation update on Plugin updates. (#31781)
- Fix SemVer link in security documentation (#32320)
- Update security model of Airflow (#32098)
- Update references to restructured documentation from Airflow core (#32282)
- Separate out advanced logging configuration (#32131)
- Add ``™`` to Airflow in prominent places (#31977)


Airflow 2.6.2 (2023-06-17)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^
- Cascade update of TaskInstance to TaskMap table (#31445)
- Fix Kubernetes executors detection of deleted pods (#31274)
- Use keyword parameters for migration methods for mssql (#31309)
- Control permissibility of driver config in extra from airflow.cfg (#31754)
- Fixing broken links in openapi/v1.yaml (#31619)
- Hide old alert box when testing connection with different value (#31606)
- Add TriggererStatus to OpenAPI spec (#31579)
- Resolving issue where Grid won't un-collapse when Details is collapsed (#31561)
- Fix sorting of tags (#31553)
- Add the missing ``map_index`` to the xcom key when skipping downstream tasks (#31541)
- Fix airflow users delete CLI command (#31539)
- Include triggerer health status in Airflow ``/health`` endpoint (#31529)
- Remove dependency already registered for this task warning (#31502)
- Use kube_client over default CoreV1Api for deleting pods (#31477)
- Ensure min backoff in base sensor is at least 1 (#31412)
- Fix ``max_active_tis_per_dagrun`` for Dynamic Task Mapping (#31406)
- Fix error handling when pre-importing modules in DAGs (#31401)
- Fix dropdown default and adjust tutorial to use 42 as default for proof (#31400)
- Fix crash when clearing run with task from normal to mapped (#31352)
- Make BaseJobRunner a generic on the job class (#31287)
- Fix ``url_for_asset`` fallback and 404 on DAG Audit Log (#31233)
- Don't present an undefined execution date (#31196)
- Added spinner activity while the logs load (#31165)
- Include rediss to the list of supported URL schemes (#31028)
- Optimize scheduler by skipping "non-schedulable" DAGs (#30706)
- Save scheduler execution time during search for queued dag_runs (#30699)
- Fix ExternalTaskSensor to work correctly with task groups (#30742)
- Fix DAG.access_control can't sync when clean access_control (#30340)
- Fix failing get_safe_url tests for latest Python 3.8 and 3.9 (#31766)
- Fix typing for POST user endpoint (#31767)
- Fix wrong update for nested group default args (#31776)
- Fix overriding ``default_args`` in nested task groups (#31608)
- Mark ``[secrets] backend_kwargs`` as a sensitive config (#31788)
- Executor events are not always "exited" here (#30859)
- Validate connection IDs (#31140)

Misc/Internal
"""""""""""""
- Add Python 3.11 support (#27264)
- Replace unicodecsv with standard csv library (#31693)
- Bring back unicodecsv as dependency of Airflow (#31814)
- Remove found_descendents param from get_flat_relative_ids (#31559)
- Fix typing in external task triggers (#31490)
- Wording the next and last run DAG columns better (#31467)
- Skip auto-document things with :meta private: (#31380)
- Add an example for sql_alchemy_connect_args conf (#31332)
- Convert dask upper-binding into exclusion (#31329)
- Upgrade FAB to 4.3.1 (#31203)
- Added metavar and choices to --state flag in airflow dags list-jobs CLI for suggesting valid state arguments. (#31308)
- Use only one line for tmp dir log (#31170)
- Rephrase comment in setup.py (#31312)
- Add fullname to owner on logging (#30185)
- Make connection id validation consistent across interface (#31282)
- Use single source of truth for sensitive config items (#31820)

Doc only changes
^^^^^^^^^^^^^^^^
- Add docstring and signature for _read_remote_logs (#31623)
- Remove note about triggerer being 3.7+ only (#31483)
- Fix version support information (#31468)
- Add missing BashOperator import to documentation example (#31436)
- Fix task.branch error caused by incorrect initial parameter (#31265)
- Update callbacks documentation (errors and context) (#31116)
- Add an example for dynamic task mapping with non-TaskFlow operator (#29762)
- Few doc fixes - links, grammar and wording (#31719)
- Add description in a few more places about adding airflow to pip install (#31448)
- Fix table formatting in docker build documentation (#31472)
- Update documentation for constraints installation (#31882)

Airflow 2.6.1 (2023-05-16)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Clarifications of the external Health Check mechanism and using ``Job`` classes (#31277).
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

In the past SchedulerJob and other ``*Job`` classes are known to have been used to perform
external health checks for Airflow components. Those are, however, Airflow DB ORM related classes.
The DB models and database structure of Airflow are considered as internal implementation detail, following
`public interface <https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html>`_).
Therefore, they should not be used for external health checks. Instead, you should use the
``airflow jobs check`` CLI command (introduced in Airflow 2.1) for that purpose.

Bug Fixes
^^^^^^^^^
- Fix calculation of health check threshold for SchedulerJob (#31277)
- Fix timestamp parse failure for k8s executor pod tailing (#31175)
- Make sure that DAG processor job row has filled value in ``job_type`` column (#31182)
- Fix section name reference for ``api_client_retry_configuration`` (#31174)
- Ensure the KPO runs pod mutation hooks correctly (#31173)
- Remove worrying log message about redaction from the OpenLineage plugin (#31149)
- Move ``interleave_timestamp_parser`` config to the logging section (#31102)
- Ensure that we check worker for served logs if no local or remote logs found (#31101)
- Fix ``MappedTaskGroup`` import in taskinstance file (#31100)
- Format DagBag.dagbag_report() Output (#31095)
- Mask task attribute on task detail view (#31125)
- Fix template error when iterating None value and fix params documentation (#31078)
- Fix ``apache-hive`` extra so it installs the correct package (#31068)
- Fix issue with zip files in DAGs folder when pre-importing Airflow modules (#31061)
- Move TaskInstanceKey to a separate file to fix circular import (#31033, #31204)
- Fix deleting DagRuns and TaskInstances that have a note (#30987)
- Fix ``airflow providers get`` command output (#30978)
- Fix Pool schema in the OpenAPI spec (#30973)
- Add support for dynamic tasks with template fields that contain ``pandas.DataFrame`` (#30943)
- Use the Task Group explicitly passed to 'partial' if any (#30933)
- Fix ``order_by`` request in list DAG rest api (#30926)
- Include node height/width in center-on-task logic (#30924)
- Remove print from dag trigger command (#30921)
- Improve task group UI in new graph (#30918)
- Fix mapped states in grid view (#30916)
- Fix problem with displaying graph (#30765)
- Fix backfill KeyError when try_number out of sync (#30653)
- Re-enable clear and setting state in the TaskInstance UI (#30415)
- Prevent DagRun's ``state`` and ``start_date`` from being reset when clearing a task in a running DagRun (#30125)

Misc/Internal
"""""""""""""
- Upper bind dask until they solve a side effect in their test suite (#31259)
- Show task instances affected by clearing in a table (#30633)
- Fix missing models in API documentation (#31021)

Doc only changes
""""""""""""""""
- Improve description of the ``dag_processing.processes`` metric (#30891)
- Improve Quick Start instructions (#30820)
- Add section about missing task logs to the FAQ (#30717)
- Mount the ``config`` directory in docker compose (#30662)
- Update ``version_added`` config field for ``might_contain_dag`` and ``metrics_allow_list`` (#30969)


Airflow 2.6.0 (2023-04-30)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default permissions of file task handler log directories and files has been changed to "owner + group" writeable (#29506).
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Default setting handles case where impersonation is needed and both users (airflow and the impersonated user)
have the same group set as main group. Previously the default was also other-writeable and the user might choose
to use the other-writeable setting if they wish by configuring ``file_task_handler_new_folder_permissions``
and ``file_task_handler_new_file_permissions`` in ``logging`` section.

SLA callbacks no longer add files to the dag processor manager's queue (#30076)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
This stops SLA callbacks from keeping the dag processor manager permanently busy. It means reduced CPU,
and fixes issues where SLAs stop the system from seeing changes to existing dag files. Additional metrics added to help track queue state.

The ``cleanup()`` method in BaseTrigger is now defined as asynchronous (following async/await) pattern (#30152).
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
This is potentially a breaking change for any custom trigger implementations that override the ``cleanup()``
method and uses synchronous code, however using synchronous operations in cleanup was technically wrong,
because the method was executed in the main loop of the Triggerer and it was introducing unnecessary delays
impacting other triggers. The change is unlikely to affect any existing trigger implementations.

The gauge ``scheduler.tasks.running`` no longer exist (#30374)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
The gauge has never been working and its value has always been 0. Having an accurate
value for this metric is complex so it has been decided that removing this gauge makes
more sense than fixing it with no certainty of the correctness of its value.

Consolidate handling of tasks stuck in queued under new ``task_queued_timeout`` config (#30375)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Logic for handling tasks stuck in the queued state has been consolidated, and the all configurations
responsible for timing out stuck queued tasks have been deprecated and merged into
``[scheduler] task_queued_timeout``. The configurations that have been deprecated are
``[kubernetes] worker_pods_pending_timeout``, ``[celery] stalled_task_timeout``, and
``[celery] task_adoption_timeout``. If any of these configurations are set, the longest timeout will be
respected. For example, if ``[celery] stalled_task_timeout`` is 1200, and ``[scheduler] task_queued_timeout``
is 600, Airflow will set ``[scheduler] task_queued_timeout`` to 1200.

Improvement Changes
^^^^^^^^^^^^^^^^^^^

Display only the running configuration in configurations view (#28892)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
The configurations view now only displays the running configuration. Previously, the default configuration
was displayed at the top but it was not obvious whether this default configuration was overridden or not.
Subsequently, the non-documented endpoint ``/configuration?raw=true`` is deprecated and will be removed in
Airflow 3.0. The HTTP response now returns an additional ``Deprecation`` header. The ``/config`` endpoint on
the REST API is the standard way to fetch Airflow configuration programmatically.

Explicit skipped states list for ExternalTaskSensor (#29933)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
ExternalTaskSensor now has an explicit ``skipped_states`` list

Miscellaneous Changes
^^^^^^^^^^^^^^^^^^^^^

Handle OverflowError on exponential backoff in next_run_calculation (#28172)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Maximum retry task delay is set to be 24h (86400s) by default. You can change it globally via ``core.max_task_retry_delay``
parameter.

Move Hive macros to the provider (#28538)
"""""""""""""""""""""""""""""""""""""""""
The Hive Macros (``hive.max_partition``, ``hive.closest_ds_partition``) are available only when Hive Provider is
installed. Please install Hive Provider > 5.1.0 when using those macros.

Updated app to support configuring the caching hash method for FIPS v2 (#30675)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Various updates for FIPS-compliance when running Airflow in Python 3.9+. This includes a new webserver option, ``caching_hash_method``,
for changing the default flask caching method.

New Features
^^^^^^^^^^^^
- AIP-50 Trigger DAG UI Extension with Flexible User Form Concept (#27063,#29376)
- Skip PythonVirtualenvOperator task when it returns a provided exit code (#30690)
- rename skip_exit_code to skip_on_exit_code and allow providing multiple codes (#30692)
- Add skip_on_exit_code also to ExternalPythonOperator (#30738)
- Add ``max_active_tis_per_dagrun`` for Dynamic Task Mapping (#29094)
- Add serializer for pandas dataframe (#30390)
- Deferrable ``TriggerDagRunOperator`` (#30292)
- Add command to get DAG Details via CLI (#30432)
- Adding ContinuousTimetable and support for @continuous schedule_interval (#29909)
- Allow customized rules to check if a file has dag (#30104)
- Add a new Airflow conf to specify a SSL ca cert for Kubernetes client (#30048)
- Bash sensor has an explicit retry code (#30080)
- Add filter task upstream/downstream to grid view (#29885)
- Add testing a connection via Airflow CLI (#29892)
- Support deleting the local log files when using remote logging (#29772)
- ``Blocklist`` to disable specific metric tags or metric names (#29881)
- Add a new graph inside of the grid view (#29413)
- Add database ``check_migrations`` config (#29714)
- add output format arg for ``cli.dags.trigger`` (#29224)
- Make json and yaml available in templates (#28930)
- Enable tagged metric names for existing Statsd metric publishing events | influxdb-statsd support (#29093)
- Add arg --yes to ``db export-archived`` command. (#29485)
- Make the policy functions pluggable (#28558)
- Add ``airflow db drop-archived`` command (#29309)
- Enable individual trigger logging (#27758)
- Implement new filtering options in graph view (#29226)
- Add triggers for ExternalTask (#29313)
- Add command to export purged records to CSV files (#29058)
- Add ``FileTrigger`` (#29265)
- Emit DataDog statsd metrics with metadata tags (#28961)
- Add some statsd metrics for dataset (#28907)
- Add --overwrite option to ``connections import`` CLI command (#28738)
- Add general-purpose "notifier" concept to DAGs (#28569)
- Add a new conf to wait past_deps before skipping a task (#27710)
- Add Flink on K8s Operator  (#28512)
- Allow Users to disable SwaggerUI via configuration (#28354)
- Show mapped task groups in graph (#28392)
- Log FileTaskHandler to work with KubernetesExecutor's multi_namespace_mode (#28436)
- Add a new config for adapting masked secrets to make it easier to prevent secret leakage in logs (#28239)
- List specific config section and its values using the cli (#28334)
- KubernetesExecutor multi_namespace_mode can use namespace list to avoid requiring cluster role (#28047)
- Automatically save and allow restore of recent DAG run configs (#27805)
- Added exclude_microseconds to cli (#27640)

Improvements
""""""""""""
- Rename most pod_id usage to pod_name in KubernetesExecutor (#29147)
- Update the error message for invalid use of poke-only sensors (#30821)
- Update log level in scheduler critical section edge case (#30694)
- AIP-51 Removing Executor Coupling from Core Airflow (`AIP-51 <https://github.com/apache/airflow/pulls?q=is%3Apr+is%3Amerged+label%3AAIP-51+milestone%3A%22Airflow+2.6.0%22>`__)
- Add multiple exit code handling in skip logic for BashOperator (#30739)
- Updated app to support configuring the caching hash method for FIPS v2 (#30675)
- Preload airflow imports before dag parsing to save time (#30495)
- Improve task & run actions ``UX`` in grid view (#30373)
- Speed up TaskGroups with caching property of group_id (#30284)
- Use the engine provided in the session (#29804)
- Type related import optimization for Executors (#30361)
- Add more type hints to the code base (#30503)
- Always use self.appbuilder.get_session in security managers (#30233)
- Update SQLAlchemy ``select()`` to new style (#30515)
- Refactor out xcom constants from models (#30180)
- Add exception class name to DAG-parsing error message (#30105)
- Rename statsd_allow_list and statsd_block_list to ``metrics_*_list`` (#30174)
- Improve serialization of tuples and sets (#29019)
- Make cleanup method in trigger an async one (#30152)
- Lazy load serialization modules (#30094)
- SLA callbacks no longer add files to the dag_processing manager queue (#30076)
- Add task.trigger rule to grid_data (#30130)
- Speed up log template sync by avoiding ORM (#30119)
- Separate cli_parser.py into two modules (#29962)
- Explicit skipped states list for ExternalTaskSensor (#29933)
- Add task state hover highlighting to new graph (#30100)
- Store grid tabs in url params (#29904)
- Use custom Connexion resolver to load lazily (#29992)
- Delay Kubernetes import in secret masker (#29993)
- Delay ConnectionModelView init until it's accessed (#29946)
- Scheduler, make stale DAG deactivation threshold configurable instead of using dag processing timeout (#29446)
- Improve grid view height calculations (#29563)
- Avoid importing executor during conf validation (#29569)
- Make permissions for FileTaskHandler group-writeable and configurable (#29506)
- Add colors in help outputs of Airflow CLI commands #28789 (#29116)
- Add a param for get_dags endpoint to list only unpaused dags (#28713)
- Expose updated_at filter for dag run and task instance endpoints (#28636)
- Increase length of user identifier columns (#29061)
- Update gantt chart UI to display queued state of tasks (#28686)
- Add index on log.dttm (#28944)
- Display only the running configuration in configurations view (#28892)
- Cap dropdown menu size dynamically (#28736)
- Added JSON linter to connection edit / add UI for field extra. On connection edit screen, existing extra data will be displayed indented (#28583)
- Use labels instead of pod name for pod log read in k8s exec (#28546)
- Use time not tries for queued & running re-checks. (#28586)
- CustomTTYColoredFormatter should inherit TimezoneAware formatter (#28439)
- Improve past depends handling in Airflow CLI tasks.run command (#28113)
- Support using a list of callbacks in ``on_*_callback/sla_miss_callbacks`` (#28469)
- Better table name validation for db clean (#28246)
- Use object instead of array in config.yml for config template (#28417)
- Add markdown rendering for task notes. (#28245)
- Show mapped task groups in grid view (#28208)
- Add ``renamed`` and ``previous_name`` in config sections (#28324)
- Speed up most Users/Role CLI commands (#28259)
- Speed up Airflow role list command (#28244)
- Refactor serialization (#28067, #30819, #30823)
- Allow longer pod names for k8s executor / KPO (#27736)
- Updates health check endpoint to include ``triggerer`` status (#27755)


Bug Fixes
"""""""""
- Fix static_folder for cli app (#30952)
- Initialize plugins for cli appbuilder (#30934)
- Fix dag file processor heartbeat to run only if necessary (#30899)
- Fix KubernetesExecutor sending state to scheduler (#30872)
- Count mapped upstream only if all are finished (#30641)
- ExternalTaskSensor: add external_task_group_id to template_fields (#30401)
- Improve url detection for task instance details (#30779)
- Use material icons for dag import error banner (#30771)
- Fix misc grid/graph view UI bugs (#30752)
- Add a collapse grid button (#30711)
- Fix d3 dependencies (#30702)
- Simplify logic to resolve tasks stuck in queued despite stalled_task_timeout (#30375)
- When clearing task instances try to get associated DAGs from database (#29065)
- Fix mapped tasks partial arguments when DAG default args are provided (#29913)
- Deactivate DAGs deleted from within zip files (#30608)
- Recover from ``too old resource version exception`` by retrieving the latest ``resource_version`` (#30425)
- Fix possible race condition when refreshing DAGs (#30392)
- Use custom validator for OpenAPI request body (#30596)
- Fix ``TriggerDagRunOperator`` with deferrable parameter (#30406)
- Speed up dag runs deletion (#30330)
- Do not use template literals to construct html elements (#30447)
- Fix deprecation warning in ``example_sensor_decorator`` DAG (#30513)
- Avoid logging sensitive information in triggerer job log (#30110)
- Add a new parameter for base sensor to catch the exceptions in poke method (#30293)
- Fix dag run conf encoding with non-JSON serializable values (#28777)
- Added fixes for Airflow to be usable on Windows Dask-Workers (#30249)
- Force DAG last modified time to UTC (#30243)
- Fix EmptySkipOperator in example dag (#30269)
- Make the webserver startup respect update_fab_perms (#30246)
- Ignore error when changing log folder permissions (#30123)
- Disable ordering DagRuns by note (#30043)
- Fix reading logs from finished KubernetesExecutor worker pod (#28817)
- Mask out non-access bits when comparing file modes (#29886)
- Remove Run task action from UI (#29706)
- Fix log tailing issues with legacy log view (#29496)
- Fixes to how DebugExecutor handles sensors (#28528)
- Ensure that pod_mutation_hook is called before logging the pod name (#28534)
- Handle OverflowError on exponential backoff in next_run_calculation (#28172)

Misc/Internal
"""""""""""""
- Make eager upgrade additional dependencies optional (#30811)
- Upgrade to pip 23.1.1 (#30808)
- Remove protobuf limitation from eager upgrade (#30182)
- Remove protobuf limitation from eager upgrade (#30182)
- Deprecate ``skip_exit_code`` in ``BashOperator`` (#30734)
- Remove gauge ``scheduler.tasks.running`` (#30374)
- Bump json5 to 1.0.2 and eslint-plugin-import to 2.27.5 in ``/airflow/www`` (#30568)
- Add tests to PythonOperator (#30362)
- Add asgiref as a core dependency (#30527)
- Discovery safe mode toggle comment clarification (#30459)
- Upgrade moment-timezone package to fix Tehran tz (#30455)
- Bump loader-utils from 2.0.0 to 2.0.4 in ``/airflow/www`` (#30319)
- Bump babel-loader from 8.1.0 to 9.1.0 in ``/airflow/www`` (#30316)
- DagBag: Use ``dag.fileloc`` instead of ``dag.full_filepath`` in exception message (#30610)
- Change log level of serialization information (#30239)
- Minor DagRun helper method cleanup (#30092)
- Improve type hinting in stats.py (#30024)
- Limit ``importlib-metadata`` backport to < 5.0.0 (#29924)
- Align cncf provider file names with AIP-21 (#29905)
- Upgrade FAB to 4.3.0 (#29766)
- Clear ExecutorLoader cache in tests (#29849)
- Lazy load Task Instance logs in UI (#29827)
- added warning log for max page limit exceeding api calls (#29788)
- Aggressively cache entry points in process (#29625)
- Don't use ``importlib.metadata`` to get Version for speed (#29723)
- Upgrade Mypy to 1.0 (#29468)
- Rename ``db export-cleaned`` to ``db export-archived`` (#29450)
- listener: simplify API by replacing SQLAlchemy event-listening by direct calls (#29289)
- No multi-line log entry for bash env vars (#28881)
- Switch to ruff for faster static checks (#28893)
- Remove horizontal lines in TI logs (#28876)
- Make allowed_deserialization_classes more intuitive (#28829)
- Propagate logs to stdout when in k8s executor pod (#28440, #30860)
- Fix code readability, add docstrings to json_client (#28619)
- AIP-51 - Misc. Compatibility Checks (#28375)
- Fix is_local for LocalKubernetesExecutor (#28288)
- Move Hive macros to the provider (#28538)
- Rerun flaky PinotDB integration test (#28562)
- Add pre-commit hook to check session default value (#28007)
- Refactor get_mapped_group_summaries for web UI (#28374)
- Add support for k8s 1.26 (#28320)
- Replace ``freezegun`` with time-machine (#28193)
- Completed D400 for ``airflow/kubernetes/*`` (#28212)
- Completed D400 for multiple folders (#27969)
- Drop k8s 1.21 and 1.22 support (#28168)
- Remove unused task_queue attr from k8s scheduler class (#28049)
- Completed D400 for multiple folders (#27767, #27768)


Doc only changes
""""""""""""""""
- Add instructions on how to avoid accidental airflow upgrade/downgrade (#30813)
- Add explicit information about how to write task logs (#30732)
- Better explanation on how to log from tasks (#30746)
- Use correct import path for Dataset (#30617)
- Create ``audit_logs.rst`` (#30405)
- Adding taskflow API example for sensors (#30344)
- Add clarification about timezone aware dags (#30467)
- Clarity params documentation (#30345)
- Fix unit for task duration metric (#30273)
- Update dag-run.rst for dead links of cli commands (#30254)
- Add Write efficient Python code section to Reducing DAG complexity (#30158)
- Allow to specify which connection, variable or config are being looked up in the backend using ``*_lookup_pattern`` parameters (#29580)
- Add Documentation for notification feature extension (#29191)
- Clarify that executor interface is public but instances are not (#29200)
- Add Public Interface description to Airflow documentation (#28300)
- Add documentation for task group mapping (#28001)
- Some fixes to metrics doc (#30290)


Airflow 2.5.3 (2023-04-01)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^
- Fix DagProcessorJob integration for standalone dag-processor (#30278)
- Fix proper termination of gunicorn when it hangs (#30188)
- Fix XCom.get_one exactly one exception text (#30183)
- Correct the VARCHAR size to 250. (#30178)
- Revert fix for on_failure_callback when task receives a SIGTERM (#30165)
- Move read only property to DagState to fix generated docs (#30149)
- Ensure that ``dag.partial_subset`` doesn't mutate task group properties (#30129)
- Fix inconsistent returned value of ``airflow dags next-execution`` cli command (#30117)
- Fix www/utils.dag_run_link redirection (#30098)
- Fix ``TriggerRuleDep`` when the mapped tasks count is 0 (#30084)
- Dag processor manager, add retry_db_transaction to _fetch_callbacks (#30079)
- Fix db clean command for mysql db (#29999)
- Avoid considering EmptyOperator in mini scheduler (#29979)
- Fix some long known Graph View UI problems (#29971, #30355, #30360)
- Fix dag docs toggle icon initial angle (#29970)
- Fix tags selection in DAGs UI (#29944)
- Including airflow/example_dags/sql/sample.sql in MANIFEST.in (#29883)
- Fixing broken filter in /taskinstance/list view (#29850)
- Allow generic param dicts (#29782)
- Fix update_mask in patch variable route (#29711)
- Strip markup from app_name if instance_name_has_markup = True (#28894)

Misc/Internal
^^^^^^^^^^^^^
- Revert "Also limit importlib on Python 3.9 (#30069)" (#30209)
- Add custom_operator_name to @task.sensor tasks (#30131)
- Bump webpack from 5.73.0 to 5.76.0 in /airflow/www (#30112)
- Formatted config (#30103)
- Remove upper bound limit of astroid (#30033)
- Remove accidentally merged vendor daemon patch code (#29895)
- Fix warning in airflow tasks test command regarding absence of data_interval (#27106)

Doc only changes
^^^^^^^^^^^^^^^^
- Adding more information regarding top level code (#30040)
- Update workday example (#30026)
- Fix some typos in the DAGs docs (#30015)
- Update set-up-database.rst (#29991)
- Fix some typos on the kubernetes documentation (#29936)
- Fix some punctuation and grammar (#29342)


Airflow 2.5.2 (2023-03-15)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

The date-time fields passed as API parameters or Params should be RFC3339-compliant (#29395)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

In case of API calls, it was possible that "+" passed as part of the date-time fields were not URL-encoded, and
such date-time fields could pass validation. Such date-time parameters should now be URL-encoded (as ``%2B``).

In case of parameters, we still allow IS8601-compliant date-time (so for example it is possible that
' ' was used instead of ``T`` separating date from time and no timezone was specified) but we raise
deprecation warning.

Default for ``[webserver] expose_hostname`` changed to ``False`` (#29547)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default for ``[webserver] expose_hostname`` has been set to ``False``, instead of ``True``. This means administrators must opt-in to expose webserver hostnames to end users.

Bug Fixes
^^^^^^^^^
- Fix validation of date-time field in API and Parameter schemas (#29395)
- Fix grid logs for large logs (#29390)
- Fix on_failure_callback when task receives a SIGTERM (#29743)
- Update min version of python-daemon to fix containerd file limits (#29916)
- POST ``/dagRuns`` API should 404 if dag not active (#29860)
- DAG list sorting lost when switching page (#29756)
- Fix Scheduler crash when clear a previous run of a normal task that is now a mapped task (#29645)
- Convert moment with timezone to UTC instead of raising an exception (#29606)
- Fix clear dag run ``openapi`` spec responses by adding additional return type (#29600)
- Don't display empty rendered attrs in Task Instance Details page (#29545)
- Remove section check from get-value command (#29541)
- Do not show version/node in UI traceback for unauthenticated user (#29501)
- Make ``prev_logical_date`` variable offset-aware (#29454)
- Fix nested fields rendering in mapped operators (#29451)
- Datasets, next_run_datasets, remove unnecessary timestamp filter (#29441)
- ``Edgemodifier`` refactoring w/ labels in TaskGroup edge case (#29410)
- Fix Rest API update user output (#29409)
- Ensure Serialized DAG is deleted (#29407)
- Persist DAG and task doc values in TaskFlow API if explicitly set (#29399)
- Redirect to the origin page with all the params (#29212)
- Fixing Task Duration view in case of manual DAG runs only (#22015) (#29195)
- Remove poke method to fall back to parent implementation (#29146)
- PR: Introduced fix to run tasks on Windows systems (#29107)
- Fix warning in migrations about old config. (#29092)
- Emit dagrun failed duration when timeout (#29076)
- Handling error on cluster policy itself (#29056)
- Fix kerberos authentication for the REST API. (#29054)
- Fix leak sensitive field via V1EnvVar on exception (#29016)
- Sanitize url_for arguments before they are passed (#29039)
- Fix dag run trigger with a note. (#29228)
- Write action log to DB when DAG run is triggered via API (#28998)
- Resolve all variables in pickled XCom iterator (#28982)
- Allow URI without authority and host blocks in ``airflow connections add`` (#28922)
- Be more selective when adopting pods with KubernetesExecutor (#28899)
- KubenetesExecutor sends state even when successful (#28871)
- Annotate KubernetesExecutor pods that we don't delete (#28844)
- Throttle streaming log reads (#28818)
- Introduce dag processor job (#28799)
- Fix #28391 manual task trigger from UI fails for k8s executor (#28394)
- Logging poke info when external dag is not none and task_id and task_ids are none (#28097)
- Fix inconsistencies in checking edit permissions for a DAG (#20346)

Misc/Internal
^^^^^^^^^^^^^
- Add a check for not templateable fields (#29821)
- Removed continue for not in (#29791)
- Move extra links position in grid view (#29703)
- Bump ``undici`` from ``5.9.1`` to ``5.19.1`` (#29583)
- Change expose_hostname default to false (#29547)
- Change permissions of config/password files created by airflow (#29495)
- Use newer setuptools ``v67.2.0`` (#29465)
- Increase max height for grid view elements (#29367)
- Clarify description of worker control config (#29247)
- Bump ``ua-parser-js`` from ``0.7.31`` to ``0.7.33`` in ``/airflow/www`` (#29172)
- Remove upper bound limitation for ``pytest`` (#29086)
- Check for ``run_id`` url param when linking to ``graph/gantt`` views (#29066)
- Clarify graph view dynamic task labels (#29042)
- Fixing import error for dataset (#29007)
- Update how PythonSensor returns values from ``python_callable`` (#28932)
- Add dep context description for better log message (#28875)
- Bump ``swagger-ui-dist`` from ``3.52.0`` to ``4.1.3`` in ``/airflow/www`` (#28824)
- Limit ``importlib-metadata`` backport to ``< 5.0.0`` (#29924, #30069)

Doc only changes
^^^^^^^^^^^^^^^^
- Update pipeline.rst - Fix query in ``merge_data()`` task (#29158)
- Correct argument name of Workday timetable in timetable.rst (#29896)
- Update ref anchor for env var link in Connection how-to doc (#29816)
- Better description for limit in api (#29773)
- Description of dag_processing.last_duration (#29740)
- Update docs re: template_fields typing and subclasses (#29725)
- Fix formatting of Dataset inlet/outlet note in TaskFlow concepts (#29678)
- Specific use-case: adding packages via requirements.txt in compose (#29598)
- Detect is 'docker-compose' existing (#29544)
- Add Landing Times entry to UI docs (#29511)
- Improve health checks in example docker-compose and clarify usage (#29408)
- Remove ``notes`` param from TriggerDagRunOperator docstring (#29298)
- Use ``schedule`` param rather than ``timetable`` in Timetables docs (#29255)
- Add trigger process to Airflow Docker docs (#29203)
- Update set-up-database.rst (#29104)
- Several improvements to the Params doc (#29062)
- Email Config docs more explicit env var examples (#28845)
- Listener plugin example added (#27905)


Airflow 2.5.1 (2023-01-20)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Trigger gevent ``monkeypatching`` via environment variable (#28283)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If you are using gevent for your webserver deployment and used local settings to ``monkeypatch`` gevent,
you might want to replace local settings patching with an ``_AIRFLOW_PATCH_GEVENT`` environment variable
set to 1 in your webserver. This ensures gevent patching is done as early as possible.

Bug Fixes
^^^^^^^^^
- Fix masking of non-sensitive environment variables (#28802)
- Remove swagger-ui extra from connexion and install ``swagger-ui-dist`` via npm package (#28788)
- Fix ``UIAlert`` should_show when ``AUTH_ROLE_PUBLIC`` set (#28781)
- Only patch single label when adopting pod (#28776)
- Update CSRF token to expire with session (#28730)
- Fix "airflow tasks render" cli command for mapped task instances (#28698)
- Allow XComArgs for ``external_task_ids`` of ExternalTaskSensor (#28692)
- Row-lock TIs to be removed during mapped task expansion (#28689)
- Handle ConnectionReset exception in Executor cleanup (#28685)
- Fix description of output redirection for access_log for gunicorn (#28672)
- Add back join to zombie query that was dropped in #28198 (#28544)
- Fix calendar view for CronTriggerTimeTable dags (#28411)
- After running the DAG the employees table is empty. (#28353)
- Fix ``DetachedInstanceError`` when finding zombies in Dag Parsing process (#28198)
- Nest header blocks in ``divs`` to fix ``dagid`` copy nit on dag.html (#28643)
- Fix UI caret direction (#28624)
- Guard not-yet-expanded ti in trigger rule dep (#28592)
- Move TI ``setNote`` endpoints under TaskInstance in OpenAPI (#28566)
- Consider previous run in ``CronTriggerTimetable`` (#28532)
- Ensure correct log dir in file task handler (#28477)
- Fix bad pods pickled in executor_config (#28454)
- Add ``ensure_ascii=False`` in trigger dag run API (#28451)
- Add setters to MappedOperator on_*_callbacks (#28313)
- Fix ``ti._try_number`` for deferred and up_for_reschedule tasks (#26993)
- separate ``callModal`` from dag.js (#28410)
- A manual run can't look like a scheduled one (#28397)
- Dont show task/run durations when there is no start_date (#28395)
- Maintain manual scroll position in task logs (#28386)
- Correctly select a mapped task's "previous" task (#28379)
- Trigger gevent ``monkeypatching`` via environment variable (#28283)
- Fix db clean warnings (#28243)
- Make arguments 'offset' and 'length' not required (#28234)
- Make live logs reading work for "other" k8s executors (#28213)
- Add custom pickling hooks to ``LazyXComAccess`` (#28191)
- fix next run datasets error (#28165)
- Ensure that warnings from ``@dag`` decorator are reported in dag file (#28153)
- Do not warn when airflow dags tests command is used (#28138)
- Ensure the ``dagbag_size`` metric decreases when files are deleted (#28135)
- Improve run/task grid view actions (#28130)
- Make BaseJob.most_recent_job favor "running" jobs (#28119)
- Don't emit FutureWarning when code not calling old key (#28109)
- Add ``airflow.api.auth.backend.session`` to backend sessions in compose (#28094)
- Resolve false warning about calling conf.get on moved item (#28075)
- Return list of tasks that will be changed (#28066)
- Handle bad zip files nicely when parsing DAGs. (#28011)
- Prevent double loading of providers from local paths (#27988)
- Fix deadlock when chaining multiple empty mapped tasks (#27964)
- fix: current_state method on TaskInstance doesn't filter by map_index (#27898)
- Don't log CLI actions if db not initialized (#27851)
- Make sure we can get out of a faulty scheduler state (#27834)
- dagrun, ``next_dagruns_to_examine``, add MySQL index hint (#27821)
- Handle DAG disappearing mid-flight when dag verification happens (#27720)
- fix: continue checking sla (#26968)
- Allow generation of connection URI to work when no conn type (#26765)

Misc/Internal
^^^^^^^^^^^^^
- Remove limit for ``dnspython`` after eventlet got fixed (#29004)
- Limit ``dnspython`` to < ``2.3.0`` until eventlet incompatibility is solved (#28962)
- Add automated version replacement in example dag indexes (#28090)
- Cleanup and do housekeeping with plugin examples (#28537)
- Limit ``SQLAlchemy`` to below ``2.0`` (#28725)
- Bump ``json5`` from ``1.0.1`` to ``1.0.2`` in ``/airflow/www`` (#28715)
- Fix some docs on using sensors with taskflow (#28708)
- Change Architecture and OperatingSystem classes into ``Enums`` (#28627)
- Add doc-strings and small improvement to email util (#28634)
- Fix ``Connection.get_extra`` type (#28594)
- navbar, cap dropdown size, and add scroll bar (#28561)
- Emit warnings for ``conf.get*`` from the right source location (#28543)
- Move MyPY plugins of ours to dev folder (#28498)
- Add retry to ``purge_inactive_dag_warnings`` (#28481)
- Re-enable Plyvel on ARM as it now builds cleanly (#28443)
- Add SIGUSR2 handler for LocalTaskJob and workers to aid debugging (#28309)
- Convert ``test_task_command`` to Pytest and ``unquarantine`` tests in it (#28247)
- Make invalid characters exception more readable (#28181)
- Bump decode-uri-component from ``0.2.0`` to ``0.2.2`` in ``/airflow/www`` (#28080)
- Use asserts instead of exceptions for executor not started (#28019)
- Simplify dataset ``subgraph`` logic (#27987)
- Order TIs by ``map_index`` (#27904)
- Additional info about Segmentation Fault in ``LocalTaskJob`` (#27381)

Doc only changes
^^^^^^^^^^^^^^^^
- Mention mapped operator in cluster policy doc (#28885)
- Slightly improve description of Dynamic DAG generation preamble (#28650)
- Restructure Docs  (#27235)
- Update scheduler docs about low priority tasks (#28831)
- Clarify that versioned constraints are fixed at release time (#28762)
- Clarify about docker compose (#28729)
- Adding an example dag for dynamic task mapping (#28325)
- Use docker compose v2 command (#28605)
- Add AIRFLOW_PROJ_DIR to docker-compose example (#28517)
- Remove outdated Optional Provider Feature outdated documentation (#28506)
- Add documentation for [core] mp_start_method config (#27993)
- Documentation for the LocalTaskJob return code counter (#27972)
- Note which versions of Python are supported (#27798)


Airflow 2.5.0 (2022-12-02)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

``airflow dags test`` no longer performs a backfill job (#26400)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

In order to make ``airflow dags test`` more useful as a testing and debugging tool, we no
longer run a backfill job and instead run a "local task runner". Users can still backfill
their DAGs using the ``airflow dags backfill`` command.

Airflow config section ``kubernetes`` renamed to ``kubernetes_executor`` (#26873)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

KubernetesPodOperator no longer considers any core kubernetes config params, so this section now only applies to kubernetes executor. Renaming it reduces potential for confusion.

``AirflowException`` is now thrown as soon as any dependent tasks of ExternalTaskSensor fails (#27190)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``ExternalTaskSensor`` no longer hangs indefinitely when ``failed_states`` is set, an ``execute_date_fn`` is used, and some but not all of the dependent tasks fail.
Instead, an ``AirflowException`` is thrown as soon as any of the dependent tasks fail.
Any code handling this failure in addition to timeouts should move to caching the ``AirflowException`` ``BaseClass`` and not only the ``AirflowSensorTimeout`` subclass.

The Airflow config option ``scheduler.deactivate_stale_dags_interval`` has been renamed to ``scheduler.parsing_cleanup_interval`` (#27828).
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The old option will continue to work but will issue deprecation warnings, and will be removed entirely in Airflow 3.

New Features
^^^^^^^^^^^^
- ``TaskRunner``: notify of component start and finish (#27855)
- Add DagRun state change to the Listener plugin system(#27113)
- Metric for raw task return codes (#27155)
- Add logic for XComArg to pull specific map indexes (#27771)
- Clear TaskGroup (#26658, #28003)
- Add critical section query duration metric (#27700)
- Add: #23880 :: Audit log for ``AirflowModelViews(Variables/Connection)`` (#24079, #27994, #27923)
- Add postgres 15 support (#27444)
- Expand tasks in mapped group at run time (#27491)
- reset commits, clean submodules (#27560)
- scheduler_job, add metric for scheduler loop timer (#27605)
- Allow datasets to be used in taskflow (#27540)
- Add expanded_ti_count to ti context (#27680)
- Add user comment to task instance and dag run (#26457, #27849, #27867)
- Enable copying DagRun JSON to clipboard (#27639)
- Implement extra controls for SLAs (#27557)
- add dag parsed time in DAG view (#27573)
- Add max_wait for exponential_backoff in BaseSensor (#27597)
- Expand tasks in mapped group at parse time (#27158)
- Add disable retry flag on backfill (#23829)
- Adding sensor decorator (#22562)
- Api endpoint update ti (#26165)
- Filtering datasets by recent update events (#26942)
- Support ``Is /not`` Null filter for value is None on ``webui`` (#26584)
- Add search to datasets list (#26893)
- Split out and handle 'params' in mapped operator (#26100)
- Add authoring API for TaskGroup mapping (#26844)
- Add ``one_done`` trigger rule (#26146)
- Create a more efficient  airflow dag test command that also has better local logging (#26400)
- Support add/remove permissions to roles commands (#26338)
- Auto tail file logs in Web UI (#26169)
- Add triggerer info to task instance in API (#26249)
- Flag to deserialize value on custom XCom backend (#26343)

Improvements
^^^^^^^^^^^^
- Allow depth-first execution (#27827)
- UI: Update offset height if data changes (#27865)
- Improve TriggerRuleDep typing and readability (#27810)
- Make views requiring session, keyword only args (#27790)
- Optimize ``TI.xcom_pull()`` with explicit task_ids and map_indexes (#27699)
- Allow hyphens in pod id used by k8s executor (#27737)
- optimise task instances filtering (#27102)
- Use context managers to simplify log serve management (#27756)
- Fix formatting leftovers (#27750)
- Improve task deadlock messaging (#27734)
- Improve "sensor timeout" messaging (#27733)
- Replace urlparse with ``urlsplit`` (#27389)
- Align TaskGroup semantics to AbstractOperator (#27723)
- Add new files to parsing queue on every loop of dag processing (#27060)
- Make Kubernetes Executor & Scheduler resilient to error during PMH execution (#27611)
- Separate dataset deps into individual graphs (#27356)
- Use log.exception where more economical than log.error (#27517)
- Move validation ``branch_task_ids`` into ``SkipMixin`` (#27434)
- Coerce LazyXComAccess to list when pushed to XCom (#27251)
- Update cluster-policies.rst docs (#27362)
- Add warning if connection type already registered within the provider (#27520)
- Activate debug logging in commands with --verbose option (#27447)
- Add classic examples for Python Operators (#27403)
- change ``.first()`` to ``.scalar()`` (#27323)
- Improve reset_dag_run description (#26755)
- Add examples and ``howtos`` about sensors (#27333)
- Make grid view widths adjustable (#27273)
- Sorting plugins custom menu links by category before name (#27152)
- Simplify DagRun.verify_integrity (#26894)
- Add mapped task group info to serialization (#27027)
- Correct the JSON style used for Run config in Grid View (#27119)
- No ``extra__conn_type__`` prefix required for UI behaviors (#26995)
- Improve dataset update blurb (#26878)
- Rename kubernetes config section to kubernetes_executor (#26873)
- decode params for dataset searches (#26941)
- Get rid of the DAGRun details page & rely completely on Grid (#26837)
- Fix scheduler ``crashloopbackoff`` when using ``hostname_callable`` (#24999)
- Reduce log verbosity in KubernetesExecutor. (#26582)
- Don't iterate tis list twice for no reason (#26740)
- Clearer code for PodGenerator.deserialize_model_file (#26641)
- Don't import kubernetes unless you have a V1Pod (#26496)
- Add updated_at column to DagRun and Ti tables (#26252)
- Move the deserialization of custom XCom Backend to 2.4.0 (#26392)
- Avoid calculating all elements when one item is needed (#26377)
- Add ``__future__``.annotations automatically by isort (#26383)
- Handle list when serializing expand_kwargs (#26369)
- Apply PEP-563 (Postponed Evaluation of Annotations) to core airflow (#26290)
- Add more weekday operator and sensor examples #26071 (#26098)
- Align TaskGroup semantics to AbstractOperator (#27723)

Bug Fixes
^^^^^^^^^
- Gracefully handle whole config sections being renamed (#28008)
- Add allow list for imports during deserialization (#27887)
- Soft delete datasets that are no longer referenced in DAG schedules or task outlets (#27828)
- Redirect to home view when there are no valid tags in the URL (#25715)
- Refresh next run datasets info in dags view (#27839)
- Make MappedTaskGroup depend on its expand inputs (#27876)
- Make DagRun state updates for paused DAGs faster (#27725)
- Don't explicitly set include_examples to False on task run command (#27813)
- Fix menu border color (#27789)
- Fix  backfill  queued  task getting reset to scheduled state.  (#23720)
- Fix clearing child dag mapped tasks from parent dag (#27501)
- Handle json encoding of ``V1Pod`` in task callback (#27609)
- Fix ExternalTaskSensor can't check zipped dag (#27056)
- Avoid re-fetching DAG run in TriggerDagRunOperator (#27635)
- Continue on exception when retrieving metadata (#27665)
- External task sensor fail fix (#27190)
- Add the default None when pop actions (#27537)
- Display parameter values from serialized dag in trigger dag view. (#27482, #27944)
- Move TriggerDagRun conf check to execute (#27035)
- Resolve trigger assignment race condition (#27072)
- Update google_analytics.html (#27226)
- Fix some bug in web ui dags list page (auto-refresh & jump search null state) (#27141)
- Fixed broken URL for docker-compose.yaml (#26721)
- Fix xcom arg.py .zip bug (#26636)
- Fix 404 ``taskInstance`` errors and split into two tables (#26575)
- Fix browser warning of improper thread usage (#26551)
- template rendering issue fix (#26390)
- Clear ``autoregistered`` DAGs if there are any import errors (#26398)
- Fix ``from airflow import version`` lazy import (#26239)
- allow scroll in triggered dag runs modal (#27965)

Misc/Internal
^^^^^^^^^^^^^
- Remove ``is_mapped`` attribute (#27881)
- Simplify FAB table resetting (#27869)
- Fix old-style typing in Base Sensor (#27871)
- Switch (back) to late imports (#27730)
- Completed D400 for multiple folders (#27748)
- simplify notes accordion test (#27757)
- completed D400 for ``airflow/callbacks/* airflow/cli/*`` (#27721)
- Completed D400 for ``airflow/api_connexion/* directory`` (#27718)
- Completed D400 for ``airflow/listener/* directory`` (#27731)
- Completed D400 for ``airflow/lineage/* directory`` (#27732)
- Update API & Python Client versions (#27642)
- Completed D400 & D401 for ``airflow/api/*`` directory (#27716)
- Completed D400 for multiple folders (#27722)
- Bump ``minimatch`` from ``3.0.4 to 3.0.8`` in ``/airflow/www`` (#27688)
- Bump loader-utils from ``1.4.1 to 1.4.2 ``in ``/airflow/www`` (#27697)
- Disable nested task mapping for now (#27681)
- bump alembic minimum version (#27629)
- remove unused code.html (#27585)
- Enable python string normalization everywhere (#27588)
- Upgrade dependencies in order to avoid backtracking (#27531)
- Strengthen a bit and clarify importance of triaging issues (#27262)
- Deduplicate type hints (#27508)
- Add stub 'yield' to ``BaseTrigger.run`` (#27416)
- Remove upper-bound limit to dask (#27415)
- Limit Dask to under ``2022.10.1`` (#27383)
- Update old style typing (#26872)
- Enable string normalization for docs (#27269)
- Slightly faster up/downgrade tests (#26939)
- Deprecate use of core get_kube_client in PodManager (#26848)
- Add ``memray`` files to ``gitignore / dockerignore`` (#27001)
- Bump sphinx and ``sphinx-autoapi`` (#26743)
- Simplify ``RTIF.delete_old_records()`` (#26667)
- migrate last react files to typescript (#26112)
- Work around ``pyupgrade`` edge cases (#26384)

Doc only changes
^^^^^^^^^^^^^^^^
- Document dag_file_processor_timeouts metric as deprecated (#27067)
- Drop support for PostgreSQL 10 (#27594)
- Update index.rst (#27529)
- Add note about pushing the lazy XCom proxy to XCom (#27250)
- Fix BaseOperator link (#27441)
- [docs] best-practices add use variable with template example. (#27316)
- docs for custom view using plugin (#27244)
- Update graph view and grid view on overview page (#26909)
- Documentation fixes (#26819)
- make consistency on markup title string level (#26696)
- Add documentation to dag test function (#26713)
- Fix broken URL for ``docker-compose.yaml`` (#26726)
- Add a note against use of top level code in timetable (#26649)
- Fix example_datasets dag names (#26495)
- Update docs: zip-like effect is now possible in task mapping (#26435)
- changing to task decorator in docs from classic operator use (#25711)

Airflow 2.4.3 (2022-11-14)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Make ``RotatingFilehandler`` used in ``DagProcessor`` non-caching (#27223)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

In case you want to decrease cache memory when ``CONFIG_PROCESSOR_MANAGER_LOGGER=True``, and you have your local settings created before,
you can update ``processor_manager_handler`` to use ``airflow.utils.log.non_caching_file_handler.NonCachingRotatingFileHandler`` handler instead of ``logging.RotatingFileHandler``.

Bug Fixes
^^^^^^^^^
- Fix double logging with some task logging handler (#27591)
- Replace FAB url filtering function with Airflow's (#27576)
- Fix mini scheduler expansion of mapped task  (#27506)
- ``SLAMiss`` is nullable and not always given back when pulling task instances (#27423)
- Fix behavior of ``_`` when searching for DAGs (#27448)
- Fix getting the ``dag/task`` ids from BaseExecutor (#27550)
- Fix SQLAlchemy primary key black-out error on DDRQ (#27538)
- Fix IntegrityError during webserver startup (#27297)
- Add case insensitive constraint to username (#27266)
- Fix python external template keys (#27256)
- Reduce extraneous task log requests (#27233)
- Make ``RotatingFilehandler`` used in ``DagProcessor`` non-caching (#27223)
- Listener: Set task on SQLAlchemy TaskInstance object (#27167)
- Fix dags list page auto-refresh & jump search null state (#27141)
- Set ``executor.job_id`` to ``BackfillJob.id`` for backfills (#27020)

Misc/Internal
^^^^^^^^^^^^^
- Bump loader-utils from ``1.4.0`` to ``1.4.1`` in ``/airflow/www`` (#27552)
- Reduce log level for k8s ``TCP_KEEPALIVE`` etc warnings (#26981)

Doc only changes
^^^^^^^^^^^^^^^^
- Use correct executable in docker compose docs (#27529)
- Fix wording in DAG Runs description (#27470)
- Document that ``KubernetesExecutor`` overwrites container args (#27450)
- Fix ``BaseOperator`` links (#27441)
- Correct timer units to seconds from milliseconds. (#27360)
- Add missed import in the Trigger Rules example (#27309)
- Update SLA wording to reflect it is relative to ``Dag Run`` start. (#27111)
- Add ``kerberos`` environment variables to the docs (#27028)

Airflow 2.4.2 (2022-10-23)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default for ``[webserver] expose_stacktrace`` changed to ``False`` (#27059)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default for ``[webserver] expose_stacktrace`` has been set to ``False``, instead of ``True``. This means administrators must opt-in to expose tracebacks to end users.

Bug Fixes
^^^^^^^^^
- Make tracebacks opt-in (#27059)
- Add missing AUTOINC/SERIAL for FAB tables (#26885)
- Add separate error handler for 405(Method not allowed) errors (#26880)
- Don't re-patch pods that are already controlled by current worker (#26778)
- Handle mapped tasks in task duration chart (#26722)
- Fix task duration cumulative chart (#26717)
- Avoid 500 on dag redirect (#27064)
- Filter dataset dependency data on webserver (#27046)
- Remove double collection of dags in ``airflow dags reserialize``  (#27030)
- Fix auto refresh for graph view (#26926)
- Don't overwrite connection extra with invalid json (#27142)
- Fix next run dataset modal links (#26897)
- Change dag audit log sort by date from asc to desc (#26895)
- Bump min version of jinja2 (#26866)
- Add missing colors to ``state_color_mapping`` jinja global (#26822)
- Fix running debuggers inside ``airflow tasks test`` (#26806)
- Fix warning when using xcomarg dependencies (#26801)
- demote Removed state in priority for displaying task summaries (#26789)
- Ensure the log messages from operators during parsing go somewhere (#26779)
- Add restarting state to TaskState Enum in REST API (#26776)
- Allow retrieving error message from data.detail (#26762)
- Simplify origin string cleaning (#27143)
- Remove DAG parsing from StandardTaskRunner (#26750)
- Fix non-hidden cumulative chart on duration view (#26716)
- Remove TaskFail duplicates check (#26714)
- Fix airflow tasks run --local when dags_folder differs from that of processor (#26509)
- Fix yarn warning from d3-color (#27139)
- Fix version for a couple configurations (#26491)
- Revert "No grid auto-refresh for backfill dag runs (#25042)" (#26463)
- Retry on Airflow Schedule DAG Run DB Deadlock (#26347)

Misc/Internal
^^^^^^^^^^^^^
- Clean-ups around task-mapping code (#26879)
- Move user-facing string to template (#26815)
- add icon legend to datasets graph (#26781)
- Bump ``sphinx`` and ``sphinx-autoapi`` (#26743)
- Simplify ``RTIF.delete_old_records()`` (#26667)
- Bump FAB to ``4.1.4`` (#26393)

Doc only changes
^^^^^^^^^^^^^^^^
- Fixed triple quotes in task group example (#26829)
- Documentation fixes (#26819)
- make consistency on markup title string level (#26696)
- Add a note against use of top level code in timetable (#26649)
- Fix broken URL for ``docker-compose.yaml`` (#26726)


Airflow 2.4.1 (2022-09-30)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- When rendering template, unmap task in context (#26702)
- Fix scroll overflow for ConfirmDialog (#26681)
- Resolve deprecation warning re ``Table.exists()`` (#26616)
- Fix XComArg zip bug (#26636)
- Use COALESCE when ordering runs to handle NULL (#26626)
- Check user is active (#26635)
- No missing user warning for public admin (#26611)
- Allow MapXComArg to resolve after serialization  (#26591)
- Resolve warning about DISTINCT ON query on dags view (#26608)
- Log warning when secret backend kwargs is invalid (#26580)
- Fix grid view log try numbers (#26556)
- Template rendering issue in passing ``templates_dict`` to task decorator (#26390)
- Fix Deferrable stuck as ``scheduled`` during backfill (#26205)
- Suppress SQLALCHEMY_TRACK_MODIFICATIONS warning in db init (#26617)
- Correctly set ``json_provider_class`` on Flask app so it uses our encoder (#26554)
- Fix WSGI root app (#26549)
- Fix deadlock when mapped task with removed upstream is rerun (#26518)
- ExecutorConfigType should be ``cacheable`` (#26498)
- Fix proper joining of the path for logs retrieved from celery workers (#26493)
- DAG Deps extends ``base_template`` (#26439)
- Don't update backfill run from the scheduler (#26342)

Doc only changes
^^^^^^^^^^^^^^^^

- Clarify owner links document (#26515)
- Fix invalid RST in dataset concepts doc (#26434)
- Document the ``non-sensitive-only`` option for ``expose_config`` (#26507)
- Fix ``example_datasets`` dag names (#26495)
- Zip-like effect is now possible in task mapping (#26435)
- Use task decorator in docs instead of classic operators (#25711)

Airflow 2.4.0 (2022-09-19)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Data-aware Scheduling and ``Dataset`` concept added to Airflow
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

New to this release of Airflow is the concept of Datasets to Airflow, and with it a new way of scheduling dags:
data-aware scheduling.

This allows DAG runs to be automatically created as a result of a task "producing" a dataset. In some ways
this can be thought of as the inverse of ``TriggerDagRunOperator``, where instead of the producing DAG
controlling which DAGs get created, the consuming DAGs can "listen" for changes.

A dataset is identified by a URI:

.. code-block:: python

    from airflow import Dataset

    # The URI doesn't have to be absolute
    dataset = Dataset(uri="my-dataset")
    # Or you can use a scheme to show where it lives.
    dataset2 = Dataset(uri="s3://bucket/prefix")

To create a DAG that runs whenever a Dataset is updated use the new ``schedule`` parameter (see below) and
pass a list of 1 or more Datasets:

..  code-block:: python

    with DAG(dag_id='dataset-consumer', schedule=[dataset]):
        ...

And to mark a task as producing a dataset pass the dataset(s) to the ``outlets`` attribute:

.. code-block:: python

    @task(outlets=[dataset])
    def my_task(): ...


    # Or for classic operators
    BashOperator(task_id="update-ds", bash_command=..., outlets=[dataset])

If you have the producer and consumer in different files you do not need to use the same Dataset object, two
``Dataset()``\s created with the same URI are equal.

Datasets represent the abstract concept of a dataset, and (for now) do not have any direct read or write
capability - in this release we are adding the foundational feature that we will build upon.

For more info on Datasets please see `Datasets documentation <https://airflow.apache.org/docs/apache-airflow/2.4.0/authoring-and-scheduling/datasets.html>`_.

Expanded dynamic task mapping support
"""""""""""""""""""""""""""""""""""""

Dynamic task mapping now includes support for ``expand_kwargs``, ``zip`` and ``map``.

For more info on dynamic task mapping please see :doc:`/authoring-and-scheduling/dynamic-task-mapping`.

DAGS used in a context manager no longer need to be assigned to a module variable (#23592)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously you had to assign a DAG to a module-level variable in order for Airflow to pick it up. For example this


.. code-block:: python

   with DAG(dag_id="example") as dag:
       ...


   @dag
   def dag_maker(): ...


   dag2 = dag_maker()


can become

.. code-block:: python

   with DAG(dag_id="example"):
       ...


   @dag
   def dag_maker(): ...


   dag_maker()

If you want to disable the behaviour for any reason then set ``auto_register=False`` on the dag:

.. code-block:: python

   # This dag will not be picked up by Airflow as it's not assigned to a variable
   with DAG(dag_id="example", auto_register=False):
       ...

Deprecation of ``schedule_interval`` and ``timetable`` arguments (#25410)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

We added new DAG argument ``schedule`` that can accept a cron expression, timedelta object, *timetable* object, or list of dataset objects. Arguments ``schedule_interval`` and ``timetable`` are deprecated.

If you previously used the ``@daily`` cron preset, your DAG may have looked like this:

.. code-block:: python

    with DAG(
        dag_id="my_example",
        start_date=datetime(2021, 1, 1),
        schedule_interval="@daily",
    ):
        ...

Going forward, you should use the ``schedule`` argument instead:

.. code-block:: python

    with DAG(
        dag_id="my_example",
        start_date=datetime(2021, 1, 1),
        schedule="@daily",
    ):
        ...

The same is true if you used a custom timetable.  Previously you would have used the ``timetable`` argument:

.. code-block:: python

    with DAG(
        dag_id="my_example",
        start_date=datetime(2021, 1, 1),
        timetable=EventsTimetable(event_dates=[pendulum.datetime(2022, 4, 5)]),
    ):
        ...

Now you should use the ``schedule`` argument:

.. code-block:: python

    with DAG(
        dag_id="my_example",
        start_date=datetime(2021, 1, 1),
        schedule=EventsTimetable(event_dates=[pendulum.datetime(2022, 4, 5)]),
    ):
        ...

Removal of experimental Smart Sensors (#25507)
""""""""""""""""""""""""""""""""""""""""""""""

Smart Sensors were added in 2.0 and deprecated in favor of Deferrable operators in 2.2, and have now been removed.

``airflow.contrib`` packages and deprecated modules are dynamically generated (#26153, #26179, #26167)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``airflow.contrib`` packages and deprecated modules from Airflow 1.10 in ``airflow.hooks``, ``airflow.operators``, ``airflow.sensors`` packages are now dynamically generated modules and while users can continue using the deprecated contrib classes, they are no longer visible for static code check tools and will be reported as missing. It is recommended for the users to move to the non-deprecated classes.

``DBApiHook`` and ``SQLSensor`` have moved (#24836)
"""""""""""""""""""""""""""""""""""""""""""""""""""

``DBApiHook`` and ``SQLSensor`` have been moved to the ``apache-airflow-providers-common-sql`` provider.

DAG runs sorting logic changed in grid view (#25090)
""""""""""""""""""""""""""""""""""""""""""""""""""""

The ordering of DAG runs in the grid view has been changed to be more "natural".
The new logic generally orders by data interval, but a custom ordering can be
applied by setting the DAG to use a custom timetable.


New Features
^^^^^^^^^^^^
- Add Data-aware Scheduling (`AIP-48 <https://github.com/apache/airflow/pulls?q=is%3Apr+is%3Amerged+label%3AAIP-48+milestone%3A%22Airflow+2.4.0%22>`_)
- Add ``@task.short_circuit`` TaskFlow decorator (#25752)
- Make ``execution_date_or_run_id`` optional in ``tasks test`` command (#26114)
- Automatically register DAGs that are used in a context manager (#23592, #26398)
- Add option of sending DAG parser logs to stdout. (#25754)
- Support multiple ``DagProcessors`` parsing files from different locations. (#25935)
- Implement ``ExternalPythonOperator`` (#25780)
- Make execution_date optional for command ``dags test`` (#26111)
- Implement ``expand_kwargs()`` against a literal list (#25925)
- Add trigger rule tooltip (#26043)
- Add conf parameter to CLI for airflow dags test (#25900)
- Include scheduled slots in pools view (#26006)
- Add ``output`` property to ``MappedOperator`` (#25604)
- Add roles delete command to cli (#25854)
- Add Airflow specific warning classes (#25799)
- Add support for ``TaskGroup`` in ``ExternalTaskSensor`` (#24902)
- Add ``@task.kubernetes`` taskflow decorator (#25663)
- Add a way to import Airflow without side-effects (#25832)
- Let timetables control generated run_ids. (#25795)
- Allow per-timetable ordering override in grid view (#25633)
- Grid logs for mapped instances (#25610, #25621, #25611)
- Consolidate to one ``schedule`` param (#25410)
- DAG regex flag in backfill command (#23870)
- Adding support for owner links in the Dags view UI (#25280)
- Ability to clear a specific DAG Run's task instances via REST API (#23516)
- Possibility to document DAG with a separate markdown file (#25509)
- Add parsing context to DAG Parsing (#25161)
- Implement ``CronTriggerTimetable`` (#23662)
- Add option to mask sensitive data in UI configuration page (#25346)
- Create new databases from the ORM (#24156)
- Implement ``XComArg.zip(*xcom_args)`` (#25176)
- Introduce ``sla_miss`` metric (#23402)
- Implement ``map()`` semantic (#25085)
- Add override method to TaskGroupDecorator (#25160)
- Implement ``expand_kwargs()`` (#24989)
- Add parameter to turn off SQL query logging (#24570)
- Add ``DagWarning`` model, and a check for missing pools (#23317)
- Add Task Logs to Grid details panel (#24249)
- Added small health check server and endpoint in scheduler(#23905)
- Add built-in External Link for ``ExternalTaskMarker`` operator (#23964)
- Add default task retry delay config (#23861)
- Add clear DagRun endpoint. (#23451)
- Add support for timezone as string in cron interval timetable (#23279)
- Add auto-refresh to dags home page (#22900, #24770)

Improvements
^^^^^^^^^^^^

- Add more weekday operator and sensor examples #26071 (#26098)
- Add subdir parameter to dags reserialize command (#26170)
- Update zombie message to be more descriptive (#26141)
- Only send an ``SlaCallbackRequest`` if the DAG is scheduled (#26089)
- Promote ``Operator.output`` more (#25617)
- Upgrade API files to typescript (#25098)
- Less ``hacky`` double-rendering prevention in mapped task (#25924)
- Improve Audit log (#25856)
- Remove mapped operator validation code (#25870)
- More ``DAG(schedule=...)`` improvements (#25648)
- Reduce ``operator_name`` dupe in serialized JSON (#25819)
- Make grid view group/mapped summary UI more consistent (#25723)
- Remove useless statement in ``task_group_to_grid`` (#25654)
- Add optional data interval to ``CronTriggerTimetable`` (#25503)
- Remove unused code in ``/grid`` endpoint (#25481)
- Add and document description fields (#25370)
- Improve Airflow logging for operator Jinja template processing (#25452)
- Update core example DAGs to use ``@task.branch`` decorator (#25242)
- Update DAG ``audit_log`` route (#25415)
- Change stdout and stderr access mode to append in commands (#25253)
- Remove ``getTasks`` from Grid view (#25359)
- Improve taskflow type hints with ParamSpec (#25173)
- Use tables in grid details panes (#25258)
- Explicitly list ``@dag`` arguments (#25044)
- More typing in ``SchedulerJob`` and ``TaskInstance`` (#24912)
- Patch ``getfqdn`` with more resilient version (#24981)
- Replace all ``NBSP`` characters by ``whitespaces`` (#24797)
- Re-serialize all DAGs on ``airflow db upgrade`` (#24518)
- Rework contract of try_adopt_task_instances method (#23188)
- Make ``expand()`` error vague so it's not misleading (#24018)
- Add enum validation for ``[webserver]analytics_tool`` (#24032)
- Add ``dttm`` searchable field in audit log (#23794)
- Allow more parameters to be piped through via ``execute_in_subprocess`` (#23286)
- Use ``func.count`` to count rows (#23657)
- Remove stale serialized dags (#22917)
- AIP45 Remove dag parsing in airflow run local (#21877)
- Add support for queued state in DagRun update endpoint. (#23481)
- Add fields to dagrun endpoint (#23440)
- Use ``sql_alchemy_conn`` for celery result backend when ``result_backend`` is not set (#24496)

Bug Fixes
^^^^^^^^^

- Have consistent types between the ORM and the migration files (#24044, #25869)
- Disallow any dag tags longer than 100 char (#25196)
- Add the dag_id to ``AirflowDagCycleException`` message (#26204)
- Properly build URL to retrieve logs independently from system (#26337)
- For worker log servers only bind to IPV6 when dual stack is available (#26222)
- Fix ``TaskInstance.task`` not defined before ``handle_failure`` (#26040)
- Undo secrets backend config caching (#26223)
- Fix faulty executor config serialization logic (#26191)
- Show ``DAGs`` and ``Datasets`` menu links based on role permission (#26183)
- Allow setting ``TaskGroup`` tooltip via function docstring (#26028)
- Fix RecursionError on graph view of a DAG with many tasks (#26175)
- Fix backfill occasional deadlocking (#26161)
- Fix ``DagRun.start_date`` not set during backfill with ``--reset-dagruns`` True (#26135)
- Use label instead of id for dynamic task labels in graph (#26108)
- Don't fail DagRun when leaf ``mapped_task`` is SKIPPED (#25995)
- Add group prefix to decorated mapped task (#26081)
- Fix UI flash when triggering with dup logical date (#26094)
- Fix Make items nullable for ``TaskInstance`` related endpoints to avoid API errors (#26076)
- Fix ``BranchDateTimeOperator`` to be ``timezone-awreness-insensitive`` (#25944)
- Fix legacy timetable schedule interval params (#25999)
- Fix response schema for ``list-mapped-task-instance`` (#25965)
- Properly check the existence of missing mapped TIs (#25788)
- Fix broken auto-refresh on grid view (#25950)
- Use per-timetable ordering in grid UI (#25880)
- Rewrite recursion when parsing DAG into iteration (#25898)
- Find cross-group tasks in ``iter_mapped_dependants`` (#25793)
- Fail task if mapping upstream fails (#25757)
- Support ``/`` in variable get endpoint (#25774)
- Use cfg default_wrap value for grid logs (#25731)
- Add origin request args when triggering a run (#25729)
- Operator name separate from class (#22834)
- Fix incorrect data interval alignment due to assumption on input time alignment (#22658)
- Return None if an ``XComArg`` fails to resolve (#25661)
- Correct ``json`` arg help in ``airflow variables set`` command (#25726)
- Added MySQL index hint to use ``ti_state`` on ``find_zombies`` query (#25725)
- Only excluded actually expanded fields from render (#25599)
- Grid, fix toast for ``axios`` errors (#25703)
- Fix UI redirect (#26409)
- Require dag_id arg for dags list-runs (#26357)
- Check for queued states for dags auto-refresh (#25695)
- Fix upgrade code for the ``dag_owner_attributes`` table (#25579)
- Add map index to task logs api (#25568)
- Ensure that zombie tasks for dags with errors get cleaned up (#25550)
- Make extra link work in UI (#25500)
- Sync up plugin API schema and definition (#25524)
- First/last names can be empty (#25476)
- Refactor DAG pages to be consistent (#25402)
- Check ``expand_kwargs()`` input type before unmapping (#25355)
- Filter XCOM by key when calculating map lengths (#24530)
- Fix ``ExternalTaskSensor`` not working with dynamic task (#25215)
- Added exception catching to send default email if template file raises any exception (#24943)
- Bring ``MappedOperator`` members in sync with ``BaseOperator`` (#24034)


Misc/Internal
^^^^^^^^^^^^^

- Add automatically generated ``ERD`` schema for the ``MetaData`` DB (#26217)
- Mark serialization functions as internal (#26193)
- Remove remaining deprecated classes and replace them with ``PEP562`` (#26167)
- Move ``dag_edges`` and ``task_group_to_dict`` to corresponding util modules (#26212)
- Lazily import many modules to improve import speed (#24486, #26239)
- FIX Incorrect typing information (#26077)
- Add missing contrib classes to deprecated dictionaries (#26179)
- Re-configure/connect the ``ORM`` after forking to run a DAG processor (#26216)
- Remove cattrs from lineage processing. (#26134)
- Removed deprecated contrib files and replace them with ``PEP-562`` getattr (#26153)
- Make ``BaseSerialization.serialize`` "public" to other classes. (#26142)
- Change the template to use human readable task_instance description (#25960)
- Bump ``moment-timezone`` from ``0.5.34`` to ``0.5.35`` in ``/airflow/www`` (#26080)
- Fix Flask deprecation warning (#25753)
- Add ``CamelCase`` to generated operations types (#25887)
- Fix migration issues and tighten the CI upgrade/downgrade test (#25869)
- Fix type annotations in ``SkipMixin`` (#25864)
- Workaround setuptools editable packages path issue (#25848)
- Bump ``undici`` from ``5.8.0 to 5.9.1`` in /airflow/www (#25801)
- Add custom_operator_name attr to ``_BranchPythonDecoratedOperator`` (#25783)
- Clarify ``filename_template`` deprecation message (#25749)
- Use ``ParamSpec`` to replace ``...`` in Callable (#25658)
- Remove deprecated modules (#25543)
- Documentation on task mapping additions (#24489)
- Remove Smart Sensors (#25507)
- Fix ``elasticsearch`` test config to avoid warning on deprecated template (#25520)
- Bump ``terser`` from ``4.8.0 to 4.8.1`` in /airflow/ui (#25178)
- Generate ``typescript`` types from rest ``API`` docs (#25123)
- Upgrade utils files to ``typescript`` (#25089)
- Upgrade remaining context file to ``typescript``. (#25096)
- Migrate files to ``ts`` (#25267)
- Upgrade grid Table component to ``ts.`` (#25074)
- Skip mapping against mapped ``ti`` if it returns None (#25047)
- Refactor ``js`` file structure (#25003)
- Move mapped kwargs introspection to separate type (#24971)
- Only assert stuff for mypy when type checking (#24937)
- Bump ``moment`` from ``2.29.3 to 2.29.4`` in ``/airflow/www`` (#24885)
- Remove "bad characters" from our codebase (#24841)
- Remove ``xcom_push`` flag from ``BashOperator`` (#24824)
- Move Flask hook registration to end of file (#24776)
- Upgrade more javascript files to ``typescript`` (#24715)
- Clean up task decorator type hints and docstrings (#24667)
- Preserve original order of providers' connection extra fields in UI (#24425)
- Rename ``charts.css`` to ``chart.css`` (#24531)
- Rename ``grid.css`` to ``chart.css`` (#24529)
- Misc: create new process group by ``set_new_process_group`` utility (#24371)
- Airflow UI fix Prototype Pollution (#24201)
- Bump ``moto`` version (#24222)
- Remove unused ``[github_enterprise]`` from ref docs (#24033)
- Clean up ``f-strings`` in logging calls (#23597)
- Add limit for ``JPype1`` (#23847)
- Simply json responses (#25518)
- Add min attrs version (#26408)

Doc only changes
^^^^^^^^^^^^^^^^
- Add url prefix setting for ``Celery`` Flower (#25986)
- Updating deprecated configuration in examples (#26037)
- Fix wrong link for taskflow tutorial (#26007)
- Reorganize tutorials into a section (#25890)
- Fix concept doc for dynamic task map (#26002)
- Update code examples from "classic" operators to taskflow (#25845, #25657)
- Add instructions on manually fixing ``MySQL`` Charset problems (#25938)
- Prefer the local Quick Start in docs (#25888)
- Fix broken link to ``Trigger Rules`` (#25840)
- Improve docker documentation (#25735)
- Correctly link to Dag parsing context in docs (#25722)
- Add note on ``task_instance_mutation_hook`` usage (#25607)
- Note that TaskFlow API automatically passes data between tasks (#25577)
- Update DAG run to clarify when a DAG actually runs (#25290)
- Update tutorial docs to include a definition of operators (#25012)
- Rewrite the Airflow documentation home page (#24795)
- Fix ``task-generated mapping`` example (#23424)
- Add note on subtle logical date change in ``2.2.0`` (#24413)
- Add missing import in best-practices code example (#25391)



Airflow 2.3.4 (2022-08-23)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Added new config ``[logging]log_formatter_class`` to fix timezone display for logs on UI (#24811)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If you are using a custom Formatter subclass in your ``[logging]logging_config_class``, please inherit from ``airflow.utils.log.timezone_aware.TimezoneAware`` instead of ``logging.Formatter``.
For example, in your ``custom_config.py``:

.. code-block:: python

    from airflow.utils.log.timezone_aware import TimezoneAware


    # before
    class YourCustomFormatter(logging.Formatter): ...


    # after
    class YourCustomFormatter(TimezoneAware): ...


    AIRFLOW_FORMATTER = LOGGING_CONFIG["formatters"]["airflow"]
    AIRFLOW_FORMATTER["class"] = "somewhere.your.custom_config.YourCustomFormatter"
    # or use TimezoneAware class directly. If you don't have custom Formatter.
    AIRFLOW_FORMATTER["class"] = "airflow.utils.log.timezone_aware.TimezoneAware"

Bug Fixes
^^^^^^^^^

- Disable ``attrs`` state management on ``MappedOperator`` (#24772)
- Serialize ``pod_override`` to JSON before pickling ``executor_config`` (#24356)
- Fix ``pid`` check (#24636)
- Rotate session id during login (#25771)
- Fix mapped sensor with reschedule mode (#25594)
- Cache the custom secrets backend so the same instance gets reused (#25556)
- Add right padding (#25554)
- Fix reducing mapped length of a mapped task at runtime after a clear (#25531)
- Fix ``airflow db reset`` when dangling tables exist (#25441)
- Change ``disable_verify_ssl`` behaviour (#25023)
- Set default task group in dag.add_task method (#25000)
- Removed interfering force of index. (#25404)
- Remove useless logging line (#25347)
- Adding mysql index hint to use index on ``task_instance.state`` in critical section query (#25673)
- Configurable umask to all daemonized processes. (#25664)
- Fix the errors raised when None is passed to template filters (#25593)
- Allow wildcarded CORS origins (#25553)
- Fix "This Session's transaction has been rolled back" (#25532)
- Fix Serialization error in ``TaskCallbackRequest`` (#25471)
- fix - resolve bash by absolute path (#25331)
- Add ``__repr__`` to ParamsDict class (#25305)
- Only load distribution of a name once (#25296)
- convert ``TimeSensorAsync`` ``target_time`` to utc on call time (#25221)
- call ``updateNodeLabels`` after ``expandGroup`` (#25217)
- Stop SLA callbacks gazumping other callbacks and DOS'ing the ``DagProcessorManager`` queue (#25147)
- Fix ``invalidateQueries`` call (#25097)
- ``airflow/www/package.json``: Add name, version fields. (#25065)
- No grid auto-refresh for backfill dag runs (#25042)
- Fix tag link on dag detail page (#24918)
- Fix zombie task handling with multiple schedulers (#24906)
- Bind log server on worker to ``IPv6`` address (#24755) (#24846)
- Add ``%z`` for ``%(asctime)s`` to fix timezone for logs on UI (#24811)
- ``TriggerDagRunOperator.operator_extra_links`` is attr (#24676)
- Send DAG timeout callbacks to processor outside of ``prohibit_commit`` (#24366)
- Don't rely on current ORM structure for db clean command (#23574)
- Clear next method when clearing TIs (#23929)
- Two typing fixes (#25690)

Doc only changes
^^^^^^^^^^^^^^^^

- Update set-up-database.rst (#24983)
- Fix syntax in mysql setup documentation (#24893 (#24939)
- Note how DAG policy works with default_args (#24804)
- Update PythonVirtualenvOperator Howto (#24782)
- Doc: Add hyperlinks to Github PRs for Release Notes (#24532)

Misc/Internal
^^^^^^^^^^^^^

- Remove depreciation warning when use default remote tasks logging handlers (#25764)
- clearer method name in scheduler_job.py (#23702)
- Bump cattrs version (#25689)
- Include missing mention of ``external_executor_id`` in ``sql_engine_collation_for_ids`` docs (#25197)
- Refactor ``DR.task_instance_scheduling_decisions`` (#24774)
- Sort operator extra links (#24992)
- Extends ``resolve_xcom_backend`` function level documentation (#24965)
- Upgrade FAB to 4.1.3 (#24884)
- Limit Flask to <2.3 in the wake of 2.2 breaking our tests (#25511)
- Limit astroid version to < 2.12 (#24982)
- Move javascript compilation to host (#25169)
- Bump typing-extensions and mypy for ParamSpec (#25088)


Airflow 2.3.3 (2022-07-09)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

We've upgraded Flask App Builder to a major version 4.* (#24399)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Flask App Builder is one of the important components of Airflow Webserver, as
it uses a lot of dependencies that are essential to run the webserver and integrate it
in enterprise environments - especially authentication.

The FAB 4.* upgrades a number of dependencies to major releases, which upgrades them to versions
that have a number of security issues fixed. A lot of tests were performed to bring the dependencies
in a backwards-compatible way, however the dependencies themselves implement breaking changes in their
internals so it might be that some of those changes might impact the users in case they are using the
libraries for their own purposes.

One important change that you likely will need to apply to Oauth configuration is to add
``server_metadata_url`` or ``jwks_uri`` and you can read about it more
in `this issue <https://github.com/dpgaspar/Flask-AppBuilder/issues/1861>`_.

Here is the list of breaking changes in dependencies that comes together with FAB 4:

  * ``Flask`` from 1.X to 2.X `breaking changes <https://flask.palletsprojects.com/en/2.0.x/changes/#version-2-0-0>`__

  * ``flask-jwt-extended`` 3.X to 4.X `breaking changes: <https://flask-jwt-extended.readthedocs.io/en/stable/v4_upgrade_guide/>`__

  * ``Jinja2`` 2.X to 3.X `breaking changes: <https://jinja.palletsprojects.com/en/3.0.x/changes/#version-3-0-0>`__

  * ``Werkzeug`` 1.X to 2.X `breaking changes <https://werkzeug.palletsprojects.com/en/2.0.x/changes/#version-2-0-0>`__

  * ``pyJWT`` 1.X to 2.X `breaking changes: <https://pyjwt.readthedocs.io/en/stable/changelog.html#v2-0-0>`__

  * ``Click`` 7.X to 8.X `breaking changes: <https://click.palletsprojects.com/en/8.0.x/changes/#version-8-0-0>`__

  * ``itsdangerous`` 1.X to 2.X `breaking changes <https://github.com/pallets/itsdangerous/blob/main/CHANGES.rst#version-200>`__

Bug Fixes
^^^^^^^^^

- Fix exception in mini task scheduler (#24865)
- Fix cycle bug with attaching label to task group (#24847)
- Fix timestamp defaults for ``sensorinstance`` (#24638)
- Move fallible ``ti.task.dag`` assignment back inside ``try/except`` block (#24533) (#24592)
- Add missing types to ``FSHook`` (#24470)
- Mask secrets in ``stdout`` for ``airflow tasks test`` (#24362)
- ``DebugExecutor`` use ``ti.run()`` instead of ``ti._run_raw_task`` (#24357)
- Fix bugs in ``URI`` constructor for ``MySQL`` connection (#24320)
- Missing ``scheduleinterval`` nullable true added in ``openapi`` (#24253)
- Unify ``return_code`` interface for task runner (#24093)
- Handle occasional deadlocks in trigger with retries (#24071)
- Remove special serde logic for mapped ``op_kwargs`` (#23860)
- ``ExternalTaskSensor`` respects ``soft_fail`` if the external task enters a ``failed_state`` (#23647)
- Fix ``StatD`` timing metric units (#21106)
- Add ``cache_ok`` flag to sqlalchemy TypeDecorators. (#24499)
- Allow for ``LOGGING_LEVEL=DEBUG`` (#23360)
- Fix grid date ticks (#24738)
- Debounce status highlighting in Grid view (#24710)
- Fix Grid vertical scrolling (#24684)
- don't try to render child rows for closed groups (#24637)
- Do not calculate grid root instances (#24528)
- Maintain grid view selection on filtering upstream (#23779)
- Speed up ``grid_data`` endpoint by 10x (#24284)
- Apply per-run log templates to log handlers (#24153)
- Don't crash scheduler if exec config has old k8s objects (#24117)
- ``TI.log_url`` fix for ``map_index`` (#24335)
- Fix migration ``0080_2_0_2`` - Replace null values before setting column not null (#24585)
- Patch ``sql_alchemy_conn`` if old Postgres schemes used (#24569)
- Seed ``log_template`` table (#24511)
- Fix deprecated ``log_id_template`` value (#24506)
- Fix toast messages (#24505)
- Add indexes for CASCADE deletes for ``task_instance`` (#24488)
- Return empty dict if Pod JSON encoding fails (#24478)
- Improve grid rendering performance with a custom tooltip (#24417, #24449)
- Check for ``run_id`` for grid group summaries (#24327)
- Optimize calendar view for cron scheduled DAGs (#24262)
- Use ``get_hostname`` instead of ``socket.getfqdn`` (#24260)
- Check that edge nodes actually exist (#24166)
- Fix ``useTasks`` crash on error (#24152)
- Do not fail re-queued TIs (#23846)
- Reduce grid view API calls (#24083)
- Rename Permissions to Permission Pairs. (#24065)
- Replace ``use_task_execution_date`` with ``use_task_logical_date`` (#23983)
- Grid fix details button truncated and small UI tweaks (#23934)
- Add TaskInstance State ``REMOVED`` to finished states and success states (#23797)
- Fix mapped task immutability after clear (#23667)
- Fix permission issue for dag that has dot in name (#23510)
- Fix closing connection ``dbapi.get_pandas_df`` (#23452)
- Check bag DAG ``schedule_interval`` match timetable (#23113)
- Parse error for task added to multiple groups (#23071)
- Fix flaky order of returned dag runs (#24405)
- Migrate ``jsx`` files that affect run/task selection to ``tsx`` (#24509)
- Fix links to sources for examples (#24386)
- Set proper ``Content-Type`` and ``chartset`` on ``grid_data`` endpoint (#24375)

Doc only changes
^^^^^^^^^^^^^^^^

- Update templates doc to mention ``extras`` and format Airflow ``Vars`` / ``Conns`` (#24735)
- Document built in Timetables (#23099)
- Alphabetizes two tables (#23923)
- Clarify that users should not use Maria DB (#24556)
- Add imports to deferring code samples (#24544)
- Add note about image regeneration in June 2022 (#24524)
- Small cleanup of ``get_current_context()`` chapter (#24482)
- Fix default 2.2.5 ``log_id_template`` (#24455)
- Update description of installing providers separately from core (#24454)
- Mention context variables and logging (#24304)

Misc/Internal
^^^^^^^^^^^^^

- Remove internet explorer support (#24495)
- Removing magic status code numbers from ``api_connexion`` (#24050)
- Upgrade FAB to ``4.1.2`` (#24619)
- Switch Markdown engine to ``markdown-it-py`` (#19702)
- Update ``rich`` to latest version across the board. (#24186)
- Get rid of ``TimedJSONWebSignatureSerializer`` (#24519)
- Update flask-appbuilder ``authlib``/ ``oauth`` dependency (#24516)
- Upgrade to ``webpack`` 5 (#24485)
- Add ``typescript`` (#24337)
- The JWT claims in the request to retrieve logs have been standardized: we use ``nbf`` and ``aud`` claims for
  maturity and audience of the requests. Also "filename" payload field is used to keep log name. (#24519)
- Address all ``yarn`` test warnings (#24722)
- Upgrade to react 18 and chakra 2 (#24430)
- Refactor ``DagRun.verify_integrity`` (#24114)
- Upgrade FAB to ``4.1.1`` (#24399)
- We now need at least ``Flask-WTF 0.15`` (#24621)


Airflow 2.3.2 (2022-06-04)
--------------------------

No significant changes.

Bug Fixes
^^^^^^^^^

- Run the ``check_migration`` loop at least once
- Fix grid view for mapped tasks (#24059)
- Icons in grid view for different DAG run types (#23970)
- Faster grid view (#23951)
- Disallow calling expand with no arguments (#23463)
- Add missing ``is_mapped`` field to Task response. (#23319)
- DagFileProcessorManager: Start a new process group only if current process not a session leader (#23872)
- Mask sensitive values for not-yet-running TIs (#23807)
- Add cascade to ``dag_tag`` to ``dag`` foreign key (#23444)
- Use ``--subdir`` argument value for standalone dag processor. (#23864)
- Highlight task states by hovering on legend row (#23678)
- Fix and speed up grid view (#23947)
- Prevent UI from crashing if grid task instances are null (#23939)
- Remove redundant register exit signals in ``dag-processor`` command (#23886)
- Add ``__wrapped__`` property to ``_TaskDecorator`` (#23830)
- Fix UnboundLocalError when ``sql`` is empty list in DbApiHook (#23816)
- Enable clicking on DAG owner in autocomplete dropdown (#23804)
- Simplify flash message for ``_airflow_moved`` tables (#23635)
- Exclude missing tasks from the gantt view (#23627)

Doc only changes
^^^^^^^^^^^^^^^^

- Add column names for DB Migration Reference (#23853)

Misc/Internal
^^^^^^^^^^^^^

- Remove pinning for xmltodict (#23992)


Airflow 2.3.1 (2022-05-25)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^
No significant changes.

Bug Fixes
^^^^^^^^^

- Automatically reschedule stalled queued tasks in ``CeleryExecutor`` (#23690)
- Fix expand/collapse all buttons (#23590)
- Grid view status filters (#23392)
- Expand/collapse all groups (#23487)
- Fix retrieval of deprecated non-config values (#23723)
- Fix secrets rendered in UI when task is not executed. (#22754)
- Fix provider import error matching (#23825)
- Fix regression in ignoring symlinks (#23535)
- Fix ``dag-processor`` fetch metadata database config (#23575)
- Fix auto upstream dep when expanding non-templated field (#23771)
- Fix task log is not captured (#23684)
- Add ``reschedule`` to the serialized fields for the ``BaseSensorOperator`` (#23674)
- Modify db clean to also catch the ProgrammingError exception (#23699)
- Remove titles from link buttons (#23736)
- Fix grid details header text overlap (#23728)
- Ensure ``execution_timeout`` as timedelta (#23655)
- Don't run pre-migration checks for downgrade (#23634)
- Add index for event column in log table (#23625)
- Implement ``send_callback`` method for ``CeleryKubernetesExecutor`` and ``LocalKubernetesExecutor`` (#23617)
- Fix ``PythonVirtualenvOperator`` templated_fields (#23559)
- Apply specific ID collation to ``root_dag_id`` too (#23536)
- Prevent ``KubernetesJobWatcher`` getting stuck on resource too old (#23521)
- Fix scheduler crash when expanding with mapped task that returned none (#23486)
- Fix broken dagrun links when many runs start at the same time (#23462)
- Fix: Exception when parsing log #20966 (#23301)
- Handle invalid date parsing in webserver views. (#23161)
- Pools with negative open slots should not block other pools (#23143)
- Move around overflow, position and padding (#23044)
- Change approach to finding bad rows to LEFT OUTER JOIN. (#23528)
- Only count bad refs when ``moved`` table exists (#23491)
- Visually distinguish task group summary (#23488)
- Remove color change for highly nested groups (#23482)
- Optimize 2.3.0 pre-upgrade check queries (#23458)
- Add backward compatibility for ``core__sql_alchemy_conn__cmd`` (#23441)
- Fix literal cross product expansion (#23434)
- Fix broken task instance link in xcom list (#23367)
- Fix connection test button (#23345)
- fix cli ``airflow dags show`` for mapped operator (#23339)
- Hide some task instance attributes (#23338)
- Don't show grid actions if server would reject with permission denied (#23332)
- Use run_id for ``ti.mark_success_url`` (#23330)
- Fix update user auth stats (#23314)
- Use ``<Time />`` in Mapped Instance table (#23313)
- Fix duplicated Kubernetes DeprecationWarnings (#23302)
- Store grid view selection in url params (#23290)
- Remove custom signal handling in Triggerer (#23274)
- Override pool for TaskInstance when pool is passed from cli. (#23258)
- Show warning if '/' is used in a DAG run ID (#23106)
- Use kubernetes queue in kubernetes hybrid executors (#23048)
- Add tags inside try block. (#21784)

Doc only changes
^^^^^^^^^^^^^^^^

- Move ``dag_processing.processor_timeouts`` to counters section (#23393)
- Clarify that bundle extras should not be used for PyPi installs (#23697)
- Synchronize support for Postgres and K8S in docs (#23673)
- Replace DummyOperator references in docs (#23502)
- Add doc notes for keyword-only args for ``expand()`` and ``partial()`` (#23373)
- Document fix for broken elasticsearch logs with 2.3.0+ upgrade (#23821)

Misc/Internal
^^^^^^^^^^^^^

- Add typing for airflow/configuration.py (#23716)
- Disable Flower by default from docker-compose (#23685)
- Added postgres 14 to support versions(including breeze) (#23506)
- add K8S 1.24 support (#23637)
- Refactor code references from tree to grid (#23254)


Airflow 2.3.0 (2022-04-30)
--------------------------

For production docker image related changes, see the `Docker Image Changelog <https://airflow.apache.org/docs/docker-stack/changelog.html>`_.

Significant Changes
^^^^^^^^^^^^^^^^^^^

Passing ``execution_date`` to ``XCom.set()``, ``XCom.clear()`` , ``XCom.get_one()`` , and ``XCom.get_many()`` is deprecated (#19825)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Continuing the effort to bind TaskInstance to a DagRun, XCom entries are now also tied to a DagRun. Use the ``run_id`` argument to specify the DagRun instead.

Task log templates are now read from the metadata database instead of ``airflow.cfg`` (#20165)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously, a task's log is dynamically rendered from the ``[core] log_filename_template`` and ``[elasticsearch] log_id_template`` config values at runtime. This resulted in unfortunate characteristics, e.g. it is impractical to modify the config value after an Airflow instance is running for a while, since all existing task logs have be saved under the previous format and cannot be found with the new config value.

A new ``log_template`` table is introduced to solve this problem. This table is synchronized with the aforementioned config values every time Airflow starts, and a new field ``log_template_id`` is added to every DAG run to point to the format used by tasks (``NULL`` indicates the first ever entry for compatibility).

Minimum kubernetes library version bumped from ``3.0.0`` to ``21.7.0`` (#20759)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

.. note::

   This is only about changing the ``kubernetes`` library, not the Kubernetes cluster. Airflow support for
   Kubernetes version is described in `Installation prerequisites <https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html>`_.

No change in behavior is expected.  This was necessary in order to take advantage of a `bugfix <https://github.com/kubernetes-client/python-base/commit/70b78cd8488068c014b6d762a0c8d358273865b4>`_ concerning refreshing of Kubernetes API tokens with EKS, which enabled the removal of some `workaround code <https://github.com/apache/airflow/pull/20759>`_.

XCom now defined by ``run_id`` instead of ``execution_date`` (#20975)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

As a continuation to the TaskInstance-DagRun relation change started in Airflow 2.2, the ``execution_date`` columns on XCom has been removed from the database, and replaced by an `association proxy <https://docs.sqlalchemy.org/en/13/orm/extensions/associationproxy.html>`_ field at the ORM level. If you access Airflow's metadata database directly, you should rewrite the implementation to use the ``run_id`` column instead.

Note that Airflow's metadatabase definition on both the database and ORM levels are considered implementation detail without strict backward compatibility guarantees.

Non-JSON-serializable params deprecated (#21135).
"""""""""""""""""""""""""""""""""""""""""""""""""

It was previously possible to use dag or task param defaults that were not JSON-serializable.

For example this worked previously:

.. code-block:: python

  @dag.task(params={"a": {1, 2, 3}, "b": pendulum.now()})
  def datetime_param(value):
      print(value)


  datetime_param("{{ params.a }} | {{ params.b }}")

Note the use of ``set`` and ``datetime`` types, which are not JSON-serializable.  This behavior is problematic because to override these values in a dag run conf, you must use JSON, which could make these params non-overridable.  Another problem is that the support for param validation assumes JSON.  Use of non-JSON-serializable params will be removed in Airflow 3.0 and until then, use of them will produce a warning at parse time.

You must use ``postgresql://`` instead of ``postgres://`` in ``sql_alchemy_conn`` for SQLAlchemy 1.4.0+ (#21205)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

When you use SQLAlchemy 1.4.0+, you need to use ``postgresql://`` as the scheme in the ``sql_alchemy_conn``.
In the previous versions of SQLAlchemy it was possible to use ``postgres://`` , but using it in
SQLAlchemy 1.4.0+ results in:

.. code-block::

  >       raise exc.NoSuchModuleError(
              "Can't load plugin: %s:%s" % (self.group, name)
          )
  E       sqlalchemy.exc.NoSuchModuleError: Can't load plugin: sqlalchemy.dialects:postgres

If you cannot change the scheme of your URL immediately, Airflow continues to work with SQLAlchemy
1.3 and you can downgrade SQLAlchemy, but we recommend updating the scheme.
Details in the `SQLAlchemy Changelog <https://docs.sqlalchemy.org/en/14/changelog/changelog_14.html#change-3687655465c25a39b968b4f5f6e9170b>`_.

``auth_backends`` replaces ``auth_backend`` configuration setting (#21472)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously, only one backend was used to authorize use of the REST API. In 2.3 this was changed to support multiple backends, separated by comma. Each will be tried in turn until a successful response is returned.

``airflow.models.base.Operator`` is removed (#21505)
""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously, there was an empty class ``airflow.models.base.Operator`` for "type hinting". This class was never really useful for anything (everything it did could be done better with ``airflow.models.baseoperator.BaseOperator``), and has been removed. If you are relying on the class's existence, use ``BaseOperator`` (for concrete operators), ``airflow.models.abstractoperator.AbstractOperator`` (the base class of both ``BaseOperator`` and the AIP-42 ``MappedOperator``), or ``airflow.models.operator.Operator`` (a union type ``BaseOperator | MappedOperator`` for type annotation).

Zip files in the DAGs folder can no longer have a ``.py`` extension (#21538)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

It was previously possible to have any extension for zip files in the DAGs folder. Now ``.py`` files are going to be loaded as modules without checking whether it is a zip file, as it leads to less IO. If a ``.py`` file in the DAGs folder is a zip compressed file, parsing it will fail with an exception.

``auth_backends`` includes session (#21640)
"""""""""""""""""""""""""""""""""""""""""""

To allow the Airflow UI to use the API, the previous default authorization backend ``airflow.api.auth.backend.deny_all`` is changed to ``airflow.api.auth.backend.session``, and this is automatically added to the list of API authorization backends if a non-default value is set.

Default templates for log filenames and elasticsearch log_id changed (#21734)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

In order to support Dynamic Task Mapping the default templates for per-task instance logging has changed. If your config contains the old default values they will be upgraded-in-place.

If you are happy with the new config values you should *remove* the setting in ``airflow.cfg`` and let the default value be used. Old default values were:


* ``[core] log_filename_template``: ``{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log``
* ``[elasticsearch] log_id_template``: ``{dag_id}-{task_id}-{execution_date}-{try_number}``

``[core] log_filename_template`` now uses "hive partition style" of ``dag_id=<id>/run_id=<id>`` by default, which may cause problems on some older FAT filesystems. If this affects you then you will have to change the log template.

If you have customized the templates you should ensure that they contain ``{{ ti.map_index }}`` if you want to use dynamically mapped tasks.

If after upgrading you find your task logs are no longer accessible, try adding a row in the ``log_template`` table with ``id=0``
containing your previous ``log_id_template`` and ``log_filename_template``. For example, if you used the defaults in 2.2.5:

.. code-block:: sql

    INSERT INTO log_template (id, filename, elasticsearch_id, created_at) VALUES (0, '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log', '{dag_id}-{task_id}-{execution_date}-{try_number}', NOW());

BaseOperatorLink's ``get_link`` method changed to take a ``ti_key`` keyword argument (#21798)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

In v2.2 we "deprecated" passing an execution date to XCom.get methods, but there was no other option for operator links as they were only passed an execution_date.

Now in 2.3 as part of Dynamic Task Mapping (AIP-42) we will need to add map_index to the XCom row to support the "reduce" part of the API.

In order to support that cleanly we have changed the interface for BaseOperatorLink to take an TaskInstanceKey as the ``ti_key`` keyword argument (as execution_date + task is no longer unique for mapped operators).

The existing signature will be detected (by the absence of the ``ti_key`` argument) and continue to work.

``ReadyToRescheduleDep`` now only runs when ``reschedule`` is *True* (#21815)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

When a ``ReadyToRescheduleDep`` is run, it now checks whether the ``reschedule`` attribute on the operator, and always reports itself as *passed* unless it is set to *True*. If you use this dep class on your custom operator, you will need to add this attribute to the operator class. Built-in operator classes that use this dep class (including sensors and all subclasses) already have this attribute and are not affected.

The ``deps`` attribute on an operator class should be a class level attribute (#21815)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

To support operator-mapping (AIP 42), the ``deps`` attribute on operator class must be a set at the class level. This means that if a custom operator implements this as an instance-level variable, it will not be able to be used for operator-mapping. This does not affect existing code, but we highly recommend you to restructure the operator's dep logic in order to support the new feature.

Deprecation: ``Connection.extra`` must be JSON-encoded dict (#21816)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

TLDR
~~~~

From Airflow 3.0, the ``extra`` field in airflow connections must be a JSON-encoded Python dict.

What, why, and when?
~~~~~~~~~~~~~~~~~~~~

Airflow's Connection is used for storing credentials.  For storage of information that does not
fit into user / password / host / schema / port, we have the ``extra`` string field.  Its intention
was always to provide for storage of arbitrary key-value pairs, like ``no_host_key_check`` in the SSH
hook, or ``keyfile_dict`` in GCP.

But since the field is string, it's technically been permissible to store any string value.  For example
one could have stored the string value ``'my-website.com'`` and used this in the hook.  But this is a very
bad practice. One reason is intelligibility: when you look at the value for ``extra`` , you don't have any idea
what its purpose is.  Better would be to store ``{"api_host": "my-website.com"}`` which at least tells you
*something* about the value.  Another reason is extensibility: if you store the API host as a simple string
value, what happens if you need to add more information, such as the API endpoint, or credentials?  Then
you would need to convert the string to a dict, and this would be a breaking change.

For these reason, starting in Airflow 3.0 we will require that the ``Connection.extra`` field store
a JSON-encoded Python dict.

How will I be affected?
~~~~~~~~~~~~~~~~~~~~~~~

For users of providers that are included in the Airflow codebase, you should not have to make any changes
because in the Airflow codebase we should not allow hooks to misuse the ``Connection.extra`` field in this way.

However, if you have any custom hooks that store something other than JSON dict, you will have to update it.
If you do, you should see a warning any time that this connection is retrieved or instantiated (e.g. it should show up in
task logs).

To see if you have any connections that will need to be updated, you can run this command:

.. code-block:: shell

  airflow connections export - 2>&1 >/dev/null | grep 'non-JSON'

This will catch any warnings about connections that are storing something other than JSON-encoded Python dict in the ``extra`` field.

The ``tree`` default view setting has been renamed to ``grid`` (#22167)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If you set the ``dag_default_view`` config option or the ``default_view`` argument to ``DAG()`` to ``tree`` you will need to update your deployment. The old name will continue to work but will issue warnings.

Database configuration moved to new section (#22284)
""""""""""""""""""""""""""""""""""""""""""""""""""""

The following configurations have been moved from ``[core]`` to the new ``[database]`` section. However when reading the new option, the old option will be checked to see if it exists. If it does a DeprecationWarning will be issued and the old option will be used instead.

* sql_alchemy_conn
* sql_engine_encoding
* sql_engine_collation_for_ids
* sql_alchemy_pool_enabled
* sql_alchemy_pool_size
* sql_alchemy_max_overflow
* sql_alchemy_pool_recycle
* sql_alchemy_pool_pre_ping
* sql_alchemy_schema
* sql_alchemy_connect_args
* load_default_connections
* max_db_retries

Remove requirement that custom connection UI fields be prefixed (#22607)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Hooks can define custom connection fields for their connection type by implementing method ``get_connection_form_widgets``.  These custom fields appear in the web UI as additional connection attributes, but internally they are stored in the connection ``extra`` dict field.  For technical reasons, previously, when stored in the ``extra`` dict, the custom field's dict key had to take the form ``extra__<conn type>__<field name>``.  This had the consequence of making it more cumbersome to define connections outside of the UI, since the prefix ``extra__<conn type>__`` makes it tougher to read and work with. With #22607, we make it so that you can now define custom fields such that they can be read from and stored in ``extra`` without the prefix.

To enable this, update the dict returned by the ``get_connection_form_widgets`` method to remove the prefix from the keys.  Internally, the providers manager will still use a prefix to ensure each custom field is globally unique, but the absence of a prefix in the returned widget dict will signal to the Web UI to read and store custom fields without the prefix.  Note that this is only a change to the Web UI behavior; when updating your hook in this way, you must make sure that when your *hook* reads the ``extra`` field, it will also check for the prefixed value for backward compatibility.

The webserver.X_FRAME_ENABLED configuration works according to description now (#23222).
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

In Airflow 2.0.0 - 2.2.4 the webserver.X_FRAME_ENABLED parameter worked the opposite of its description,
setting the value to "true" caused "X-Frame-Options" header to "DENY" (not allowing Airflow to be used
in an iframe). When you set it to "false", the header was not added, so Airflow could be embedded in an
iframe. By default Airflow could not be embedded in an iframe.

In Airflow 2.2.5 there was a bug introduced that made it impossible to disable Airflow to
work in iframe. No matter what the configuration was set, it was possible to embed Airflow in an iframe.

Airflow 2.3.0 restores the original meaning to the parameter. If you set it to "true" (default) Airflow
can be embedded in an iframe (no header is added), but when you set it to "false" the header is added
and Airflow cannot be embedded in an iframe.


New Features
^^^^^^^^^^^^

- Add dynamic task mapping (`AIP-42 <https://github.com/apache/airflow/pulls?q=is%3Apr+is%3Amerged+label%3Aarea%3Adynamic-task-mapping+milestone%3A%22Airflow+2.3.0%22>`_)
- New Grid View replaces Tree View (#18675)
- Templated ``requirements.txt`` in Python Operators (#17349)
- Allow reuse of decorated tasks (#22941)
- Move the database configuration to a new section (#22284)
- Add ``SmoothOperator`` (#22813)
- Make operator's ``execution_timeout`` configurable (#22389)
- Events Timetable (#22332)
- Support dag serialization with custom ``ti_deps`` rules (#22698)
- Support log download in task log view (#22804)
- support for continue backfill on failures (#22697)
- Add ``dag-processor`` cli command (#22305)
- Add possibility to create users in LDAP mode (#22619)
- Add ``ignore_first_depends_on_past`` for scheduled jobs (#22491)
- Update base sensor operator to support XCOM return value (#20656)
- Add an option for run id in the ui trigger screen (#21851)
- Enable JSON serialization for connections (#19857)
- Add REST API endpoint for bulk update of DAGs (#19758)
- Add queue button to click-on-DagRun interface. (#21555)
- Add ``list-import-errors`` to ``airflow dags`` command (#22084)
- Store callbacks in database if ``standalone_dag_processor`` config is True. (#21731)
- Add LocalKubernetesExecutor (#19729)
- Add ``celery.task_timeout_error`` metric (#21602)
- Airflow ``db downgrade`` cli command (#21596)
- Add ``ALL_SKIPPED`` trigger rule (#21662)
- Add ``db clean`` CLI command for purging old data (#20838)
- Add ``celery_logging_level`` (#21506)
- Support different timeout value for dag file parsing (#21501)
- Support generating SQL script for upgrades (#20962)
- Add option to compress Serialized dag data (#21332)
- Branch python operator decorator (#20860)
- Add Audit Log View to Dag View (#20733)
- Add missing StatsD metric for failing SLA Callback notification (#20924)
- Add ``ShortCircuitOperator`` configurability for respecting downstream trigger rules (#20044)
- Allow using Markup in page title in Webserver (#20888)
- Add Listener Plugin API that tracks TaskInstance state changes (#20443)
- Add context var hook to inject more env vars (#20361)
- Add a button to set all tasks to skipped (#20455)
- Cleanup pending pods (#20438)
- Add config to warn public deployment exposure in UI (#18557)
- Log filename template records (#20165)
- Added windows extensions (#16110)
- Showing approximate time until next dag_run in Airflow  (#20273)
- Extend config window on UI (#20052)
- Add show dag dependencies feature to CLI (#19985)
- Add cli command for 'airflow dags reserialize` (#19471)
- Add missing description field to Pool schema(REST API) (#19841)
- Introduce DagRun action to change state to queued. (#19353)
- Add DAG run details page (#19705)
- Add role export/import to cli tools (#18916)
- Adding ``dag_id_pattern`` parameter to the ``/dags`` endpoint (#18924)


Improvements
^^^^^^^^^^^^

- Show schedule_interval/timetable description in UI (#16931)
- Added column duration to DAG runs view (#19482)
- Enable use of custom conn extra fields without prefix (#22607)
- Initialize finished counter at zero (#23080)
- Improve logging of optional provider features messages (#23037)
- Meaningful error message in resolve_template_files (#23027)
- Update ImportError items instead of deleting and recreating them (#22928)
- Add option ``--skip-init`` to db reset command (#22989)
- Support importing connections from files with ".yml" extension (#22872)
- Support glob syntax in ``.airflowignore`` files (#21392) (#22051)
- Hide pagination when data is a single page (#22963)
- Support for sorting DAGs in the web UI (#22671)
- Speed up ``has_access`` decorator by ~200ms (#22858)
- Add XComArg to lazy-imported list of Airflow module (#22862)
- Add more fields to REST API dags/dag_id/details endpoint (#22756)
- Don't show irrelevant/duplicated/"internal" Task attrs in UI (#22812)
- No need to load whole ti in current_state (#22764)
- Pickle dag exception string fix (#22760)
- Better verification of Localexecutor's parallelism option (#22711)
- log backfill exceptions to sentry (#22704)
- retry commit on MySQL deadlocks during backfill (#22696)
- Add more fields to REST API get DAG(dags/dag_id) endpoint (#22637)
- Use timetable to generate planned days for current year (#22055)
- Disable connection pool for celery worker (#22493)
- Make date picker label visible in trigger dag view (#22379)
- Expose ``try_number`` in airflow vars (#22297)
- Add generic connection type (#22310)
- Add a few more fields to the taskinstance finished log message (#22262)
- Pause auto-refresh if scheduler isn't running (#22151)
- Show DagModel details. (#21868)
- Add pip_install_options to PythonVirtualenvOperator (#22158)
- Show import error for ``airflow dags list`` CLI command (#21991)
- Pause auto-refresh when page is hidden (#21904)
- Default args type check (#21809)
- Enhance magic methods on XComArg for UX (#21882)
- py files don't have to be checked ``is_zipfiles`` in refresh_dag (#21926)
- Fix TaskDecorator type hints (#21881)
- Add 'Show record' option for variables (#21342)
- Use DB where possible for quicker ``airflow dag`` subcommands (#21793)
- REST API: add rendered fields in task instance. (#21741)
- Change the default auth backend to session (#21640)
- Don't check if ``py`` DAG files are zipped during parsing (#21538)
- Switch XCom implementation to use ``run_id`` (#20975)
- Action log on Browse Views (#21569)
- Implement multiple API auth backends (#21472)
- Change logging level details of connection info in ``get_connection()`` (#21162)
- Support mssql in airflow db shell (#21511)
- Support config ``worker_enable_remote_control`` for celery (#21507)
- Log memory usage in ``CgroupTaskRunner`` (#21481)
- Modernize DAG-related URL routes and rename "tree" to "grid" (#20730)
- Move Zombie detection to ``SchedulerJob`` (#21181)
- Improve speed to run ``airflow`` by 6x (#21438)
- Add more SQL template fields renderers (#21237)
- Simplify fab has access lookup (#19294)
- Log context only for default method (#21244)
- Log trigger status only if at least one is running (#21191)
- Add optional features in providers. (#21074)
- Better multiple_outputs inferral for @task.python (#20800)
- Improve handling of string type and non-attribute ``template_fields`` (#21054)
- Remove un-needed deps/version requirements (#20979)
- Correctly specify overloads for TaskFlow API for type-hinting (#20933)
- Introduce notification_sent to SlaMiss view (#20923)
- Rewrite the task decorator as a composition (#20868)
- Add "Greater/Smaller than or Equal" to filters in the browse views (#20602) (#20798)
- Rewrite DAG run retrieval in task command (#20737)
- Speed up creation of DagRun for large DAGs (5k+ tasks) by 25-130% (#20722)
- Make native environment Airflow-flavored like sandbox (#20704)
- Better error when param value has unexpected type (#20648)
- Add filter by state in DagRun REST API (List Dag Runs) (#20485)
- Prevent exponential memory growth in Tasks with custom logging handler  (#20541)
- Set default logger in logging Mixin (#20355)
- Reduce deprecation warnings from www (#20378)
- Add hour and minute to time format on x-axis of all charts using nvd3.lineChart (#20002)
- Add specific warning when Task asks for more slots than pool defined with (#20178)
- UI: Update duration column for better human readability (#20112)
- Use Viewer role as example public role (#19215)
- Properly implement DAG param dict copying (#20216)
- ``ShortCircuitOperator`` push XCom by returning python_callable result (#20071)
- Add clear logging to tasks killed due to a Dagrun timeout (#19950)
- Change log level for Zombie detection messages (#20204)
- Better confirmation prompts (#20183)
- Only execute TIs of running DagRuns (#20182)
- Check and run migration in commands if necessary (#18439)
- Log only when Zombies exists (#20118)
- Increase length of the email and username (#19932)
- Add more filtering options for TI's in the UI (#19910)
- Dynamically enable "Test Connection" button by connection type (#19792)
- Avoid littering postgres server logs with "could not obtain lock" with HA schedulers (#19842)
- Renamed ``Connection.get_hook`` parameter to make it the same as in ``SqlSensor`` and ``SqlOperator``. (#19849)
- Add hook_params in SqlSensor using the latest changes from PR #18718. (#18431)
- Speed up webserver boot time by delaying provider initialization (#19709)
- Configurable logging of ``XCOM`` value in PythonOperator (#19378)
- Minimize production js files (#19658)
- Add ``hook_params`` in ``BaseSqlOperator`` (#18718)
- Add missing "end_date" to hash components (#19281)
- More friendly output of the airflow plugins command + add timetables (#19298)
- Add sensor default timeout config (#19119)
- Update ``taskinstance`` REST API schema to include dag_run_id field (#19105)
- Adding feature in bash operator to append the user defined env variable to system env variable (#18944)
- Duplicate Connection: Added logic to query if a connection id exists before creating one (#18161)


Bug Fixes
^^^^^^^^^

- Use inherited 'trigger_tasks' method (#23016)
- In DAG dependency detector, use class type instead of class name (#21706)
- Fix tasks being wrongly skipped by schedule_after_task_execution (#23181)
- Fix X-Frame enabled behaviour (#23222)
- Allow ``extra`` to be nullable in connection payload as per schema(REST API). (#23183)
- Fix ``dag_id`` extraction for dag level access checks in web ui (#23015)
- Fix timezone display for logs on UI (#23075)
- Include message in graph errors (#23021)
- Change trigger dropdown left position (#23013)
- Don't add planned tasks for legacy DAG runs (#23007)
- Add dangling rows check for TaskInstance references (#22924)
- Validate the input params in connection ``CLI`` command (#22688)
- Fix trigger event payload is not persisted in db (#22944)
- Drop "airflow moved" tables in command ``db reset`` (#22990)
- Add max width to task group tooltips (#22978)
- Add template support for ``external_task_ids``. (#22809)
- Allow ``DagParam`` to hold falsy values (#22964)
- Fix regression in pool metrics (#22939)
- Priority order tasks even when using pools (#22483)
- Do not clear XCom when resuming from deferral (#22932)
- Handle invalid JSON metadata in ``get_logs_with_metadata endpoint``. (#22898)
- Fix pre-upgrade check for rows dangling w.r.t. dag_run (#22850)
- Fixed backfill interference with scheduler (#22701)
- Support conf param override for backfill runs (#22837)
- Correctly interpolate pool name in ``PoolSlotsAvailableDep`` statues (#22807)
- Fix ``email_on_failure`` with ``render_template_as_native_obj`` (#22770)
- Fix processor cleanup on ``DagFileProcessorManager`` (#22685)
- Prevent meta name clash for task instances (#22783)
- remove json parse for gantt chart (#22780)
- Check for missing dagrun should know version (#22752)
- Fixes ``ScheduleInterval`` spec (#22635)
- Fixing task status for non-running and non-committed tasks  (#22410)
- Do not log the hook connection details even at DEBUG level (#22627)
- Stop crashing when empty logs are received from kubernetes client (#22566)
- Fix bugs about timezone change (#22525)
- Fix entire DAG stops when one task has end_date (#20920)
- Use logger to print message during task execution. (#22488)
- Make sure finalizers are not skipped during exception handling (#22475)
- update smart sensor docs and minor fix on ``is_smart_sensor_compatible()`` (#22386)
- Fix ``run_id`` k8s and elasticsearch compatibility with Airflow 2.1 (#22385)
- Allow to ``except_skip`` None on ``BranchPythonOperator`` (#20411)
- Fix incorrect datetime details (DagRun views) (#21357)
- Remove incorrect deprecation warning in secrets backend (#22326)
- Remove ``RefreshConfiguration`` workaround for K8s token refreshing (#20759)
- Masking extras in GET ``/connections/<connection>`` endpoint (#22227)
- Set ``queued_dttm`` when submitting task to directly to executor (#22259)
- Addressed some issues in the tutorial mentioned in discussion #22233 (#22236)
- Change default python executable to python3 for docker decorator (#21973)
- Don't validate that Params are JSON when NOTSET (#22000)
- Add per-DAG delete permissions (#21938)
- Fix handling some None parameters in kubernetes 23 libs. (#21905)
- Fix handling of empty (None) tags in ``bulk_write_to_db`` (#21757)
- Fix DAG date range bug (#20507)
- Removed ``request.referrer`` from views.py  (#21751)
- Make ``DbApiHook`` use ``get_uri`` from Connection (#21764)
- Fix some migrations (#21670)
- [de]serialize resources on task correctly (#21445)
- Add params ``dag_id``, ``task_id`` etc to ``XCom.serialize_value`` (#19505)
- Update test connection functionality to use custom form fields (#21330)
- fix all "high" npm vulnerabilities (#21526)
- Fix bug incorrectly removing action from role, rather than permission. (#21483)
- Fix relationship join bug in FAB/SecurityManager with SQLA 1.4 (#21296)
- Use Identity instead of Sequence in SQLAlchemy 1.4 for MSSQL (#21238)
- Ensure ``on_task_instance_running`` listener can get at task (#21157)
- Return to the same place when triggering a DAG (#20955)
- Fix task ID deduplication in ``@task_group`` (#20870)
- Add downgrade to some FAB migrations (#20874)
- Only validate Params when DAG is triggered (#20802)
- Fix ``airflow trigger`` cli (#20781)
- Fix task instances iteration in a pool to prevent blocking (#20816)
- Allow depending to a ``@task_group`` as a whole (#20671)
- Use original task's ``start_date`` if a task continues after deferral (#20062)
- Disabled edit button in task instances list view page (#20659)
- Fix a package name import error (#20519) (#20519)
- Remove ``execution_date`` label when get cleanup pods list (#20417)
- Remove unneeded FAB REST API endpoints (#20487)
- Fix parsing of Cloudwatch log group arn containing slashes (#14667) (#19700)
- Sanity check for MySQL's TIMESTAMP column (#19821)
- Allow using default celery command group with executors subclassed from Celery-based executors. (#18189)
- Move ``class_permission_name`` to mixin so it applies to all classes (#18749)
- Adjust trimmed_pod_id and replace '.' with '-' (#19036)
- Pass custom_headers to send_email and send_email_smtp (#19009)
- Ensure ``catchup=False`` is used in example dags (#19396)
- Edit permalinks in OpenApi description file (#19244)
- Navigate directly to DAG when selecting from search typeahead list (#18991)
- [Minor] Fix padding on home page (#19025)


Doc only changes
^^^^^^^^^^^^^^^^

- Update doc for DAG file processing (#23209)
- Replace changelog/updating with release notes and ``towncrier`` now (#22003)
- Fix wrong reference in tracking-user-activity.rst (#22745)
- Remove references to ``rbac = True`` from docs (#22725)
- Doc: Update description for executor-bound dependencies (#22601)
- Update check-health.rst (#22372)
- Stronger language about Docker Compose customizability (#22304)
- Update logging-tasks.rst (#22116)
- Add example config of ``sql_alchemy_connect_args`` (#22045)
- Update best-practices.rst (#22053)
- Add information on DAG pausing/deactivation/deletion (#22025)
- Add brief examples of integration test dags you might want (#22009)
- Run inclusive language check on CHANGELOG (#21980)
- Add detailed email docs for Sendgrid (#21958)
- Add docs for ``db upgrade`` / ``db downgrade`` (#21879)
- Update modules_management.rst (#21889)
- Fix UPDATING section on SqlAlchemy 1.4 scheme changes (#21887)
- Update TaskFlow tutorial doc to show how to pass "operator-level" args. (#21446)
- Fix doc - replace decreasing by increasing (#21805)
- Add another way to dynamically generate DAGs to docs (#21297)
- Add extra information about time synchronization needed (#21685)
- Update debug.rst docs (#21246)
- Replaces the usage of ``postgres://`` with ``postgresql://`` (#21205)
- Fix task execution process in ``CeleryExecutor`` docs (#20783)


Misc/Internal
^^^^^^^^^^^^^

- Bring back deprecated security manager functions (#23243)
- Replace usage of ``DummyOperator`` with ``EmptyOperator`` (#22974)
- Deprecate ``DummyOperator`` in favor of ``EmptyOperator`` (#22832)
- Remove unnecessary python 3.6 conditionals (#20549)
- Bump ``moment`` from 2.29.1 to 2.29.2 in /airflow/www (#22873)
- Bump ``prismjs`` from 1.26.0 to 1.27.0 in /airflow/www (#22823)
- Bump ``nanoid`` from 3.1.23 to 3.3.2 in /airflow/www (#22803)
- Bump ``minimist`` from 1.2.5 to 1.2.6 in /airflow/www (#22798)
- Remove dag parsing from db init command (#22531)
- Update our approach for executor-bound dependencies (#22573)
- Use ``Airflow.Base.metadata`` in FAB models (#22353)
- Limit docutils to make our documentation pretty again (#22420)
- Add Python 3.10 support (#22050)
- [FEATURE] add 1.22 1.23 K8S support (#21902)
- Remove pandas upper limit now that SQLA is 1.4+ (#22162)
- Patch ``sql_alchemy_conn`` if old postgres scheme used (#22333)
- Protect against accidental misuse of XCom.get_value() (#22244)
- Order filenames for migrations (#22168)
- Don't try to auto generate migrations for Celery tables (#22120)
- Require SQLAlchemy 1.4 (#22114)
- bump sphinx-jinja (#22101)
- Add compat shim for SQLAlchemy to avoid warnings (#21959)
- Rename ``xcom.dagrun_id`` to ``xcom.dag_run_id`` (#21806)
- Deprecate non-JSON ``conn.extra`` (#21816)
- Bump upper bound version of ``jsonschema`` to 5.0 (#21712)
- Deprecate helper utility ``days_ago`` (#21653)
- Remove ```:type``` lines now ``sphinx-autoapi`` supports type hints (#20951)
- Silence deprecation warning in tests (#20900)
- Use ``DagRun.run_id`` instead of ``execution_date`` when updating state of TIs (UI & REST API) (#18724)
- Add Context stub to Airflow packages (#20817)
- Update Kubernetes library version (#18797)
- Rename ``PodLauncher`` to ``PodManager`` (#20576)
- Removes Python 3.6 support (#20467)
- Add deprecation warning for non-json-serializable params (#20174)
- Rename TaskMixin to DependencyMixin (#20297)
- Deprecate passing execution_date to XCom methods (#19825)
- Remove ``get_readable_dags`` and ``get_editable_dags``, and ``get_accessible_dags``. (#19961)
- Remove postgres 9.6 support (#19987)
- Removed hardcoded connection types. Check if hook is instance of DbApiHook. (#19639)
- add kubernetes 1.21 support (#19557)
- Add FAB base class and set import_name explicitly. (#19667)
- Removes unused state transitions to handle auto-changing view permissions. (#19153)
- Chore: Use enum for ``__var`` and ``__type`` members (#19303)
- Use fab models (#19121)
- Consolidate method names between Airflow Security Manager and FAB default (#18726)
- Remove distutils usages for Python 3.10 (#19064)
- Removing redundant ``max_tis_per_query`` initialisation on SchedulerJob (#19020)
- Remove deprecated usage of ``init_role()`` from API (#18820)
- Remove duplicate code on dbapi hook (#18821)


Airflow 2.2.5, (2022-04-04)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""
- Check and disallow a relative path for sqlite (#22530)
- Fixed dask executor and tests (#22027)
- Fix broken links to celery documentation (#22364)
- Fix incorrect data provided to tries & landing times charts (#21928)
- Fix assignment of unassigned triggers (#21770)
- Fix triggerer ``--capacity`` parameter (#21753)
- Fix graph auto-refresh on page load (#21736)
- Fix filesystem sensor for directories (#21729)
- Fix stray ``order_by(TaskInstance.execution_date)`` (#21705)
- Correctly handle multiple '=' in LocalFileSystem secrets. (#21694)
- Log exception in local executor (#21667)
- Disable ``default_pool`` delete on web ui (#21658)
- Extends ``typing-extensions`` to be installed with python 3.8+ #21566 (#21567)
- Dispose unused connection pool (#21565)
- Fix logging JDBC SQL error when task fails (#21540)
- Filter out default configs when overrides exist. (#21539)
- Fix Resources ``__eq__`` check (#21442)
- Fix ``max_active_runs=1`` not scheduling runs when ``min_file_process_interval`` is high (#21413)
- Reduce DB load incurred by Stale DAG deactivation (#21399)
- Fix race condition between triggerer and scheduler (#21316)
- Fix trigger dag redirect from task instance log view (#21239)
- Log traceback in trigger exceptions (#21213)
- A trigger might use a connection; make sure we mask passwords (#21207)
- Update ``ExternalTaskSensorLink`` to handle templated ``external_dag_id`` (#21192)
- Ensure ``clear_task_instances`` sets valid run state (#21116)
- Fix: Update custom connection field processing (#20883)
- Truncate stack trace to DAG user code for exceptions raised during execution (#20731)
- Fix duplicate trigger creation race condition (#20699)
- Fix Tasks getting stuck in scheduled state (#19747)
- Fix: Do not render undefined graph edges (#19684)
- Set ``X-Frame-Options`` header to DENY only if ``X_FRAME_ENABLED`` is set to true. (#19491)

Doc only changes
""""""""""""""""
- adding ``on_execute_callback`` to callbacks docs (#22362)
- Add documentation on specifying a DB schema. (#22347)
- Fix postgres part of pipeline example of tutorial (#21586)
- Extend documentation for states of DAGs & tasks and update trigger rules docs (#21382)
- DB upgrade is required when updating Airflow (#22061)
- Remove misleading MSSQL information from the docs (#21998)

Misc
""""
- Add the new Airflow Trove Classifier to setup.cfg (#22241)
- Rename ``to_delete`` to ``to_cancel`` in TriggerRunner (#20658)
- Update Flask-AppBuilder to ``3.4.5`` (#22596)

Airflow 2.2.4, (2022-02-22)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Smart sensors deprecated
""""""""""""""""""""""""

Smart sensors, an "early access" feature added in Airflow 2, are now deprecated and will be removed in Airflow 2.4.0. They have been superseded by Deferrable Operators, added in Airflow 2.2.0.

See `Migrating to Deferrable Operators <https://airflow.apache.org/docs/apache-airflow/2.2.4/concepts/smart-sensors.html#migrating-to-deferrable-operators>`_ for details on how to migrate.

Bug Fixes
^^^^^^^^^

- Adding missing login provider related methods from Flask-Appbuilder (#21294)
- Fix slow DAG deletion due to missing ``dag_id`` index for job table (#20282)
- Add a session backend to store session data in the database (#21478)
- Show task status only for running dags or only for the last finished dag (#21352)
- Use compat data interval shim in log handlers (#21289)
- Fix mismatch in generated run_id and logical date of DAG run (#18707)
- Fix TriggerDagRunOperator extra link (#19410)
- Add possibility to create user in the Remote User mode (#19963)
- Avoid deadlock when rescheduling task (#21362)
- Fix the incorrect scheduling time for the first run of dag (#21011)
- Fix Scheduler crash when executing task instances of missing DAG (#20349)
- Deferred tasks does not cancel when DAG is marked fail (#20649)
- Removed duplicated dag_run join in ``Dag.get_task_instances()`` (#20591)
- Avoid unintentional data loss when deleting DAGs (#20758)
- Fix session usage in ``/rendered-k8s`` view (#21006)
- Fix ``airflow dags backfill --reset-dagruns`` errors when run twice (#21062)
- Do not set ``TaskInstance.max_tries`` in ``refresh_from_task`` (#21018)
- Don't require dag_id in body in dagrun REST API endpoint (#21024)
- Add Roles from Azure OAUTH Response in internal Security Manager (#20707)
- Allow Viewing DagRuns and TIs if a user has DAG "read" perms (#20663)
- Fix running ``airflow dags test <dag_id> <execution_dt>`` results in error when run twice (#21031)
- Switch to non-vendored latest connexion library (#20910)
- Bump flask-appbuilder to ``>=3.3.4`` (#20628)
- upgrade celery to ``5.2.3`` (#19703)
- Bump croniter from ``<1.1`` to ``<1.2`` (#20489)
- Avoid calling ``DAG.following_schedule()`` for ``TaskInstance.get_template_context()`` (#20486)
- Fix(standalone): Remove hardcoded Webserver port (#20429)
- Remove unnecessary logging in experimental API (#20356)
- Un-ignore DeprecationWarning (#20322)
- Deepcopying Kubernetes Secrets attributes causing issues (#20318)
- Fix(dag-dependencies): fix arrow styling (#20303)
- Adds retry on taskinstance retrieval lock (#20030)
- Correctly send timing metrics when using dogstatsd (fix schedule_delay metric) (#19973)
- Enhance ``multiple_outputs`` inference of dict typing (#19608)
- Fixing Amazon SES email backend (#18042)
- Pin MarkupSafe until we are able to upgrade Flask/Jinja (#21664)

Doc only changes
^^^^^^^^^^^^^^^^

- Added explaining concept of logical date in DAG run docs (#21433)
- Add note about Variable precedence with env vars (#21568)
- Update error docs to include before_send option (#21275)
- Augment xcom docs (#20755)
- Add documentation and release policy on "latest" constraints (#21093)
- Add a link to the DAG model in the Python API reference (#21060)
- Added an enum param example (#20841)
- Compare taskgroup and subdag (#20700)
- Add note about reserved ``params`` keyword (#20640)
- Improve documentation on ``Params`` (#20567)
- Fix typo in MySQL Database creation code (Set up DB docs)  (#20102)
- Add requirements.txt description (#20048)
- Clean up ``default_args`` usage in docs (#19803)
- Add docker-compose explanation to conn localhost (#19076)
- Update CSV ingest code for tutorial (#18960)
- Adds Pendulum 1.x -> 2.x upgrade documentation (#18955)
- Clean up dynamic ``start_date`` values from docs (#19607)
- Docs for multiple pool slots (#20257)
- Update upgrading.rst with detailed code example of how to resolve post-upgrade warning (#19993)

Misc
^^^^

- Deprecate some functions in the experimental API (#19931)
- Deprecate smart sensors (#20151)

Airflow 2.2.3, (2021-12-21)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- Lazy Jinja2 context (#20217)
- Exclude ``snowflake-sqlalchemy`` v1.2.5 (#20245)
- Move away from legacy ``importlib.resources`` API (#19091)
- Move ``setgid`` as the first command executed in forked task runner (#20040)
- Fix race condition when starting ``DagProcessorAgent`` (#19935)
- Limit ``httpx`` to <0.20.0 (#20218)
- Log provider import errors as debug warnings (#20172)
- Bump minimum required ``alembic`` version (#20153)
- Fix log link in gantt view (#20121)
- fixing #19028 by moving chown to use sudo (#20114)
- Lift off upper bound for ``MarkupSafe`` (#20113)
- Fix infinite recursion on redact log (#20039)
- Fix db downgrades (#19994)
- Context class handles deprecation (#19886)
- Fix possible reference to undeclared variable (#19933)
- Validate ``DagRun`` state is valid on assignment (#19898)
- Workaround occasional deadlocks with MSSQL (#19856)
- Enable task run setting to be able reinitialize (#19845)
- Fix log endpoint for same task (#19672)
- Cast macro datetime string inputs explicitly (#19592)
- Do not crash with stacktrace when task instance is missing (#19478)
- Fix log timezone in task log view (#19342) (#19401)
- Fix: Add taskgroup tooltip to graph view (#19083)
- Rename execution date in forms and tables (#19063)
- Simplify "invalid TI state" message (#19029)
- Handle case of nonexistent file when preparing file path queue (#18998)
- Do not create dagruns for DAGs with import errors  (#19367)
- Fix field relabeling when switching between conn types (#19411)
- ``KubernetesExecutor`` should default to template image if used (#19484)
- Fix task instance api cannot list task instances with ``None`` state (#19487)
- Fix IntegrityError in ``DagFileProcessor.manage_slas`` (#19553)
- Declare data interval fields as serializable (#19616)
- Relax timetable class validation (#19878)
- Fix labels used to find queued ``KubernetesExecutor`` pods (#19904)
- Fix moved data migration check for MySQL when replication is used (#19999)

Doc only changes
^^^^^^^^^^^^^^^^

- Warn without tracebacks when example_dags are missing deps (#20295)
- Deferrable operators doc clarification (#20150)
- Ensure the example DAGs are all working (#19355)
- Updating core example DAGs to use TaskFlow API where applicable (#18562)
- Add xcom clearing behaviour on task retries (#19968)
- Add a short chapter focusing on adapting secret format for connections (#19859)
- Add information about supported OS-es for Apache Airflow (#19855)
- Update docs to reflect that changes to the ``base_log_folder`` require updating other configs (#19793)
- Disclaimer in ``KubernetesExecutor`` pod template docs (#19686)
- Add upgrade note on ``execution_date`` -> ``run_id`` (#19593)
- Expanding ``.output`` operator property information in TaskFlow tutorial doc (#19214)
- Add example SLA DAG (#19563)
- Add a proper example to patch DAG (#19465)
- Add DAG file processing description to Scheduler Concepts (#18954)
- Updating explicit arg example in TaskFlow API tutorial doc (#18907)
- Adds back documentation about context usage in Python/@task (#18868)
- Add release date for when an endpoint/field is added in the REST API (#19203)
- Better ``pod_template_file`` examples (#19691)
- Add description on how you can customize image entrypoint (#18915)
- Dags-in-image pod template example should not have dag mounts (#19337)

Airflow 2.2.2 (2021-11-15)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- Fix bug when checking for existence of a Variable (#19395)
- Fix Serialization when ``relativedelta`` is passed as ``schedule_interval``  (#19418)
- Fix moving of dangling TaskInstance rows for SQL Server (#19425)
- Fix task instance modal in gantt view (#19258)
- Fix serialization of ``Params`` with set data type (#19267)
- Check if job object is ``None`` before calling ``.is_alive()`` (#19380)
- Task should fail immediately when pod is unprocessable (#19359)
- Fix downgrade for a DB Migration (#19390)
- Only mark SchedulerJobs as failed, not any jobs (#19375)
- Fix message on "Mark as" confirmation page (#19363)
- Bugfix: Check next run exists before reading data interval (#19307)
- Fix MySQL db migration with default encoding/collation (#19268)
- Fix hidden tooltip position (#19261)
- ``sqlite_default`` Connection has been hard-coded to ``/tmp``, use ``gettempdir`` instead (#19255)
- Fix Toggle Wrap on DAG code page (#19211)
- Clarify "dag not found" error message in CLI (#19338)
- Add Note to SLA regarding ``schedule_interval`` (#19173)
- Use ``execution_date`` to check for existing ``DagRun`` for ``TriggerDagRunOperator`` (#18968)
- Add explicit session parameter in ``PoolSlotsAvailableDep`` (#18875)
- FAB still requires ``WTForms<3.0`` (#19466)
- Fix missing dagruns when ``catchup=True`` (#19528)

Doc only changes
^^^^^^^^^^^^^^^^

- Add missing parameter documentation for "timetable" (#19282)
- Improve Kubernetes Executor docs (#19339)
- Update image tag used in docker docs

Airflow 2.2.1 (2021-10-29)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

``Param``'s default value for ``default`` removed
"""""""""""""""""""""""""""""""""""""""""""""""""

``Param``, introduced in Airflow 2.2.0, accidentally set the default value to ``None``. This default has been removed. If you want ``None`` as your default, explicitly set it as such. For example:

.. code-block:: python

   Param(None, type=["null", "string"])

Now if you resolve a ``Param`` without a default and don't pass a value, you will get an ``TypeError``. For Example:

.. code-block:: python

   Param().resolve()  # raises TypeError

``max_queued_runs_per_dag`` configuration has been removed
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``max_queued_runs_per_dag`` configuration option in ``[core]`` section has been removed. Previously, this controlled the number of queued dagrun
the scheduler can create in a dag. Now, the maximum number is controlled internally by the DAG's ``max_active_runs``

Bug Fixes
^^^^^^^^^

- Fix Unexpected commit error in SchedulerJob (#19213)
- Add DagRun.logical_date as a property (#19198)
- Clear ``ti.next_method`` and ``ti.next_kwargs`` on task finish (#19183)
- Faster PostgreSQL db migration to Airflow 2.2 (#19166)
- Remove incorrect type comment in ``Swagger2Specification._set_defaults`` classmethod (#19065)
- Add TriggererJob to jobs check command (#19179, #19185)
- Hide tooltip when next run is ``None`` (#19112)
- Create TI context with data interval compat layer (#19148)
- Fix queued dag runs changes ``catchup=False`` behaviour (#19130, #19145)
- add detailed information to logging when a dag or a task finishes. (#19097)
- Warn about unsupported Python 3.10 (#19060)
- Fix catchup by limiting queued dagrun creation using ``max_active_runs`` (#18897)
- Prevent scheduler crash when serialized dag is missing (#19113)
- Don't install SQLAlchemy/Pendulum adapters for other DBs (#18745)
- Workaround ``libstdcpp`` TLS error (#19010)
- Change ``ds``, ``ts``, etc. back to use logical date (#19088)
- Ensure task state doesn't change when marked as failed/success/skipped (#19095)
- Relax packaging requirement (#19087)
- Rename trigger page label to Logical Date (#19061)
- Allow Param to support a default value of ``None`` (#19034)
- Upgrade old DAG/task param format when deserializing from the DB (#18986)
- Don't bake ENV and _cmd into tmp config for non-sudo (#18772)
- CLI: Fail ``backfill`` command before loading DAGs if missing args (#18994)
- BugFix: Null execution date on insert to ``task_fail`` violating NOT NULL (#18979)
- Try to move "dangling" rows in db upgrade (#18953)
- Row lock TI query in ``SchedulerJob._process_executor_events`` (#18975)
- Sentry before send fallback (#18980)
- Fix ``XCom.delete`` error in Airflow 2.2.0 (#18956)
- Check python version before starting triggerer (#18926)

Doc only changes
^^^^^^^^^^^^^^^^

- Update access control documentation for TaskInstances and DagRuns (#18644)
- Add information about keepalives for managed Postgres (#18850)
- Doc: Add Callbacks Section to Logging & Monitoring (#18842)
- Group PATCH DAGrun together with other DAGRun endpoints (#18885)

Airflow 2.2.0 (2021-10-11)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Note: Upgrading the database to ``2.2.0`` or later can take some time to complete, particularly if you have a large ``task_instance`` table.

``worker_log_server_port`` configuration has been moved to the ``logging`` section.
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``worker_log_server_port`` configuration option has been moved from ``[celery]`` section to ``[logging]`` section to allow for reuse between different executors.

``pandas`` is now an optional dependency
""""""""""""""""""""""""""""""""""""""""""""

Previously ``pandas`` was a core requirement so when you run ``pip install apache-airflow`` it looked for ``pandas``
library and installed it if it does not exist.

If you want to install ``pandas`` compatible with Airflow, you can use ``[pandas]`` extra while
installing Airflow, example for Python 3.8 and Airflow 2.1.2:

.. code-block:: shell

   pip install -U "apache-airflow[pandas]==2.1.2" \
     --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.1.2/constraints-3.8.txt"

``none_failed_or_skipped`` trigger rule has been deprecated
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``TriggerRule.NONE_FAILED_OR_SKIPPED`` is replaced by ``TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS``.
This is only name change, no functionality changes made.
This change is backward compatible however ``TriggerRule.NONE_FAILED_OR_SKIPPED`` will be removed in next major release.

Dummy trigger rule has been deprecated
""""""""""""""""""""""""""""""""""""""

``TriggerRule.DUMMY`` is replaced by ``TriggerRule.ALWAYS``.
This is only name change, no functionality changes made.
This change is backward compatible however ``TriggerRule.DUMMY`` will be removed in next major release.

DAG concurrency settings have been renamed
""""""""""""""""""""""""""""""""""""""""""

``[core] dag_concurrency`` setting in ``airflow.cfg`` has been renamed to ``[core] max_active_tasks_per_dag``
for better understanding.

It is the maximum number of task instances allowed to run concurrently in each DAG. To calculate
the number of tasks that is running concurrently for a DAG, add up the number of running
tasks for all DAG runs of the DAG.

This is configurable at the DAG level with ``max_active_tasks`` and a default can be set in ``airflow.cfg`` as
``[core] max_active_tasks_per_dag``.

**Before**\ :

.. code-block:: ini

   [core]
   dag_concurrency = 16

**Now**\ :

.. code-block:: ini

   [core]
   max_active_tasks_per_dag = 16

Similarly, ``DAG.concurrency`` has been renamed to ``DAG.max_active_tasks``.

**Before**\ :

.. code-block:: python

   dag = DAG(
       dag_id="example_dag",
       start_date=datetime(2021, 1, 1),
       catchup=False,
       concurrency=3,
   )

**Now**\ :

.. code-block:: python

   dag = DAG(
       dag_id="example_dag",
       start_date=datetime(2021, 1, 1),
       catchup=False,
       max_active_tasks=3,
   )

If you are using DAGs Details API endpoint, use ``max_active_tasks`` instead of ``concurrency``.

Task concurrency parameter has been renamed
"""""""""""""""""""""""""""""""""""""""""""

``BaseOperator.task_concurrency`` has been deprecated and renamed to ``max_active_tis_per_dag`` for
better understanding.

This parameter controls the number of concurrent running task instances across ``dag_runs``
per task.

**Before**\ :

.. code-block:: python

   with DAG(dag_id="task_concurrency_example"):
       BashOperator(task_id="t1", task_concurrency=2, bash_command="echo Hi")

**After**\ :

.. code-block:: python

   with DAG(dag_id="task_concurrency_example"):
       BashOperator(task_id="t1", max_active_tis_per_dag=2, bash_command="echo Hi")

``processor_poll_interval`` config have been renamed to ``scheduler_idle_sleep_time``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``[scheduler] processor_poll_interval`` setting in ``airflow.cfg`` has been renamed to ``[scheduler] scheduler_idle_sleep_time``
for better understanding.

It controls the 'time to sleep' at the end of the Scheduler loop if nothing was scheduled inside ``SchedulerJob``.

**Before**\ :

.. code-block:: ini

   [scheduler]
   processor_poll_interval = 16

**Now**\ :

.. code-block:: ini

   [scheduler]
   scheduler_idle_sleep_time = 16

Marking success/failed automatically clears failed downstream tasks
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

When marking a task success/failed in Graph View, its downstream tasks that are in failed/upstream_failed state are automatically cleared.

``[core] store_dag_code`` has been removed
""""""""""""""""""""""""""""""""""""""""""""""

While DAG Serialization is a strict requirements since Airflow 2, we allowed users to control
where the Webserver looked for when showing the **Code View**.

If ``[core] store_dag_code`` was set to ``True``\ , the Scheduler stored the code in the DAG file in the
DB (in ``dag_code`` table) as a plain string, and the webserver just read it from the same table.
If the value was set to ``False``\ , the webserver read it from the DAG file.

While this setting made sense for Airflow < 2, it caused some confusion to some users where they thought
this setting controlled DAG Serialization.

From Airflow 2.2, Airflow will only look for DB when a user clicks on **Code View** for a DAG.

Clearing a running task sets its state to ``RESTARTING``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously, clearing a running task sets its state to ``SHUTDOWN``. The task gets killed and goes into ``FAILED`` state. After `#16681 <https://github.com/apache/airflow/pull/16681>`_\ , clearing a running task sets its state to ``RESTARTING``. The task is eligible for retry without going into ``FAILED`` state.

Remove ``TaskInstance.log_filepath`` attribute
""""""""""""""""""""""""""""""""""""""""""""""""""

This method returned incorrect values for a long time, because it did not take into account the different
logger configuration and task retries. We have also started supporting more advanced tools that don't use
files, so it is impossible to determine the correct file path in every case e.g. Stackdriver doesn't use files
but identifies logs based on labels.  For this reason, we decided to delete this attribute.

If you need to read logs, you can use ``airflow.utils.log.log_reader.TaskLogReader`` class, which does not have
the above restrictions.

If a sensor times out, it will not retry
""""""""""""""""""""""""""""""""""""""""

Previously, a sensor is retried when it times out until the number of ``retries`` are exhausted. So the effective timeout of a sensor is ``timeout * (retries + 1)``. This behaviour is now changed. A sensor will immediately fail without retrying if ``timeout`` is reached. If it's desirable to let the sensor continue running for longer time, set a larger ``timeout`` instead.

Default Task Pools Slots can be set using ``[core] default_pool_task_slot_count``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

By default tasks are running in ``default_pool``. ``default_pool`` is initialized with ``128`` slots and user can change the
number of slots through UI/CLI/API for an existing deployment.

For new deployments, you can use ``default_pool_task_slot_count`` setting in ``[core]`` section. This setting would
not have any effect in an existing deployment where the ``default_pool`` already exists.

Previously this was controlled by ``non_pooled_task_slot_count`` in ``[core]`` section, which was not documented.

Webserver DAG refresh buttons removed
"""""""""""""""""""""""""""""""""""""

Now that the DAG parser syncs DAG permissions there is no longer a need for manually refreshing DAGs. As such, the buttons to refresh a DAG have been removed from the UI.

In addition, the ``/refresh`` and ``/refresh_all`` webserver endpoints have also been removed.

TaskInstances now *require* a DagRun
""""""""""""""""""""""""""""""""""""""""

Under normal operation every TaskInstance row in the database would have DagRun row too, but it was possible to manually delete the DagRun and Airflow would still schedule the TaskInstances.

In Airflow 2.2 we have changed this and now there is a database-level foreign key constraint ensuring that every TaskInstance has a DagRun row.

Before updating to this 2.2 release you will have to manually resolve any inconsistencies (add back DagRun rows, or delete TaskInstances) if you have any "dangling" TaskInstance" rows.

As part of this change the ``clean_tis_without_dagrun_interval`` config option under ``[scheduler]`` section has been removed and has no effect.

TaskInstance and TaskReschedule now define ``run_id`` instead of ``execution_date``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

As a part of the TaskInstance-DagRun relation change, the ``execution_date`` columns on TaskInstance and TaskReschedule have been removed from the database, and replaced by `association proxy <https://docs.sqlalchemy.org/en/13/orm/extensions/associationproxy.html>`_ fields at the ORM level. If you access Airflow's metadatabase directly, you should rewrite the implementation to use the ``run_id`` columns instead.

Note that Airflow's metadatabase definition on both the database and ORM levels are considered implementation detail without strict backward compatibility guarantees.

DaskExecutor - Dask Worker Resources and queues
"""""""""""""""""""""""""""""""""""""""""""""""

If dask workers are not started with complementary resources to match the specified queues, it will now result in an ``AirflowException``\ , whereas before it would have just ignored the ``queue`` argument.

Logical date of a DAG run triggered from the web UI now have its sub-second component set to zero
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Due to a change in how the logical date (``execution_date``) is generated for a manual DAG run, a manual DAG run's logical date may not match its time-of-trigger, but have its sub-second part zero-ed out. For example, a DAG run triggered on ``2021-10-11T12:34:56.78901`` would have its logical date set to ``2021-10-11T12:34:56.00000``.

This may affect some logic that expects on this quirk to detect whether a run is triggered manually or not. Note that ``dag_run.run_type`` is a more authoritative value for this purpose. Also, if you need this distinction between automated and manually-triggered run for "next execution date" calculation, please also consider using the new data interval variables instead, which provide a more consistent behavior between the two run types.

New Features
^^^^^^^^^^^^

- AIP-39: Add (customizable) Timetable class to Airflow for richer scheduling behaviour (#15397, #16030,
  #16352, #17030, #17122, #17414, #17552, #17755, #17989, #18084, #18088, #18244, #18266, #18420, #18434,
  #18421, #18475, #18499, #18573, #18522, #18729, #18706, #18742, #18786, #18804)
- AIP-40: Add Deferrable "Async" Tasks (#15389, #17564, #17565, #17601, #17745, #17747, #17748, #17875,
  #17876, #18129, #18210, #18214, #18552, #18728, #18414)
- Add a Docker Taskflow decorator (#15330, #18739)
- Add Airflow Standalone command (#15826)
- Display alert messages on dashboard from local settings (#18284)
- Advanced Params using json-schema (#17100)
- Ability to test connections from UI or API (#15795, #18750)
- Add Next Run to UI (#17732)
- Add default weight rule configuration option (#18627)
- Add a calendar field to choose the execution date of the DAG when triggering it (#16141)
- Allow setting specific ``cwd`` for BashOperator (#17751)
- Show import errors in DAG views (#17818)
- Add pre/post execution hooks [Experimental] (#17576)
- Added table to view providers in Airflow ui under admin tab (#15385)
- Adds secrets backend/logging/auth information to provider yaml (#17625)
- Add date format filters to Jinja environment (#17451)
- Introduce ``RESTARTING`` state (#16681)
- Webserver: Unpause DAG on manual trigger (#16569)
- API endpoint to create new user (#16609)
- Add ``insert_args`` for support transfer replace (#15825)
- Add recursive flag to glob in filesystem sensor (#16894)
- Add conn to jinja template context (#16686)
- Add ``default_args`` for ``TaskGroup`` (#16557)
- Allow adding duplicate connections from UI (#15574)
- Allow specifying multiple URLs via the CORS config option (#17941)
- Implement API endpoint for DAG deletion (#17980)
- Add DAG run endpoint for marking a dagrun success or failed(#17839)
- Add support for ``kinit`` options ``[-f|-F]`` and ``[-a|-A]`` (#17816)
- Queue support for ``DaskExecutor`` using Dask Worker Resources (#16829, #18720)
- Make auto refresh interval configurable (#18107)

Improvements
^^^^^^^^^^^^

- Small improvements for Airflow UI (#18715, #18795)
- Rename ``processor_poll_interval`` to ``scheduler_idle_sleep_time`` (#18704)
- Check the allowed values for the logging level (#18651)
- Fix error on triggering a dag that doesn't exist using ``dagrun_conf`` (#18655)
- Add muldelete action to ``TaskInstanceModelView`` (#18438)
- Avoid importing DAGs during clean DB installation (#18450)
- Require can_edit on DAG privileges to modify TaskInstances and DagRuns (#16634)
- Make Kubernetes job description fit on one log line (#18377)
- Always draw borders if task instance state is null or undefined (#18033)
- Inclusive Language (#18349)
- Improved log handling for zombie tasks (#18277)
- Adding ``Variable.update`` method and improving detection of variable key collisions (#18159)
- Add note about params on trigger DAG page (#18166)
- Change ``TaskInstance`` and ``TaskReschedule`` PK from ``execution_date`` to ``run_id`` (#17719)
- Adding ``TaskGroup`` support in ``BaseOperator.chain()`` (#17456)
- Allow filtering DAGS by tags in the REST API (#18090)
- Optimize imports of Providers Manager (#18052)
- Adds capability of Warnings for incompatible community providers (#18020)
- Serialize the ``template_ext`` attribute to show it in UI (#17985)
- Add ``robots.txt`` and ``X-Robots-Tag`` header (#17946)
- Refactor ``BranchDayOfWeekOperator``, ``DayOfWeekSensor`` (#17940)
- Update error message to guide the user into self-help mostly (#17929)
- Update to Celery 5 (#17397)
- Add links to provider's documentation (#17736)
- Remove Marshmallow schema warnings (#17753)
- Rename ``none_failed_or_skipped`` by ``none_failed_min_one_success`` trigger rule (#17683)
- Remove ``[core] store_dag_code`` & use DB to get Dag Code (#16342)
- Rename ``task_concurrency`` to ``max_active_tis_per_dag`` (#17708)
- Import Hooks lazily individually in providers manager (#17682)
- Adding support for multiple task-ids in the external task sensor (#17339)
- Replace ``execution_date`` with ``run_id`` in airflow tasks run command (#16666)
- Make output from users cli command more consistent (#17642)
- Open relative extra links in place (#17477)
- Move ``worker_log_server_port`` option to the logging section (#17621)
- Use gunicorn to serve logs generated by worker (#17591)
- Improve validation of Group id (#17578)
- Simplify 404 page (#17501)
- Add XCom.clear so it's hookable in custom XCom backend (#17405)
- Add deprecation notice for ``SubDagOperator`` (#17488)
- Support DAGS folder being in different location on scheduler and runners (#16860)
- Remove /dagrun/create and disable edit form generated by F.A.B (#17376)
- Enable specifying dictionary paths in ``template_fields_renderers`` (#17321)
- error early if virtualenv is missing (#15788)
- Handle connection parameters added to Extra and custom fields (#17269)
- Fix ``airflow celery stop`` to accept the pid file. (#17278)
- Remove DAG refresh buttons (#17263)
- Deprecate dummy trigger rule in favor of always (#17144)
- Be verbose about failure to import ``airflow_local_settings`` (#17195)
- Include exit code in ``AirflowException`` str when ``BashOperator`` fails. (#17151)
- Adding EdgeModifier support for chain() (#17099)
- Only allows supported field types to be used in custom connections (#17194)
- Secrets backend failover (#16404)
- Warn on Webserver when using ``SQLite`` or ``SequentialExecutor`` (#17133)
- Extend ``init_containers`` defined in ``pod_override`` (#17537)
- Client-side filter dag dependencies (#16253)
- Improve executor validation in CLI (#17071)
- Prevent running ``airflow db init/upgrade`` migrations and setup in parallel. (#17078)
- Update ``chain()`` and ``cross_downstream()`` to support ``XComArgs`` (#16732)
- Improve graph view refresh (#16696)
- When a task instance fails with exception, log it (#16805)
- Set process title for ``serve-logs`` and ``LocalExecutor`` (#16644)
- Rename ``test_cycle`` to ``check_cycle`` (#16617)
- Add schema as ``DbApiHook`` instance attribute (#16521, #17423)
- Improve compatibility with MSSQL (#9973)
- Add transparency for unsupported connection type (#16220)
- Call resource based fab methods (#16190)
- Format more dates with timezone (#16129)
- Replace deprecated ``dag.sub_dag`` with ``dag.partial_subset`` (#16179)
- Treat ``AirflowSensorTimeout`` as immediate failure without retrying (#12058)
- Marking success/failed automatically clears failed downstream tasks  (#13037)
- Add close/open indicator for import dag errors (#16073)
- Add collapsible import errors (#16072)
- Always return a response in TI's ``action_clear`` view (#15980)
- Add cli command to delete user by email (#15873)
- Use resource and action names for FAB permissions (#16410)
- Rename DAG concurrency (``[core] dag_concurrency``) settings for easier understanding (#16267, #18730)
- Calendar UI improvements (#16226)
- Refactor: ``SKIPPED`` should not be logged again as ``SUCCESS`` (#14822)
- Remove  version limits for ``dnspython`` (#18046, #18162)
- Accept custom run ID in TriggerDagRunOperator (#18788)

Bug Fixes
^^^^^^^^^

- Make REST API patch user endpoint work the same way as the UI (#18757)
- Properly set ``start_date`` for cleared tasks (#18708)
- Ensure task_instance exists before running update on its state(REST API) (#18642)
- Make ``AirflowDateTimePickerWidget`` a required field (#18602)
- Retry deadlocked transactions on deleting old rendered task fields (#18616)
- Fix ``retry_exponential_backoff`` divide by zero error when retry delay is zero (#17003)
- Improve how UI handles datetimes (#18611, #18700)
- Bugfix: dag_bag.get_dag should return None, not raise exception (#18554)
- Only show the task modal if it is a valid instance (#18570)
- Fix accessing rendered ``{{ task.x }}`` attributes from within templates (#18516)
- Add missing email type of connection (#18502)
- Don't use flash for "same-page" UI messages. (#18462)
- Fix task group tooltip (#18406)
- Properly fix dagrun update state endpoint (#18370)
- Properly handle ti state difference between executor and scheduler (#17819)
- Fix stuck "queued" tasks in KubernetesExecutor (#18152)
- Don't permanently add zip DAGs to ``sys.path`` (#18384)
- Fix random deadlocks in MSSQL database (#18362)
- Deactivating DAGs which have been removed from files (#17121)
- When syncing dags to db remove ``dag_tag`` rows that are now unused (#8231)
- Graceful scheduler shutdown on error (#18092)
- Fix mini scheduler not respecting ``wait_for_downstream`` dep (#18338)
- Pass exception to ``run_finished_callback`` for Debug Executor (#17983)
- Make ``XCom.get_one`` return full, not abbreviated values (#18274)
- Use try/except when closing temporary file in task_runner (#18269)
- show next run if not none (#18273)
- Fix DB session handling in ``XCom.set`` (#18240)
- Fix external_executor_id not being set for manually run jobs (#17207)
- Fix deleting of zipped Dags in Serialized Dag Table (#18243)
- Return explicit error on user-add for duplicated email (#18224)
- Remove loading dots even when last run data is empty (#18230)
- Swap dag import error dropdown icons (#18207)
- Automatically create section when migrating config (#16814)
- Set encoding to utf-8 by default while reading task logs (#17965)
- Apply parent dag permissions to subdags (#18160)
- Change id collation for MySQL to case-sensitive (#18072)
- Logs task launch exception in ``StandardTaskRunner`` (#17967)
- Applied permissions to ``self._error_file`` (#15947)
- Fix blank dag dependencies view (#17990)
- Add missing menu access for dag dependencies and configurations pages (#17450)
- Fix passing Jinja templates in ``DateTimeSensor`` (#17959)
- Fixing bug which restricted the visibility of ImportErrors (#17924)
- Fix grammar in ``traceback.html`` (#17942)
- Fix ``DagRunState`` enum query for ``MySQLdb`` driver (#17886)
- Fixed button size in "Actions" group. (#17902)
- Only show import errors for DAGs a user can access (#17835)
- Show all import_errors from zip files (#17759)
- fix EXTRA_LOGGER_NAMES param and related docs (#17808)
- Use one interpreter for Airflow and gunicorn (#17805)
- Fix: Mysql 5.7 id utf8mb3 (#14535)
- Fix dag_processing.last_duration metric random holes (#17769)
- Automatically use ``utf8mb3_general_ci`` collation for MySQL (#17729)
- fix: filter condition of ``TaskInstance`` does not work #17535 (#17548)
- Dont use TaskInstance in CeleryExecutor.trigger_tasks (#16248)
- Remove locks for upgrades in MSSQL (#17213)
- Create virtualenv via python call (#17156)
- Ensure a DAG is acyclic when running ``DAG.cli()`` (#17105)
- Translate non-ascii characters (#17057)
- Change the logic of ``None`` comparison in ``model_list`` template (#16893)
- Have UI and POST /task_instances_state API endpoint have same behaviour (#16539)
- ensure task is skipped if missing sla (#16719)
- Fix direct use of ``cached_property`` module (#16710)
- Fix TI success confirm page (#16650)
- Modify return value check in python virtualenv jinja template (#16049)
- Fix dag dependency search (#15924)
- Make custom JSON encoder support ``Decimal`` (#16383)
- Bugfix: Allow clearing tasks with just ``dag_id`` and empty ``subdir`` (#16513)
- Convert port value to a number before calling test connection (#16497)
- Handle missing/null serialized DAG dependencies (#16393)
- Correctly set ``dag.fileloc`` when using the ``@dag`` decorator (#16384)
- Fix TI success/failure links (#16233)
- Correctly implement autocomplete early return in ``airflow/www/views.py`` (#15940)
- Backport fix to allow pickling of Loggers to Python 3.6 (#18798)
- Fix bug that Backfill job fail to run when there are tasks run into ``reschedule`` state (#17305, #18806)

Doc only changes
^^^^^^^^^^^^^^^^

- Update ``dagbag_size`` documentation (#18824)
- Update documentation about bundle extras (#18828)
- Fix wrong Postgres ``search_path`` set up instructions (#17600)
- Remove ``AIRFLOW_GID`` from Docker images (#18747)
- Improve error message for BranchPythonOperator when no task_id to follow (#18471)
- Improve guidance to users telling them what to do on import timeout (#18478)
- Explain scheduler fine-tuning better (#18356)
- Added example JSON for airflow pools import (#18376)
- Add ``sla_miss_callback`` section to the documentation (#18305)
- Explain sentry default environment variable for subprocess hook (#18346)
- Refactor installation pages (#18282)
- Improves quick-start docker-compose warnings and documentation (#18164)
- Production-level support for MSSQL (#18382)
- Update non-working example in documentation (#18067)
- Remove default_args pattern + added get_current_context() use for Core Airflow example DAGs (#16866)
- Update max_tis_per_query to better render on the webpage (#17971)
- Adds Github Oauth example with team based authorization (#17896)
- Update docker.rst (#17882)
- Example xcom update (#17749)
- Add doc warning about connections added via env vars (#17915)
- fix wrong documents around upgrade-check.rst (#17903)
- Add Brent to Committers list (#17873)
- Improves documentation about modules management (#17757)
- Remove deprecated metrics from metrics.rst (#17772)
- Make sure "production-readiness" of docker-compose is well explained (#17731)
- Doc: Update Upgrade to v2 docs with Airflow 1.10.x EOL dates (#17710)
- Doc: Replace deprecated param from docstrings (#17709)
- Describe dag owner more carefully (#17699)
- Update note so avoid misinterpretation (#17701)
- Docs: Make ``DAG.is_active`` read-only in API (#17667)
- Update documentation regarding Python 3.9 support (#17611)
- Fix MySQL database character set instruction (#17603)
- Document overriding ``XCom.clear`` for data lifecycle management (#17589)
- Path correction in docs for airflow core (#17567)
- docs(celery): reworded, add actual multiple queues example (#17541)
- Doc: Add FAQ to speed up parsing with tons of dag files (#17519)
- Improve image building documentation for new users (#17409)
- Doc: Strip unnecessary arguments from MariaDB JIRA URL (#17296)
- Update warning about MariaDB and multiple schedulers (#17287)
- Doc: Recommend using same configs on all Airflow components (#17146)
- Move docs about masking to a new page (#17007)
- Suggest use of Env vars instead of Airflow Vars in best practices doc (#16926)
- Docs: Better description for ``pod_template_file`` (#16861)
- Add Aneesh Joseph as Airflow Committer (#16835)
- Docs: Added new pipeline example for the tutorial docs (#16548)
- Remove upstart from docs (#16672)
- Add new committers: ``Jed`` and ``TP`` (#16671)
- Docs: Fix ``flask-ouathlib`` to ``flask-oauthlib`` in Upgrading docs (#16320)
- Docs: Fix creating a connection docs (#16312)
- Docs: Fix url for ``Elasticsearch`` (#16275)
- Small improvements for README.md files (#16244)
- Fix docs for ``dag_concurrency`` (#16177)
- Check syntactic correctness for code-snippets (#16005)
- Add proper link for wheel packages in docs. (#15999)
- Add Docs for ``default_pool`` slots (#15997)
- Add memory usage warning in quick-start documentation (#15967)
- Update example ``KubernetesExecutor`` ``git-sync`` pod template file (#15904)
- Docs: Fix Taskflow API docs (#16574)
- Added new pipeline example for the tutorial docs (#16084)
- Updating the DAG docstring to include ``render_template_as_native_obj`` (#16534)
- Update docs on setting up SMTP (#16523)
- Docs: Fix API verb from ``POST`` to ``PATCH`` (#16511)

Misc/Internal
^^^^^^^^^^^^^

- Renaming variables to be consistent with code logic (#18685)
- Simplify strings previously split across lines (#18679)
- fix exception string of ``BranchPythonOperator`` (#18623)
- Add multiple roles when creating users (#18617)
- Move FABs base Security Manager into Airflow. (#16647)
- Remove unnecessary css state colors (#18461)
- Update ``boto3`` to ``<1.19`` (#18389)
- Improve coverage for ``airflow.security.kerberos module`` (#18258)
- Fix Amazon Kinesis test (#18337)
- Fix provider test accessing ``importlib-resources`` (#18228)
- Silence warnings in tests from using SubDagOperator (#18275)
- Fix usage of ``range(len())`` to ``enumerate`` (#18174)
- Test coverage on the autocomplete view (#15943)
- Add "packaging" to core requirements (#18122)
- Adds LoggingMixins to BaseTrigger (#18106)
- Fix building docs in ``main`` builds (#18035)
- Remove upper-limit on ``tenacity`` (#17593)
- Remove  redundant ``numpy`` dependency (#17594)
- Bump ``mysql-connector-python`` to latest version (#17596)
- Make ``pandas`` an optional core dependency (#17575)
- Add more typing to airflow.utils.helpers (#15582)
- Chore: Some code cleanup in ``airflow/utils/db.py`` (#17090)
- Refactor: Remove processor_factory from DAG processing (#16659)
- Remove AbstractDagFileProcessorProcess from dag processing (#16816)
- Update TaskGroup typing (#16811)
- Update ``click`` to 8.x (#16779)
- Remove remaining Pylint disables (#16760)
- Remove duplicated try, there is already a try in create_session (#16701)
- Removes pylint from our toolchain (#16682)
- Refactor usage of unneeded function call (#16653)
- Add type annotations to setup.py (#16658)
- Remove SQLAlchemy <1.4 constraint (#16630) (Note: our dependencies still have a requirement on <1.4)
- Refactor ``dag.clear`` method (#16086)
- Use ``DAG_ACTIONS`` constant (#16232)
- Use updated ``_get_all_non_dag_permissions`` method (#16317)
- Add updated-name wrappers for built-in FAB methods (#16077)
- Remove ``TaskInstance.log_filepath`` attribute (#15217)
- Removes unnecessary function call in ``airflow/www/app.py`` (#15956)
- Move ``plyvel`` to google provider extra (#15812)
- Update permission migrations to use new naming scheme (#16400)
- Use resource and action names for FAB (#16380)
- Swap out calls to ``find_permission_view_menu`` for ``get_permission`` wrapper (#16377)
- Fix deprecated default for ``fab_logging_level`` to ``WARNING`` (#18783)
- Allow running tasks from UI when using ``CeleryKubernetesExecutor`` (#18441)

Airflow 2.1.4 (2021-09-18)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- Fix deprecation error message rather than silencing it (#18126)
- Limit the number of queued dagruns created by the Scheduler (#18065)
- Fix ``DagRun`` execution order from queued to running not being properly followed (#18061)
- Fix ``max_active_runs`` not allowing moving of queued dagruns to running (#17945)
- Avoid redirect loop for users with no permissions (#17838)
- Avoid endless redirect loop when user has no roles (#17613)
- Fix log links on graph TI modal (#17862)
- Hide variable import form if user lacks permission (#18000)
- Improve dag/task concurrency check (#17786)
- Fix Clear task instances endpoint resets all DAG runs bug (#17961)
- Fixes incorrect parameter passed to views (#18083) (#18085)
- Fix Sentry handler from ``LocalTaskJob`` causing error (#18119)
- Limit ``colorlog`` version (6.x is incompatible) (#18099)
- Only show Pause/Unpause tooltip on hover (#17957)
- Improve graph view load time for dags with open groups (#17821)
- Increase width for Run column (#17817)
- Fix wrong query on running tis (#17631)
- Add root to tree refresh url (#17633)
- Do not delete running DAG from the UI (#17630)
- Improve discoverability of Provider packages' functionality
- Do not let ``create_dagrun`` overwrite explicit ``run_id`` (#17728)
- Regression on pid reset to allow task start after heartbeat (#17333)
- Set task state to failed when pod is DELETED while running (#18095)
- Advises the kernel to not cache log files generated by Airflow (#18054)
- Sort adopted tasks in ``_check_for_stalled_adopted_tasks`` method (#18208)

Doc only changes
^^^^^^^^^^^^^^^^

- Update version added fields in airflow/config_templates/config.yml (#18128)
- Improve the description of how to handle dynamic task generation (#17963)
- Improve cross-links to operators and hooks references (#17622)
- Doc: Fix replacing Airflow version for Docker stack (#17711)
- Make the providers operators/hooks reference much more usable (#17768)
- Update description about the new ``connection-types`` provider meta-data
- Suggest to use secrets backend for variable when it contains sensitive data (#17319)
- Separate Installing from sources section and add more details (#18171)
- Doc: Use ``closer.lua`` script for downloading sources (#18179)
- Doc: Improve installing from sources (#18194)
- Improves installing from sources pages for all components (#18251)

Airflow 2.1.3 (2021-08-23)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- Fix task retries when they receive ``sigkill`` and have retries and properly handle ``sigterm`` (#16301)
- Fix redacting secrets in context exceptions. (#17618)
- Fix race condition with dagrun callbacks (#16741)
- Add 'queued' to DagRunState (#16854)
- Add 'queued' state to DagRun (#16401)
- Fix external elasticsearch logs link (#16357)
- Add proper warning message when recorded PID is different from current PID (#17411)
- Fix running tasks with ``default_impersonation`` config (#17229)
- Rescue if a DagRun's DAG was removed from db (#17544)
- Fixed broken json_client (#17529)
- Handle and log exceptions raised during task callback (#17347)
- Fix CLI ``kubernetes cleanup-pods`` which fails on invalid label key (#17298)
- Show serialization exceptions in DAG parsing log (#17277)
- Fix: ``TaskInstance`` does not show ``queued_by_job_id`` & ``external_executor_id`` (#17179)
- Adds more explanatory message when ``SecretsMasker`` is not configured (#17101)
- Enable the use of ``__init_subclass__`` in subclasses of ``BaseOperator`` (#17027)
- Fix task instance retrieval in XCom view (#16923)
- Validate type of ``priority_weight`` during parsing (#16765)
- Correctly handle custom ``deps`` and ``task_group`` during DAG Serialization (#16734)
- Fix slow (cleared) tasks being be adopted by Celery worker. (#16718)
- Fix calculating duration in tree view (#16695)
- Fix ``AttributeError``: ``datetime.timezone`` object has no attribute ``name`` (#16599)
- Redact conn secrets in webserver logs (#16579)
- Change graph focus to top of view instead of center (#16484)
- Fail tasks in scheduler when executor reports they failed (#15929)
- fix(smart_sensor): Unbound variable errors (#14774)
- Add back missing permissions to ``UserModelView`` controls. (#17431)
- Better diagnostics and self-healing of docker-compose (#17484)
- Improve diagnostics message when users have ``secret_key`` misconfigured (#17410)
- Stop checking ``execution_date`` in ``task_instance.refresh_from_db`` (#16809)

Improvements
^^^^^^^^^^^^

- Run mini scheduler in ``LocalTaskJob`` during task exit (#16289)
- Remove ``SQLAlchemy<1.4`` constraint (#16630)
- Bump Jinja2 upper-bound from 2.12.0 to 4.0.0 (#16595)
- Bump ``dnspython`` (#16698)
- Updates to ``FlaskAppBuilder`` 3.3.2+ (#17208)
- Add State types for tasks and DAGs (#15285)
- Set Process title for Worker when using ``LocalExecutor`` (#16623)
- Move ``DagFileProcessor`` and ``DagFileProcessorProcess`` out of ``scheduler_job.py`` (#16581)

Doc only changes
^^^^^^^^^^^^^^^^

- Fix inconsistencies in configuration docs (#17317)
- Fix docs link for using SQLite as Metadata DB (#17308)

Misc
^^^^

- Switch back http provider after requests removes LGPL dependency (#16974)

Airflow 2.1.2 (2021-07-14)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- Only allow webserver to request from the worker log server (#16754)
- Fix "Invalid JSON configuration, must be a dict" bug (#16648)
- Fix ``CeleryKubernetesExecutor`` (#16700)
- Mask value if the key is ``token`` (#16474)
- Fix impersonation issue with ``LocalTaskJob`` (#16852)
- Resolve all npm vulnerabilities including bumping ``jQuery`` to ``3.5`` (#16440)

Misc
^^^^

- Add Python 3.9 support (#15515)


Airflow 2.1.1 (2021-07-02)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

``activate_dag_runs`` argument of the function ``clear_task_instances`` is replaced with ``dag_run_state``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

To achieve the previous default behaviour of ``clear_task_instances`` with ``activate_dag_runs=True``\ , no change is needed. To achieve the previous behaviour of ``activate_dag_runs=False``\ , pass ``dag_run_state=False`` instead. (The previous parameter is still accepted, but is deprecated)

``dag.set_dag_runs_state`` is deprecated
""""""""""""""""""""""""""""""""""""""""""""

The method ``set_dag_runs_state`` is no longer needed after a bug fix in PR: `#15382 <https://github.com/apache/airflow/pull/15382>`_. This method is now deprecated and will be removed in a future version.

Bug Fixes
^^^^^^^^^

- Don't crash attempting to mask secrets in dict with non-string keys (#16601)
- Always install sphinx_airflow_theme from ``PyPI`` (#16594)
- Remove limitation for elasticsearch library (#16553)
- Adding extra requirements for build and runtime of the PROD image. (#16170)
- Cattrs 1.7.0 released by the end of May 2021 break lineage usage (#16173)
- Removes unnecessary packages from setup_requires (#16139)
- Pins docutils to <0.17 until breaking behaviour is fixed (#16133)
- Improvements for Docker Image docs (#14843)
- Ensure that ``dag_run.conf`` is a dict (#15057)
- Fix CLI connections import and migrate logic from secrets to Connection model (#15425)
- Fix Dag Details start date bug (#16206)
- Fix DAG run state not updated while DAG is paused (#16343)
- Allow null value for operator field in task_instance schema(REST API) (#16516)
- Avoid recursion going too deep when redacting logs (#16491)
- Backfill: Don't create a DagRun if no tasks match task regex (#16461)
- Tree View UI for larger DAGs & more consistent spacing in Tree View (#16522)
- Correctly handle None returns from Query.scalar() (#16345)
- Adding ``only_active`` parameter to /dags endpoint (#14306)
- Don't show stale Serialized DAGs if they are deleted in DB (#16368)
- Make REST API List DAGs endpoint consistent with UI/CLI behaviour (#16318)
- Support remote logging in elasticsearch with ``filebeat 7`` (#14625)
- Queue tasks with higher priority and earlier execution_date first. (#15210)
- Make task ID on legend have enough width and width of line chart to be 100%.  (#15915)
- Fix normalize-url vulnerability (#16375)
- Validate retries value on init for better errors (#16415)
- add num_runs query param for tree refresh (#16437)
- Fix templated default/example values in config ref docs (#16442)
- Add ``passphrase`` and ``private_key`` to default sensitive field names (#16392)
- Fix tasks in an infinite slots pool were never scheduled (#15247)
- Fix Orphaned tasks stuck in CeleryExecutor as running (#16550)
- Don't fail to log if we can't redact something (#16118)
- Set max tree width to 1200 pixels (#16067)
- Fill the "job_id" field for ``airflow task run`` without ``--local``/``--raw`` for KubeExecutor (#16108)
- Fixes problem where conf variable was used before initialization (#16088)
- Fix apply defaults for task decorator (#16085)
- Parse recently modified files even if just parsed (#16075)
- Ensure that we don't try to mask empty string in logs (#16057)
- Don't die when masking ``log.exception`` when there is no exception (#16047)
- Restores apply_defaults import in base_sensor_operator (#16040)
- Fix auto-refresh in tree view When webserver ui is not in ``/`` (#16018)
- Fix dag.clear() to set multiple dags to running when necessary (#15382)
- Fix Celery executor getting stuck randomly because of reset_signals in multiprocessing (#15989)


Airflow 2.1.0 (2021-05-21)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

New "deprecated_api" extra
""""""""""""""""""""""""""

We have a new '[deprecated_api]' extra that should be used when installing airflow when the deprecated API
is going to be used. This is now an optional feature of Airflow now because it pulls in ``requests`` which
(as of 14 May 2021) pulls LGPL ``chardet`` dependency.

The ``http`` provider is not installed by default
"""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``http`` provider is now optional and not installed by default, until ``chardet`` becomes an optional
dependency of ``requests``.
See `PR to replace chardet with charset-normalizer <https://github.com/psf/requests/pull/5797>`_

``@apply_default`` decorator isn't longer necessary
"""""""""""""""""""""""""""""""""""""""""""""""""""""""

This decorator is now automatically added to all operators via the metaclass on BaseOperator

Change the configuration options for field masking
""""""""""""""""""""""""""""""""""""""""""""""""""

We've improved masking for sensitive data in Web UI and logs. As part of it, the following configurations have been changed:


* ``hide_sensitive_variable_fields`` option in ``admin`` section has been replaced by ``hide_sensitive_var_conn_fields`` section in ``core`` section,
* ``sensitive_variable_fields`` option in ``admin`` section has been replaced by ``sensitive_var_conn_names`` section in ``core`` section.

Deprecated PodDefaults and add_xcom_sidecar in airflow.kubernetes.pod_generator
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

We have moved PodDefaults from ``airflow.kubernetes.pod_generator.PodDefaults`` to
``airflow.providers.cncf.kubernetes.utils.xcom_sidecar.PodDefaults`` and moved add_xcom_sidecar
from ``airflow.kubernetes.pod_generator.PodGenerator.add_xcom_sidecar``\ to
``airflow.providers.cncf.kubernetes.utils.xcom_sidecar.add_xcom_sidecar``.
This change will allow us to modify the KubernetesPodOperator XCom functionality without requiring airflow upgrades.

Removed pod_launcher from core airflow
""""""""""""""""""""""""""""""""""""""

Moved the pod launcher from ``airflow.kubernetes.pod_launcher`` to ``airflow.providers.cncf.kubernetes.utils.pod_launcher``

This will allow users to update the pod_launcher for the KubernetesPodOperator without requiring an airflow upgrade

Default ``[webserver] worker_refresh_interval`` is changed to ``6000`` seconds
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default value for ``[webserver] worker_refresh_interval`` was ``30`` seconds for
Airflow <=2.0.1. However, since Airflow 2.0 DAG Serialization is a hard requirement
and the Webserver used the serialized DAGs, there is no need to kill an existing
worker and create a new one as frequently as ``30`` seconds.

This setting can be raised to an even higher value, currently it is
set to ``6000`` seconds (100 minutes) to
serve as a DagBag cache burst time.

``default_queue`` configuration has been moved to the ``operators`` section.
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``default_queue`` configuration option has been moved from ``[celery]`` section to ``[operators]`` section to allow for reuse between different executors.

New Features
^^^^^^^^^^^^

- Add ``PythonVirtualenvDecorator`` to Taskflow API (#14761)
- Add ``Taskgroup`` decorator (#15034)
- Create a DAG Calendar View (#15423)
- Create cross-DAG dependencies view (#13199)
- Add rest API to query for providers (#13394)
- Mask passwords and sensitive info in task logs and UI (#15599)
- Add ``SubprocessHook`` for running commands from operators (#13423)
- Add DAG Timeout in UI page "DAG Details" (#14165)
- Add ``WeekDayBranchOperator`` (#13997)
- Add JSON linter to DAG Trigger UI (#13551)
- Add DAG Description Doc to Trigger UI Page (#13365)
- Add airflow webserver URL into SLA miss email. (#13249)
- Add read only REST API endpoints for users (#14735)
- Add files to generate Airflow's Python SDK (#14739)
- Add dynamic fields to snowflake connection (#14724)
- Add read only REST API endpoint for roles and permissions (#14664)
- Add new datetime branch operator (#11964)
- Add Google leveldb hook and operator (#13109) (#14105)
- Add plugins endpoint to the REST API (#14280)
- Add ``worker_pod_pending_timeout`` support (#15263)
- Add support for labeling DAG edges (#15142)
- Add CUD REST API endpoints for Roles (#14840)
- Import connections from a file (#15177)
- A bunch of ``template_fields_renderers`` additions (#15130)
- Add REST API query sort and order to some endpoints (#14895)
- Add timezone context in new ui (#15096)
- Add query mutations to new UI (#15068)
- Add different modes to sort dag files for parsing (#15046)
- Auto refresh on Tree View (#15474)
- BashOperator to raise ``AirflowSkipException`` on exit code 99 (by default, configurable) (#13421) (#14963)
- Clear tasks by task ids in REST API (#14500)
- Support jinja2 native Python types (#14603)
- Allow celery workers without gossip or mingle modes (#13880)
- Add ``airflow jobs check`` CLI command to check health of jobs (Scheduler etc) (#14519)
- Rename ``DateTimeBranchOperator`` to ``BranchDateTimeOperator`` (#14720)

Improvements
^^^^^^^^^^^^

- Add optional result handler callback to ``DbApiHook`` (#15581)
- Update Flask App Builder limit to recently released 3.3 (#15792)
- Prevent creating flask sessions on REST API requests (#15295)
- Sync DAG specific permissions when parsing (#15311)
- Increase maximum length of pool name on Tasks to 256 characters (#15203)
- Enforce READ COMMITTED isolation when using mysql (#15714)
- Auto-apply ``apply_default`` to subclasses of ``BaseOperator`` (#15667)
- Emit error on duplicated DAG ID (#15302)
- Update ``KubernetesExecutor`` pod templates to allow access to IAM permissions (#15669)
- More verbose logs when running ``airflow db check-migrations`` (#15662)
- When one_success mark task as failed if no success (#15467)
- Add an option to trigger a dag w/o changing conf (#15591)
- Add Airflow UI instance_name configuration option (#10162)
- Add a decorator to retry functions with DB transactions (#14109)
- Add return to PythonVirtualenvOperator's execute method (#14061)
- Add verify_ssl config for kubernetes (#13516)
- Add description about ``secret_key`` when Webserver > 1 (#15546)
- Add Traceback in LogRecord in ``JSONFormatter`` (#15414)
- Add support for arbitrary json in conn uri format (#15100)
- Adds description field in variable (#12413) (#15194)
- Add logs to show last modified in SFTP, FTP and Filesystem sensor (#15134)
- Execute ``on_failure_callback`` when SIGTERM is received (#15172)
- Allow hiding of all edges when highlighting states (#15281)
- Display explicit error in case UID has no actual username (#15212)
- Serve logs with Scheduler when using Local or Sequential Executor (#15557)
- Deactivate trigger, refresh, and delete controls on dag detail view. (#14144)
- Turn off autocomplete for connection forms (#15073)
- Increase default ``worker_refresh_interval`` to ``6000`` seconds (#14970)
- Only show User's local timezone if it's not UTC (#13904)
- Suppress LOG/WARNING for a few tasks CLI for better CLI experience (#14567)
- Configurable API response (CORS) headers (#13620)
- Allow viewers to see all docs links (#14197)
- Update Tree View date ticks (#14141)
- Make the tooltip to Pause / Unpause a DAG clearer (#13642)
- Warn about precedence of env var when getting variables (#13501)
- Move ``[celery] default_queue`` config to ``[operators] default_queue`` to reuse between executors  (#14699)

Bug Fixes
^^^^^^^^^

- Fix 500 error from ``updateTaskInstancesState`` API endpoint when ``dry_run`` not passed (#15889)
- Ensure that task preceding a PythonVirtualenvOperator doesn't fail (#15822)
- Prevent mixed case env vars from crashing processes like worker (#14380)
- Fixed type annotations in DAG decorator (#15778)
- Fix on_failure_callback when task receive SIGKILL (#15537)
- Fix dags table overflow (#15660)
- Fix changing the parent dag state on subdag clear (#15562)
- Fix reading from zip package to default to text (#13962)
- Fix wrong parameter for ``drawDagStatsForDag`` in dags.html (#13884)
- Fix QueuedLocalWorker crashing with EOFError (#13215)
- Fix typo in ``NotPreviouslySkippedDep`` (#13933)
- Fix parallelism after KubeExecutor pod adoption (#15555)
- Fix kube client on mac with keepalive enabled (#15551)
- Fixes wrong limit for dask for python>3.7 (should be <3.7) (#15545)
- Fix Task Adoption in ``KubernetesExecutor`` (#14795)
- Fix timeout when using XCom with ``KubernetesPodOperator`` (#15388)
- Fix deprecated provider aliases in "extras" not working (#15465)
- Fixed default XCom deserialization. (#14827)
- Fix used_group_ids in ``dag.partial_subset`` (#13700) (#15308)
- Further fix trimmed ``pod_id`` for ``KubernetesPodOperator`` (#15445)
- Bugfix: Invalid name when trimmed ``pod_id`` ends with hyphen in ``KubernetesPodOperator`` (#15443)
- Fix incorrect slots stats when TI ``pool_slots > 1`` (#15426)
- Fix DAG last run link (#15327)
- Fix ``sync-perm`` to work correctly when update_fab_perms = False (#14847)
- Fixes limits on Arrow for plexus test (#14781)
- Fix UI bugs in tree view (#14566)
- Fix AzureDataFactoryHook failing to instantiate its connection (#14565)
- Fix permission error on non-POSIX filesystem (#13121)
- Fix spelling in "ignorable" (#14348)
- Fix get_context_data doctest import (#14288)
- Correct typo in ``GCSObjectsWtihPrefixExistenceSensor``  (#14179)
- Fix order of failed deps (#14036)
- Fix critical ``CeleryKubernetesExecutor`` bug (#13247)
- Fix four bugs in ``StackdriverTaskHandler`` (#13784)
- ``func.sum`` may return ``Decimal`` that break rest APIs (#15585)
- Persist tags params in pagination (#15411)
- API: Raise ``AlreadyExists`` exception when the ``execution_date`` is same (#15174)
- Remove duplicate call to ``sync_metadata`` inside ``DagFileProcessorManager`` (#15121)
- Extra ``docker-py`` update to resolve docker op issues (#15731)
- Ensure executors end method is called (#14085)
- Remove ``user_id`` from API schema (#15117)
- Prevent clickable bad links on disabled pagination (#15074)
- Acquire lock on db for the time of migration (#10151)
- Skip SLA check only if SLA is None (#14064)
- Print right version in airflow info command (#14560)
- Make ``airflow info`` work with pipes (#14528)
- Rework client-side script for connection form. (#14052)
- API: Add ``CollectionInfo`` in all Collections that have ``total_entries`` (#14366)
- Fix ``task_instance_mutation_hook`` when importing airflow.models.dagrun (#15851)

Doc only changes
^^^^^^^^^^^^^^^^

- Fix docstring of SqlSensor (#15466)
- Small changes on "DAGs and Tasks documentation" (#14853)
- Add note on changes to configuration options (#15696)
- Add docs to the ``markdownlint`` and ``yamllint`` config files (#15682)
- Rename old "Experimental" API to deprecated in the docs. (#15653)
- Fix documentation error in ``git_sync_template.yaml`` (#13197)
- Fix doc link permission name (#14972)
- Fix link to Helm chart docs (#14652)
- Fix docstrings for Kubernetes code (#14605)
- docs: Capitalize & minor fixes (#14283) (#14534)
- Fixed reading from zip package to default to text. (#13984)
- An initial rework of the "Concepts" docs (#15444)
- Improve docstrings for various modules (#15047)
- Add documentation on database connection URI (#14124)
- Add Helm Chart logo to docs index (#14762)
- Create a new documentation package for Helm Chart (#14643)
- Add docs about supported logging levels (#14507)
- Update docs about tableau and salesforce provider (#14495)
- Replace deprecated doc links to the correct one (#14429)
- Refactor redundant doc url logic to use utility (#14080)
- docs: NOTICE: Updated 2016-2019 to 2016-now (#14248)
- Skip DAG perm sync during parsing if possible (#15464)
- Add picture and examples for Edge Labels (#15310)
- Add example DAG & how-to guide for sqlite (#13196)
- Add links to new modules for deprecated modules (#15316)
- Add note in Updating.md about FAB data model change (#14478)

Misc/Internal
^^^^^^^^^^^^^

- Fix ``logging.exception`` redundancy (#14823)
- Bump ``stylelint`` to remove vulnerable sub-dependency (#15784)
- Add resolution to force dependencies to use patched version of lodash (#15777)
- Update croniter to 1.0.x series (#15769)
- Get rid of Airflow 1.10 in Breeze (#15712)
- Run helm chart tests in parallel (#15706)
- Bump ``ssri`` from 6.0.1 to 6.0.2 in /airflow/www (#15437)
- Remove the limit on Gunicorn dependency (#15611)
- Better "dependency already registered" warning message for tasks #14613 (#14860)
- Pin pandas-gbq to <0.15.0 (#15114)
- Use Pip 21.* to install airflow officially (#15513)
- Bump mysqlclient to support the 1.4.x and 2.x series (#14978)
- Finish refactor of DAG resource name helper (#15511)
- Refactor/Cleanup Presentation of Graph Task and Path Highlighting (#15257)
- Standardize default fab perms (#14946)
- Remove ``datepicker`` for task instance detail view (#15284)
- Turn provider's import warnings into debug logs (#14903)
- Remove left-over fields from required in provider_info schema. (#14119)
- Deprecate ``tableau`` extra (#13595)
- Use built-in ``cached_property`` on Python 3.8 where possible (#14606)
- Clean-up JS code in UI templates (#14019)
- Bump elliptic from 6.5.3 to 6.5.4 in /airflow/www (#14668)
- Switch to f-strings using ``flynt``. (#13732)
- use ``jquery`` ready instead of vanilla js (#15258)
- Migrate task instance log (ti_log) js (#15309)
- Migrate graph js (#15307)
- Migrate dags.html javascript (#14692)
- Removes unnecessary AzureContainerInstance connection type (#15514)
- Separate Kubernetes pod_launcher from core airflow (#15165)
- update remaining old import paths of operators (#15127)
- Remove broken and undocumented "demo mode" feature (#14601)
- Simplify configuration/legibility of ``Webpack`` entries (#14551)
- remove inline tree js (#14552)
- Js linting and inline migration for simple scripts (#14215)
- Remove use of repeated constant in AirflowConfigParser (#14023)
- Deprecate email credentials from environment variables. (#13601)
- Remove unused 'context' variable in task_instance.py (#14049)
- Disable suppress_logs_and_warning in cli when debugging (#13180)


Airflow 2.0.2 (2021-04-19)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default ``[kubernetes] enable_tcp_keepalive`` is changed to ``True``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This allows Airflow to work more reliably with some environments (like Azure) by default.

``sync-perm`` CLI no longer syncs DAG specific permissions by default
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``sync-perm`` CLI command will no longer sync DAG specific permissions by default as they are now being handled during
DAG parsing. If you need or want the old behavior, you can pass ``--include-dags`` to have ``sync-perm`` also sync DAG
specific permissions.

Bug Fixes
^^^^^^^^^

* Bugfix: ``TypeError`` when Serializing & sorting iterable properties of DAGs (#15395)
* Fix missing ``on_load`` trigger for folder-based plugins (#15208)
* ``kubernetes cleanup-pods`` subcommand will only clean up Airflow-created Pods (#15204)
* Fix password masking in CLI action_logging (#15143)
* Fix url generation for TriggerDagRunOperatorLink (#14990)
* Restore base lineage backend (#14146)
* Unable to trigger backfill or manual jobs with Kubernetes executor. (#14160)
* Bugfix: Task docs are not shown in the Task Instance Detail View (#15191)
* Bugfix: Fix overriding ``pod_template_file`` in KubernetesExecutor (#15197)
* Bugfix: resources in ``executor_config`` breaks Graph View in UI (#15199)
* Fix celery executor bug trying to call len on map (#14883)
* Fix bug in airflow.stats timing that broke dogstatsd mode (#15132)
* Avoid scheduler/parser manager deadlock by using non-blocking IO (#15112)
* Re-introduce ``dagrun.schedule_delay`` metric (#15105)
* Compare string values, not if strings are the same object in Kube executor(#14942)
* Pass queue to BaseExecutor.execute_async like in airflow 1.10 (#14861)
* Scheduler: Remove TIs from starved pools from the critical path. (#14476)
* Remove extra/needless deprecation warnings from airflow.contrib module (#15065)
* Fix support for long dag_id and task_id in KubernetesExecutor (#14703)
* Sort lists, sets and tuples in Serialized DAGs (#14909)
* Simplify cleaning string passed to origin param (#14738) (#14905)
* Fix error when running tasks with Sentry integration enabled. (#13929)
* Webserver: Sanitize string passed to origin param (#14738)
* Fix losing duration < 1 secs in tree (#13537)
* Pin SQLAlchemy to <1.4 due to breakage of sqlalchemy-utils (#14812)
* Fix KubernetesExecutor issue with deleted pending pods (#14810)
* Default to Celery Task model when backend model does not exist (#14612)
* Bugfix: Plugins endpoint was unauthenticated (#14570)
* BugFix: fix DAG doc display (especially for TaskFlow DAGs) (#14564)
* BugFix: TypeError in airflow.kubernetes.pod_launcher's monitor_pod (#14513)
* Bugfix: Fix wrong output of tags and owners in dag detail API endpoint (#14490)
* Fix logging error with task error when JSON logging is enabled (#14456)
* Fix StatsD metrics not sending when using daemon mode (#14454)
* Gracefully handle missing start_date and end_date for DagRun (#14452)
* BugFix: Serialize max_retry_delay as a timedelta (#14436)
* Fix crash when user clicks on  "Task Instance Details" caused by start_date being None (#14416)
* BugFix: Fix TaskInstance API call fails if a task is removed from running DAG (#14381)
* Scheduler should not fail when invalid ``executor_config`` is passed (#14323)
* Fix bug allowing task instances to survive when dagrun_timeout is exceeded (#14321)
* Fix bug where DAG timezone was not always shown correctly in UI tooltips (#14204)
* Use ``Lax`` for ``cookie_samesite`` when empty string is passed (#14183)
* [AIRFLOW-6076] fix ``dag.cli()`` KeyError (#13647)
* Fix running child tasks in a subdag after clearing a successful subdag (#14776)

Improvements
^^^^^^^^^^^^

* Remove unused JS packages causing false security alerts (#15383)
* Change default of ``[kubernetes] enable_tcp_keepalive`` for new installs to ``True`` (#15338)
* Fixed #14270: Add error message in OOM situations (#15207)
* Better compatibility/diagnostics for arbitrary UID in docker image (#15162)
* Updates 3.6 limits for latest versions of a few libraries (#15209)
* Adds Blinker dependency which is missing after recent changes (#15182)
* Remove 'conf' from search_columns in DagRun View (#15099)
* More proper default value for namespace in K8S cleanup-pods CLI (#15060)
* Faster default role syncing during webserver start (#15017)
* Speed up webserver start when there are many DAGs (#14993)
* Much easier to use and better documented Docker image (#14911)
* Use ``libyaml`` C library when available. (#14577)
* Don't create unittest.cfg when not running in unit test mode (#14420)
* Webserver: Allow Filtering TaskInstances by queued_dttm (#14708)
* Update Flask-AppBuilder dependency to allow 3.2 (and all 3.x series) (#14665)
* Remember expanded task groups in browser local storage (#14661)
* Add plain format output to cli tables (#14546)
* Make ``airflow dags show`` command display TaskGroups (#14269)
* Increase maximum size of ``extra`` connection field. (#12944)
* Speed up clear_task_instances by doing a single sql delete for TaskReschedule (#14048)
* Add more flexibility with FAB menu links (#13903)
* Add better description and guidance in case of sqlite version mismatch (#14209)

Doc only changes
^^^^^^^^^^^^^^^^

* Add documentation create/update community providers (#15061)
* Fix mistake and typos in airflow.utils.timezone docstrings (#15180)
* Replace new url for Stable Airflow Docs (#15169)
* Docs: Clarify behavior of delete_worker_pods_on_failure (#14958)
* Create a documentation package for Docker image (#14846)
* Multiple minor doc (OpenAPI) fixes (#14917)
* Replace Graph View Screenshot to show Auto-refresh (#14571)

Misc/Internal
^^^^^^^^^^^^^

* Import Connection lazily in hooks to avoid cycles (#15361)
* Rename last_scheduler_run into last_parsed_time, and ensure it's updated in DB (#14581)
* Make TaskInstance.pool_slots not nullable with a default of 1 (#14406)
* Log migrations info in consistent way (#14158)

Airflow 2.0.1 (2021-02-08)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Permission to view Airflow Configurations has been removed from ``User`` and ``Viewer`` role
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously, Users with ``User`` or ``Viewer`` role were able to get/view configurations using
the REST API or in the Webserver. From Airflow 2.0.1, only users with ``Admin`` or ``Op`` role would be able
to get/view Configurations.

To allow users with other roles to view configuration, add ``can read on Configurations`` permissions to that role.

Note that if ``[webserver] expose_config`` is set to ``False``\ , the API will throw a ``403`` response even if
the user has role with ``can read on Configurations`` permission.

Default ``[celery] worker_concurrency`` is changed to ``16``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default value for ``[celery] worker_concurrency`` was ``16`` for Airflow <2.0.0.
However, it was unintentionally changed to ``8`` in 2.0.0.

From Airflow 2.0.1, we revert to the old default of ``16``.

Default ``[scheduler] min_file_process_interval`` is changed to ``30``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default value for ``[scheduler] min_file_process_interval`` was ``0``\ ,
due to which the CPU Usage mostly stayed around 100% as the DAG files are parsed
constantly.

From Airflow 2.0.0, the scheduling decisions have been moved from
DagFileProcessor to Scheduler, so we can keep the default a bit higher: ``30``.

Bug Fixes
^^^^^^^^^

- Bugfix: Return XCom Value in the XCom Endpoint API (#13684)
- Bugfix: Import error when using custom backend and ``sql_alchemy_conn_secret`` (#13260)
- Allow PID file path to be relative when daemonize a process (scheduler, kerberos, etc) (#13232)
- Bugfix: no generic ``DROP CONSTRAINT`` in MySQL during ``airflow db upgrade`` (#13239)
- Bugfix: Sync Access Control defined in DAGs when running ``sync-perm`` (#13377)
- Stop sending Callback Requests if no callbacks are defined on DAG (#13163)
- BugFix: Dag-level Callback Requests were not run (#13651)
- Stop creating duplicate Dag File Processors (#13662)
- Filter DagRuns with Task Instances in removed State while Scheduling (#13165)
- Bump ``datatables.net`` from 1.10.21 to 1.10.22 in /airflow/www (#13143)
- Bump ``datatables.net`` JS to 1.10.23 (#13253)
- Bump ``dompurify`` from 2.0.12 to 2.2.6 in /airflow/www (#13164)
- Update minimum ``cattrs`` version (#13223)
- Remove inapplicable arg 'output' for CLI pools import/export (#13071)
- Webserver: Fix the behavior to deactivate the authentication option and add docs (#13191)
- Fix: add support for no-menu plugin views (#11742)
- Add ``python-daemon`` limit for Python 3.8+ to fix daemon crash (#13540)
- Change the default celery ``worker_concurrency`` to 16 (#13612)
- Audit Log records View should not contain link if ``dag_id`` is None (#13619)
- Fix invalid ``continue_token`` for cleanup list pods (#13563)
- Switches to latest version of snowflake connector (#13654)
- Fix backfill crash on task retry or reschedule (#13712)
- Setting ``max_tis_per_query`` to ``0`` now correctly removes the limit (#13512)
- Fix race conditions in task callback invocations (#10917)
- Fix webserver exiting when gunicorn master crashes (#13518)(#13780)
- Fix SQL syntax to check duplicate connections (#13783)
- ``BaseBranchOperator`` will push to xcom by default (#13704) (#13763)
- Fix Deprecation for ``configuration.getsection`` (#13804)
- Fix TaskNotFound in log endpoint (#13872)
- Fix race condition when using Dynamic DAGs (#13893)
- Fix: Linux/Chrome window bouncing in Webserver
- Fix db shell for sqlite (#13907)
- Only compare updated time when Serialized DAG exists (#13899)
- Fix dag run type enum query for mysqldb driver (#13278)
- Add authentication to lineage endpoint for experimental API (#13870)
- Do not add User role perms to custom roles. (#13856)
- Do not add ``Website.can_read`` access to default roles. (#13923)
- Fix invalid value error caused by long Kubernetes pod name (#13299)
- Fix DB Migration for SQLite to upgrade to 2.0 (#13921)
- Bugfix: Manual DagRun trigger should not skip scheduled runs (#13963)
- Stop loading Extra Operator links in Scheduler (#13932)
- Added missing return parameter in read function of ``FileTaskHandler`` (#14001)
- Bugfix: Do not try to create a duplicate Dag Run in Scheduler (#13920)
- Make ``v1/config`` endpoint respect webserver ``expose_config`` setting (#14020)
- Disable row level locking for Mariadb and MySQL <8 (#14031)
- Bugfix: Fix permissions to triggering only specific DAGs (#13922)
- Fix broken SLA Mechanism (#14056)
- Bugfix: Scheduler fails if task is removed at runtime (#14057)
- Remove permissions to read Configurations for User and Viewer roles (#14067)
- Fix DB Migration from 2.0.1rc1

Improvements
^^^^^^^^^^^^

- Increase the default ``min_file_process_interval`` to decrease CPU Usage (#13664)
- Dispose connections when running tasks with ``os.fork`` & ``CeleryExecutor`` (#13265)
- Make function purpose clearer in ``example_kubernetes_executor`` example dag (#13216)
- Remove unused libraries - ``flask-swagger``, ``funcsigs`` (#13178)
- Display alternative tooltip when a Task has yet to run (no TI) (#13162)
- User werkzeug's own type conversion for request args (#13184)
- UI: Add ``queued_by_job_id`` & ``external_executor_id`` Columns to TI View (#13266)
- Make ``json-merge-patch`` an optional library and unpin it (#13175)
- Adds missing LDAP "extra" dependencies to ldap provider. (#13308)
- Refactor ``setup.py`` to better reflect changes in providers (#13314)
- Pin ``pyjwt`` and Add integration tests for Apache Pinot (#13195)
- Removes provider-imposed requirements from ``setup.cfg`` (#13409)
- Replace deprecated decorator (#13443)
- Streamline & simplify ``__eq__`` methods in models Dag and BaseOperator (#13449)
- Additional properties should be allowed in provider schema (#13440)
- Remove unused dependency - ``contextdecorator`` (#13455)
- Remove 'typing' dependency (#13472)
- Log migrations info in consistent way (#13458)
- Unpin ``mysql-connector-python`` to allow ``8.0.22`` (#13370)
- Remove thrift as a core dependency (#13471)
- Add ``NotFound`` response for DELETE methods in OpenAPI YAML (#13550)
- Stop Log Spamming when ``[core] lazy_load_plugins`` is ``False`` (#13578)
- Display message and docs link when no plugins are loaded (#13599)
- Unpin restriction for ``colorlog`` dependency (#13176)
- Add missing Dag Tag for Example DAGs (#13665)
- Support tables in DAG docs (#13533)
- Add ``python3-openid`` dependency (#13714)
- Add ``__repr__`` for Executors (#13753)
- Add description to hint if ``conn_type`` is missing (#13778)
- Upgrade Azure blob to v12 (#12188)
- Add extra field to ``get_connnection`` REST endpoint (#13885)
- Make Smart Sensors DB Migration idempotent (#13892)
- Improve the error when DAG does not exist when running dag pause command (#13900)
- Update ``airflow_local_settings.py`` to fix an error message (#13927)
- Only allow passing JSON Serializable conf to ``TriggerDagRunOperator`` (#13964)
- Bugfix: Allow getting details of a DAG with null ``start_date`` (REST API) (#13959)
- Add params to the DAG details endpoint (#13790)
- Make the role assigned to anonymous users customizable (#14042)
- Retry critical methods in Scheduler loop in case of ``OperationalError`` (#14032)

Doc only changes
^^^^^^^^^^^^^^^^

- Add Missing StatsD Metrics in Docs (#13708)
- Add Missing Email configs in Configuration doc (#13709)
- Add quick start for Airflow on Docker (#13660)
- Describe which Python versions are supported (#13259)
- Add note block to 2.x migration docs (#13094)
- Add documentation about webserver_config.py (#13155)
- Add missing version information to recently added configs (#13161)
- API: Use generic information in UpdateMask component (#13146)
- Add Airflow 2.0.0 to requirements table (#13140)
- Avoid confusion in doc for CeleryKubernetesExecutor (#13116)
- Update docs link in REST API spec (#13107)
- Add link to PyPI Repository to provider docs (#13064)
- Fix link to Airflow master branch documentation (#13179)
- Minor enhancements to Sensors docs (#13381)
- Use 2.0.0 in Airflow docs & Breeze (#13379)
- Improves documentation regarding providers and custom connections (#13375)(#13410)
- Fix malformed table in production-deployment.rst (#13395)
- Update celery.rst to fix broken links (#13400)
- Remove reference to scheduler run_duration param in docs (#13346)
- Set minimum SQLite version supported (#13412)
- Fix installation doc (#13462)
- Add docs about mocking variables and connections (#13502)
- Add docs about Flask CLI (#13500)
- Fix Upgrading to 2 guide to use ``rbac`` UI (#13569)
- Make docs clear that Auth can not be disabled for Stable API (#13568)
- Remove archived links from docs & add link for AIPs (#13580)
- Minor fixes in upgrading-to-2.rst (#13583)
- Fix Link in Upgrading to 2.0 guide (#13584)
- Fix heading for Mocking section in best-practices.rst (#13658)
- Add docs on how to use custom operators within plugins folder (#13186)
- Update docs to register Operator Extra Links (#13683)
- Improvements for database setup docs (#13696)
- Replace module path to Class with just Class Name (#13719)
- Update DAG Serialization docs (#13722)
- Fix link to Apache Airflow docs in webserver (#13250)
- Clarifies differences between extras and provider packages (#13810)
- Add information about all access methods to the environment (#13940)
- Docs: Fix FAQ on scheduler latency (#13969)
- Updated taskflow api doc to show dependency with sensor (#13968)
- Add deprecated config options to docs (#13883)
- Added a FAQ section to the Upgrading to 2 doc (#13979)

Airflow 2.0.0 (2020-12-18)
--------------------------

The full changelog is about 3,000 lines long (already excluding everything backported to 1.10)
so please check `Airflow 2.0.0 Highlights Blog Post <https://airflow.apache.org/blog/airflow-two-point-oh-is-here/>`_
instead.

Significant Changes
^^^^^^^^^^^^^^^^^^^

The 2.0 release of the Airflow is a significant upgrade, and includes substantial major changes,
and some of them may be breaking. Existing code written for earlier versions of this project will may require updates
to use this version. Sometimes necessary configuration changes are also required.
This document describes the changes that have been made, and what you need to do to update your usage.

If you experience issues or have questions, please file `an issue <https://github.com/apache/airflow/issues/new/choose>`_.

Major changes
"""""""""""""

This section describes the major changes that have been made in this release.

The experimental REST API is disabled by default
""""""""""""""""""""""""""""""""""""""""""""""""

The experimental REST API is disabled by default. To restore these APIs while migrating to
the stable REST API, set ``enable_experimental_api`` option in ``[api]`` section to ``True``.

Please note that the experimental REST API do not have access control.
The authenticated user has full access.

SparkJDBCHook default connection
""""""""""""""""""""""""""""""""

For SparkJDBCHook default connection was ``spark-default``\ , and for SparkSubmitHook it was
``spark_default``. Both hooks now use the ``spark_default`` which is a common pattern for the connection
names used across all providers.

Changes to output argument in commands
""""""""""""""""""""""""""""""""""""""

From Airflow 2.0, We are replacing `tabulate <https://pypi.org/project/tabulate/>`_ with `rich <https://github.com/willmcgugan/rich>`_ to render commands output. Due to this change, the ``--output`` argument
will no longer accept formats of tabulate tables. Instead, it now accepts:


* ``table`` - will render the output in predefined table
* ``json`` - will render the output as a json
* ``yaml`` - will render the output as yaml

By doing this we increased consistency and gave users possibility to manipulate the
output programmatically (when using json or yaml).

Affected commands:


* ``airflow dags list``
* ``airflow dags report``
* ``airflow dags list-runs``
* ``airflow dags list-jobs``
* ``airflow connections list``
* ``airflow connections get``
* ``airflow pools list``
* ``airflow pools get``
* ``airflow pools set``
* ``airflow pools delete``
* ``airflow pools import``
* ``airflow pools export``
* ``airflow role list``
* ``airflow providers list``
* ``airflow providers get``
* ``airflow providers hooks``
* ``airflow tasks states-for-dag-run``
* ``airflow users list``
* ``airflow variables list``

Azure Wasb Hook does not work together with Snowflake hook
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The WasbHook in Apache Airflow use a legacy version of Azure library. While the conflict is not
significant for most of the Azure hooks, it is a problem for Wasb Hook because the ``blob`` folders
for both libraries overlap. Installing both Snowflake and Azure extra will result in non-importable
WasbHook.

Rename ``all`` to ``devel_all`` extra
"""""""""""""""""""""""""""""""""""""""""""""

The ``all`` extras were reduced to include only user-facing dependencies. This means
that this extra does not contain development dependencies. If you were relying on
``all`` extra then you should use now ``devel_all`` or figure out if you need development
extras at all.

Context variables ``prev_execution_date_success`` and ``prev_execution_date_success`` are now ``pendulum.DateTime``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Rename policy to task_policy
""""""""""""""""""""""""""""

Because Airflow introduced DAG level policy (\ ``dag_policy``\ ) we decided to rename existing ``policy``
function to ``task_policy`` to make the distinction more profound and avoid any confusion.

Users using cluster policy need to rename their ``policy`` functions in ``airflow_local_settings.py``
to ``task_policy``.

Default value for ``[celery] operation_timeout`` has changed to ``1.0``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

From Airflow 2, by default Airflow will retry 3 times to publish task to Celery broker. This is controlled by
``[celery] task_publish_max_retries``. Because of this we can now have a lower Operation timeout that raises
``AirflowTaskTimeout``. This generally occurs during network blips or intermittent DNS issues.

Adding Operators and Sensors via plugins is no longer supported
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Operators and Sensors should no longer be registered or imported via Airflow's plugin mechanism -- these types of classes are just treated as plain python classes by Airflow, so there is no need to register them with Airflow.

If you previously had a ``plugins/my_plugin.py`` and you used it like this in a DAG:

.. code-block::

   from airflow.operators.my_plugin import MyOperator

You should instead import it as:

.. code-block::

   from my_plugin import MyOperator

The name under ``airflow.operators.`` was the plugin name, where as in the second example it is the python module name where the operator is defined.

See https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html for more info.

Importing Hooks via plugins is no longer supported
""""""""""""""""""""""""""""""""""""""""""""""""""

Importing hooks added in plugins via ``airflow.hooks.<plugin_name>`` is no longer supported, and hooks should just be imported as regular python modules.

.. code-block::

   from airflow.hooks.my_plugin import MyHook

You should instead import it as:

.. code-block::

   from my_plugin import MyHook

It is still possible (but not required) to "register" hooks in plugins. This is to allow future support for dynamically populating the Connections form in the UI.

See https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html for more info.

The default value for ``[core] enable_xcom_pickling`` has been changed to ``False``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The pickle type for XCom messages has been replaced to JSON by default to prevent RCE attacks.
Note that JSON serialization is stricter than pickling, so for example if you want to pass
raw bytes through XCom you must encode them using an encoding like ``base64``.
If you understand the risk and still want to use `pickling <https://docs.python.org/3/library/pickle.html>`_\ ,
set ``enable_xcom_pickling = True`` in your Airflow config's ``core`` section.

Airflowignore of base path
""""""""""""""""""""""""""

There was a bug fixed in https://github.com/apache/airflow/pull/11993 that the "airflowignore" checked
the base path of the dag folder for forbidden dags, not only the relative part. This had the effect
that if the base path contained the excluded word the whole dag folder could have been excluded. For
example if the airflowignore file contained x, and the dags folder was '/var/x/dags', then all dags in
the folder would be excluded. The fix only matches the relative path only now which means that if you
previously used full path as ignored, you should change it to relative one. For example if your dag
folder was '/var/dags/' and your airflowignore contained '/var/dag/excluded/', you should change it
to 'excluded/'.

``ExternalTaskSensor`` provides all task context variables to ``execution_date_fn`` as keyword arguments
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The old syntax of passing ``context`` as a dictionary will continue to work with the caveat that the argument must be named ``context``. The following will break. To fix it, change ``ctx`` to ``context``.

.. code-block:: python

   def execution_date_fn(execution_date, ctx): ...

``execution_date_fn`` can take in any number of keyword arguments available in the task context dictionary. The following forms of ``execution_date_fn`` are all supported:

.. code-block:: python

   def execution_date_fn(dt): ...


   def execution_date_fn(execution_date): ...


   def execution_date_fn(execution_date, ds_nodash): ...


   def execution_date_fn(execution_date, ds_nodash, dag): ...

The default value for ``[webserver] cookie_samesite`` has been changed to ``Lax``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

As `recommended <https://flask.palletsprojects.com/en/1.1.x/config/#SESSION_COOKIE_SAMESITE>`_ by Flask, the
``[webserver] cookie_samesite`` has been changed to ``Lax`` from ``''`` (empty string) .

Changes to import paths
~~~~~~~~~~~~~~~~~~~~~~~

Formerly the core code was maintained by the original creators - Airbnb. The code that was in the contrib
package was supported by the community. The project was passed to the Apache community and currently the
entire code is maintained by the community, so now the division has no justification, and it is only due
to historical reasons. In Airflow 2.0, we want to organize packages and move integrations
with third party services to the ``airflow.providers`` package.

All changes made are backward compatible, but if you use the old import paths you will
see a deprecation warning. The old import paths can be abandoned in the future.

According to `AIP-21 <https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths>`_
``_operator`` suffix has been removed from operators. A deprecation warning has also been raised for paths
importing with the suffix.

The following table shows changes in import paths.

.. list-table::
   :header-rows: 1

   * - Old path
     - New path
   * - ``airflow.hooks.base_hook.BaseHook``
     - ``airflow.hooks.base.BaseHook``
   * - ``airflow.hooks.dbapi_hook.DbApiHook``
     - ``airflow.hooks.dbapi.DbApiHook``
   * - ``airflow.operators.dummy_operator.DummyOperator``
     - ``airflow.operators.dummy.DummyOperator``
   * - ``airflow.operators.dagrun_operator.TriggerDagRunOperator``
     - ``airflow.operators.trigger_dagrun.TriggerDagRunOperator``
   * - ``airflow.operators.branch_operator.BaseBranchOperator``
     - ``airflow.operators.branch.BaseBranchOperator``
   * - ``airflow.operators.subdag_operator.SubDagOperator``
     - ``airflow.operators.subdag.SubDagOperator``
   * - ``airflow.sensors.base_sensor_operator.BaseSensorOperator``
     - ``airflow.sensors.base.BaseSensorOperator``
   * - ``airflow.sensors.date_time_sensor.DateTimeSensor``
     - ``airflow.sensors.date_time.DateTimeSensor``
   * - ``airflow.sensors.external_task_sensor.ExternalTaskMarker``
     - ``airflow.sensors.external_task.ExternalTaskMarker``
   * - ``airflow.sensors.external_task_sensor.ExternalTaskSensor``
     - ``airflow.sensors.external_task.ExternalTaskSensor``
   * - ``airflow.sensors.sql_sensor.SqlSensor``
     - ``airflow.sensors.sql.SqlSensor``
   * - ``airflow.sensors.time_delta_sensor.TimeDeltaSensor``
     - ``airflow.sensors.time_delta.TimeDeltaSensor``
   * - ``airflow.contrib.sensors.weekday_sensor.DayOfWeekSensor``
     - ``airflow.sensors.weekday.DayOfWeekSensor``


Database schema changes
"""""""""""""""""""""""

In order to migrate the database, you should use the command ``airflow db upgrade``\ , but in
some cases manual steps are required.

Unique conn_id in connection table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously, Airflow allowed users to add more than one connection with the same ``conn_id`` and on access it would choose one connection randomly. This acted as a basic load balancing and fault tolerance technique, when used in conjunction with retries.

This behavior caused some confusion for users, and there was no clear evidence if it actually worked well or not.

Now the ``conn_id`` will be unique. If you already have duplicates in your metadata database, you will have to manage those duplicate connections before upgrading the database.

Not-nullable conn_type column in connection table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``conn_type`` column in the ``connection`` table must contain content. Previously, this rule was enforced
by application logic, but was not enforced by the database schema.

If you made any modifications to the table directly, make sure you don't have
null in the ``conn_type`` column.

Configuration changes
"""""""""""""""""""""

This release contains many changes that require a change in the configuration of this application or
other application that integrate with it.

This section describes the changes that have been made, and what you need to do to.

airflow.contrib.utils.log has been moved
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Formerly the core code was maintained by the original creators - Airbnb. The code that was in the contrib
package was supported by the community. The project was passed to the Apache community and currently the
entire code is maintained by the community, so now the division has no justification, and it is only due
to historical reasons. In Airflow 2.0, we want to organize packages and move integrations
with third party services to the ``airflow.providers`` package.

To clean up, the following packages were moved:

.. list-table::
   :header-rows: 1

   * - Old package
     - New package
   * - ``airflow.contrib.utils.log``
     - ``airflow.utils.log``
   * - ``airflow.utils.log.gcs_task_handler``
     - ``airflow.providers.google.cloud.log.gcs_task_handler``
   * - ``airflow.utils.log.wasb_task_handler``
     -  ``airflow.providers.microsoft.azure.log.wasb_task_handler``
   * - ``airflow.utils.log.stackdriver_task_handler``
     -  ``airflow.providers.google.cloud.log.stackdriver_task_handler``
   * - ``airflow.utils.log.s3_task_handler``
     - ``airflow.providers.amazon.aws.log.s3_task_handler``
   * - ``airflow.utils.log.es_task_handler``
     - ``airflow.providers.elasticsearch.log.es_task_handler``
   * - ``airflow.utils.log.cloudwatch_task_handler``
     - ``airflow.providers.amazon.aws.log.cloudwatch_task_handler``

You should update the import paths if you are setting log configurations with the ``logging_config_class`` option.
The old import paths still works but can be abandoned.

SendGrid emailer has been moved
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Formerly the core code was maintained by the original creators - Airbnb. The code that was in the contrib
package was supported by the community. The project was passed to the Apache community and currently the
entire code is maintained by the community, so now the division has no justification, and it is only due
to historical reasons.

To clean up, the ``send_mail`` function from the ``airflow.contrib.utils.sendgrid`` module has been moved.

If your configuration file looks like this:

.. code-block:: ini

   [email]
   email_backend = airflow.contrib.utils.sendgrid.send_email

It should look like this now:

.. code-block:: ini

   [email]
   email_backend = airflow.providers.sendgrid.utils.emailer.send_email

The old configuration still works but can be abandoned.

Unify ``hostname_callable`` option in ``core`` section
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The previous option used a colon(\ ``:``\ ) to split the module from function. Now the dot(\ ``.``\ ) is used.

The change aims to unify the format of all options that refer to objects in the ``airflow.cfg`` file.

Custom executors is loaded using full import path
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In previous versions of Airflow it was possible to use plugins to load custom executors. It is still
possible, but the configuration has changed. Now you don't have to create a plugin to configure a
custom executor, but you need to provide the full path to the module in the ``executor`` option
in the ``core`` section. The purpose of this change is to simplify the plugin mechanism and make
it easier to configure executor.

If your module was in the path ``my_acme_company.executors.MyCustomExecutor``  and the plugin was
called ``my_plugin`` then your configuration looks like this

.. code-block:: ini

   [core]
   executor = my_plugin.MyCustomExecutor

And now it should look like this:

.. code-block:: ini

   [core]
   executor = my_acme_company.executors.MyCustomExecutor

The old configuration is still works but can be abandoned at any time.

Use ``CustomSQLAInterface`` instead of ``SQLAInterface`` for custom data models.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

From Airflow 2.0, if you want to define your own Flask App Builder data models you need to use CustomSQLAInterface
instead of SQLAInterface.

For Non-RBAC replace:

.. code-block:: python

   from flask_appbuilder.models.sqla.interface import SQLAInterface

   datamodel = SQLAInterface(your_data_model)

with RBAC (in 1.10):

.. code-block:: python

   from airflow.www_rbac.utils import CustomSQLAInterface

   datamodel = CustomSQLAInterface(your_data_model)

and in 2.0:

.. code-block:: python

   from airflow.www.utils import CustomSQLAInterface

   datamodel = CustomSQLAInterface(your_data_model)

Drop plugin support for stat_name_handler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In previous version, you could use plugins mechanism to configure ``stat_name_handler``. You should now use the ``stat_name_handler``
option in ``[scheduler]`` section to achieve the same effect.

If your plugin looked like this and was available through the ``test_plugin`` path:

.. code-block:: python

   def my_stat_name_handler(stat):
       return stat


   class AirflowTestPlugin(AirflowPlugin):
       name = "test_plugin"
       stat_name_handler = my_stat_name_handler

then your ``airflow.cfg`` file should look like this:

.. code-block:: ini

   [scheduler]
   stat_name_handler=test_plugin.my_stat_name_handler

This change is intended to simplify the statsd configuration.

Logging configuration has been moved to new section
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following configurations have been moved from ``[core]`` to the new ``[logging]`` section.


* ``base_log_folder``
* ``remote_logging``
* ``remote_log_conn_id``
* ``remote_base_log_folder``
* ``encrypt_s3_logs``
* ``logging_level``
* ``fab_logging_level``
* ``logging_config_class``
* ``colored_console_log``
* ``colored_log_format``
* ``colored_formatter_class``
* ``log_format``
* ``simple_log_format``
* ``task_log_prefix_template``
* ``log_filename_template``
* ``log_processor_filename_template``
* ``dag_processor_manager_log_location``
* ``task_log_reader``

Metrics configuration has been moved to new section
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following configurations have been moved from ``[scheduler]`` to the new ``[metrics]`` section.


* ``statsd_on``
* ``statsd_host``
* ``statsd_port``
* ``statsd_prefix``
* ``statsd_allow_list``
* ``stat_name_handler``
* ``statsd_datadog_enabled``
* ``statsd_datadog_tags``
* ``statsd_custom_client_path``

Changes to Elasticsearch logging provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When JSON output to stdout is enabled, log lines will now contain the ``log_id`` & ``offset`` fields, this should make reading task logs from elasticsearch on the webserver work out of the box. Example configuration:

.. code-block:: ini

   [logging]
   remote_logging = True
   [elasticsearch]
   host = http://es-host:9200
   write_stdout = True
   json_format = True

Note that the webserver expects the log line data itself to be present in the ``message`` field of the document.

Remove gcp_service_account_keys option in airflow.cfg file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This option has been removed because it is no longer supported by the Google Kubernetes Engine. The new
recommended service account keys for the Google Cloud management method is
`Workload Identity <https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity>`_.

Fernet is enabled by default
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The fernet mechanism is enabled by default to increase the security of the default installation.  In order to
restore the previous behavior, the user must consciously set an empty key in the ``fernet_key`` option of
section ``[core]`` in the ``airflow.cfg`` file.

At the same time, this means that the ``apache-airflow[crypto]`` extra-packages are always installed.
However, this requires that your operating system has ``libffi-dev`` installed.

Changes to propagating Kubernetes worker annotations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``kubernetes_annotations`` configuration section has been removed.
A new key ``worker_annotations`` has been added to existing ``kubernetes`` section instead.
That is to remove restriction on the character set for k8s annotation keys.
All key/value pairs from ``kubernetes_annotations`` should now go to ``worker_annotations`` as a json. I.e. instead of e.g.

.. code-block::

   [kubernetes_annotations]
   annotation_key = annotation_value
   annotation_key2 = annotation_value2

it should be rewritten to

.. code-block::

   [kubernetes]
   worker_annotations = { "annotation_key" : "annotation_value", "annotation_key2" : "annotation_value2" }

Remove run_duration
~~~~~~~~~~~~~~~~~~~

We should not use the ``run_duration`` option anymore. This used to be for restarting the scheduler from time to time, but right now the scheduler is getting more stable and therefore using this setting is considered bad and might cause an inconsistent state.

Rename pool statsd metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~

Used slot has been renamed to running slot to make the name self-explanatory
and the code more maintainable.

This means ``pool.used_slots.<pool_name>`` metric has been renamed to
``pool.running_slots.<pool_name>``. The ``Used Slots`` column in Pools Web UI view
has also been changed to ``Running Slots``.

Removal of Mesos Executor
~~~~~~~~~~~~~~~~~~~~~~~~~

The Mesos Executor is removed from the code base as it was not widely used and not maintained. `Mailing List Discussion on deleting it <https://lists.apache.org/thread.html/daa9500026b820c6aaadeffd66166eae558282778091ebbc68819fb7@%3Cdev.airflow.apache.org%3E>`_.

Change dag loading duration metric name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Change DAG file loading duration metric from
``dag.loading-duration.<dag_id>`` to ``dag.loading-duration.<dag_file>``. This is to
better handle the case when a DAG file has multiple DAGs.

Sentry is disabled by default
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sentry is disabled by default. To enable these integrations, you need set ``sentry_on`` option
in ``[sentry]`` section to ``"True"``.

Simplified GCSTaskHandler configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In previous versions, in order to configure the service account key file, you had to create a connection entry.
In the current version, you can configure ``google_key_path`` option in ``[logging]`` section to set
the key file path.

Users using Application Default Credentials (ADC) need not take any action.

The change aims to simplify the configuration of logging, to prevent corruption of
the instance configuration by changing the value controlled by the user - connection entry. If you
configure a backend secret, it also means the webserver doesn't need to connect to it. This
simplifies setups with multiple GCP projects, because only one project will require the Secret Manager API
to be enabled.

Changes to the core operators/hooks
"""""""""""""""""""""""""""""""""""

We strive to ensure that there are no changes that may affect the end user and your files, but this
release may contain changes that will require changes to your DAG files.

This section describes the changes that have been made, and what you need to do to update your DAG File,
if you use core operators or any other.

BaseSensorOperator now respects the trigger_rule of downstream tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously, BaseSensorOperator with setting ``soft_fail=True`` skips itself
and skips all its downstream tasks unconditionally, when it fails i.e the trigger_rule of downstream tasks is not
respected.

In the new behavior, the trigger_rule of downstream tasks is respected.
User can preserve/achieve the original behaviour by setting the trigger_rule of each downstream task to ``all_success``.

BaseOperator uses metaclass
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``BaseOperator`` class uses a ``BaseOperatorMeta`` as a metaclass. This meta class is based on
``abc.ABCMeta``. If your custom operator uses different metaclass then you will have to adjust it.

Remove SQL support in BaseHook
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Remove ``get_records`` and ``get_pandas_df`` and ``run`` from BaseHook, which only apply for SQL-like hook,
If want to use them, or your custom hook inherit them, please use ``airflow.hooks.dbapi.DbApiHook``

Assigning task to a DAG using bitwise shift (bit-shift) operators are no longer supported
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously, you could assign a task to a DAG as follows:

.. code-block:: python

   dag = DAG("my_dag")
   dummy = DummyOperator(task_id="dummy")

   dag >> dummy

This is no longer supported. Instead, we recommend using the DAG as context manager:

.. code-block:: python

   with DAG("my_dag") as dag:
       dummy = DummyOperator(task_id="dummy")

Removed deprecated import mechanism
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The deprecated import mechanism has been removed so the import of modules becomes more consistent and explicit.

For example: ``from airflow.operators import BashOperator``
becomes ``from airflow.operators.bash_operator import BashOperator``

Changes to sensor imports
~~~~~~~~~~~~~~~~~~~~~~~~~

Sensors are now accessible via ``airflow.sensors`` and no longer via ``airflow.operators.sensors``.

For example: ``from airflow.operators.sensors import BaseSensorOperator``
becomes ``from airflow.sensors.base import BaseSensorOperator``

Skipped tasks can satisfy wait_for_downstream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously, a task instance with ``wait_for_downstream=True`` will only run if the downstream task of
the previous task instance is successful. Meanwhile, a task instance with ``depends_on_past=True``
will run if the previous task instance is either successful or skipped. These two flags are close siblings
yet they have different behavior. This inconsistency in behavior made the API less intuitive to users.
To maintain consistent behavior, both successful or skipped downstream task can now satisfy the
``wait_for_downstream=True`` flag.

``airflow.utils.helpers.cross_downstream``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.utils.helpers.chain``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``chain`` and ``cross_downstream`` methods are now moved to airflow.models.baseoperator module from
``airflow.utils.helpers`` module.

The ``baseoperator`` module seems to be a better choice to keep
closely coupled methods together. Helpers module is supposed to contain standalone helper methods
that can be imported by all classes.

The ``chain`` method and ``cross_downstream`` method both use BaseOperator. If any other package imports
any classes or functions from helpers module, then it automatically has an
implicit dependency to BaseOperator. That can often lead to cyclic dependencies.

More information in `AIRFLOW-6392 <https://issues.apache.org/jira/browse/AIRFLOW-6392>`_

In Airflow < 2.0 you imported those two methods like this:

.. code-block:: python

   from airflow.utils.helpers import chain
   from airflow.utils.helpers import cross_downstream

In Airflow 2.0 it should be changed to:

.. code-block:: python

   from airflow.models.baseoperator import chain
   from airflow.models.baseoperator import cross_downstream

``airflow.operators.python.BranchPythonOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``BranchPythonOperator`` will now return a value equal to the ``task_id`` of the chosen branch,
where previously it returned None. Since it inherits from BaseOperator it will do an
``xcom_push`` of this value if ``do_xcom_push=True``. This is useful for downstream decision-making.

``airflow.sensors.sql_sensor.SqlSensor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SQLSensor now consistent with python ``bool()`` function and the ``allow_null`` parameter has been removed.

It will resolve after receiving any value  that is casted to ``True`` with python ``bool(value)``. That
changes the previous response receiving ``NULL`` or ``'0'``. Earlier ``'0'`` has been treated as success
criteria. ``NULL`` has been treated depending on value of ``allow_null``\ parameter.  But all the previous
behaviour is still achievable setting param ``success`` to ``lambda x: x is None or str(x) not in ('0', '')``.

``airflow.operators.trigger_dagrun.TriggerDagRunOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The TriggerDagRunOperator now takes a ``conf`` argument to which a dict can be provided as conf for the DagRun.
As a result, the ``python_callable`` argument was removed. PR: https://github.com/apache/airflow/pull/6317.

``airflow.operators.python.PythonOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``provide_context`` argument on the PythonOperator was removed. The signature of the callable passed to the PythonOperator is now inferred and argument values are always automatically provided. There is no need to explicitly provide or not provide the context anymore. For example:

.. code-block:: python

   def myfunc(execution_date):
       print(execution_date)


   python_operator = PythonOperator(task_id="mytask", python_callable=myfunc, dag=dag)

Notice you don't have to set provide_context=True, variables from the task context are now automatically detected and provided.

All context variables can still be provided with a double-asterisk argument:

.. code-block:: python

   def myfunc(**context):
       print(context)  # all variables will be provided to context


   python_operator = PythonOperator(task_id="mytask", python_callable=myfunc)

The task context variable names are reserved names in the callable function, hence a clash with ``op_args`` and ``op_kwargs`` results in an exception:

.. code-block:: python

   def myfunc(dag):
       # raises a ValueError because "dag" is a reserved name
       # valid signature example: myfunc(mydag)
       print("output")


   python_operator = PythonOperator(
       task_id="mytask",
       op_args=[1],
       python_callable=myfunc,
   )

The change is backwards compatible, setting ``provide_context`` will add the ``provide_context`` variable to the ``kwargs`` (but won't do anything).

PR: `#5990 <https://github.com/apache/airflow/pull/5990>`_

``airflow.providers.standard.sensors.filesystem.FileSensor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

FileSensor is now takes a glob pattern, not just a filename. If the filename you are looking for has ``*``\ , ``?``\ , or ``[`` in it then you should replace these with ``[*]``\ , ``[?]``\ , and ``[[]``.

``airflow.operators.subdag_operator.SubDagOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``SubDagOperator`` is changed to use Airflow scheduler instead of backfill
to schedule tasks in the subdag. User no longer need to specify the executor
in ``SubDagOperator``.

``airflow.providers.google.cloud.operators.datastore.CloudDatastoreExportEntitiesOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.datastore.CloudDatastoreImportEntitiesOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.ssh.operators.ssh.SSHOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.microsoft.winrm.operators.winrm.WinRMOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.operators.bash.BashOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.docker.operators.docker.DockerOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.http.operators.http.SimpleHttpOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``do_xcom_push`` flag (a switch to push the result of an operator to xcom or not) was appearing in different incarnations in different operators. It's function has been unified under a common name (\ ``do_xcom_push``\ ) on ``BaseOperator``. This way it is also easy to globally disable pushing results to xcom.

The following operators were affected:


* DatastoreExportOperator (Backwards compatible)
* DatastoreImportOperator (Backwards compatible)
* KubernetesPodOperator (Not backwards compatible)
* SSHOperator (Not backwards compatible)
* WinRMOperator (Not backwards compatible)
* BashOperator (Not backwards compatible)
* DockerOperator (Not backwards compatible)
* SimpleHttpOperator (Not backwards compatible)

See `AIRFLOW-3249 <https://jira.apache.org/jira/browse/AIRFLOW-3249>`_ for details

``airflow.operators.latest_only_operator.LatestOnlyOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In previous versions, the ``LatestOnlyOperator`` forcefully skipped all (direct and indirect) downstream tasks on its own. From this version on the operator will **only skip direct downstream** tasks and the scheduler will handle skipping any further downstream dependencies.

No change is needed if only the default trigger rule ``all_success`` is being used.

If the DAG relies on tasks with other trigger rules (i.e. ``all_done``\ ) being skipped by the ``LatestOnlyOperator``\ , adjustments to the DAG need to be made to accommodate the change in behaviour, i.e. with additional edges from the ``LatestOnlyOperator``.

The goal of this change is to achieve a more consistent and configurable cascading behaviour based on the ``BaseBranchOperator`` (see `AIRFLOW-2923 <https://jira.apache.org/jira/browse/AIRFLOW-2923>`_ and `AIRFLOW-1784 <https://jira.apache.org/jira/browse/AIRFLOW-1784>`_\ ).

Changes to the core Python API
""""""""""""""""""""""""""""""

We strive to ensure that there are no changes that may affect the end user, and your Python files, but this
release may contain changes that will require changes to your plugins, DAG File or other integration.

Only changes unique to this provider are described here. You should still pay attention to the changes that
have been made to the core (including core operators) as they can affect the integration behavior
of this provider.

This section describes the changes that have been made, and what you need to do to update your Python files.

Removed sub-package imports from ``airflow/__init__.py``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The imports ``LoggingMixin``\ , ``conf``\ , and ``AirflowException`` have been removed from ``airflow/__init__.py``.
All implicit references of these objects will no longer be valid. To migrate, all usages of each old path must be
replaced with its corresponding new path.

.. list-table::
   :header-rows: 1

   * - Old Path (Implicit Import)
     - New Path (Explicit Import)
   * - ``airflow.LoggingMixin``
     - ``airflow.utils.log.logging_mixin.LoggingMixin``
   * - ``airflow.conf``
     - ``airflow.configuration.conf``
   * - ``airflow.AirflowException``
     - ``airflow.exceptions.AirflowException``


Variables removed from the task instance context
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following variables were removed from the task instance context:


* end_date
* latest_date
* tables

``airflow.contrib.utils.Weekday``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Formerly the core code was maintained by the original creators - Airbnb. The code that was in the contrib
package was supported by the community. The project was passed to the Apache community and currently the
entire code is maintained by the community, so now the division has no justification, and it is only due
to historical reasons.

To clean up, ``Weekday`` enum has been moved from ``airflow.contrib.utils`` into ``airflow.utils`` module.

``airflow.models.connection.Connection``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The connection module has new deprecated methods:


* ``Connection.parse_from_uri``
* ``Connection.log_info``
* ``Connection.debug_info``

and one deprecated function:


* ``parse_netloc_to_hostname``

Previously, users could create a connection object in two ways

.. code-block::

   conn_1 = Connection(conn_id="conn_a", uri="mysql://AAA/")
   # or
   conn_2 = Connection(conn_id="conn_a")
   conn_2.parse_uri(uri="mysql://AAA/")

Now the second way is not supported.

``Connection.log_info`` and ``Connection.debug_info`` method have been deprecated. Read each Connection field individually or use the
default representation (\ ``__repr__``\ ).

The old method is still works but can be abandoned at any time. The changes are intended to delete method
that are rarely used.

``airflow.models.dag.DAG.create_dagrun``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DAG.create_dagrun accepts run_type and does not require run_id
This change is caused by adding ``run_type`` column to ``DagRun``.

Previous signature:

.. code-block:: python

   def create_dagrun(
       self,
       run_id,
       state,
       execution_date=None,
       start_date=None,
       external_trigger=False,
       conf=None,
       session=None,
   ): ...

current:

.. code-block:: python

   def create_dagrun(
       self,
       state,
       execution_date=None,
       run_id=None,
       start_date=None,
       external_trigger=False,
       conf=None,
       run_type=None,
       session=None,
   ): ...

If user provides ``run_id`` then the ``run_type`` will be derived from it by checking prefix, allowed types
: ``manual``\ , ``scheduled``\ , ``backfill`` (defined by ``airflow.utils.types.DagRunType``\ ).

If user provides ``run_type`` and ``execution_date`` then ``run_id`` is constructed as
``{run_type}__{execution_data.isoformat()}``.

Airflow should construct dagruns using ``run_type`` and ``execution_date``\ , creation using
``run_id`` is preserved for user actions.

``airflow.models.dagrun.DagRun``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use DagRunType.SCHEDULED.value instead of DagRun.ID_PREFIX

All the run_id prefixes for different kind of DagRuns have been grouped into a single
enum in ``airflow.utils.types.DagRunType``.

Previously, there were defined in various places, example as ``ID_PREFIX`` class variables for
``DagRun``\ , ``BackfillJob`` and in ``_trigger_dag`` function.

Was:

.. code-block:: pycon

   >> from airflow.models.dagrun import DagRun
   >> DagRun.ID_PREFIX
   scheduled__

Replaced by:

.. code-block:: pycon

   >> from airflow.utils.types import DagRunType
   >> DagRunType.SCHEDULED.value
   scheduled

``airflow.utils.file.TemporaryDirectory``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We remove ``airflow.utils.file.TemporaryDirectory``
Since Airflow dropped support for Python < 3.5 there's no need to have this custom
implementation of ``TemporaryDirectory`` because the same functionality is provided by
``tempfile.TemporaryDirectory``.

Now users instead of ``import from airflow.utils.files import TemporaryDirectory`` should
do ``from tempfile import TemporaryDirectory``. Both context managers provide the same
interface, thus no additional changes should be required.

``airflow.AirflowMacroPlugin``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We removed ``airflow.AirflowMacroPlugin`` class. The class was there in airflow package but it has not been used (apparently since 2015).
It has been removed.

``airflow.settings.CONTEXT_MANAGER_DAG``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CONTEXT_MANAGER_DAG was removed from settings. Its role has been taken by ``DagContext`` in
'airflow.models.dag'. One of the reasons was that settings should be rather static than store
dynamic context from the DAG, but the main one is that moving the context out of settings allowed to
untangle cyclic imports between DAG, BaseOperator, SerializedDAG, SerializedBaseOperator which was
part of AIRFLOW-6010.

``airflow.utils.log.logging_mixin.redirect_stderr``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.utils.log.logging_mixin.redirect_stdout``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Function ``redirect_stderr`` and ``redirect_stdout`` from ``airflow.utils.log.logging_mixin`` module has
been deleted because it can be easily replaced by the standard library.
The functions of the standard library are more flexible and can be used in larger cases.

The code below

.. code-block:: python

   import logging

   from airflow.utils.log.logging_mixin import redirect_stderr, redirect_stdout

   logger = logging.getLogger("custom-logger")
   with redirect_stdout(logger, logging.INFO), redirect_stderr(logger, logging.WARN):
       print("I love Airflow")

can be replaced by the following code:

.. code-block:: python

   from contextlib import redirect_stdout, redirect_stderr
   import logging

   from airflow.utils.log.logging_mixin import StreamLogWriter

   logger = logging.getLogger("custom-logger")

   with (
       redirect_stdout(StreamLogWriter(logger, logging.INFO)),
       redirect_stderr(StreamLogWriter(logger, logging.WARN)),
   ):
       print("I Love Airflow")

``airflow.models.baseoperator.BaseOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now, additional arguments passed to BaseOperator cause an exception. Previous versions of Airflow took additional arguments and displayed a message on the console. When the
message was not noticed by users, it caused very difficult to detect errors.

In order to restore the previous behavior, you must set an ``True`` in  the ``allow_illegal_arguments``
option of section ``[operators]`` in the ``airflow.cfg`` file. In the future it is possible to completely
delete this option.

``airflow.models.dagbag.DagBag``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Passing ``store_serialized_dags`` argument to DagBag.\ **init** and accessing ``DagBag.store_serialized_dags`` property
are deprecated and will be removed in future versions.

**Previous signature**\ :

.. code-block:: python

   def __init__(
       dag_folder=None,
       include_examples=conf.getboolean("core", "LOAD_EXAMPLES"),
       safe_mode=conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE"),
       store_serialized_dags=False,
   ): ...

**current**\ :

.. code-block:: python

   def __init__(
       dag_folder=None,
       include_examples=conf.getboolean("core", "LOAD_EXAMPLES"),
       safe_mode=conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE"),
       read_dags_from_db=False,
   ): ...

If you were using positional arguments, it requires no change but if you were using keyword
arguments, please change ``store_serialized_dags`` to ``read_dags_from_db``.

Similarly, if you were using ``DagBag().store_serialized_dags`` property, change it to
``DagBag().read_dags_from_db``.

Changes in ``google`` provider package
""""""""""""""""""""""""""""""""""""""""""

We strive to ensure that there are no changes that may affect the end user and your Python files, but this
release may contain changes that will require changes to your configuration, DAG Files or other integration
e.g. custom operators.

Only changes unique to this provider are described here. You should still pay attention to the changes that
have been made to the core (including core operators) as they can affect the integration behavior
of this provider.

This section describes the changes that have been made, and what you need to do to update your if
you use operators or hooks which integrate with Google services (including Google Cloud - GCP).

Direct impersonation added to operators communicating with Google services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Directly impersonating a service account <https://cloud.google.com/iam/docs/understanding-service-accounts#directly_impersonating_a_service_account>`_
has been made possible for operators communicating with Google services via new argument called ``impersonation_chain``
(\ ``google_impersonation_chain`` in case of operators that also communicate with services of other cloud providers).
As a result, GCSToS3Operator no longer derivatives from GCSListObjectsOperator.

Normalize gcp_conn_id for Google Cloud
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously not all hooks and operators related to Google Cloud use
``gcp_conn_id`` as parameter for GCP connection. There is currently one parameter
which apply to most services. Parameters like ``datastore_conn_id``\ , ``bigquery_conn_id``\ ,
``google_cloud_storage_conn_id`` and similar have been deprecated. Operators that require two connections are not changed.

Following components were affected by normalization:


* ``airflow.providers.google.cloud.hooks.datastore.DatastoreHook``
* ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook``
* ``airflow.providers.google.cloud.hooks.gcs.GoogleCloudStorageHook``
* ``airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator``
* ``airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator``
* ``airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator``
* ``airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator``
* ``airflow.providers.google.cloud.operators.bigquery.BigQueryOperator``
* ``airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteDatasetOperator``
* ``airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyDatasetOperator``
* ``airflow.providers.google.cloud.operators.bigquery.BigQueryTableDeleteOperator``
* ``airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageCreateBucketOperator``
* ``airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageListOperator``
* ``airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageDownloadOperator``
* ``airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageDeleteOperator``
* ``airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageBucketCreateAclEntryOperator``
* ``airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageObjectCreateAclEntryOperator``
* ``airflow.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator``
* ``airflow.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator``
* ``airflow.operators.gcs_to_s3.GoogleCloudStorageToS3Operator``
* ``airflow.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator``
* ``airflow.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator``
* ``airflow.operators.local_to_gcs.FileToGoogleCloudStorageOperator``
* ``airflow.operators.cassandra_to_gcs.CassandraToGoogleCloudStorageOperator``
* ``airflow.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator``

Changes to import paths and names of GCP operators and hooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

According to `AIP-21 <https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths>`_
operators related to Google Cloud has been moved from contrib to core.
The following table shows changes in import paths.

.. list-table::
   :header-rows: 1

   * - Old path
     - New path
   * - ``airflow.contrib.hooks.bigquery_hook.BigQueryHook``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook``
   * - ``airflow.contrib.hooks.datastore_hook.DatastoreHook``
     - ``airflow.providers.google.cloud.hooks.datastore.DatastoreHook``
   * - ``airflow.contrib.hooks.gcp_bigtable_hook.BigtableHook``
     - ``airflow.providers.google.cloud.hooks.bigtable.BigtableHook``
   * - ``airflow.contrib.hooks.gcp_cloud_build_hook.CloudBuildHook``
     - ``airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook``
   * - ``airflow.contrib.hooks.gcp_container_hook.GKEClusterHook``
     - ``airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook``
   * - ``airflow.contrib.hooks.gcp_compute_hook.GceHook``
     - ``airflow.providers.google.cloud.hooks.compute.ComputeEngineHook``
   * - ``airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook``
     - ``airflow.providers.google.cloud.hooks.dataflow.DataflowHook``
   * - ``airflow.contrib.hooks.gcp_dataproc_hook.DataProcHook``
     - ``airflow.providers.google.cloud.hooks.dataproc.DataprocHook``
   * - ``airflow.contrib.hooks.gcp_dlp_hook.CloudDLPHook``
     - ``airflow.providers.google.cloud.hooks.dlp.CloudDLPHook``
   * - ``airflow.contrib.hooks.gcp_function_hook.GcfHook``
     - ``airflow.providers.google.cloud.hooks.functions.CloudFunctionsHook``
   * - ``airflow.contrib.hooks.gcp_kms_hook.GoogleCloudKMSHook``
     - ``airflow.providers.google.cloud.hooks.kms.CloudKMSHook``
   * - ``airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook``
     - ``airflow.providers.google.cloud.hooks.mlengine.MLEngineHook``
   * - ``airflow.contrib.hooks.gcp_natural_language_hook.CloudNaturalLanguageHook``
     - ``airflow.providers.google.cloud.hooks.natural_language.CloudNaturalLanguageHook``
   * - ``airflow.contrib.hooks.gcp_pubsub_hook.PubSubHook``
     - ``airflow.providers.google.cloud.hooks.pubsub.PubSubHook``
   * - ``airflow.contrib.hooks.gcp_speech_to_text_hook.GCPSpeechToTextHook``
     - ``airflow.providers.google.cloud.hooks.speech_to_text.CloudSpeechToTextHook``
   * - ``airflow.contrib.hooks.gcp_spanner_hook.CloudSpannerHook``
     - ``airflow.providers.google.cloud.hooks.spanner.SpannerHook``
   * - ``airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook``
     - ``airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook``
   * - ``airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook``
     - ``airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook``
   * - ``airflow.contrib.hooks.gcp_tasks_hook.CloudTasksHook``
     - ``airflow.providers.google.cloud.hooks.tasks.CloudTasksHook``
   * - ``airflow.contrib.hooks.gcp_text_to_speech_hook.GCPTextToSpeechHook``
     - ``airflow.providers.google.cloud.hooks.text_to_speech.CloudTextToSpeechHook``
   * - ``airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook``
     - ``airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.CloudDataTransferServiceHook``
   * - ``airflow.contrib.hooks.gcp_translate_hook.CloudTranslateHook``
     - ``airflow.providers.google.cloud.hooks.translate.CloudTranslateHook``
   * - ``airflow.contrib.hooks.gcp_video_intelligence_hook.CloudVideoIntelligenceHook``
     - ``airflow.providers.google.cloud.hooks.video_intelligence.CloudVideoIntelligenceHook``
   * - ``airflow.contrib.hooks.gcp_vision_hook.CloudVisionHook``
     - ``airflow.providers.google.cloud.hooks.vision.CloudVisionHook``
   * - ``airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook``
     - ``airflow.providers.google.cloud.hooks.gcs.GCSHook``
   * - ``airflow.contrib.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator``
     - ``airflow.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator``
   * - ``airflow.contrib.operators.bigquery_check_operator.BigQueryCheckOperator``
     - ``airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator``
   * - ``airflow.contrib.operators.bigquery_check_operator.BigQueryIntervalCheckOperator``
     - ``airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator``
   * - ``airflow.contrib.operators.bigquery_check_operator.BigQueryValueCheckOperator``
     - ``airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator``
   * - ``airflow.contrib.operators.bigquery_get_data.BigQueryGetDataOperator``
     - ``airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator``
   * - ``airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyDatasetOperator``
     - ``airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyDatasetOperator``
   * - ``airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyTableOperator``
     - ``airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyTableOperator``
   * - ``airflow.contrib.operators.bigquery_operator.BigQueryCreateExternalTableOperator``
     - ``airflow.providers.google.cloud.operators.bigquery.BigQueryCreateExternalTableOperator``
   * - ``airflow.contrib.operators.bigquery_operator.BigQueryDeleteDatasetOperator``
     - ``airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteDatasetOperator``
   * - ``airflow.contrib.operators.bigquery_operator.BigQueryOperator``
     - ``airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator``
   * - ``airflow.contrib.operators.bigquery_table_delete_operator.BigQueryTableDeleteOperator``
     - ``airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteTableOperator``
   * - ``airflow.contrib.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator``
     - ``airflow.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator``
   * - ``airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator``
     - ``airflow.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator``
   * - ``airflow.contrib.operators.bigquery_to_mysql_operator.BigQueryToMySqlOperator``
     - ``airflow.operators.bigquery_to_mysql.BigQueryToMySqlOperator``
   * - ``airflow.contrib.operators.dataflow_operator.DataFlowJavaOperator``
     - ``airflow.providers.google.cloud.operators.dataflow.DataFlowJavaOperator``
   * - ``airflow.contrib.operators.dataflow_operator.DataFlowPythonOperator``
     - ``airflow.providers.google.cloud.operators.dataflow.DataFlowPythonOperator``
   * - ``airflow.contrib.operators.dataflow_operator.DataflowTemplateOperator``
     - ``airflow.providers.google.cloud.operators.dataflow.DataflowTemplateOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataProcHadoopOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataProcHiveOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataProcJobBaseOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataProcPigOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataProcSparkOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataProcSparkSqlOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataprocClusterCreateOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataprocClusterDeleteOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataprocClusterScaleOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocScaleClusterOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataprocOperationBaseOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocOperationBaseOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateInlineOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateInlineWorkflowTemplateOperator``
   * - ``airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateOperator``
     - ``airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateWorkflowTemplateOperator``
   * - ``airflow.contrib.operators.datastore_export_operator.DatastoreExportOperator``
     - ``airflow.providers.google.cloud.operators.datastore.DatastoreExportOperator``
   * - ``airflow.contrib.operators.datastore_import_operator.DatastoreImportOperator``
     - ``airflow.providers.google.cloud.operators.datastore.DatastoreImportOperator``
   * - ``airflow.contrib.operators.file_to_gcs.FileToGoogleCloudStorageOperator``
     - ``airflow.providers.google.cloud.transfers.local_to_gcs.FileToGoogleCloudStorageOperator``
   * - ``airflow.contrib.operators.gcp_bigtable_operator.BigtableClusterUpdateOperator``
     - ``airflow.providers.google.cloud.operators.bigtable.BigtableUpdateClusterOperator``
   * - ``airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceCreateOperator``
     - ``airflow.providers.google.cloud.operators.bigtable.BigtableCreateInstanceOperator``
   * - ``airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceDeleteOperator``
     - ``airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator``
   * - ``airflow.contrib.operators.gcp_bigtable_operator.BigtableTableCreateOperator``
     - ``airflow.providers.google.cloud.operators.bigtable.BigtableCreateTableOperator``
   * - ``airflow.contrib.operators.gcp_bigtable_operator.BigtableTableDeleteOperator``
     - ``airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator``
   * - ``airflow.contrib.operators.gcp_bigtable_operator.BigtableTableWaitForReplicationSensor``
     - ``airflow.providers.google.cloud.sensors.bigtable.BigtableTableReplicationCompletedSensor``
   * - ``airflow.contrib.operators.gcp_cloud_build_operator.CloudBuildCreateBuildOperator``
     - ``airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateBuildOperator``
   * - ``airflow.contrib.operators.gcp_compute_operator.GceBaseOperator``
     - ``airflow.providers.google.cloud.operators.compute.GceBaseOperator``
   * - ``airflow.contrib.operators.gcp_compute_operator.GceInstanceGroupManagerUpdateTemplateOperator``
     - ``airflow.providers.google.cloud.operators.compute.GceInstanceGroupManagerUpdateTemplateOperator``
   * - ``airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator``
     - ``airflow.providers.google.cloud.operators.compute.GceInstanceStartOperator``
   * - ``airflow.contrib.operators.gcp_compute_operator.GceInstanceStopOperator``
     - ``airflow.providers.google.cloud.operators.compute.GceInstanceStopOperator``
   * - ``airflow.contrib.operators.gcp_compute_operator.GceInstanceTemplateCopyOperator``
     - ``airflow.providers.google.cloud.operators.compute.GceInstanceTemplateCopyOperator``
   * - ``airflow.contrib.operators.gcp_compute_operator.GceSetMachineTypeOperator``
     - ``airflow.providers.google.cloud.operators.compute.GceSetMachineTypeOperator``
   * - ``airflow.contrib.operators.gcp_container_operator.GKEClusterCreateOperator``
     - ``airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateClusterOperator``
   * - ``airflow.contrib.operators.gcp_container_operator.GKEClusterDeleteOperator``
     - ``airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator``
   * - ``airflow.contrib.operators.gcp_container_operator.GKEPodOperator``
     - ``airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPCancelDLPJobOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPCancelDLPJobOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateDLPJobOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDLPJobOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateDeidentifyTemplateOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDeidentifyTemplateOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateInspectTemplateOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPCreateInspectTemplateOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateJobTriggerOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPCreateJobTriggerOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateStoredInfoTypeOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPCreateStoredInfoTypeOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeidentifyContentOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPDeidentifyContentOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteDeidentifyTemplateOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDeidentifyTemplateOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteDlpJobOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteInspectTemplateOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteInspectTemplateOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteJobTriggerOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteJobTriggerOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteStoredInfoTypeOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteStoredInfoTypeOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDeidentifyTemplateOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPGetDeidentifyTemplateOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDlpJobOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetInspectTemplateOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPGetInspectTemplateOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetJobTripperOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPGetJobTriggerOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetStoredInfoTypeOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPGetStoredInfoTypeOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPInspectContentOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPInspectContentOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPListDeidentifyTemplatesOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPListDeidentifyTemplatesOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPListDlpJobsOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPListDLPJobsOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPListInfoTypesOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPListInfoTypesOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPListInspectTemplatesOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPListInspectTemplatesOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPListJobTriggersOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPListJobTriggersOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPListStoredInfoTypesOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPListStoredInfoTypesOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPRedactImageOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPRedactImageOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPReidentifyContentOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPReidentifyContentOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateDeidentifyTemplateOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateDeidentifyTemplateOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateInspectTemplateOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateInspectTemplateOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateJobTriggerOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateJobTriggerOperator``
   * - ``airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateStoredInfoTypeOperator``
     - ``airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateStoredInfoTypeOperator``
   * - ``airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator``
     - ``airflow.providers.google.cloud.operators.functions.GcfFunctionDeleteOperator``
   * - ``airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator``
     - ``airflow.providers.google.cloud.operators.functions.GcfFunctionDeployOperator``
   * - ``airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageAnalyzeEntitiesOperator``
     - ``airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitiesOperator``
   * - ``airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageAnalyzeEntitySentimentOperator``
     - ``airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitySentimentOperator``
   * - ``airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageAnalyzeSentimentOperator``
     - ``airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeSentimentOperator``
   * - ``airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageClassifyTextOperator``
     - ``airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageClassifyTextOperator``
   * - ``airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeleteOperator``
     - ``airflow.providers.google.cloud.operators.spanner.SpannerDeleteDatabaseInstanceOperator``
   * - ``airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeployOperator``
     - ``airflow.providers.google.cloud.operators.spanner.SpannerDeployDatabaseInstanceOperator``
   * - ``airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseQueryOperator``
     - ``airflow.providers.google.cloud.operators.spanner.SpannerQueryDatabaseInstanceOperator``
   * - ``airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseUpdateOperator``
     - ``airflow.providers.google.cloud.operators.spanner.SpannerUpdateDatabaseInstanceOperator``
   * - ``airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeleteOperator``
     - ``airflow.providers.google.cloud.operators.spanner.SpannerDeleteInstanceOperator``
   * - ``airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeployOperator``
     - ``airflow.providers.google.cloud.operators.spanner.SpannerDeployInstanceOperator``
   * - ``airflow.contrib.operators.gcp_speech_to_text_operator.GcpSpeechToTextRecognizeSpeechOperator``
     - ``airflow.providers.google.cloud.operators.speech_to_text.CloudSpeechToTextRecognizeSpeechOperator``
   * - ``airflow.contrib.operators.gcp_text_to_speech_operator.GcpTextToSpeechSynthesizeOperator``
     - ``airflow.providers.google.cloud.operators.text_to_speech.CloudTextToSpeechSynthesizeOperator``
   * - ``airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobCreateOperator``
     - ``airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCreateJobOperator``
   * - ``airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobDeleteOperator``
     - ``airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceDeleteJobOperator``
   * - ``airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobUpdateOperator``
     - ``airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceUpdateJobOperator``
   * - ``airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationCancelOperator``
     - ``airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCancelOperationOperator``
   * - ``airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationGetOperator``
     - ``airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceGetOperationOperator``
   * - ``airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationPauseOperator``
     - ``airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServicePauseOperationOperator``
   * - ``airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationResumeOperator``
     - ``airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceResumeOperationOperator``
   * - ``airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationsListOperator``
     - ``airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceListOperationsOperator``
   * - ``airflow.contrib.operators.gcp_transfer_operator.GoogleCloudStorageToGoogleCloudStorageTransferOperator``
     - ``airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceGCSToGCSOperator``
   * - ``airflow.contrib.operators.gcp_translate_operator.CloudTranslateTextOperator``
     - ``airflow.providers.google.cloud.operators.translate.CloudTranslateTextOperator``
   * - ``airflow.contrib.operators.gcp_translate_speech_operator.GcpTranslateSpeechOperator``
     - ``airflow.providers.google.cloud.operators.translate_speech.GcpTranslateSpeechOperator``
   * - ``airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoExplicitContentOperator``
     - ``airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoExplicitContentOperator``
   * - ``airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoLabelsOperator``
     - ``airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoLabelsOperator``
   * - ``airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoShotsOperator``
     - ``airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoShotsOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionAddProductToProductSetOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionAddProductToProductSetOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionAnnotateImageOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectDocumentTextOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageLabelsOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageLabelsOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageSafeSearchOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageSafeSearchOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectTextOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionDetectTextOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionProductCreateOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionProductDeleteOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionProductGetOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetCreateOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetDeleteOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetGetOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetUpdateOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionProductUpdateOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionReferenceImageCreateOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator``
   * - ``airflow.contrib.operators.gcp_vision_operator.CloudVisionRemoveProductFromProductSetOperator``
     - ``airflow.providers.google.cloud.operators.vision.CloudVisionRemoveProductFromProductSetOperator``
   * - ``airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageBucketCreateAclEntryOperator``
     - ``airflow.providers.google.cloud.operators.gcs.GCSBucketCreateAclEntryOperator``
   * - ``airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageObjectCreateAclEntryOperator``
     - ``airflow.providers.google.cloud.operators.gcs.GCSObjectCreateAclEntryOperator``
   * - ``airflow.contrib.operators.gcs_delete_operator.GoogleCloudStorageDeleteOperator``
     - ``airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator``
   * - ``airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator``
     - ``airflow.providers.google.cloud.operators.gcs.GCSToLocalFilesystemOperator``
   * - ``airflow.contrib.operators.gcs_list_operator.GoogleCloudStorageListOperator``
     - ``airflow.providers.google.cloud.operators.gcs.GCSListObjectsOperator``
   * - ``airflow.contrib.operators.gcs_operator.GoogleCloudStorageCreateBucketOperator``
     - ``airflow.providers.google.cloud.operators.gcs.GCSCreateBucketOperator``
   * - ``airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator``
     - ``airflow.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator``
   * - ``airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator``
     - ``airflow.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator``
   * - ``airflow.contrib.operators.gcs_to_s3.GoogleCloudStorageToS3Operator``
     - ``airflow.operators.gcs_to_s3.GCSToS3Operator``
   * - ``airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator``
     - ``airflow.providers.google.cloud.operators.mlengine.MLEngineStartBatchPredictionJobOperator``
   * - ``airflow.contrib.operators.mlengine_operator.MLEngineModelOperator``
     - ``airflow.providers.google.cloud.operators.mlengine.MLEngineManageModelOperator``
   * - ``airflow.contrib.operators.mlengine_operator.MLEngineTrainingOperator``
     - ``airflow.providers.google.cloud.operators.mlengine.MLEngineStartTrainingJobOperator``
   * - ``airflow.contrib.operators.mlengine_operator.MLEngineVersionOperator``
     - ``airflow.providers.google.cloud.operators.mlengine.MLEngineManageVersionOperator``
   * - ``airflow.contrib.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator``
     - ``airflow.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator``
   * - ``airflow.contrib.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator``
     - ``airflow.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator``
   * - ``airflow.contrib.operators.postgres_to_gcs_operator.PostgresToGoogleCloudStorageOperator``
     - ``airflow.operators.postgres_to_gcs.PostgresToGoogleCloudStorageOperator``
   * - ``airflow.contrib.operators.pubsub_operator.PubSubPublishOperator``
     - ``airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator``
   * - ``airflow.contrib.operators.pubsub_operator.PubSubSubscriptionCreateOperator``
     - ``airflow.providers.google.cloud.operators.pubsub.PubSubCreateSubscriptionOperator``
   * - ``airflow.contrib.operators.pubsub_operator.PubSubSubscriptionDeleteOperator``
     - ``airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator``
   * - ``airflow.contrib.operators.pubsub_operator.PubSubTopicCreateOperator``
     - ``airflow.providers.google.cloud.operators.pubsub.PubSubCreateTopicOperator``
   * - ``airflow.contrib.operators.pubsub_operator.PubSubTopicDeleteOperator``
     - ``airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator``
   * - ``airflow.contrib.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator``
     - ``airflow.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator``
   * - ``airflow.contrib.sensors.bigquery_sensor.BigQueryTableSensor``
     - ``airflow.providers.google.cloud.sensors.bigquery.BigQueryTableExistenceSensor``
   * - ``airflow.contrib.sensors.gcp_transfer_sensor.GCPTransferServiceWaitForJobStatusSensor``
     - ``airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.DataTransferServiceJobStatusSensor``
   * - ``airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectSensor``
     - ``airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor``
   * - ``airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectUpdatedSensor``
     - ``airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor``
   * - ``airflow.contrib.sensors.gcs_sensor.GoogleCloudStoragePrefixSensor``
     - ``airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor``
   * - ``airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageUploadSessionCompleteSensor``
     - ``airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor``
   * - ``airflow.contrib.sensors.pubsub_sensor.PubSubPullSensor``
     - ``airflow.providers.google.cloud.sensors.pubsub.PubSubPullSensor``


Unify default conn_id for Google Cloud
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously not all hooks and operators related to Google Cloud use
``google_cloud_default`` as a default conn_id. There is currently one default
variant. Values like ``google_cloud_storage_default``\ , ``bigquery_default``\ ,
``google_cloud_datastore_default`` have been deprecated. The configuration of
existing relevant connections in the database have been preserved. To use those
deprecated GCP conn_id, you need to explicitly pass their conn_id into
operators/hooks. Otherwise, ``google_cloud_default`` will be used as GCP's conn_id
by default.

``airflow.providers.google.cloud.hooks.dataflow.DataflowHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To use project_id argument consistently across GCP hooks and operators, we did the following changes:


* Changed order of arguments in DataflowHook.start_python_dataflow. Uses
    with positional arguments may break.
* Changed order of arguments in DataflowHook.is_job_dataflow_running. Uses
    with positional arguments may break.
* Changed order of arguments in DataflowHook.cancel_job. Uses
    with positional arguments may break.
* Added optional project_id argument to DataflowCreateJavaJobOperator
    constructor.
* Added optional project_id argument to DataflowTemplatedJobStartOperator
    constructor.
* Added optional project_id argument to DataflowCreatePythonJobOperator
    constructor.

``airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To provide more precise control in handling of changes to objects in
underlying GCS Bucket the constructor of this sensor now has changed.


* Old Behavior: This constructor used to optionally take ``previous_num_objects: int``.
* New replacement constructor kwarg: ``previous_objects: Optional[Set[str]]``.

Most users would not specify this argument because the bucket begins empty
and the user wants to treat any files as new.

Example of Updating usage of this sensor:
Users who used to call:

``GCSUploadSessionCompleteSensor(bucket='my_bucket', prefix='my_prefix', previous_num_objects=1)``

Will now call:

``GCSUploadSessionCompleteSensor(bucket='my_bucket', prefix='my_prefix', previous_num_objects={'.keep'})``

Where '.keep' is a single file at your prefix that the sensor should not consider new.

``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To simplify BigQuery operators (no need of ``Cursor``\ ) and standardize usage of hooks within all GCP integration methods from ``BiqQueryBaseCursor``
were moved to ``BigQueryHook``. Using them by from ``Cursor`` object is still possible due to preserved backward compatibility but they will raise ``DeprecationWarning``.
The following methods were moved:

.. list-table::
   :header-rows: 1

   * - Old path
     - New path
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.cancel_query``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.cancel_query``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_empty_dataset``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_dataset``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_empty_table``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_table``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_external_table``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_external_table``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.delete_dataset``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.delete_dataset``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset_tables``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset_tables_list``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables_list``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_datasets_list``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_datasets_list``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_schema``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_schema``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_tabledata``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_tabledata``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.insert_all``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_all``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.patch_dataset``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_dataset``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.patch_table``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_table``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.poll_job_complete``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.poll_job_complete``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_copy``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_copy``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_extract``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_extract``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_grant_dataset_view_access``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_grant_dataset_view_access``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_load``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_load``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_query``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_query``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_table_delete``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_delete``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_table_upsert``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_upsert``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_with_configuration``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration``
   * - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.update_dataset``
     - ``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_dataset``


``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since BigQuery is the part of the GCP it was possible to simplify the code by handling the exceptions
by usage of the ``airflow.providers.google.common.hooks.base.GoogleBaseHook.catch_http_exception`` decorator however it changes
exceptions raised by the following methods:


* ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_table_delete`` raises ``AirflowException`` instead of ``Exception``.
* ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_empty_dataset`` raises ``AirflowException`` instead of ``ValueError``.
* ``airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset`` raises ``AirflowException`` instead of ``ValueError``.

``airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyTableOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyDatasetOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Idempotency was added to ``BigQueryCreateEmptyTableOperator`` and ``BigQueryCreateEmptyDatasetOperator``.
But to achieve that try / except clause was removed from ``create_empty_dataset`` and ``create_empty_table``
methods of ``BigQueryHook``.

``airflow.providers.google.cloud.hooks.dataflow.DataflowHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.hooks.mlengine.MLEngineHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.hooks.pubsub.PubSubHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The change in GCP operators implies that GCP Hooks for those operators require now keyword parameters rather
than positional ones in all methods where ``project_id`` is used. The methods throw an explanatory exception
in case they are called using positional parameters.

Other GCP hooks are unaffected.

``airflow.providers.google.cloud.hooks.pubsub.PubSubHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.pubsub.PubSubTopicCreateOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.pubsub.PubSubSubscriptionCreateOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.pubsub.PubSubTopicDeleteOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.pubsub.PubSubSubscriptionDeleteOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.pubsub.PubSubPublishOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.sensors.pubsub.PubSubPullSensor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the ``PubSubPublishOperator`` and ``PubSubHook.publish`` method the data field in a message should be bytestring (utf-8 encoded) rather than base64 encoded string.

Due to the normalization of the parameters within GCP operators and hooks a parameters like ``project`` or ``topic_project``
are deprecated and will be substituted by parameter ``project_id``.
In ``PubSubHook.create_subscription`` hook method in the parameter ``subscription_project`` is replaced by ``subscription_project_id``.
Template fields are updated accordingly and old ones may not work.

It is required now to pass key-word only arguments to ``PubSub`` hook.

These changes are not backward compatible.

``airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The gcp_conn_id parameter in GKEPodOperator is required. In previous versions, it was possible to pass
the ``None`` value to the ``gcp_conn_id`` in the GKEStartPodOperator
operator, which resulted in credentials being determined according to the
`Application Default Credentials <https://cloud.google.com/docs/authentication/production>`_ strategy.

Now this parameter requires a value. To restore the previous behavior, configure the connection without
specifying the service account.

Detailed information about connection management is available:
`Google Cloud Connection <https://airflow.apache.org/howto/connection/gcp.html>`_.

``airflow.providers.google.cloud.hooks.gcs.GCSHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


*
  The following parameters have been replaced in all the methods in GCSHook:


  * ``bucket`` is changed to ``bucket_name``
  * ``object`` is changed to ``object_name``

*
  The ``maxResults`` parameter in ``GoogleCloudStorageHook.list`` has been renamed to ``max_results`` for consistency.

``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 'properties' and 'jars' properties for the Dataproc related operators (\ ``DataprocXXXOperator``\ ) have been renamed from
``dataproc_xxxx_properties`` and ``dataproc_xxx_jars``  to ``dataproc_properties``
and ``dataproc_jars``\ respectively.
Arguments for dataproc_properties dataproc_jars

``airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCreateJobOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To obtain pylint compatibility the ``filter`` argument in ``CloudDataTransferServiceCreateJobOperator``
has been renamed to ``request_filter``.

``airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.CloudDataTransferServiceHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 To obtain pylint compatibility the ``filter`` argument in ``CloudDataTransferServiceHook.list_transfer_job`` and
 ``CloudDataTransferServiceHook.list_transfer_operations`` has been renamed to ``request_filter``.

``airflow.providers.google.cloud.hooks.bigquery.BigQueryHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In general all hook methods are decorated with ``@GoogleBaseHook.fallback_to_default_project_id`` thus
parameters to hook can only be passed via keyword arguments.


* ``create_empty_table`` method accepts now ``table_resource`` parameter. If provided all
  other parameters are ignored.
* ``create_empty_dataset`` will now use values from ``dataset_reference`` instead of raising error
  if parameters were passed in ``dataset_reference`` and as arguments to method. Additionally validation
  of ``dataset_reference`` is done using ``Dataset.from_api_repr``. Exception and log messages has been
  changed.
* ``update_dataset`` requires now new ``fields`` argument (breaking change)
* ``delete_dataset`` has new signature (dataset_id, project_id, ...)
  previous one was (project_id, dataset_id, ...) (breaking change)
* ``get_tabledata`` returns list of rows instead of API response in dict format. This method is deprecated in
  favor of ``list_rows``. (breaking change)

``airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateBuildOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``api_version`` has been removed and will not be used since we migrate ``CloudBuildHook`` from using
 Discovery API to native google-cloud-build python library.

The ``body`` parameter in ``CloudBuildCreateBuildOperator`` has been deprecated.
 Instead, you should pass body using the ``build`` parameter.

``airflow.providers.google.cloud.hooks.dataflow.DataflowHook.start_python_dataflow``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.hooks.dataflow.DataflowHook.start_python_dataflow``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Change python3 as Dataflow Hooks/Operators default interpreter

Now the ``py_interpreter`` argument for DataFlow Hooks/Operators has been changed from python2 to python3.

``airflow.providers.google.common.hooks.base_google.GoogleBaseHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To simplify the code, the decorator provide_gcp_credential_file has been moved from the inner-class.

Instead of ``@GoogleBaseHook._Decorators.provide_gcp_credential_file``\ ,
you should write ``@GoogleBaseHook.provide_gcp_credential_file``

``airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is highly recommended to have 1TB+ disk size for Dataproc to have sufficient throughput:
https://cloud.google.com/compute/docs/disks/performance

Hence, the default value for ``master_disk_size`` in ``DataprocCreateClusterOperator`` has been changed from 500GB to 1TB.

Generating Cluster Config
"""""""""""""""""""""""""

If you are upgrading from Airflow 1.10.x and are not using **CLUSTER_CONFIG**\ ,
You can easily generate config using **make()** of ``airflow.providers.google.cloud.operators.dataproc.ClusterGenerator``

This has been proved specially useful if you are using **metadata** argument from older API, refer `AIRFLOW-16911 <https://github.com/apache/airflow/issues/16911>`_ for details.

eg. your cluster creation may look like this in **v1.10.x**

.. code-block:: python

   path = f"gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh"

   create_cluster = DataprocClusterCreateOperator(
       task_id="create_dataproc_cluster",
       cluster_name="test",
       project_id="test",
       zone="us-central1-a",
       region="us-central1",
       master_machine_type="n1-standard-4",
       worker_machine_type="n1-standard-4",
       num_workers=2,
       storage_bucket="test_bucket",
       init_actions_uris=[path],
       metadata={"PIP_PACKAGES": "pyyaml requests pandas openpyxl"},
   )

After upgrading to **v2.x.x** and using **CLUSTER_CONFIG**\ , it will look like followed:

.. code-block:: python

   path = f"gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh"

   CLUSTER_CONFIG = ClusterGenerator(
       project_id="test",
       zone="us-central1-a",
       master_machine_type="n1-standard-4",
       worker_machine_type="n1-standard-4",
       num_workers=2,
       storage_bucket="test",
       init_actions_uris=[path],
       metadata={"PIP_PACKAGES": "pyyaml requests pandas openpyxl"},
   ).make()

   create_cluster_operator = DataprocClusterCreateOperator(
       task_id="create_dataproc_cluster",
       cluster_name="test",
       project_id="test",
       region="us-central1",
       cluster_config=CLUSTER_CONFIG,
   )

``airflow.providers.google.cloud.operators.bigquery.BigQueryGetDatasetTablesOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We changed signature of ``BigQueryGetDatasetTablesOperator``.

Before:

.. code-block:: python

   def __init__(
       dataset_id: str,
       dataset_resource: dict,
       # ...
   ): ...

After:

.. code-block:: python

   def __init__(
       dataset_resource: dict,
       dataset_id: Optional[str] = None,
       # ...
   ): ...

Changes in ``amazon`` provider package
""""""""""""""""""""""""""""""""""""""""""

We strive to ensure that there are no changes that may affect the end user, and your Python files, but this
release may contain changes that will require changes to your configuration, DAG Files or other integration
e.g. custom operators.

Only changes unique to this provider are described here. You should still pay attention to the changes that
have been made to the core (including core operators) as they can affect the integration behavior
of this provider.

This section describes the changes that have been made, and what you need to do to update your if
you use operators or hooks which integrate with Amazon services (including Amazon Web Service - AWS).

Migration of AWS components
~~~~~~~~~~~~~~~~~~~~~~~~~~~

All AWS components (hooks, operators, sensors, example DAGs) will be grouped together as decided in
`AIP-21 <https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths>`_. Migrated
components remain backwards compatible but raise a ``DeprecationWarning`` when imported from the old module.
Migrated are:

.. list-table::
   :header-rows: 1

   * - Old path
     - New path
   * - ``airflow.hooks.S3_hook.S3Hook``
     - ``airflow.providers.amazon.aws.hooks.s3.S3Hook``
   * - ``airflow.contrib.hooks.aws_athena_hook.AWSAthenaHook``
     - ``airflow.providers.amazon.aws.hooks.athena.AWSAthenaHook``
   * - ``airflow.contrib.hooks.aws_lambda_hook.AwsLambdaHook``
     - ``airflow.providers.amazon.aws.hooks.lambda_function.AwsLambdaHook``
   * - ``airflow.contrib.hooks.aws_sqs_hook.SQSHook``
     - ``airflow.providers.amazon.aws.hooks.sqs.SQSHook``
   * - ``airflow.contrib.hooks.aws_sns_hook.AwsSnsHook``
     - ``airflow.providers.amazon.aws.hooks.sns.AwsSnsHook``
   * - ``airflow.contrib.operators.aws_athena_operator.AWSAthenaOperator``
     - ``airflow.providers.amazon.aws.operators.athena.AWSAthenaOperator``
   * - ``airflow.contrib.operators.awsbatch.AWSBatchOperator``
     - ``airflow.providers.amazon.aws.operators.batch.AwsBatchOperator``
   * - ``airflow.contrib.operators.awsbatch.BatchProtocol``
     - ``airflow.providers.amazon.aws.hooks.batch_client.AwsBatchProtocol``
   * - private attrs and methods on ``AWSBatchOperator``
     - ``airflow.providers.amazon.aws.hooks.batch_client.AwsBatchClient``
   * - n/a
     - ``airflow.providers.amazon.aws.hooks.batch_waiters.AwsBatchWaiters``
   * - ``airflow.contrib.operators.aws_sqs_publish_operator.SQSPublishOperator``
     - ``airflow.providers.amazon.aws.operators.sqs.SQSPublishOperator``
   * - ``airflow.contrib.operators.aws_sns_publish_operator.SnsPublishOperator``
     - ``airflow.providers.amazon.aws.operators.sns.SnsPublishOperator``
   * - ``airflow.contrib.sensors.aws_athena_sensor.AthenaSensor``
     - ``airflow.providers.amazon.aws.sensors.athena.AthenaSensor``
   * - ``airflow.contrib.sensors.aws_sqs_sensor.SQSSensor``
     - ``airflow.providers.amazon.aws.sensors.sqs.SQSSensor``


``airflow.providers.amazon.aws.hooks.emr.EmrHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.amazon.aws.operators.emr_add_steps.EmrAddStepsOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.amazon.aws.operators.emr_create_job_flow.EmrCreateJobFlowOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.amazon.aws.operators.emr_terminate_job_flow.EmrTerminateJobFlowOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default value for the `aws_conn_id <https://airflow.apache.org/howto/manage-connections.html#amazon-web-services>`_ was accidentally set to 's3_default' instead of 'aws_default' in some of the emr operators in previous
versions. This was leading to EmrStepSensor not being able to find their corresponding emr cluster. With the new
changes in the EmrAddStepsOperator, EmrTerminateJobFlowOperator and EmrCreateJobFlowOperator this issue is
solved.

``airflow.providers.amazon.aws.operators.batch.AwsBatchOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``AwsBatchOperator`` was refactored to extract an ``AwsBatchClient`` (and inherit from it).  The
changes are mostly backwards compatible and clarify the public API for these classes; some
private methods on ``AwsBatchOperator`` for polling a job status were relocated and renamed
to surface new public methods on ``AwsBatchClient`` (and via inheritance on ``AwsBatchOperator``\ ).  A
couple of job attributes are renamed on an instance of ``AwsBatchOperator``\ ; these were mostly
used like private attributes but they were surfaced in the public API, so any use of them needs
to be updated as follows:


* ``AwsBatchOperator().jobId`` -> ``AwsBatchOperator().job_id``
* ``AwsBatchOperator().jobName`` -> ``AwsBatchOperator().job_name``

The ``AwsBatchOperator`` gets a new option to define a custom model for waiting on job status changes.
The ``AwsBatchOperator`` can use a new ``waiters`` parameter, an instance of ``AwsBatchWaiters``\ , to
specify that custom job waiters will be used to monitor a batch job.  See the latest API
documentation for details.

``airflow.providers.amazon.aws.sensors.athena.AthenaSensor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Replace parameter ``max_retires`` with ``max_retries`` to fix typo.

``airflow.providers.amazon.aws.hooks.s3.S3Hook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Note: The order of arguments has changed for ``check_for_prefix``.
The ``bucket_name`` is now optional. It falls back to the ``connection schema`` attribute.
The ``delete_objects`` now returns ``None`` instead of a response, since the method now makes multiple api requests when the keys list length is > 1000.

Changes in other provider packages
""""""""""""""""""""""""""""""""""

We strive to ensure that there are no changes that may affect the end user and your Python files, but this
release may contain changes that will require changes to your configuration, DAG Files or other integration
e.g. custom operators.

Only changes unique to providers are described here. You should still pay attention to the changes that
have been made to the core (including core operators) as they can affect the integration behavior
of this provider.

This section describes the changes that have been made, and what you need to do to update your if
you use any code located in ``airflow.providers`` package.

Changed return type of ``list_prefixes`` and ``list_keys`` methods in ``S3Hook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously, the ``list_prefixes`` and ``list_keys`` methods returned ``None`` when there were no
results. The behavior has been changed to return an empty list instead of ``None`` in this
case.

Removed HipChat integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

HipChat has reached end of life and is no longer available.

For more information please see
https://community.atlassian.com/t5/Stride-articles/Stride-and-Hipchat-Cloud-have-reached-End-of-Life-updated/ba-p/940248

``airflow.providers.salesforce.hooks.salesforce.SalesforceHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Replace parameter ``sandbox`` with ``domain``. According to change in simple-salesforce package.

Rename ``sign_in`` function to ``get_conn``.

``airflow.providers.apache.pinot.hooks.pinot.PinotAdminHook.create_segment``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rename parameter name from ``format`` to ``segment_format`` in PinotAdminHook function create_segment for pylint compatible

``airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook.get_partitions``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rename parameter name from ``filter`` to ``partition_filter`` in HiveMetastoreHook function get_partitions for pylint compatible

``airflow.providers.ftp.hooks.ftp.FTPHook.list_directory``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Remove unnecessary parameter ``nlst`` in FTPHook function ``list_directory`` for pylint compatible

``airflow.providers.postgres.hooks.postgres.PostgresHook.copy_expert``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Remove unnecessary parameter ``open`` in PostgresHook function ``copy_expert`` for pylint compatible

``airflow.providers.opsgenie.operators.opsgenie_alert.OpsgenieAlertOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Change parameter name from ``visibleTo`` to ``visible_to`` in OpsgenieAlertOperator for pylint compatible

``airflow.providers.imap.hooks.imap.ImapHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.providers.imap.sensors.imap_attachment.ImapAttachmentSensor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ImapHook:


* The order of arguments has changed for ``has_mail_attachment``\ ,
  ``retrieve_mail_attachments`` and ``download_mail_attachments``.
* A new ``mail_filter`` argument has been added to each of those.

``airflow.providers.http.hooks.http.HttpHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The HTTPHook is now secured by default: ``verify=True`` (before: ``verify=False``\ )
This can be overwritten by using the extra_options param as ``{'verify': False}``.

``airflow.providers.cloudant.hooks.cloudant.CloudantHook``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


* upgraded cloudant version from ``>=0.5.9,<2.0`` to ``>=2.0``
* removed the use of the ``schema`` attribute in the connection
* removed ``db`` function since the database object can also be retrieved by calling ``cloudant_session['database_name']``

For example:

.. code-block:: python

   from airflow.providers.cloudant.hooks.cloudant import CloudantHook

   with CloudantHook().get_conn() as cloudant_session:
       database = cloudant_session["database_name"]

See the `docs <https://python-cloudant.readthedocs.io/en/latest/>`_ for more information on how to use the new cloudant version.

``airflow.providers.snowflake``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When initializing a Snowflake hook or operator, the value used for ``snowflake_conn_id`` was always ``snowflake_conn_id``\ , regardless of whether or not you specified a value for it. The default ``snowflake_conn_id`` value is now switched to ``snowflake_default`` for consistency and will be properly overridden when specified.

Other changes
"""""""""""""

This release also includes changes that fall outside any of the sections above.

Standardized "extra" requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We standardized the Extras names and synchronized providers package names with the main airflow extras.

We deprecated a number of extras in 2.0.

.. list-table::
   :header-rows: 1

   * - Deprecated extras
     - New extras
   * - atlas
     - apache.atlas
   * - aws
     - amazon
   * - azure
     - microsoft.azure
   * - azure_blob_storage
     - microsoft.azure
   * - azure_data_lake
     - microsoft.azure
   * - azure_cosmos
     - microsoft.azure
   * - azure_container_instances
     - microsoft.azure
   * - cassandra
     - apache.cassandra
   * - druid
     - apache.druid
   * - gcp
     - google
   * - gcp_api
     - google
   * - hdfs
     - apache.hdfs
   * - hive
     - apache.hive
   * - kubernetes
     - cncf.kubernetes
   * - mssql
     - microsoft.mssql
   * - pinot
     - apache.pinot
   * - webhdfs
     - apache.webhdfs
   * - winrm
     - apache.winrm


For example:

If you want to install integration for Apache Atlas, then instead of ``pip install apache-airflow[atlas]``
you should use ``pip install apache-airflow[apache.atlas]``.

NOTE!

If you want to install integration for Microsoft Azure, then instead of

.. code-block::

   pip install 'apache-airflow[azure_blob_storage,azure_data_lake,azure_cosmos,azure_container_instances]'

you should run ``pip install 'apache-airflow[microsoft.azure]'``

If you want to install integration for Amazon Web Services, then instead of
``pip install 'apache-airflow[s3,emr]'``\ , you should execute ``pip install 'apache-airflow[aws]'``

The deprecated extras will be removed in 3.0.

Simplify the response payload of endpoints /dag_stats and /task_stats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The response of endpoints ``/dag_stats`` and ``/task_stats`` help UI fetch brief statistics about DAGs and Tasks. The format was like

.. code-block:: json

   {
       "example_http_operator": [
           {
               "state": "success",
               "count": 0,
               "dag_id": "example_http_operator",
               "color": "green"
           },
           {
               "state": "running",
               "count": 0,
               "dag_id": "example_http_operator",
               "color": "lime"
           }
       ]
   }

The ``dag_id`` was repeated in the payload, which makes the response payload unnecessarily bigger.

Now the ``dag_id`` will not appear repeated in the payload, and the response format is like

.. code-block:: json

   {
       "example_http_operator": [
           {
               "state": "success",
               "count": 0,
               "color": "green"
           },
           {
               "state": "running",
               "count": 0,
               "color": "lime"
           }
       ]
   }


.. spelling::

    nvd
    lineChart
