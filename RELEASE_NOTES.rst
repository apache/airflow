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
See :ref:`Usage data collection FAQ <usage-data-collection>` for more information.

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
- Update hatchling to latest version (1.22.5) (#38780)
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
- Upgrade to latest version of hatchling as build dependency (#40387)
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
- Limit importlib_resources as it breaks ``pytest_rewrites`` (#38095, #38139)
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

Also we implement multiple license files support coming from Draft, not yet accepted (but supported by hatchling) PEP:
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

* Released airflow package does not contain ``devel``, ``devel-*``, ``doc`` and ``doc-gen`` extras.
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
- Standardize airflow build process and switch to Hatchling build backend (#36537)
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

``airflow.sensors.filesystem.FileSensor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Airflow 1.10.15 (2021-03-17)
----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- Fix ``airflow db upgrade`` to upgrade db as intended (#13267)
- Moved boto3 limitation to snowflake (#13286)
- ``KubernetesExecutor`` should accept images from ``executor_config`` (#13074)
- Scheduler should acknowledge active runs properly (#13803)
- Include ``airflow/contrib/executors`` in the dist package
- Pin Click version for Python 2.7 users
- Ensure all StatsD timers use millisecond values. (#10633)
- [``kubernetes_generate_dag_yaml``] - Fix dag yaml generate function (#13816)
- Fix ``airflow tasks clear`` cli command with ``--yes`` (#14188)
- Fix permission error on non-POSIX filesystem (#13121) (#14383)
- Fixed deprecation message for "variables" command (#14457)
- BugFix: fix the ``delete_dag`` function of json_client (#14441)
- Fix merging of secrets and configmaps for ``KubernetesExecutor`` (#14090)
- Fix webserver exiting when gunicorn master crashes (#13470)
- Bump ``ini`` from 1.3.5 to 1.3.8 in ``airflow/www_rbac``
- Bump ``datatables.net`` from 1.10.21 to 1.10.23 in ``airflow/www_rbac``
- Webserver: Sanitize string passed to origin param (#14738)
- Make ``rbac_app``'s ``db.session`` use the same timezone with ``@provide_session`` (#14025)

Improvements
^^^^^^^^^^^^

- Adds airflow as viable docker command in official image (#12878)
- ``StreamLogWriter``: Provide (no-op) close method (#10885)
- Add 'airflow variables list' command for 1.10.x transition version (#14462)

Doc only changes
^^^^^^^^^^^^^^^^

- Update URL for Airflow docs (#13561)
- Clarifies version args for installing 1.10 in Docker (#12875)

Airflow 1.10.14 (2020-12-10)
----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

``[scheduler] max_threads`` config has been renamed to ``[scheduler] parsing_processes``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

From Airflow 1.10.14, ``max_threads`` config under ``[scheduler]`` section has been renamed to ``parsing_processes``.

This is to align the name with the actual code where the Scheduler launches the number of processes defined by
``[scheduler] parsing_processes`` to parse the DAG files.

Airflow CLI changes in line with 2.0
""""""""""""""""""""""""""""""""""""

The Airflow CLI has been organized so that related commands are grouped together as subcommands,
which means that if you use these commands in your scripts, they will now raise a DeprecationWarning and
you have to make changes to them before you upgrade to Airflow 2.0.

This section describes the changes that have been made, and what you need to do to update your script.

The ability to manipulate users from the command line has been changed. ``airflow create_user``\ ,  ``airflow delete_user``
 and ``airflow list_users`` has been grouped to a single command ``airflow users`` with optional flags ``create``\ , ``list`` and ``delete``.

The ``airflow list_dags`` command is now ``airflow dags list``\ , ``airflow pause`` is ``airflow dags pause``\ , etc.

In Airflow 1.10 and 2.0 there is an ``airflow config`` command but there is a difference in behavior. In Airflow 1.10,
it prints all config options while in Airflow 2.0, it's a command group. ``airflow config`` is now ``airflow config list``.
You can check other options by running the command ``airflow config --help``

Compatibility with the old CLI has been maintained, but they will no longer appear in the help

You can learn about the commands by running ``airflow --help``. For example to get help about the ``celery`` group command,
you have to run the help command: ``airflow celery --help``.

.. list-table::
   :header-rows: 1

   * - Old command
     - New command
     - Group
   * - ``airflow worker``
     - ``airflow celery worker``
     - ``celery``
   * - ``airflow flower``
     - ``airflow celery flower``
     - ``celery``
   * - ``airflow trigger_dag``
     - ``airflow dags trigger``
     - ``dags``
   * - ``airflow delete_dag``
     - ``airflow dags delete``
     - ``dags``
   * - ``airflow show_dag``
     - ``airflow dags show``
     - ``dags``
   * - ``airflow list_dag``
     - ``airflow dags list``
     - ``dags``
   * - ``airflow dag_status``
     - ``airflow dags status``
     - ``dags``
   * - ``airflow backfill``
     - ``airflow dags backfill``
     - ``dags``
   * - ``airflow list_dag_runs``
     - ``airflow dags list-runs``
     - ``dags``
   * - ``airflow pause``
     - ``airflow dags pause``
     - ``dags``
   * - ``airflow unpause``
     - ``airflow dags unpause``
     - ``dags``
   * - ``airflow next_execution``
     - ``airflow dags next-execution``
     - ``dags``
   * - ``airflow test``
     - ``airflow tasks test``
     - ``tasks``
   * - ``airflow clear``
     - ``airflow tasks clear``
     - ``tasks``
   * - ``airflow list_tasks``
     - ``airflow tasks list``
     - ``tasks``
   * - ``airflow task_failed_deps``
     - ``airflow tasks failed-deps``
     - ``tasks``
   * - ``airflow task_state``
     - ``airflow tasks state``
     - ``tasks``
   * - ``airflow run``
     - ``airflow tasks run``
     - ``tasks``
   * - ``airflow render``
     - ``airflow tasks render``
     - ``tasks``
   * - ``airflow initdb``
     - ``airflow db init``
     - ``db``
   * - ``airflow resetdb``
     - ``airflow db reset``
     - ``db``
   * - ``airflow upgradedb``
     - ``airflow db upgrade``
     - ``db``
   * - ``airflow checkdb``
     - ``airflow db check``
     - ``db``
   * - ``airflow shell``
     - ``airflow db shell``
     - ``db``
   * - ``airflow pool``
     - ``airflow pools``
     - ``pools``
   * - ``airflow create_user``
     - ``airflow users create``
     - ``users``
   * - ``airflow delete_user``
     - ``airflow users delete``
     - ``users``
   * - ``airflow list_users``
     - ``airflow users list``
     - ``users``
   * - ``airflow rotate_fernet_key``
     - ``airflow rotate-fernet-key``
     -
   * - ``airflow sync_perm``
     - ``airflow sync-perm``
     -

Bug Fixes
^^^^^^^^^

- BugFix: Tasks with ``depends_on_past`` or ``task_concurrency`` are stuck (#12663)
- Fix issue with empty Resources in executor_config (#12633)
- Fix: Deprecated config ``force_log_out_after`` was not used (#12661)
- Fix empty asctime field in JSON formatted logs (#10515)
- [AIRFLOW-2809] Fix security issue regarding Flask SECRET_KEY (#3651)
- [AIRFLOW-2884] Fix Flask SECRET_KEY security issue in www_rbac (#3729)
- [AIRFLOW-2886] Generate random Flask SECRET_KEY in default config (#3738)
- Add missing comma in setup.py (#12790)
- Bugfix: Unable to import Airflow plugins on Python 3.8 (#12859)
- Fix setup.py missing comma in ``setup_requires`` (#12880)
- Don't emit first_task_scheduling_delay metric for only-once dags (#12835)

Improvements
^^^^^^^^^^^^

- Update setup.py to get non-conflicting set of dependencies (#12636)
- Rename ``[scheduler] max_threads`` to ``[scheduler] parsing_processes`` (#12605)
- Add metric for scheduling delay between first run task & expected start time (#9544)
- Add new-style 2.0 command names for Airflow 1.10.x (#12725)
- Add Kubernetes cleanup-pods CLI command for Helm Chart (#11802)
- Don't let webserver run with dangerous config (#12747)
- Replace pkg_resources with ``importlib.metadata`` to avoid VersionConflict errors (#12694)

Doc only changes
^^^^^^^^^^^^^^^^

- Clarified information about supported Databases

Airflow 1.10.13 (2020-11-25)
----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

TimeSensor is now timezone aware
""""""""""""""""""""""""""""""""

Previously ``TimeSensor`` always compared the ``target_time`` with the current time in UTC.

Now it will compare ``target_time`` with the current time in the timezone of the DAG,
defaulting to the ``default_timezone`` in the global config.

Removed Kerberos support for HDFS hook
""""""""""""""""""""""""""""""""""""""

The HDFS hook's Kerberos support has been removed due to removed ``python-krbV`` dependency from PyPI
and generally lack of support for SSL in Python3 (Snakebite-py3 we use as dependency has no
support for SSL connection to HDFS).

SSL support still works for WebHDFS hook.

Unify user session lifetime configuration
"""""""""""""""""""""""""""""""""""""""""

In previous version of Airflow user session lifetime could be configured by
``session_lifetime_days`` and ``force_log_out_after`` options. In practice only ``session_lifetime_days``
had impact on session lifetime, but it was limited to values in day.
We have removed mentioned options and introduced new ``session_lifetime_minutes``
option which simplify session lifetime configuration.

Before

.. code-block:: ini

   [webserver]
   force_log_out_after = 0
   session_lifetime_days = 30

After

.. code-block:: ini

   [webserver]
   session_lifetime_minutes = 43200

Adding Operators, Hooks and Sensors via Airflow Plugins is deprecated
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The ability to import Operators, Hooks and Sensors via the plugin mechanism has been deprecated and will raise warnings
in Airflow 1.10.13 and will be removed completely in Airflow 2.0.

Check https://airflow.apache.org/docs/1.10.13/howto/custom-operator.html to see how you can create and import
Custom Hooks, Operators and Sensors.

New Features
^^^^^^^^^^^^

- Add "already checked" to failed pods in K8sPodOperator (#11368)
- Pass SQLAlchemy engine options to FAB based UI (#11395)
- [AIRFLOW-4438] Add Gzip compression to S3_hook (#8571)
- Add permission "extra_links" for Viewer role and above (#10719)
- Add generate_yaml command to easily test KubernetesExecutor before deploying pods (#10677)
- Add Secrets backend for Microsoft Azure Key Vault (#10898)

Bug Fixes
^^^^^^^^^

- SkipMixin: Handle empty branches (#11120)
- [AIRFLOW-5274] dag loading duration metric name too long (#5890)
- Handle no Dagrun in DagrunIdDep (#8389) (#11343)
- Fix Kubernetes Executor logs for long dag names (#10942)
- Add on_kill support for the KubernetesPodOperator (#10666)
- KubernetesPodOperator template fix (#10963)
- Fix displaying of add serialized_dag table migration
- Fix Start Date tooltip on DAGs page (#10637)
- URL encode execution date in the Last Run link (#10595)
- Fixes issue with affinity backcompat in Airflow 1.10
- Fix KubernetesExecutor import in views.py
- Fix issues with Gantt View (#12419)
- Fix Entrypoint and _CMD config variables (#12411)
- Fix operator field update for SerializedBaseOperator (#10924)
- Limited cryptography to < 3.2 for Python 2.7
- Install cattr on Python 3.7 - Fix docs build on RTD (#12045)
- Limit version of marshmallow-sqlalchemy
- Pin ``kubernetes`` to a max version of 11.0.0 (#11974)
- Use snakebite-py3 for HDFS dependency for Python3 (#12340)
- Removes snakebite kerberos dependency (#10865)
- Fix failing dependencies for FAB and Celery (#10828)
- Fix pod_mutation_hook for 1.10.13 (#10850)
- Fix formatting of Host information
- Fix Logout Google Auth issue in Non-RBAC UI (#11890)
- Add missing imports to app.py (#10650)
- Show Generic Error for Charts & Query View in old UI (#12495)
- TimeSensor should respect the default_timezone config (#9699)
- TimeSensor should respect DAG timezone (#9882)
- Unify user session lifetime configuration (#11970)
- Handle outdated webserver session timeout gracefully. (#12332)


Improvements
^^^^^^^^^^^^

- Add XCom.deserialize_value to Airflow 1.10.13 (#12328)
- Mount airflow.cfg to pod_template_file (#12311)
- All k8s object must comply with JSON Schema (#12003)
- Validate Airflow chart values.yaml & values.schema.json (#11990)
- Pod template file uses custom custom env variable (#11480)
- Bump attrs and cattrs dependencies (#11969)
- Bump attrs to > 20.0 (#11799)
- [AIRFLOW-3607] Only query DB once per DAG run for TriggerRuleDep (#4751)
- Rename task with duplicate task_id
- Manage Flask AppBuilder Tables using Alembic Migrations (#12352)
- ``airflow test`` only works for tasks in 1.10, not whole dags (#11191)
- Improve warning messaging for duplicate task_ids in a DAG (#11126)
- Pins moto to 1.3.14 (#10986)
- DbApiHook: Support kwargs in get_pandas_df (#9730)
- Make grace_period_seconds option on K8sPodOperator (#10727)
- Fix syntax error in Dockerfile 'maintainer' Label (#10899)
- The entrypoints in Docker Image should be owned by Airflow (#10853)
- Make dockerfiles Google Shell Guide Compliant (#10734)
- clean-logs script for Dockerfile: trim logs before sleep (#10685)
- When sending tasks to celery from a sub-process, reset signal handlers (#11278)
- SkipMixin: Add missing session.commit() and test (#10421)
- Webserver: Further Sanitize values passed to origin param (#12459)
- Security upgrade lodash from 4.17.19 to 4.17.20 (#11095)
- Log instead of raise an Error for unregistered OperatorLinks (#11959)
- Mask Password in Log table when using the CLI (#11468)
- [AIRFLOW-3607] Optimize dep checking when depends on past set and concurrency limit
- Execute job cancel HTTPRequest in Dataproc Hook (#10361)
- Use rst lexer to format Airflow upgrade check output (#11259)
- Remove deprecation warning from contrib/kubernetes/pod.py
- adding body as templated field for CloudSqlImportOperator (#10510)
- Change log level for User's session to DEBUG (#12414)

Deprecations
^^^^^^^^^^^^

- Deprecate importing Hooks from plugin-created module (#12133)
- Deprecate adding Operators and Sensors via plugins (#12069)

Doc only changes
^^^^^^^^^^^^^^^^

- [Doc] Correct description for macro task_instance_key_str (#11062)
- Checks if all the libraries in setup.py are listed in installation.rst file (#12023)
- Revise "Project Focus" copy (#12011)
- Move Project focus and Principles higher in the README (#11973)
- Remove archived link from README.md (#11945)
- Update download url for Airflow Version (#11800)
- Add Project URLs for PyPI page (#11801)
- Move Backport Providers docs to our docsite (#11136)
- Refactor rebase copy (#11030)
- Add missing images for kubernetes executor docs (#11083)
- Fix indentation in executor_config example (#10467)
- Enhanced the Kubernetes Executor doc  (#10433)
- Refactor content to a markdown table (#10863)
- Rename "Beyond the Horizon" section and refactor content (#10802)
- Refactor official source section to use bullets (#10801)
- Add section for official source code (#10678)
- Add redbubble link to Airflow merchandise (#10359)
- README Doc: Link to Airflow directory in ASF Directory (#11137)
- Fix the default value for VaultBackend's config_path (#12518)

Airflow 1.10.12 (2020-08-25)
----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Clearing tasks skipped by SkipMixin will skip them
""""""""""""""""""""""""""""""""""""""""""""""""""

Previously, when tasks skipped by SkipMixin (such as BranchPythonOperator, BaseBranchOperator and ShortCircuitOperator) are cleared, they execute. Since 1.10.12, when such skipped tasks are cleared,
they will be skipped again by the newly introduced NotPreviouslySkippedDep.

The pod_mutation_hook function will now accept a kubernetes V1Pod object
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

As of airflow 1.10.12, using the ``airflow.contrib.kubernetes.Pod`` class in the ``pod_mutation_hook`` is now deprecated. Instead we recommend that users
treat the ``pod`` parameter as a ``kubernetes.client.models.V1Pod`` object. This means that users now have access to the full Kubernetes API
when modifying airflow pods

pod_template_file option now available in the KubernetesPodOperator
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Users can now offer a path to a yaml for the KubernetesPodOperator using the ``pod_template_file`` parameter.

New Features
^^^^^^^^^^^^

- Add DateTimeSensor (#9697)
- Add AirflowClusterPolicyViolation support to Airflow local settings (#10282)
- Get Airflow configs with sensitive data from Secret Backends (#9645)
- [AIRFLOW-4734] Upsert functionality for PostgresHook.insert_rows() (#8625)
- Allow defining custom XCom class (#8560)

Bug Fixes
^^^^^^^^^

- Add pre 1.10.11 Kubernetes Paths back with Deprecation Warning (#10067)
- Fixes PodMutationHook for backwards compatibility (#9903)
- Fix bug in executor_config when defining resources (#9935)
- Respect DAG Serialization setting when running sync_perm (#10321)
- Show correct duration on graph view for running task (#8311) (#8675)
- Fix regression in SQLThresholdCheckOperator (#9312)
- [AIRFLOW-6931] Fixed migrations to find all dependencies for MSSQL (#9891)
- Avoid sharing session with RenderedTaskInstanceFields write and delete (#9993)
- Fix clear future recursive when ExternalTaskMarker is used (#9515)
- Handle IntegrityError while creating TIs (#10136)
- Fix airflow-webserver startup errors when using Kerberos Auth (#10047)
- Fixes treatment of open slots in scheduler (#9316) (#9505)
- Fix KubernetesPodOperator reattachment (#10230)
- Fix more PodMutationHook issues for backwards compatibility (#10084)
- [AIRFLOW-5391] Do not re-run skipped tasks when they are cleared (#7276)
- Fix task_instance_mutation_hook (#9910)
- Fixes failing formatting of DAG file containing {} in docstring (#9779)
- Fix is_terminal_support_colors function (#9734)
- Fix PythonVirtualenvOperator when using ``provide_context=True`` (#8256)
- Fix issue with mounting volumes from secrets (#10366)
- BugFix: K8s Executor Multinamespace mode is evaluated to true by default (#10410)
- Make KubernetesExecutor recognize kubernetes_labels (#10412)
- Fix broken Kubernetes PodRuntimeInfoEnv (#10478)

Improvements
^^^^^^^^^^^^

- Use Hash of Serialized DAG to determine DAG is changed or not (#10227)
- Update Serialized DAGs in Webserver when DAGs are Updated (#9851)
- Do not Update Serialized DAGs in DB if DAG did not change (#9850)
- Add __repr__ to SerializedDagModel (#9862)
- Update JS packages to latest versions (#9811) (#9921)
- UI Graph View: Focus upstream / downstream task dependencies on mouseover (#9303)
- Allow ``image`` in ``KubernetesPodOperator`` to be templated (#10068)
- [AIRFLOW-6843] Add delete_option_kwargs to delete_namespaced_pod (#7523)
- Improve process terminating in scheduler_job (#8064)
- Replace deprecated base classes used in bigquery_check_operator (#10272)
- [AIRFLOW-5897] Allow setting -1 as pool slots value in webserver (#6550)
- Limit all google-cloud api to <2.0.0 (#10317)
- [AIRFLOW-6706] Lazy load operator extra links (#7327) (#10318)
- Add Snowflake support to SQL operator and sensor (#9843)
- Makes multi-namespace mode optional (#9570)
- Pin pyarrow < 1.0
- Pin pymongo version to <3.11.0
- Pin google-cloud-container to <2 (#9901)
- Dockerfile: Remove package.json and yarn.lock from the prod image (#9814)
- Dockerfile: The group of embedded DAGs should be root to be OpenShift compatible (#9794)
- Update upper limit of flask-swagger, gunicorn & jinja2 (#9684)
- Webserver: Sanitize values passed to origin param (#10334)
- Sort connection type list in add/edit page alphabetically (#8692)

Doc only changes
^^^^^^^^^^^^^^^^

- Add new committers: Ry Walker & Leah Cole to project.rst (#9892)
- Add Qingping Hou to committers list (#9725)
- Updated link to official documentation (#9629)
- Create a short-link for Airflow Slack Invites (#10034)
- Set language on code-block on docs/howto/email-config.rst (#10238)
- Remove duplicate line from 1.10.10 CHANGELOG (#10289)
- Improve heading on Email Configuration page (#10175)
- Fix link for the Jinja Project in docs/tutorial.rst (#10245)
- Create separate section for Cron Presets (#10247)
- Add Syntax Highlights to code-blocks in docs/best-practices.rst (#10258)
- Fix docstrings in BigQueryGetDataOperator (#10042)
- Fix typo in Task Lifecycle section (#9867)
- Make Secret Backend docs clearer about Variable & Connection View (#8913)

Airflow 1.10.11 (2020-07-10)
----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Use NULL as default value for dag.description
"""""""""""""""""""""""""""""""""""""""""""""

Now use NULL as default value for dag.description in dag table

Restrict editing DagRun State in the old UI (Flask-admin based UI)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Before 1.10.11 it was possible to edit DagRun State in the ``/admin/dagrun/`` page
 to any text.

In Airflow 1.10.11+, the user can only choose the states from the list.

Experimental API will deny all request by default.
""""""""""""""""""""""""""""""""""""""""""""""""""

The previous default setting was to allow all API requests without authentication, but this poses security
risks to users who miss this fact. This changes the default for new installs to deny all requests by default.

**Note**\ : This will not change the behavior for existing installs, please update check your airflow.cfg

If you wish to have the experimental API work, and aware of the risks of enabling this without authentication
(or if you have your own authentication layer in front of Airflow) you can get
the previous behaviour on a new install by setting this in your airflow.cfg:

.. code-block::

   [api]
   auth_backend = airflow.api.auth.backend.default

XCom Values can no longer be added or changed from the Webserver
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Since XCom values can contain pickled data, we would no longer allow adding or
changing XCom values from the UI.

Default for ``run_as_user`` configured has been changed to 50000 from 0
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The UID to run the first process of the Worker PODs when using has been changed to ``50000``
from the previous default of ``0``. The previous default was an empty string but the code used ``0`` if it was
empty string.

**Before**\ :

.. code-block:: ini

   [kubernetes]
   run_as_user =

**After**\ :

.. code-block:: ini

   [kubernetes]
   run_as_user = 50000

This is done to avoid running the container as ``root`` user.

New Features
^^^^^^^^^^^^

- Add task instance mutation hook (#8852)
- Allow changing Task States Colors (#9520)
- Add support for AWS Secrets Manager as Secrets Backend (#8186)
- Add Airflow info command to the CLI (#8704)
- Add Local Filesystem Secret Backend (#8596)
- Add Airflow config CLI command (#8694)
- Add Support for Python 3.8 (#8836)(#8823)
- Allow K8S worker pod to be configured from JSON/YAML file (#6230)
- Add quarterly to crontab presets (#6873)
- Add support for ephemeral storage on KubernetesPodOperator (#6337)
- Add AirflowFailException to fail without any retry (#7133)
- Add SQL Branch Operator (#8942)

Bug Fixes
^^^^^^^^^

- Use NULL as dag.description default value (#7593)
- BugFix: DAG trigger via UI error in RBAC UI (#8411)
- Fix logging issue when running tasks (#9363)
- Fix JSON encoding error in DockerOperator (#8287)
- Fix alembic crash due to typing import (#6547)
- Correctly restore upstream_task_ids when deserializing Operators (#8775)
- Correctly store non-default Nones in serialized tasks/dags (#8772)
- Correctly deserialize dagrun_timeout field on DAGs (#8735)
- Fix tree view if config contains " (#9250)
- Fix Dag Run UI execution date with timezone cannot be saved issue (#8902)
- Fix Migration for MSSQL (#8385)
- RBAC ui: Fix missing Y-axis labels with units in plots (#8252)
- RBAC ui: Fix missing task runs being rendered as circles instead (#8253)
- Fix: DagRuns page renders the state column with artifacts in old UI (#9612)
- Fix task and dag stats on home page (#8865)
- Fix the trigger_dag api in the case of nested subdags (#8081)
- UX Fix: Prevent undesired text selection with DAG title selection in Chrome (#8912)
- Fix connection add/edit for spark (#8685)
- Fix retries causing constraint violation on MySQL with DAG Serialization (#9336)
- [AIRFLOW-4472] Use json.dumps/loads for templating lineage data (#5253)
- Restrict google-cloud-texttospeech to <v2 (#9137)
- Fix pickling failure when spawning processes (#8671)
- Pin Version of azure-cosmos to <4 (#8956)
- Azure storage 0.37.0 is not installable any more (#8833)
- Fix modal_backdrop z-index in the UI (#7313)
- Fix Extra Links in Gantt View (#8308)
- Bug fix for EmrAddStepOperator init with cluster_name error (#9235)
- Fix KubernetesPodOperator pod name length validation (#8829)
- Fix non updating DAG code by checking against last modification time (#8266)
- BugFix: Unpausing a DAG with catchup=False creates an extra DAG run (#8776)


Improvements
^^^^^^^^^^^^

- Improve add_dag_code_table migration (#8176)
- Persistent display/filtering of DAG status (#8106)
- Set unique logger names (#7330)
- Update the version of cattrs from 0.9 to 1.0 to support Python 3.8 (#7100)
- Reduce response payload size of /dag_stats and /task_stats (#8655)
- Add authenticator parameter to snowflake_hook (#8642)
- Show "Task Reschedule" table in Airflow Webserver (#9521)
- Change worker_refresh_interval fallback to default of 30 (#9588)
- Use pformat instead of str to render arguments in WebUI (#9587)
- Simplify DagFileProcessorManager (#7521)
- Reload gunicorn when plugins has been changed (#8997)
- Fix the default value for store_dag_code (#9554)
- Add support for fetching logs from running pods (#8626)
- Persist start/end date and duration for DummyOperator Task Instance (#8663)
- Ensure "started"/"ended" in tooltips are not shown if job not started (#8667)
- Add context to execution_date_fn in ExternalTaskSensor (#8702)
- Avoid color info in response of ``/dag_stats`` & ``/task_stats`` (#8742)
- Make loading plugins from entrypoint fault-tolerant (#8732)
- Refactor Kubernetes worker config (#7114)
- Add default ``conf`` parameter to Spark JDBC Hook (#8787)
- Allow passing backend_kwargs to AWS SSM client (#8802)
- Filter dags by clicking on tag (#8897)
- Support k8s auth method in Vault Secrets provider (#8640)
- Monitor pods by labels instead of names (#6377)
- Optimize count query on /home (#8729)
- Fix JSON string escape in tree view (#8551)
- Add TaskInstance state to TI Tooltip to be colour-blind friendlier (#8910)
- Add a tip to trigger DAG screen (#9049)
- Use Markup for htmlcontent for landing_times (#9242)
- Pinning max pandas version to 2.0 (lesser than) to allow pandas 1.0 (#7954)
- Update example webserver_config.py to show correct CSRF config (#8944)
- Fix displaying Executor Class Name in "Base Job" table (#8679)
- Use existing DagBag for 'dag_details' & 'trigger' Endpoints (#8501)
- Flush pending Sentry exceptions before exiting (#7232)
- Display DAG run conf in the list view (#6794)
- Fix performance degradation when updating dagrun state (#8435)
- Don't use the ``|safe`` filter in code, it's risky (#9180)
- Validate only task commands are run by executors (#9178)
- Show Deprecation warning on duplicate Task ids (#8728)
- Move DAG._schedule_interval logic out of ``DAG.__init__`` (#8225)
- Make retrieving Paused Dag ids a separate method (#7587)
- Bulk fetch paused_dag_ids (#7476)
- Add a configurable DAGs volume mount path for Kubernetes (#8147)
- Add schedulername option for KubernetesPodOperator (#6088)
- Support running git sync container as root (#6312)
- Add extra options for Slack Webhook operator and Slack hook (#9409)
- Monkey patch greenlet Celery pools (#8559)
- Decrypt secrets from SystemsManagerParameterStoreBackend (#9214)
- Prevent clickable sorting on non sortable columns in TI view (#8681)
- Make hive macros py3 compatible (#8598)
- Fix SVG tooltip positioning with custom scripting (#8269)
- Avoid unnecessary sleep to maintain local task job heart rate (#6553)
- Include some missing RBAC roles on User and Viewer roles (#9133)
- Show Dag's Markdown docs on Tree View (#9448)
- Improved compatibility with Python 3.5+ - Convert signal.SIGTERM to int (#9207)
- Add 'main' param to template_fields in DataprocSubmitPySparkJobOperator (#9154)
- Make it possible to silence warnings from Airflow (#9208)
- Remove redundant count query in BaseOperator.clear() (#9362)
- Fix DB migration message (#8988)
- Fix awkward log info in dbapi_hook (#8482)
- Fix Celery default to no longer allow pickle (#7205)
- Further validation that only task commands are run by executors (#9240)
- Remove vendored nvd3 and slugify libraries (#9136)
- Enable configurable git sync depth  (#9094)
- Reduce the required resources for the Kubernetes's sidecar (#6062)
- Refactor K8S codebase with k8s API models (#5481)
- Move k8s executor out of contrib to closer match master (#8904)
- Allow filtering using "event" and "owner" in "Log" view (#4881)
- Add Yandex.Cloud custom connection to 1.10 (#8791)
- Add table-hover css class to DAGs table (#5033)
- Show un/pause errors in dags view. (#7669)
- Restructure database queries on /home (#4872)
- Add Cross Site Scripting defense (#6913)
- Make Gantt tooltip the same as Tree and Graph view (#8220)
- Add config to only delete worker pod on task failure (#7507)(#8312)
- Remove duplicate error message on chart connection failure (#8476)
- Remove default value spark_binary (#8508)
- Expose Airflow Webserver Port in Production Docker Image (#8228)
- Commit after each alembic migration (#4797)
- KubernetesPodOperator fixes and test (#6524)
- Docker Image: Add ADDITIONAL_AIRFLOW_EXTRAS (#9032)
- Docker Image: Add ADDITIONAL_PYTHON_DEPS (#9031)
- Remove httplib2 from Google requirements (#9194)
- Merging multiple SQL operators (#9124)
- Adds hive as extra in pyhive dependency (#9075)
- Change default auth for experimental backend to deny_all (#9611)
- Restrict changing XCom values from the Webserver (#9614)
- Add __repr__ for DagTag so tags display properly in /dagmodel/show (#8719)
- Functionality to shuffle HMS connections used by HiveMetastoreHook facilitating load balancing (#9280)
- Expose SQLAlchemy's connect_args and make it configurable (#6478)

Doc only changes
^^^^^^^^^^^^^^^^

- Add docs on using DAGRun.conf (#9578)
- Enforce code-block directives in doc (#9443)
- Carefully parse warning messages when building documentation (#8693)
- Make KubernetesPodOperator clear in docs (#8444)
- Improve language in Pod Mutation Hook docs (#8445)
- Fix formatting of Pool docs in concepts.rst (#8443)
- Make doc clearer about Airflow Variables using Environment Variables (#8427)
- Fix pools doc for LocalExecutor (#7643)
- Add Local and Sequential Executors to Doc (#8084)
- Add documentation for CLI command Airflow dags test (#8251)
- Fix typo in DAG Serialization documentation (#8317)
- Add scheduler in production section (#7351)
- Add a structural dag validation example (#6727)
- Adding Task re-run documentation (#6295)
- Fix outdated doc on settings.policy (#7532)
- Add docs about reload_on_plugin_change option (#9575)
- Add copy button to Code Blocks in Airflow Docs (#9450)
- Update commands in docs for v1.10+ (#9585)
- Add more info on dry-run CLI option (#9582)
- Document default timeout value for SSHOperator (#8744)
- Fix docs on creating CustomOperator (#8678)
- Enhanced documentation around Cluster Policy (#8661)
- Use sphinx syntax in concepts.rst (#7729)
- Update README to remove Python 3.8 limitation for Master (#9451)
- Add note about using dag_run.conf in BashOperator (#9143)
- Improve tutorial - Include all imports statements (#8670)
- Added more precise Python requirements to README.md (#8455)
- Fix Airflow Stable version in README.md (#9360)
- Update AWS connection example to show how to set from env var (#9191)
- Fix list formatting of plugins doc. (#8873)
- Add 'Version Added' on Secrets Backend docs (#8264)
- Simplify language re roll-your-own secrets backend (#8257)
- Add installation description for repeatable PyPi installation (#8513)
- Add note extra links only render on when using RBAC webserver  (#8788)
- Remove unused Airflow import from docs (#9274)
- Add PR/issue note in Contribution Workflow Example (#9177)
- Use inclusive language - language matters (#9174)
- Add docs to change Colors on the Webserver (#9607)
- Change 'initiate' to 'initialize' in installation.rst (#9619)
- Replace old Variables View Screenshot with new (#9620)
- Replace old SubDag zoom screenshot with new (#9621)
- Update docs about the change to default auth for experimental API (#9617)


Airflow 1.10.10 (2020-04-09)
----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Setting Empty string to a Airflow Variable will return an empty string
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously when you set an Airflow Variable with an empty string (\ ``''``\ ), the value you used to get
back was ``None``. This will now return an empty string (\ ``'''``\ )

Example:

.. code-block:: python

   Variable.set("test_key", "")
   Variable.get("test_key")

The above code returned ``None`` previously, now it will return ``''``.

Make behavior of ``none_failed`` trigger rule consistent with documentation
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The behavior of the ``none_failed`` trigger rule is documented as "all parents have not failed (\ ``failed`` or
    ``upstream_failed``\ ) i.e. all parents have succeeded or been skipped." As previously implemented, the actual behavior
    would skip if all parents of a task had also skipped.

Add new trigger rule ``none_failed_or_skipped``
"""""""""""""""""""""""""""""""""""""""""""""""""""

The fix to ``none_failed`` trigger rule breaks workflows that depend on the previous behavior.
    If you need the old behavior, you should change the tasks with ``none_failed`` trigger rule to ``none_failed_or_skipped``.

Success Callback will be called when a task in marked as success from UI
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

When a task is marked as success by a user from Airflow UI - ``on_success_callback`` will be called

New Features
^^^^^^^^^^^^

- [AIRFLOW-7048] Allow user to chose timezone to use in UI (#8046)
- Add Production Docker image support (#7832)
- Get Airflow Variables from Environment Variables (#7923)
- Get Airflow Variables from Hashicorp Vault (#7944)
- Get Airflow Variables from AWS Systems Manager Parameter Store (#7945)
- Get Airflow Variables from GCP Secrets Manager (#7946)
- [AIRFLOW-5705] Add secrets backend and support for AWS SSM / Get Airflow Connections from AWS Parameter Store(#6376)
- [AIRFLOW-7104] Add Secret backend for GCP Secrets Manager / Get Airflow Connections from GCP Secrets Manager (#7795)
- [AIRFLOW-7076] Add support for HashiCorp Vault as Secrets Backend / Get Airflow Connections from Hashicorp Vault (#7741)
- [AIRFLOW-6685] Add ThresholdCheckOperator (#7353)
- [AIRFLOW-7080] Add API endpoint to return a DAG's paused state (#7737)

Bug Fixes
^^^^^^^^^

- BugFix: Show task_id in the Graph View tooltip (#7859)
- [AIRFLOW-6730] Use total_seconds instead of seconds (#7363)
- [AIRFLOW-6167] Escape column name in create table in hive (#6741)
- [AIRFLOW-6628] DAG auto-complete now suggests from all accessible DAGs (#7251)
- [AIRFLOW-7113] Fix gantt render error (#7913)
- [AIRFLOW-6399] Add _access control to validate deserialized DAGs (#7896)
- [AIRFLOW-6399] Serialization: DAG access_control field should be decorated field in DAG serialization (#7879)
- [AIRFLOW-4453] Make behavior of ``none_failed`` consistent with documentation (#7464)
- [AIRFLOW-4363] Fix JSON encoding error (#7628)
- [AIRFLOW-6683] Run REST API tests when DAGs are serialized (#7352)
- [AIRFLOW-6704] Copy common TaskInstance attributes from Task (#7324)
- [AIRFLOW-6734] Use configured base_template instead of hard-coding (#7367)
- [AIRFLOW-7098] Simple salesforce release 1.0.0 breaks the build (#7775)
- [AIRFLOW-6062] Executor would only delete workers in its own namespace (#7123)
- [AIRFLOW-7074] Add Permissions to view SubDAGs (#7752)
- [AIRFLOW-7025] Fix SparkSqlHook.run_query to handle its parameter properly (#7677)
- [AIRFLOW-6855] Escape project_dataset_table in SQL query in gcs to bq operator (#7475)
- [AIRFLOW-6949] Respect explicit conf to SparkSubmitOperator (#7575)
- [AIRFLOW-6588] write_stdout and json_format are boolean (#7199)
- [AIRFLOW-3439] Decode logs with  'utf-8' (#4474)
- [AIRFLOW-6878] Fix misconfigured default value for kube_client_request_args
- [AIRFLOW-5167] Update dependencies for GCP packages (#7116)
- [AIRFLOW-6821] Success callback not called when task marked as success from UI (#7447)
- [AIRFLOW-6740] Remove Undocumented, deprecated, dysfunctional PROXY_FIX_NUM_PROXIES (#7359)
- [AIRFLOW-6728] Change various DAG info methods to POST (#7364)
- [AIRFLOW-6997] Make sure worker pods initcontainers obtain env vars from config (#7663)
- [AIRFLOW-7062] Fix pydruid release breaking the build (#7720)
- [AIRFLOW-6040] ReadTimoutError in KubernetesExecutor should not raise exception (#7616)
- [AIRFLOW-6943] Fix utf-8 encoded description in DAG in Python 2 (#7567)
- [AIRFLOW-6892] Fix broken non-wheel releases (#7514)
- [AIRFLOW-6789] BugFix: Fix Default Worker concurrency (#7494)
- [AIRFLOW-6840] Bump up version of future (#7471)
- [AIRFLOW-5705] Fix bugs in AWS SSM Secrets Backend (#7745)
- [AIRFLOW-5705] Fix bug in Secrets Backend (#7742)
- Fix CloudSecretsManagerBackend invalid connections_prefix (#7861)
- [AIRFLOW-7045] BugFix: DebugExecutor fails to change task state. (#8073)
- BugFix: Datetimepicker is stuck on the UI (#8092)
- [AIRFLOW-5277] Gantt chart respects per-user the Timezone UI setting (#8096)
- Fix timezones displayed in Task Instance tooltip (#8103)
- BugFix: Fix writing & deleting Dag Code for Serialized DAGs (#8151)
- Make the default TI pool slots '1' (#8153)
- Fix 500 error in Security screens (#8165)
- Fix Viewing Dag Code for Stateless Webserver (#8159)
- Fix issue with sqlalchemy 1.3.16 (#8230)


Improvements
^^^^^^^^^^^^

- Use same tooltip for Graph and Tree views for TaskInstances (#8043)
- Allow DateTimePicker in Webserver to actually pick times too (#8034)
- [AIRFLOW-5590] Add run_id to trigger DAG run API response (#6256)
- [AIRFLOW-6695] Can now pass dagrun conf when triggering dags via UI (#7312)
- [AIRFLOW-5336] Add ability to make updating FAB perms on webserver in it optional (#5940)
- [AIRFLOW-1467] Allow tasks to use more than one pool slot (#7160)
- [AIRFLOW-6987] Avoid creating default connections (#7629)
- [AIRFLOW-4175] S3Hook load_file should support ACL policy parameter (#7733)
- [AIRFLOW-4438] Add Gzip compression to S3_hook (#7680)
- Allow setting Airflow Variable values to empty string (#8021)
- Dont schedule dummy tasks (#7880)
- Prevent sequential scan of task instance table (#8014)
- [AIRFLOW-7017] Respect default dag view in trigger dag origin (#7667)
- [AIRFLOW-6837] Limit description length of a Dag on HomePage (#7457)
- [AIRFLOW-6989] Display Rendered template_fields without accessing Dag files (#7633)
- [AIRFLOW-5944] Rendering templated_fields without accessing DAG files (#6788)
- [AIRFLOW-5946] DAG Serialization: Store source code in db (#7217)
- [AIRFLOW-7079] Remove redundant code for storing template_fields (#7750)
- [AIRFLOW-7024] Add the verbose parameter support to SparkSqlOperator (#7676)
- [AIRFLOW-6733] Extend not replace template (#7366)
- [AIRFLOW-7001] Further fix for the MySQL 5.7 UtcDateTime (#7655)
- [AIRFLOW-6014] Handle pods which are preempted & deleted by kubernetes but not restarted (#6606)
- [AIRFLOW-6950] Remove refresh_executor_config from ti.refresh_from_db (#7577)
- [AIRFLOW-7016] Sort dag tags in the UI (#7661)
- [AIRFLOW-6762] Fix link to "Suggest changes on this page" (#7387)
- [AIRFLOW-6948] Remove ASCII Airflow from version command (#7572)
- [AIRFLOW-6767] Correct name for default Athena workgroup (#7394)
- [AIRFLOW-6905] Update pin.svg with new pinwheel (#7524)
- [AIRFLOW-6801] Make use of ImportError.timestamp (#7425)
- [AIRFLOW-6830] Add Subject/MessageAttributes to SNS hook and operator (#7451)
- [AIRFLOW-6630] Resolve handlebars advisory (#7284)
- [AIRFLOW-6945] MySQL 5.7 is used in v1-10-test as an option
- [AIRFLOW-6871] Optimize tree view for large DAGs (#7492)
- [AIRFLOW-7063] Fix dag.clear() slowness caused by count (#7723)
- [AIRFLOW-7023] Remove duplicated package definitions in setup.py (#7675)
- [AIRFLOW-7001] Time zone removed from MySQL TIMESTAMP field inserts
- [AIRFLOW-7105] Unify Secrets Backend method interfaces (#7830)
- Make BaseSecretsBackend.build_path generic (#7948)
- Allow hvac package installation using 'hashicorp' extra (#7915)
- Standardize SecretBackend class names (#7846)
- [AIRFLOW-5705] Make AwsSsmSecretsBackend consistent with VaultBackend (#7753)
- [AIRFLOW-7045] Update SQL query to delete RenderedTaskInstanceFields (#8051)
- Handle DST better in Task Instance tool tips (#8104)

Misc/Internal
^^^^^^^^^^^^^

- Fix Flaky TriggerDAG UI test (#8022)
- Remove unnecessary messages in CI (#7951)
- Fixes too high parallelism in CI (#7947)
- Install version is not persistent in breeze (#7914)
- Fixed automated check for image rebuild (#7912)
- Move Dockerfile to Dockerfile.ci (#7829)
- Generate requirements are now sorted (#8040)
- Change name of the common environment initialization function (#7805)
- Requirements now depend on Python version (#7841)
- Bring back reset db explicitly called at CI entry (#7798)
- Fixes unclean installation of Airflow 1.10 (#7796)
- [AIRFLOW-7029] Use separate docker image for running license check (#7678)
- [AIRFLOW-5842] Switch to Debian buster image as a base (#7647)
- [AIRFLOW-5828] Move build logic out from hooks/build (#7618)
- [AIRFLOW-6839] Even more mypy speed improvements (#7460)
- [AIRFLOW-6820] split breeze into functions (#7433)
- [AIRFLOW-7097] Install gcloud beta components in CI image (#7772)
- [AIRFLOW-7018] fixing travis's job name escaping problem (#7668)
- [AIRFLOW-7054] Breeze has an option now to reset db at entry (#7710)
- [AIRFLOW-7005] Added exec command to Breeze (#7649)
- [AIRFLOW-7015] Detect Dockerhub repo/user when building on Dockerhub (#7673)
- [AIRFLOW-6727] Fix minor bugs in Release Management scripts (#7355)
- [AIRFLOW-7013] Automated check if Breeze image needs to be pulled (#7656)
- [AIRFLOW-7010] Skip in-container checks for Dockerhub builds (#7652)
- [AIRFLOW-7011] Pin JPype release to allow to build 1.10 images
- [AIRFLOW-7006] Fix missing +e in Breeze script (#7648)
- [AIRFLOW-6979] Fix breeze test-target specific test param issue (#7614)
- [AIRFLOW-6932] Add restart-environment command to Breeze
- [AIRFLOW-6919] Make Breeze DAG-test friendly (#7539)
- [AIRFLOW-6838] Introduce real subcommands for Breeze (#7515)
- [AIRFLOW-6763] Make systems tests ready for backport tests (#7389)
- [AIRFLOW-6866] Fix wrong export for Mac on Breeze (#7485)
- [AIRFLOW-6842] Skip fixing ownership on Mac (#7469)
- [AIRFLOW-6841] Fixed unbounded variable on Mac (#7465)
- [AIRFLOW-7067] Pinned version of Apache Airflow (#7730)
- [AIRFLOW-7058] Add support for different DB versions (#7717)
- [AIRFLOW-7002] Get rid of yaml "parser" in bash (#7646)
- [AIRFLOW-6972] Shorter frequently used commands in Breeze (#7608)

Doc only changes
^^^^^^^^^^^^^^^^

- Fix typo for store_serialized_dags config (#7952)
- Fix broken link in README.md (#7893)
- Separate supported Postgres versions with comma (#7892)
- Fix grammar in setup.py (#7877)
- Add Jiajie Zhong to committers list (#8047)
- Update Security doc for 1.10.* for auth backends (#8072)
- Fix Example in config_templates for Secrets Backend (#8074)
- Add backticks in IMAGES.rst command description (#8075)
- Change version_added for store_dag_code config (#8076)
- [AIRFLOW-XXXX] Remove duplicate docs
- [AIRFLOW-XXXX] Remove the defunct limitation of Dag Serialization (#7716)
- [AIRFLOW-XXXX] Add prerequisite tasks for all GCP operators guide (#6049)
- [AIRFLOW-XXXX] Simplify AWS/Azure/Databricks operators listing (#6047)
- [AIRFLOW-XXXX] Add external reference to all GCP operator guide (#6048)
- [AIRFLOW-XXXX] Simplify GCP operators listing
- [AIRFLOW-XXXX] Simplify Qubole operators listing
- [AIRFLOW-XXXX] Add autogenerated TOC (#6038)
- [AIRFLOW-XXXX] Create "Using the CLI" page (#5823)
- [AIRFLOW-XXXX] Group references in one section (#5776)
- [AIRFLOW-XXXX] Fix analytics doc (#5885)
- [AIRFLOW-XXXX] Add S3 Logging section (#6039)
- [AIRFLOW-XXXX] Move Azure Logging section above operators (#6040)
- [AIRFLOW-XXXX] Update temp link to a fixed link (#7715)
- [AIRFLOW-XXXX] Add Updating.md section for 1.10.9 (#7385)
- [AIRFLOW-XXXX] Remove duplication in BaseOperator docstring (#7321)
- [AIRFLOW-XXXX] Update tests info in CONTRIBUTING.rst (#7466)
- [AIRFLOW-XXXX] Small BREEZE.rst update (#7487)
- [AIRFLOW-XXXX] Add instructions for logging to localstack S3 (#7461)
- [AIRFLOW-XXXX] Remove travis config warnings (#7467)
- [AIRFLOW-XXXX] Add communication chapter to contributing (#7204)
- [AIRFLOW-XXXX] Add known issue - example_dags/__init__.py (#7444)
- [AIRFLOW-XXXX] Fix breeze build-docs (#7445)
- [AIRFLOW-XXXX] Less verbose docker builds
- [AIRFLOW-XXXX] Speed up mypy runs (#7421)
- [AIRFLOW-XXXX] Fix location of kubernetes tests (#7373)
- [AIRFLOW-XXXX] Remove quotes from domains in Google Oauth (#4226)
- [AIRFLOW-XXXX] Add explicit info about JIRAs for code-related PRs (#7318)
- [AIRFLOW-XXXX] Fix typo in the word committer (#7392)
- [AIRFLOW-XXXX] Remove duplicated paragraph in docs (#7662)
- Fix reference to KubernetesPodOperator (#8100)


Airflow 1.10.9 (2020-02-07)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- [AIRFLOW-6751] Pin Werkzeug (dependency of a number of our dependencies) to < 1.0.0 (#7377)

Airflow 1.10.8 (2020-02-07)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Failure callback will be called when task is marked failed
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

When task is marked failed by user or task fails due to system failures - on failure call back will be called as part of clean up

See `AIRFLOW-5621 <https://jira.apache.org/jira/browse/AIRFLOW-5621>`_ for details

New Features
^^^^^^^^^^^^

- [AIRFLOW-4026] Add filter by DAG tags (#6489)
- [AIRFLOW-6613] Center dag on graph view load (#7238)
- [AIRFLOW-5843] Add conf option to Add DAG Run view (#7281)
- [AIRFLOW-4495] Allow externally triggered dags to run for future exec dates (#7038)

Improvements
^^^^^^^^^^^^

- [AIRFLOW-6438] Filter DAGs returned by blocked (#7019)
- [AIRFLOW-6666] Resolve js-yaml advisory (#7283)
- [AIRFLOW-6632] Bump dagre-d3 to resolve lodash CVE advisory (#7280)
- [AIRFLOW-6667] Resolve serialize-javascript advisory (#7282)
- [AIRFLOW-6451] self._print_stat() in dag_processing.py should be skipable (#7134)
- [AIRFLOW-6495] Load DAG only once when running a task using StandardTaskRunner (#7090)
- [AIRFLOW-6319] Add support for AWS Athena workgroups (#6871)
- [AIRFLOW-6677] Remove deprecation warning from SQLAlchmey (#7289)
- [AIRFLOW-6428] Fix import path for airflow.utils.dates.days_ago in Example DAGs (#7007)
- [AIRFLOW-6595] Use TaskNotFound exception instead of AirflowException (#7210)
- [AIRFLOW-6620] Mock celery in worker cli test (#7243)
- [AIRFLOW-6608] Change logging level for Bash & PyOperator Env exports
- [AIRFLOW-2279] Clear tasks across DAGs if marked by ExternalTaskMarker (#6633)
- [AIRFLOW-6359] Make Spark status_poll_interval explicit (#6978)
- [AIRFLOW-6359] spark_submit_hook.py status polling interval config (#6909)
- [AIRFLOW-6316] Use exampleinclude directives in tutorial.rst (#6868)
- [AIRFLOW-6519] Make TI logs constants in Webserver configurable (#7113)
- [AIRFLOW-6327] http_hook: Accept json= parameter for payload (#6886)
- [AIRFLOW-6261] flower_basic_auth eligible to _cmd (#6825)
- [AIRFLOW-6238] Filter dags returned by dag_stats
- [AIRFLOW-5616] Switch PrestoHook from pyhive to presto-python-client
- [AIRFLOW-6611] Add proxy_fix configs to default_airflow.cfg (#7236)
- [AIRFLOW-6557] Add test for newly added fields in BaseOperator (#7162)
- [AIRFLOW-6584] Pin cassandra driver (#7194)
- [AIRFLOW-6537] Fix backticks in RST files (#7140)
- [AIRFLOW-4428] Error if exec_date before default_args.start_date in trigger_dag (#6948)
- [AIRFLOW-6330] Show cli help when param blank or typo (#6883)
- [AIRFLOW-4113] Unpin boto3 (#6884)
- [AIRFLOW-6181] Add DebugExecutor (#6740)
- [AIRFLOW-6504] Allow specifying configmap for Airflow Local Setting (#7097)
- [AIRFLOW-6436] Cleanup for Airflow configs doc generator code (#7036)
- [AIRFLOW-6436] Add x_frame_enabled config in config.yml (#7024)
- [AIRFLOW-6436] Create & Automate docs on Airflow Configs (#7015)
- [AIRFLOW-6527] Make send_task_to_executor timeout configurable (#7143)
- [AIRFLOW-6272] Switch from npm to yarnpkg for managing front-end dependencies (#6844)
- [AIRFLOW-6350] Security - spark submit operator logging+exceptions should mask passwords
- [AIRFLOW-6358] Log details of failed task (#6908)
- [AIRFLOW-5149] Skip SLA checks config (#6923)
- [AIRFLOW-6057] Update template_fields of the PythonSensor (#6656)
- [AIRFLOW-4445] Mushroom cloud errors too verbose (#6952)
- [AIRFLOW-6394] Simplify github PR template (#6955)
- [AIRFLOW-5385] spark hook does not work on spark 2.3/2.4 (#6976)

Bug Fixes
^^^^^^^^^

- [AIRFLOW-6345] Ensure arguments to ProxyFix are integers (#6901)
- [AIRFLOW-6576] Fix scheduler crash caused by deleted task with sla misses (#7187)
- [AIRFLOW-6686] Fix syntax error constructing list of process ids (#7298)
- [AIRFLOW-6683] REST API respects store_serialized_dag setting (#7296)
- [AIRFLOW-6553] Add upstream_failed in instance state filter to WebUI (#7159)
- [AIRFLOW-6357] Highlight nodes in Graph UI if task id contains dots (#6904)
- [AIRFLOW-3349] Use None instead of False as value for encoding in StreamLogWriter (#7329)
- [AIRFLOW-6627] Email with incorrect DAG not delivered (#7250)
- [AIRFLOW-6637] Fix Airflow test command in 1.10.x
- [AIRFLOW-6636] Avoid exceptions when printing task instance
- [AIRFLOW-6522] Clear task log file before starting to fix duplication in S3TaskHandler (#7120)
- [AIRFLOW-5501] Make default ``in_cluster`` value in KubernetesPodOperator respect config (#6124)
- [AIRFLOW-6514] Use RUNNING_DEPS to check run from UI (#6367)
- [AIRFLOW-6381] Remove styling based on DAG id from DAGs page (#6985)
- [AIRFLOW-6434] Add return statement back to DockerOperator.execute (#7013)
- [AIRFLOW-2516] Fix mysql deadlocks (#6988)
- [AIRFLOW-6528] Disable flake8 W503 line break before binary operator (#7124)
- [AIRFLOW-6517] Make merge_dicts function recursive (#7111)
- [AIRFLOW-5621] Failure callback is not triggered when marked Failed on UI (#7025)
- [AIRFLOW-6353] Security - ui - add click jacking defense (#6995)
- [AIRFLOW-6348] Security - cli.py is currently printing logs with password (#6915)
- [AIRFLOW-6323] Remove non-ascii letters from default config (#6878)
- [AIRFLOW-6506] Fix do_xcom_push defaulting to True in KubernetesPodOperator (#7122)
- [AIRFLOW-6516] BugFix: airflow.cfg does not exist in Volume Mounts (#7109)
- [AIRFLOW-6427] Fix broken example_qubole_operator dag (#7005)
- [AIRFLOW-6385] BugFix: SlackAPIPostOperator fails when blocks not set (#7022)
- [AIRFLOW-6347] BugFix: Can't get task logs when serialization is enabled (#7092)
- [AIRFLOW-XXXX] Fix downgrade of db migration 0e2a74e0fc9f (#6859)
- [AIRFLOW-6366] Fix migrations for MS SQL Server (#6920)
- [AIRFLOW-5406] Allow spark without kubernetes (#6921)
- [AIRFLOW-6229] SparkSubmitOperator polls forever if status JSON can't… (#6918)
- [AIRFLOW-6352] Security - ui - add login timeout (#6912)
- [AIRFLOW-6397] Ensure sub_process attribute exists before trying to kill it (#6958)
- [AIRFLOW-6400] Fix pytest not working on Windows (#6964)
- [AIRFLOW-6418] Remove SystemTest.skip decorator (#6991)
- [AIRFLOW-6425] Serialization: Add missing DAG parameters to JSON Schema (#7002)

Misc/Internal
^^^^^^^^^^^^^

- [AIRFLOW-6467] Use self.dag i/o creating a new one (#7067)
- [AIRFLOW-6490] Improve time delta comparison in local task job tests (#7083)
- [AIRFLOW-5814] Implementing Presto hook tests (#6491)
- [AIRFLOW-5704] Improve Kind Kubernetes scripts for local testing (#6516)
- [AIRFLOW-XXXX] Move airflow-config-yaml pre-commit before pylint (#7108)
- [AIRFLOW-XXXX] Improve clarity of confirm message (#7110)
- [AIRFLOW-6662] install dumb init (#7300)
- [AIRFLOW-6705] One less chatty message at breeze initialisation (#7326)
- [AIRFLOW-6705] Less chatty integration/backend checks (#7325)
- [AIRFLOW-6662] Switch to --init docker flag for signal propagation (#7278)
- [AIRFLOW-6661] Fail after 50 failing tests (#7277)
- [AIRFLOW-6607] Get rid of old local scripts for Breeze (#7225)
- [AIRFLOW-6589] BAT tests run in pre-commit on bash script changes (#7203)
- [AIRFLOW-6592] Doc build is moved to test phase (#7208)
- [AIRFLOW-6641] Better diagnostics for kubernetes flaky tests (#7261)
- [AIRFLOW-6642] Make local task job test less flaky (#7262)
- [AIRFLOW-6643] Fix flakiness of kerberos tests
- [AIRFLOW-6638] Remove flakiness test from test_serialized_db remove
- [AIRFLOW-6701] Rat is downloaded from stable backup/mirrors (#7323)
- [AIRFLOW-6702] Dumping kind logs to file.io. (#7319)
- [AIRFLOW-6491] Improve handling of Breeze parameters (#7084)
- [AIRFLOW-6470] Avoid pipe to file when do curl (#7063)
- [AIRFLOW-6471] Add pytest-instafail plugin (#7064)
- [AIRFLOW-6462] Limit exported variables in Dockerfile/Breeze (#7057)
- [AIRFLOW-6465] Add bash autocomplete for Airflow in Breeze (#7060)
- [AIRFLOW-6464] Add cloud providers CLI tools in Breeze (#7059)
- [AIRFLOW-6461] Remove silent flags in Dockerfile (#7052)
- [AIRFLOW-6459] Increase verbosity of pytest (#7049)
- [AIRFLOW-6370] Skip Cassandra tests if cluster is not up (#6926)
- [AIRFLOW-6511] Remove BATS docker containers (#7103)
- [AIRFLOW-6475] Remove duplication of volume mount specs in Breeze.. (#7065)
- [AIRFLOW-6489] Add BATS support for Bash unit testing (#7081)
- [AIRFLOW-6387] print details of success/skipped task (#6956)
- [AIRFLOW-6568] Add Emacs related files to .gitignore (#7175)
- [AIRFLOW-6575] Entropy source for CI tests is changed to unblocking (#7185)
- [AIRFLOW-6496] Separate integrations in tests (#7091)
- [AIRFLOW-6634] Set PYTHONPATH in interactive Breeze
- [AIRFLOW-6564] Additional diagnostics information on CI check failure (#7172)
- [AIRFLOW-6383] Add no trailing-whitespace pre-commit hook (#6941)

Doc only changes
^^^^^^^^^^^^^^^^

- [AIRFLOW-XXXX] Consistency fixes in new documentation (#7207)
- [AIRFLOW-XXXX] Improve grammar and structure in FAQ doc (#7291)
- [AIRFLOW-XXXX] Fix email configuration link in CONTRIBUTING.rst (#7311)
- [AIRFLOW-XXXX] Update docs with new BranchPythonOperator behaviour (#4682)
- [AIRFLOW-XXXX] Fix Typo in scripts/ci/ci_run_airflow_testing.sh (#7235)
- [AIRFLOW-XXXX] Screenshot showing disk space configuration for OSX (#7226)
- [AIRFLOW-XXXX] Add mentoring information to contributing docs (#7202)
- [AIRFLOW-XXXX] Add rebase info to contributing (#7201)
- [AIRFLOW-XXXX] Increase verbosity of static checks in CI (#7200)
- [AIRFLOW-XXXX] Adds branching strategy to documentation (#7193)
- [AIRFLOW-XXXX] Move email configuration from the concept page (#7189)
- [AIRFLOW-XXXX] Update task lifecycle diagram (#7161)
- [AIRFLOW-XXXX] Fix reference in concepts doc (#7135)
- [AIRFLOW-XXXX] Clear debug docs (#7104)
- [AIRFLOW-XXXX] Fix typos and broken links in development docs (#7086)
- [AIRFLOW-XXXX] Clarify wait_for_downstream and execution_date (#6999)
- [AIRFLOW-XXXX] Add ``airflow dags show`` command guide (#7014)
- [AIRFLOW-XXXX] Update operation chaining documentation (#7018)
- [AIRFLOW-XXXX] Add ``.autoenv_leave.zsh`` to .gitignore (#6986)
- [AIRFLOW-XXXX] Fix development packages installation instructions (#6942)
- [AIRFLOW-XXXX] Update committers list (#7212)
- [AIRFLOW-XXXX] Move UPDATING changes into correct versions (#7166)
- [AIRFLOW-XXXX] Add Documentation for check_slas flag (#6974)
- [AIRFLOW-XXXX] Fix gcp keyfile_dict typo (#6962)
- [AIRFLOW-XXXX] Add tips for writing a note in UPDATIND.md (#6960)
- [AIRFLOW-XXXX] Add note warning that bash>4.0 is required for docs build script (#6947)
- [AIRFLOW-XXXX] Add autoenv to gitignore (#6946)
- [AIRFLOW-XXXX] Fix GCSTaskHandler Comment Typo (#6928)
- [AIRFLOW-XXXX] Fix broken DAG Serialization Link (#6891)
- [AIRFLOW-XXXX] Add versions_added field to configs


Airflow 1.10.7 (2019-12-24)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Changes in experimental API execution_date microseconds replacement
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default behavior was to strip the microseconds (and milliseconds, etc) off of all dag runs triggered by
by the experimental REST API.  The default behavior will change when an explicit execution_date is
passed in the request body.  It will also now be possible to have the execution_date generated, but
keep the microseconds by sending ``replace_microseconds=false`` in the request body.  The default
behavior can be overridden by sending ``replace_microseconds=true`` along with an explicit execution_date

Infinite pool size and pool size query optimization
"""""""""""""""""""""""""""""""""""""""""""""""""""

Pool size can now be set to -1 to indicate infinite size (it also includes
optimization of pool query which lead to poor task n^2 performance of task
pool queries in MySQL).

Viewer won't have edit permissions on DAG view.
"""""""""""""""""""""""""""""""""""""""""""""""

Google Cloud Storage Hook
"""""""""""""""""""""""""

The ``GoogleCloudStorageDownloadOperator`` can either write to a supplied ``filename`` or
return the content of a file via xcom through ``store_to_xcom_key`` - both options are mutually exclusive.

New Features
^^^^^^^^^^^^

- [AIRFLOW-5088][AIP-24] Persisting serialized DAG in DB for webserver scalability (#5992)
- [AIRFLOW-6083] Adding ability to pass custom configuration to AWS Lambda client. (#6678)
- [AIRFLOW-5117] Automatically refresh EKS API tokens when needed (#5731)
- [AIRFLOW-5118] Add ability to specify optional components in DataprocClusterCreateOperator (#5821)
- [AIRFLOW-5681] Allow specification of a tag or hash for the git_sync init container (#6350)
- [AIRFLOW-6025] Add label to uniquely identify creator of Pod (#6621)
- [AIRFLOW-4843] Allow orchestration via Docker Swarm (SwarmOperator) (#5489)
- [AIRFLOW-5751] add get_uri method to Connection (#6426)
- [AIRFLOW-6056] Allow EmrAddStepsOperator to accept job_flow_name as alternative to job_flow_id (#6655)
- [AIRFLOW-2694] Declare permissions in DAG definition (#4642)
- [AIRFLOW-4940] Add DynamoDB to S3 operator (#5663)
- [AIRFLOW-4161] BigQuery to MySQL Operator (#5711)
- [AIRFLOW-6041] Add user agent to the Discovery API client (#6636)
- [AIRFLOW-6089] Reorder setup.py dependencies and add ci (#6681)
- [AIRFLOW-5921] Add bulk_load_custom to MySqlHook (#6575)
- [AIRFLOW-5854] Add support for ``tty`` parameter in Docker related operators (#6542)
- [AIRFLOW-4758] Add GcsToGDriveOperator operator (#5822)

Improvements
^^^^^^^^^^^^

- [AIRFLOW-3656] Show doc link for the current installed version (#6690)
- [AIRFLOW-5665] Add path_exists method to SFTPHook (#6344)
- [AIRFLOW-5729] Make InputDataConfig optional in Sagemaker's training config (#6398)
- [AIRFLOW-5045] Add ability to create Google Dataproc cluster with custom image from a different project (#5752)
- [AIRFLOW-6132] Allow to pass in tags for the AzureContainerInstancesOperator (#6694)
- [AIRFLOW-5945] Make inbuilt OperatorLinks work when using Serialization (#6715)
- [AIRFLOW-5947] Make the JSON backend pluggable for DAG Serialization (#6630)
- [AIRFLOW-6239] Filter dags return by last_dagruns (to only select visible dags, not all dags) (#6804)
- [AIRFLOW-6095] Filter dags returned by task_stats (to only select visible dags, not all dags) (#6684)
- [AIRFLOW-4482] Add execution_date to "trigger DagRun" API response (#5260)
- [AIRFLOW-1076] Add get method for template variable accessor (#6793)
- [AIRFLOW-5194] Add error handler to action log (#5883)
- [AIRFLOW-5936] Allow explicit get_pty in SSHOperator (#6586)
- [AIRFLOW-5474] Add Basic auth to Druid hook (#6095)
- [AIRFLOW-5726] Allow custom filename in RedshiftToS3Transfer (#6396)
- [AIRFLOW-5834] Option to skip serve_logs process with ``airflow worker`` (#6709)
- [AIRFLOW-5583] Extend the 'DAG Details' page to display the start_date / end_date (#6235)
- [AIRFLOW-6250] Ensure on_failure_callback always has a populated context (#6812)
- [AIRFLOW-6222] http hook logs response body for any failure (#6779)
- [AIRFLOW-6260] Drive _cmd config option by env var (``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD`` for example) (#6801)
- [AIRFLOW-6168] Allow proxy_fix middleware of webserver to be configurable (#6723)
- [AIRFLOW-5931] Use os.fork when appropriate to speed up task execution. (#6627)
- [AIRFLOW-4145] Allow RBAC roles permissions, ViewMenu to be over-rideable (#4960)
- [AIRFLOW-5928] Hive hooks load_file short circuit (#6582)
- [AIRFLOW-5313] Add params support for awsbatch_operator (#5900)
- [AIRFLOW-2227] Add delete method to Variable class (#4963)
- [AIRFLOW-5082] Add subject in AwsSnsHook (#5694)
- [AIRFLOW-5715] Make email, owner context available (#6385)
- [AIRFLOW-5345] Allow SqlSensor's hook to be customized by subclasses (#5946)
- [AIRFLOW-5417] Fix DB disconnects during webserver startup (#6023)
- [AIRFLOW-5730] Enable get_pandas_df on PinotDbApiHook (#6399)
- [AIRFLOW-3235] Add list function in AzureDataLakeHook (#4070)
- [AIRFLOW-5442] implementing get_pandas_df method for druid broker hook (#6057)
- [AIRFLOW-5883] Improve count() queries in a few places (#6532)
- [AIRFLOW-5811] Add metric for externally killed task count (#6466)
- [AIRFLOW-5758] Support the custom cursor classes for the PostgreSQL hook (#6432)
- [AIRFLOW-5766] Use httpbin.org in http_default (#6438)
- [AIRFLOW-5798] Set default ExternalTaskSensor.external_task_id (#6431)
- [AIRFLOW-5643] Reduce duplicated logic in S3Hook (#6313)
- [AIRFLOW-5562] Skip grant single DAG permissions for Admin role. (#6199)
- [AIRFLOW-6192] Stop creating Hook from SFTPSensor.__init__ (#6748)
- [AIRFLOW-5749][AIRFLOW-4162] Support the "blocks" component for the Slack operators (#6418)
- [AIRFLOW-5693] Support the "blocks" component for the Slack messages (#6364)
- [AIRFLOW-5714] Collect SLA miss emails only from tasks missed SLA (#6384)
- [AIRFLOW-5049] Add validation for src_fmt_configs in bigquery hook (#5671)
- [AIRFLOW-6177] Log DAG processors timeout event at error level, not info (#6731)
- [AIRFLOW-6180] Improve kerberos init in pytest conftest (#6735)
- [AIRFLOW-6159] Change logging level of the heartbeat message to DEBUG (#6716)
- [AIRFLOW-6144] Improve the log message of Airflow scheduler (#6710)
- [AIRFLOW-6045] Error on failed execution of compile_assets (#6640)
- [AIRFLOW-5144] Add confirmation on delete button click (#6745)
- [AIRFLOW-6099] Add host name to task runner log (#6688)
- [AIRFLOW-5915] Add support for the new documentation theme (#6563)
- [AIRFLOW-5897] Allow setting -1 as pool slots value in webserver (#6550)
- [AIRFLOW-5888] Use psycopg2-binary for postgres operations (#6533)
- [AIRFLOW-5870] Allow -1 for pool size and optimise pool query (#6520)

Bug Fixes
^^^^^^^^^

- [AIRFLOW-XXX] Bump Jira version to fix issue with async
- [AIRFLOW-XXX] Add encoding to fix Cyrillic output when reading back task logs (#6631)
- [AIRFLOW-5304] Fix extra links in BigQueryOperator with multiple queries (#5906)
- [AIRFLOW-6268] Prevent (expensive) ajax calls on home page when no dags visible (#6839)
- [AIRFLOW-6259] Reset page to 1 with each new search for dags (#6828)
- [AIRFLOW-6185] SQLAlchemy Connection model schema not aligned with Alembic schema (#6754)
- [AIRFLOW-3632] Only replace microseconds if execution_date is None in trigger_dag REST API (#6380)
- [AIRFLOW-5458] Bump Flask-AppBuilder to 2.2.0 (for Python >= 3.6) (#6607)
- [AIRFLOW-5072] gcs_hook should download files once (#5685)
- [AIRFLOW-5744] Environment variables not correctly set in Spark submit operator (#6796)
- [AIRFLOW-3189] Remove schema from DbHook.get_uri response if None (#6833)
- [AIRFLOW-6195] Fixed TaskInstance attrs not correct on  UI (#6758)
- [AIRFLOW-5889] Make polling for AWS Batch job status more resilient (#6765)
- [AIRFLOW-6043] Fix bug in UI when "filtering by root" to display section of dag  (#6638)
- [AIRFLOW-6033] Fix UI Crash at "Landing Times" when task_id is changed (#6635)
- [AIRFLOW-3745] Fix viewer not able to view dag details (#4569)
- [AIRFLOW-6175] Fixes bug when tasks get stuck in "scheduled" state (#6732)
- [AIRFLOW-5463] Make Variable.set when replacing an atomic operation (#6807)
- [AIRFLOW-5582] Add get_autocommit to JdbcHook (#6232)
- [AIRFLOW-5867] Fix webserver unit_test_mode data type (#6517)
- [AIRFLOW-5819] Update AWSBatchOperator default value (#6473)
- [AIRFLOW-5709] Fix regression in setting custom operator resources. (#6331)
- [AIRFLOW-5658] Fix broken navigation links (#6374)
- [AIRFLOW-5727] SqoopHook: Build --connect parameter only if port/schema are defined (#6397)
- [AIRFLOW-5695] use RUNNING_DEPS to check run from UI (#6367)
- [AIRFLOW-6254] obscure conn extra in logs (#6817)
- [AIRFLOW-4824] Add charset handling for SqlAlchemy engine for MySqlHook (#6816)
- [AIRFLOW-6091] Add flushing in execute method for BigQueryCursor (#6683)
- [AIRFLOW-6256] Ensure Jobs table is cleared when resetting DB (#6818)
- [AIRFLOW-5224] Add encoding parameter to GoogleCloudStorageToBigQuery (#6297)
- [AIRFLOW-5179] Remove top level __init__.py (#5818)
- [AIRFLOW-5660] Attempt to find the task in DB from Kubernetes pod labels (#6340)
- [AIRFLOW-6241] Fix typo in airflow/gcp/operator/dataflow.py (#6806)
- [AIRFLOW-6171] Apply .airflowignore to correct subdirectories (#6784)
- [AIRFLOW-6018] Display task instance in table during backfilling (#6612)
- [AIRFLOW-6189] Reduce the maximum test duration to 8 minutes (#6744)
- [AIRFLOW-6141] Remove ReadyToRescheduleDep if sensor mode == poke (#6704)
- [AIRFLOW-6054] Add a command that starts the database consoles (#6653)
- [AIRFLOW-6047] Simplify the logging configuration template (#6644)
- [AIRFLOW-6017] Exclude PULL_REQUEST_TEMPLATE.md from RAT check (#6611)
- [AIRFLOW-4560] Fix Tez queue parameter name in mapred_queue (#5315)
- [AIRFLOW-2143] Fix TaskTries graph counts off-by-1 (#6526)
- [AIRFLOW-5873] KubernetesPodOperator fixes and test (#6523)
- [AIRFLOW-5869] BugFix: Some Deserialized tasks have no start_date (#6519)
- [AIRFLOW-4020] Remove DAG edit permissions from Viewer role (#4845)
- [AIRFLOW-6263] Fix broken WinRM integration (#6832)
- [AIRFLOW-5836] Pin azure-storage-blob version to <12 (#6486)
- [AIRFLOW-4488] Fix typo for non-RBAC UI in max_active_runs_per_dag (#6778)
- [AIRFLOW-5942] Pin PyMSSQL to <3.0 (#6592)
- [AIRFLOW-5451] SparkSubmitHook don't set default namespace (#6072)
- [AIRFLOW-6271] Printing log files read during load_test_config (#6842)
- [AIRFLOW-6308] Unpin Kombu for Python 3

Misc/Internal
^^^^^^^^^^^^^

- [AIRFLOW-6009] Switch off travis_wait for regular tests (#6600)
- [AIRFLOW-6226] Always reset warnings in tests
- [AIRFLOW-XXX] Remove cyclic imports and pylint hacks in Serialization (#6601)
- [AIRFLOW-XXX] Bump npm from 6.4.1 to 6.13.4 in /airflow/www (#6815)
- [AIRFLOW-XXX] Remove executable permission from file
- [AIRFLOW-XXX] Group AWS & Azure dependencies (old ``[emr]`` etc. extra still work)
- [AIRFLOW-5487] Fix unused warning var (#6111)
- [AIRFLOW-5925] Relax ``funcsigs`` and psutil version requirements (#6580)
- [AIRFLOW-5740] Fix Transient failure in Slack test (#6407)
- [AIRFLOW-6058] Running tests with pytest (#6472)
- [AIRFLOW-6066] Added pre-commit checks for accidental debug stmts (#6662)
- [AIRFLOW-6060] Improve conf_vars context manager (#6658)
- [AIRFLOW-6044] Standardize the Code Structure in kube_pod_operator.py (#6639)
- [AIRFLOW-4940] Simplify tests of DynamoDBToS3Operator (#6836)
- [AIRFLOW-XXX] Update airflow-jira release management script (#6772)
- [AIRFLOW-XXX] Add simple guidelines to unit test writing (#6846)
- [AIRFLOW-6309] Fix stable build on Travis

Doc only changes
^^^^^^^^^^^^^^^^

- [AIRFLOW-6211] Doc how to use conda for local virtualenv (#6766)
- [AIRFLOW-5855] Fix broken reference in custom operator doc (#6508)
- [AIRFLOW-5875] Fix typo in example_qubole_operator.py (#6525)
- [AIRFLOW-5702] Fix common docstring issues (#6372)
- [AIRFLOW-5640] Document and test ``email`` parameters of BaseOperator (#6315)
- [AIRFLOW-XXX] Improve description OpenFaaS Hook (#6187)
- [AIRFLOW-XXX] GSoD: How to make DAGs production ready (#6515)
- [AIRFLOW-XXX] Use full command in examples (#5973)
- [AIRFLOW-XXX] Update docs to accurately describe the precedence of remote and local logs (#5607)
- [AIRFLOW-XXX] Fix example "extras" field in mysql connect doc (#5285)
- [AIRFLOW-XXX] Fix wrong inline code highlighting in docs (#5309)
- [AIRFLOW-XXX] Group executors in one section (#5834)
- [AIRFLOW-XXX] Add task lifecycle diagram to documentation (#6762)
- [AIRFLOW-XXX] Highlight code blocks (#6243)
- [AIRFLOW-XXX] Documents about task_concurrency and pool (#5262)
- [AIRFLOW-XXX] Fix incorrect docstring parameter (#6649)
- [AIRFLOW-XXX] Add link to XCom section in concepts.rst (#6791)
- [AIRFLOW-XXX] Update kubernetes doc with correct path (#6774)
- [AIRFLOW-XXX] Add information how to configure pytest runner (#6736)
- [AIRFLOW-XXX] More GSOD improvements (#6585)
- [AIRFLOW-XXX] Clarified a grammatically incorrect sentence (#6667)
- [AIRFLOW-XXX] Add notice for Mesos Executor deprecation in docs (#6712)
- [AIRFLOW-XXX] Update list of pre-commits (#6603)
- [AIRFLOW-XXX] Updates to Breeze documentation from GSOD (#6285)
- [AIRFLOW-XXX] Clarify daylight savings time behavior (#6324)
- [AIRFLOW-XXX] GSoD: Adding 'Create a custom operator' doc (#6348)
- [AIRFLOW-XXX] Add resources & links to CONTRIBUTING.rst (#6405)
- [AIRFLOW-XXX] Update chat channel details from gitter to slack (#4149)
- [AIRFLOW-XXX] Add logo info to readme (#6349)
- [AIRFLOW-XXX] Fixed case problem with CONTRIBUTING.rst (#6329)
- [AIRFLOW-XXX] Google Season of Docs updates to CONTRIBUTING doc (#6283)


Airflow 1.10.6 (2019-10-28)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

BaseOperator::render_template function signature changed
""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previous versions of the ``BaseOperator::render_template`` function required an ``attr`` argument as the first
positional argument, along with ``content`` and ``context``. This function signature was changed in 1.10.6 and
the ``attr`` argument is no longer required (or accepted).

In order to use this function in subclasses of the ``BaseOperator``\ , the ``attr`` argument must be removed:

.. code-block:: python

   result = self.render_template("myattr", self.myattr, context)  # Pre-1.10.6 call
   # ...
   result = self.render_template(self.myattr, context)  # Post-1.10.6 call

Changes to ``aws_default`` Connection's default region
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The region of Airflow's default connection to AWS (\ ``aws_default``\ ) was previously
set to ``us-east-1`` during installation.

The region now needs to be set manually, either in the connection screens in
Airflow, via the ``~/.aws`` config files, or via the ``AWS_DEFAULT_REGION`` environment
variable.

Some DAG Processing metrics have been renamed
"""""""""""""""""""""""""""""""""""""""""""""

The following metrics are deprecated and won't be emitted in Airflow 2.0:


* ``scheduler.dagbag.errors`` and ``dagbag_import_errors`` -- use ``dag_processing.import_errors`` instead
* ``dag_file_processor_timeouts`` -- use ``dag_processing.processor_timeouts`` instead
* ``collect_dags`` -- use ``dag_processing.total_parse_time`` instead
* ``dag.loading-duration.<basename>`` -- use ``dag_processing.last_duration.<basename>`` instead
* ``dag_processing.last_runtime.<basename>`` -- use ``dag_processing.last_duration.<basename>`` instead

New Features
^^^^^^^^^^^^

- [AIRFLOW-4908] Implement BigQuery Hooks/Operators for update_dataset, patch_dataset and get_dataset (#5546)
- [AIRFLOW-4741] Optionally report task errors to Sentry (#5407)
- [AIRFLOW-4939] Add default_task_retries config (#5570)
- [AIRFLOW-5508] Add config setting to limit which StatsD metrics are emitted (#6130)
- [AIRFLOW-4222] Add cli autocomplete for bash & zsh (#5789)
- [AIRFLOW-3871] Operators template fields can now render fields inside objects (#4743)

Improvements
^^^^^^^^^^^^

- [AIRFLOW-5127] Gzip support for CassandraToGoogleCloudStorageOperator (#5738)
- [AIRFLOW-5125] Add gzip support for AdlsToGoogleCloudStorageOperator (#5737)
- [AIRFLOW-5124] Add gzip support for S3ToGoogleCloudStorageOperator (#5736)
- [AIRFLOW-5653] Log AirflowSkipException in task instance log to make it clearer why tasks might be skipped (#6330)
- [AIRFLOW-5343] Remove legacy SQLAlchmey pessimistic pool disconnect handling (#6034)
- [AIRFLOW-5561] Relax httplib2 version required for gcp extra (#6194)
- [AIRFLOW-5657] Update the upper bound for dill dependency (#6334)
- [AIRFLOW-5292] Allow ECSOperator to tag tasks (#5891)
- [AIRFLOW-4939] Simplify Code for Default Task Retries (#6233)
- [AIRFLOW-5126] Read ``aws_session_token`` in extra_config of the aws hook (#6303)
- [AIRFLOW-5636] Allow adding or overriding existing Operator Links (#6302)
- [AIRFLOW-4965] Handle quote exceptions in GCP AI operators (v1.10) (#6304)
- [AIRFLOW-3783] Speed up Redshift to S3 unload with HEADERs (#6309)
- [AIRFLOW-3388] Add support to Array Jobs for AWS Batch Operator (#6153)
- [AIRFLOW-4574] add option to provide private_key in SSHHook (#6104) (#6163)
- [AIRFLOW-5530] Fix typo in AWS SQS sensors (#6012)
- [AIRFLOW-5445] Reduce the required resources for the Kubernetes's sidecar (#6062)
- [AIRFLOW-5443] Use alpine image in Kubernetes's sidecar (#6059)
- [AIRFLOW-5344] Add --proxy-user parameter to SparkSubmitOperator (#5948)
- [AIRFLOW-3888] HA for Hive metastore connection (#4708)
- [AIRFLOW-5269] Reuse session in Scheduler Job from health endpoint (#5873)
- [AIRFLOW-5153] Option to force delete non-empty BQ datasets (#5768)
- [AIRFLOW-4443] Document LatestOnly behavior for external trigger (#5214)
- [AIRFLOW-2891] Make DockerOperator container_name be templateable (#5696)
- [AIRFLOW-2891] allow configurable docker_operator container name (#5689)
- [AIRFLOW-4285] Update task dependency context definition and usage (#5079)
- [AIRFLOW-5142] Fixed flaky Cassandra test (#5758)
- [AIRFLOW-5218] Less polling of AWS Batch job status (#5825)
- [AIRFLOW-4956] Fix LocalTaskJob heartbeat log spamming (#5589)
- [AIRFLOW-3160] Load latest_dagruns asynchronously on home page (#5339)
- [AIRFLOW-5560] Allow no confirmation on reset dags in ``airflow backfill`` command (#6195)
- [AIRFLOW-5280] conn: Remove aws_default's default region name (#5879)
- [AIRFLOW-5528] end_of_log_mark should not be a log record (#6159)
- [AIRFLOW-5526] Update docs configuration due to migration of GCP docs (#6154)
- [AIRFLOW-4835] Refactor operator render_template (#5461)

Bug Fixes
^^^^^^^^^

- [AIRFLOW-5459] Use a dynamic tmp location in Dataflow operator (#6078)
- [Airflow 4923] Fix Databricks hook leaks API secret in logs (#5635)
- [AIRFLOW-5133] Keep original env state in provide_gcp_credential_file (#5747)
- [AIRFLOW-5497] Update docstring in ``airflow/utils/dag_processing.py`` (#6314)
- Revert/and then rework "[AIRFLOW-4797] Improve performance and behaviour of zombie detection (#5511)" to improve performance (#5908)
- [AIRFLOW-5634] Don't allow editing of DagModelView (#6308)
- [AIRFLOW-4309] Remove Broken Dag error after Dag is deleted (#6102)
- [AIRFLOW-5387] Fix "show paused" pagination bug (#6100)
- [AIRFLOW-5489] Remove unneeded assignment of variable (#6106)
- [AIRFLOW-5491] mark_tasks pydoc is incorrect (#6108)
- [AIRFLOW-5492] added missing docstrings (#6107)
- [AIRFLOW-5503] Fix tree view layout on HDPI screen (#6125)
- [AIRFLOW-5481] Allow Deleting Renamed DAGs (#6101)
- [AIRFLOW-3857] spark_submit_hook cannot kill driver pod in Kubernetes (#4678)
- [AIRFLOW-4391] Fix tooltip for None-State Tasks in 'Recent Tasks' (#5909)
- [AIRFLOW-5554] Require StatsD 3.3.0 minimum (#6185)
- [AIRFLOW-5306] Fix the display of links when they contain special characters (#5904)
- [AIRFLOW-3705] Fix PostgresHook get_conn to use conn_name_attr (#5841)
- [AIRFLOW-5581] Cleanly shutdown KubernetesJobWatcher for safe Scheduler shutdown on SIGTERM (#6237)
- [AIRFLOW-5634] Don't allow disabled fields to be edited in DagModelView (#6307)
- [AIRFLOW-4833] Allow to set Jinja env options in DAG declaration (#5943)
- [AIRFLOW-5408] Fix env variable name in Kubernetes template (#6016)
- [AIRFLOW-5102] Worker jobs should terminate themselves if they can't heartbeat (#6284)
- [AIRFLOW-5572] Clear task reschedules when clearing task instances (#6217)
- [AIRFLOW-5543] Fix tooltip disappears in tree and graph view (RBAC UI) (#6174)
- [AIRFLOW-5444] Fix action_logging so that request.form for POST is logged (#6064)
- [AIRFLOW-5484] fix PigCliHook has incorrect named parameter (#6112)
- [AIRFLOW-5342] Fix MSSQL breaking task_instance db migration (#6014)
- [AIRFLOW-5556] Add separate config for timeout from scheduler dag processing (#6186)
- [AIRFLOW-4858] Deprecate "Historical convenience functions" in airflow.configuration (#5495) (#6144)
- [AIRFLOW-774] Fix long-broken DAG parsing StatsD metrics (#6157)
- [AIRFLOW-5419] Use ``sudo`` to kill cleared tasks when running with impersonation (#6026) (#6176)
- [AIRFLOW-5537] Yamllint is not needed as dependency on host
- [AIRFLOW-5536] Better handling of temporary output files
- [AIRFLOW-5535] Fix name of VERBOSE parameter
- [AIRFLOW-5519] Fix sql_to_gcs operator missing multi-level default args by adding apply_defaults decorator  (#6146)
- [AIRFLOW-5210] Make finding template files more efficient (#5815)
- [AIRFLOW-5447] Scheduler stalls because second watcher thread in default args (#6129)

Doc-only changes
^^^^^^^^^^^^^^^^

- [AIRFLOW-5574] Fix Google Analytics script loading (#6218)
- [AIRFLOW-5588] Add Celery's architecture diagram (#6247)
- [AIRFLOW-5521] Fix link to GCP documentation (#6150)
- [AIRFLOW-5398] Update contrib example DAGs to context manager (#5998)
- [AIRFLOW-5268] Apply same DAG naming conventions as in literature (#5874)
- [AIRFLOW-5101] Fix inconsistent owner value in examples (#5712)
- [AIRFLOW-XXX] Fix typo - AWS DynamoDB Hook (#6319)
- [AIRFLOW-XXX] Fix Documentation for adding extra Operator Links (#6301)
- [AIRFLOW-XXX] Add section on task lifecycle & correct casing in docs (#4681)
- [AIRFLOW-XXX] Make it clear that 1.10.5 was not accidentally omitted from UPDATING.md (#6240)
- [AIRFLOW-XXX] Improve format in code-block directives (#6242)
- [AIRFLOW-XXX] Format Sendgrid docs (#6245)
- [AIRFLOW-XXX] Update to new logo (#6066)
- [AIRFLOW-XXX] Typo in FAQ - schedule_interval (#6291)
- [AIRFLOW-XXX] Add message about breaking change in DAG#get_task_instances in 1.10.4 (#6226)
- [AIRFLOW-XXX] Fix incorrect units in docs for metrics using Timers (#6152)
- [AIRFLOW-XXX] Fix backtick issues in .rst files & Add Precommit hook (#6162)
- [AIRFLOW-XXX] Update documentation about variables forcing answer (#6158)
- [AIRFLOW-XXX] Add a third way to configure authorization (#6134)
- [AIRFLOW-XXX] Add example of running pre-commit hooks on single file (#6143)
- [AIRFLOW-XXX] Add information about default pool to docs (#6019)
- [AIRFLOW-XXX] Make Breeze The default integration test environment (#6001)

Misc/Internal
^^^^^^^^^^^^^

- [AIRFLOW-5687] Upgrade pip to 19.0.2 in CI build pipeline (#6358) (#6361)
- [AIRFLOW-5533] Fixed failing CRON build (#6167)
- [AIRFLOW-5130] Use GOOGLE_APPLICATION_CREDENTIALS constant from library (#5744)
- [AIRFLOW-5369] Adds interactivity to pre-commits (#5976)
- [AIRFLOW-5531] Replace deprecated log.warn() with log.warning() (#6165)
- [AIRFLOW-4686] Make dags Pylint compatible (#5753)
- [AIRFLOW-4864] Remove calls to load_test_config (#5502)
- [AIRFLOW-XXX] Pin version of mypy so we are stable over time (#6198)
- [AIRFLOW-XXX] Add tests that got missed from #5127
- [AIRFLOW-4928] Move config parses to class properties inside DagBag (#5557)
- [AIRFLOW-5003] Making AWS Hooks pylint compatible (#5627)
- [AIRFLOW-5580] Add base class for system test (#6229)

Airflow 1.10.5 (2019-09-04)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

New Features
^^^^^^^^^^^^

- [AIRFLOW-1498] Add feature for users to add Google Analytics to Airflow UI (#5850)
- [AIRFLOW-4074] Add option to add labels to Dataproc jobs (#5606)
- [AIRFLOW-4846] Allow specification of an existing secret containing git credentials for init containers (#5475)

Improvements
^^^^^^^^^^^^

- [AIRFLOW-5335] Update GCSHook methods so they need min IAM perms (#5939)
- [AIRFLOW-2692] Allow AWS Batch Operator to use templates in job_name parameter (#3557)
- [AIRFLOW-4768] Add Timeout parameter in example_gcp_video_intelligence (#5862)
- [AIRFLOW-5165] Make Dataproc highly available (#5781)
- [AIRFLOW-5139] Allow custom ES configs (#5760)
- [AIRFLOW-5340] Fix GCP DLP example (#594)
- [AIRFLOW-5211] Add pass_value to template_fields BigQueryValueCheckOperator (#5816)
- [AIRFLOW-5113] Support icon url in slack web hook (#5724)
- [AIRFLOW-4230] bigquery schema update options should be a list (#5766)
- [AIRFLOW-1523] Clicking on Graph View should display related DAG run (#5866)
- [AIRFLOW-5027] Generalized CloudWatch log grabbing for ECS and SageMaker operators (#5645)
- [AIRFLOW-5244] Add all possible themes to default_webserver_config.py (#5849)
- [AIRFLOW-5245] Add more metrics around the scheduler (#5853)
- [AIRFLOW-5048] Improve display of Kubernetes resources (#5665)
- [AIRFLOW-5284] Replace deprecated log.warn by log.warning (#5881)
- [AIRFLOW-5276] Remove unused helpers from airflow.utils.helpers (#5878)
- [AIRFLOW-4316] Support setting kubernetes_environment_variables config section from env var (#5668)

Bug fixes
^^^^^^^^^

- [AIRFLOW-5168] Fix Dataproc operators that failed in 1.10.4 (#5928)
- [AIRFLOW-5136] Fix Bug with Incorrect template_fields in DataProc{*} Operators (#5751)
- [AIRFLOW-5169] Pass GCP Project ID explicitly to StorageClient in GCSHook (#5783)
- [AIRFLOW-5302] Fix bug in none_skipped Trigger Rule (#5902)
- [AIRFLOW-5350] Fix bug in the num_retires field in BigQueryHook (#5955)
- [AIRFLOW-5145] Fix rbac ui presents false choice to encrypt or not encrypt variable values (#5761)
- [AIRFLOW-5104] Set default schedule for GCP Transfer operators (#5726)
- [AIRFLOW-4462] Use datetime2 column types when using MSSQL backend (#5707)
- [AIRFLOW-5282] Add default timeout on kubeclient & catch HTTPError (#5880)
- [AIRFLOW-5315] TaskInstance not updating from DB when user changes executor_config (#5926)
- [AIRFLOW-4013] Mark success/failed is picking all execution date (#5616)
- [AIRFLOW-5152] Fix autodetect default value in GoogleCloudStorageToBigQueryOperator(#5771)
- [AIRFLOW-5100] Airflow scheduler does not respect safe mode setting (#5757)
- [AIRFLOW-4763] Allow list in DockerOperator.command (#5408)
- [AIRFLOW-5260] Allow empty uri arguments in connection strings (#5855)
- [AIRFLOW-5257] Fix ElasticSearch log handler errors when attempting to close logs (#5863)
- [AIRFLOW-1772] Google Updated Sensor doesn't work with CRON expressions (#5730)
- [AIRFLOW-5085] When you run kubernetes git-sync test from TAG, it fails (#5699)
- [AIRFLOW-5258] ElasticSearch log handler, has 2 times of hours (%H and %I) in _clean_execution_dat (#5864)
- [AIRFLOW-5348] Escape Label in deprecated chart view when set via JS (#5952)
- [AIRFLOW-5357] Fix Content-Type for exported variables.json file (#5963)
- [AIRFLOW-5109] Fix process races when killing processes (#5721)
- [AIRFLOW-5240] Latest version of Kombu is breaking Airflow for py2

Misc/Internal
^^^^^^^^^^^^^

- [AIRFLOW-5111] Remove apt-get upgrade from the Dockerfile (#5722)
- [AIRFLOW-5209] Fix Documentation build (#5814)
- [AIRFLOW-5083] Check licence image building can be faster and moved to before-install (#5695)
- [AIRFLOW-5119] Cron job should always rebuild everything from scratch (#5733)
- [AIRFLOW-5108] In the CI local environment long-running kerberos might fail sometimes (#5719)
- [AIRFLOW-5092] Latest Python image should be pulled locally in force_pull_and_build (#5705)
- [AIRFLOW-5225] Consistent licences can be added automatically for all JS files (#5827)
- [AIRFLOW-5229] Add licence to all other file types (#5831)
- [AIRFLOW-5227] Consistent licences for all .sql files (#5829)
- [AIRFLOW-5161] Add pre-commit hooks to run static checks for only changed files (#5777)
- [AIRFLOW-5159] Optimise checklicence image build (do not build if not needed) (#5774)
- [AIRFLOW-5263] Show diff on failure of pre-commit checks (#5869)
- [AIRFLOW-5204] Shell files should be checked with shellcheck and have identical licence (#5807)
- [AIRFLOW-5233] Check for consistency in whitespace (tabs/eols) and common problems (#5835)
- [AIRFLOW-5247] Getting all dependencies from NPM can be moved up in Dockerfile (#5870)
- [AIRFLOW-5143] Corrupted rat.jar became part of the Docker image (#5759)
- [AIRFLOW-5226] Consistent licences for all html JINJA templates (#5828)
- [AIRFLOW-5051] Coverage is not properly reported in the new CI system (#5732)
- [AIRFLOW-5239] Small typo and incorrect tests in CONTRIBUTING.md (#5844)
- [AIRFLOW-5287] Checklicence base image is not pulled (#5886)
- [AIRFLOW-5301] Some not-yet-available files from breeze are committed to master (#5901)
- [AIRFLOW-5285] Pre-commit pylint runs over todo files (#5884)
- [AIRFLOW-5288] Temporary container for static checks should be auto-removed (#5887)
- [AIRFLOW-5206] All .md files should have all common licence, TOC (where applicable) (#5809)
- [AIRFLOW-5329] Easy way to add local files to docker (#5933)
- [AIRFLOW-4027] Make experimental api tests more stateless (#4854)

Doc-only changes
^^^^^^^^^^^^^^^^

- [AIRFLOW-XXX] Fixed Azkaban link (#5865)
- [AIRFLOW-XXX] Remove duplicate lines from CONTRIBUTING.md (#5830)
- [AIRFLOW-XXX] Fix incorrect docstring parameter in SchedulerJob (#5729)

Airflow 1.10.4 (2019-08-06)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Export MySQL timestamps as UTC
""""""""""""""""""""""""""""""

``MySqlToGoogleCloudStorageOperator`` now exports TIMESTAMP columns as UTC
by default, rather than using the default timezone of the MySQL server.
This is the correct behavior for use with BigQuery, since BigQuery
assumes that TIMESTAMP columns without time zones are in UTC. To
preserve the previous behavior, set ``ensure_utc`` to ``False.``

Changes to DatastoreHook
""""""""""""""""""""""""


* removed argument ``version`` from ``get_conn`` function and added it to the hook's ``__init__`` function instead and renamed it to ``api_version``
* renamed the ``partialKeys`` argument of function ``allocate_ids`` to ``partial_keys``

Changes to GoogleCloudStorageHook
"""""""""""""""""""""""""""""""""


* the discovery-based api (\ ``googleapiclient.discovery``\ ) used in ``GoogleCloudStorageHook`` is now replaced by the recommended client based api (\ ``google-cloud-storage``\ ). To know the difference between both the libraries, read https://cloud.google.com/apis/docs/client-libraries-explained. PR: `#5054 <https://github.com/apache/airflow/pull/5054>`_
*
  as a part of this replacement, the ``multipart`` & ``num_retries`` parameters for ``GoogleCloudStorageHook.upload`` method have been deprecated.

  The client library uses multipart upload automatically if the object/blob size is more than 8 MB - `source code <https://github.com/googleapis/google-cloud-python/blob/11c543ce7dd1d804688163bc7895cf592feb445f/storage/google/cloud/storage/blob.py#L989-L997>`_. The client also handles retries automatically

*
  the ``generation`` parameter is deprecated in ``GoogleCloudStorageHook.delete`` and ``GoogleCloudStorageHook.insert_object_acl``.

Updating to ``google-cloud-storage >= 1.16`` changes the signature of the upstream ``client.get_bucket()`` method from ``get_bucket(bucket_name: str)`` to ``get_bucket(bucket_or_name: Union[str, Bucket])``. This method is not directly exposed by the airflow hook, but any code accessing the connection directly (\ ``GoogleCloudStorageHook().get_conn().get_bucket(...)`` or similar) will need to be updated.

Changes in writing Logs to Elasticsearch
""""""""""""""""""""""""""""""""""""""""

The ``elasticsearch_`` prefix has been removed from all config items under the ``[elasticsearch]`` section. For example ``elasticsearch_host`` is now just ``host``.

Removal of ``non_pooled_task_slot_count`` and ``non_pooled_backfill_task_slot_count``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``non_pooled_task_slot_count`` and ``non_pooled_backfill_task_slot_count``
are removed in favor of a real pool, e.g. ``default_pool``.

By default tasks are running in ``default_pool``.
``default_pool`` is initialized with 128 slots and user can change the
number of slots through UI/CLI. ``default_pool`` cannot be removed.

``pool`` config option in Celery section to support different Celery pool implementation
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The new ``pool`` config option allows users to choose different pool
implementation. Default value is "prefork", while choices include "prefork" (default),
"eventlet", "gevent" or "solo". This may help users achieve better concurrency performance
in different scenarios.

For more details about Celery pool implementation, please refer to:


* https://docs.celeryproject.org/en/latest/userguide/workers.html#concurrency
* https://docs.celeryproject.org/en/latest/userguide/concurrency/eventlet.html

Change to method signature in ``BaseOperator`` and ``DAG`` classes
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The signature of the ``get_task_instances`` method in the ``BaseOperator`` and ``DAG`` classes has changed. The change does not change the behavior of the method in either case.

For ``BaseOperator``
~~~~~~~~~~~~~~~~~~~~~~~~

Old signature:

.. code-block:: python

   def get_task_instances(self, session, start_date=None, end_date=None): ...

New signature:

.. code-block:: python

   @provide_session
   def get_task_instances(self, start_date=None, end_date=None, session=None): ...

For ``DAG``
~~~~~~~~~~~~~~~

Old signature:

.. code-block:: python

   def get_task_instances(self, session, start_date=None, end_date=None, state=None): ...

New signature:

.. code-block:: python

   @provide_session
   def get_task_instances(self, start_date=None, end_date=None, state=None, session=None): ...

In either case, it is necessary to rewrite calls to the ``get_task_instances`` method that currently provide the ``session`` positional argument. New calls to this method look like:

.. code-block:: python

   # if you can rely on @provide_session
   dag.get_task_instances()
   # if you need to provide the session
   dag.get_task_instances(session=your_session)

New Features
^^^^^^^^^^^^

- [AIRFLOW-4811] Implement GCP Data Loss Prevention Hook and Operators (#5539)
- [AIRFLOW-5035] Replace multiprocessing.Manager with a golang-"channel" style (#5615)
- [AIRFLOW-4883] Kill hung file process managers (#5605)
- [AIRFLOW-4929] Pretty print JSON Variables in UI (#5573)
- [AIRFLOW-4884] Roll up import_errors in RBAC UI (#5516)
- [AIRFLOW-4871] Allow creating DagRuns via RBAC UI (#5507)
- [AIRFLOW-4591] Make default_pool a real pool (#5349)
- [AIRFLOW-4844] Add optional is_paused_upon_creation argument to DAG (#5473)
- [AIRFLOW-4456] Add sub-classable BaseBranchOperator (#5231)
- [AIRFLOW-4343] Show warning in UI if scheduler is not running (#5127)
- [AIRFLOW-4739] Add ability to arbitrarily define kubernetes worker pod labels (#5376)
- [AIRFLOW-4348] Add GCP console link in BigQueryOperator (#5195)
- [AIRFLOW-4306] Global operator extra links (#5094)
- [AIRFLOW-4812] Add batch images annotation (#5433)
- [AIRFLOW-4135] Add Google Cloud Build operator and hook (#5251)
- [AIRFLOW-4781] Add the ability to specify ports in KubernetesPodOperator (#5410)
- [AIRFLOW-4521] Pause dag also pause its subdags (#5283)
- [AIRFLOW-4738] Enforce exampleinclude for example DAGs (#5375)
- [AIRFLOW-4326] Airflow AWS SQS Operator (#5110)
- [AIRFLOW-3729] Support ``DownwardAPI`` in env variables for KubernetesPodOperator (#4554)
- [AIRFLOW-4585] Implement Kubernetes Pod Mutation Hook (#5359)
- [AIRFLOW-161] New redirect route and extra links (#5059)
- [AIRFLOW-4420] Backfill respects task_concurrency (#5221)
- [AIRFLOW-4147] Add Operator to publish event to Redis (#4967)
- [AIRFLOW-3359] Add option to pass customer encryption keys to Dataproc (#4200)
- [AIRFLOW-4318] Create Google Cloud Translate Speech Operator (#5102)
- [AIRFLOW-3960] Adds Google Cloud Speech operators (#4780)
- [AIRFLOW-1501] Add GoogleCloudStorageDeleteOperator (#5230)
- [AIRFLOW-3672] Add support for Mongo DB DNS Seedlist Connection Format (#4481)
- [AIRFLOW-4397] add integrations docs manually for gcs sensors (#5204)
- [AIRFLOW-4251] Instrument DagRun schedule delay (#5050)
- [AIRFLOW-4118] instrument DagRun duration (#4946)
- [AIRFLOW-4361] Fix flaky test_integration_run_dag_with_scheduler_failure (#5182)
- [AIRFLOW-4361] Fix flaky test_integration_run_dag_with_scheduler_failure (#5140)
- [AIRFLOW-4168] Create Google Cloud Video Intelligence Operators (#4985)
- [AIRFLOW-4397] Add GCSUploadSessionCompleteSensor (#5166)
- [AIRFLOW-4335] Add default num_retries to GCP connection (#5117)
- [AIRFLOW-3808] Add cluster_fields to BigQueryHook's create_empty_table (#4654)
- [AIRFLOW-4362] Fix test_execution_limited_parallelism (#5141)
- [AIRFLOW-4307] Backfill respects concurrency limit (#5128)
- [AIRFLOW-4268] Add MsSqlToGoogleCloudStorageOperator (#5077)
- [AIRFLOW-4169] Add Google Cloud Vision Detect Operators (#4986)
- [AIRFLOW-XXX] Fix WS-2019-0032 (#5384)
- [AIRFLOW-XXX] Fix CVE-2019-11358 (#5197)
- [AIRFLOW-XXX] Change allowed version of Jinja2 to fix CVE-2019-10906 (#5075)

Improvement
^^^^^^^^^^^

- [AIRFLOW-5022] Fix DockerHook for registries with port numbers (#5644)
- [AIRFLOW-4961] Insert TaskFail.duration as int match DB schema column type (#5593)
- [AIRFLOW-5038] skip pod deleted log message when pod deletion is disabled (#5656)
- [AIRFLOW-5067] Update pagination symbols (#5682)
- [AIRFLOW-4981][AIRFLOW-4788] Always use pendulum DateTimes in task instance context (#5654)
- [AIRFLOW-4880] Add success, failure and fail_on_empty params to SqlSensor (#5488)
- [AIRFLOW-3617] Add gpu limits option in configurations for Kube executor and pod (#5643)
- [AIRFLOW-4998] Run multiple queries in BigQueryOperator (#5619)
- [AIRFLOW-4929] Improve display of JSON Variables in UI (#5641)
- [AIRFLOW-4959] Add .hql support for the DataProcHiveOperator (#5591)
- [AIRFLOW-4962] Fix Werkzeug v0.15 deprecation notice for DispatcherMiddleware import (#5595)
- [AIRFLOW-4797] Improve performance and behaviour of zombie detection (#5511)
- [AIRFLOW-4911] Silence the FORBIDDEN errors from the KubernetesExecutor (#5547)
- [AIRFLOW-3495] Validate one of query and query_uri passed to DataProcSparkSqlOperator (#5510)
- [AIRFLOW-4925] Improve css style for Variables Import file field (#5552)
- [AIRFLOW-4906] Improve debugging for the SparkSubmitHook (#5542)
- [AIRFLOW-4904] unittest.cfg name and path can be overridden by setting $AIRFLOW_TEST_CONFIG (#5540)
- [AIRFLOW-4920] Use html.escape instead of cgi.escape to fix DeprecationWarning (#5551)
- [AIRFLOW-4919] DataProcJobBaseOperator dataproc_*_properties templated (#5555)
- [AIRFLOW-4478] Lazily instantiate default resources objects. (#5259)
- [AIRFLOW-4564] AzureContainerInstance bugfixes and improvements (#5319)
- [AIRFLOW-4237] Including Try Number of Task in Gantt Chart (#5037)
- [AIRFLOW-4862] Allow directly using IP address as hostname for webserver logs (#5501)
- [AIRFLOW-4857] Add templated fields to SlackWebhookOperator (#5490)
- [AIRFLOW-3502] Add Celery config option for setting "pool" (#4308)
- [AIRFLOW-3217] Button to toggle line wrapping in log and code views  (#4277)
- [AIRFLOW-4491] Add a "Jump to end" button for logs (#5266)
- [AIRFLOW-4422] Pool utilization stats (#5453)
- [AIRFLOW-4805] Add py_file as templated field in DataflowPythonOperator (#5451)
- [AIRFLOW-4838] Surface Athena errors in AWSAthenaOperator (#5467)
- [AIRFLOW-4831] conf.has_option no longer throws if section is missing. (#5455)
- [AIRFLOW-4829] More descriptive exceptions for EMR sensors (#5452)
- [AIRFLOW-4414] AWSAthenaOperator: Push QueryExecutionID to XCom (#5276)
- [AIRFLOW-4791] add "schema" keyword arg to SnowflakeOperator (#5415)
- [AIRFLOW-4759] Don't error when marking successful run as failed (#5435)
- [AIRFLOW-4716] Instrument dag loading time duration (#5350)
- [AIRFLOW-3958] Support list tasks as upstream in chain (#4779)
- [AIRFLOW-4409] Prevent task duration break by null value (#5178)
- [AIRFLOW-4418] Add "failed only" option to task modal (#5193)
- [AIRFLOW-4740] Accept string ``end_date`` in DAG default_args (#5381)
- [AIRFLOW-4423] Improve date handling in mysql to gcs operator. (#5196)
- [AIRFLOW-4447] Display task duration as human friendly format in UI (#5218)
- [AIRFLOW-4377] Remove needless object conversion in DAG.owner() (#5144)
- [AIRFLOW-4766] Add autoscaling option for DataprocClusterCreateOperator (#5425)
- [AIRFLOW-4795] Upgrade alembic to latest release. (#5411)
- [AIRFLOW-4793] Add signature_name to mlengine operator (#5417)
- [AIRFLOW-3211] Reattach to GCP Dataproc jobs upon Airflow restart  (#4083)
- [AIRFLOW-4750] Log identified zombie task instances (#5389)
- [AIRFLOW-3870] STFPOperator: Update log level and return value (#4355)
- [AIRFLOW-4759] Batch queries in set_state API. (#5403)
- [AIRFLOW-2737] Restore original license header to airflow.api.auth.backend.kerberos_auth
- [AIRFLOW-3635] Fix incorrect logic in delete_dag (introduced in PR#4406) (#4445)
- [AIRFLOW-3599] Removed Dagbag from delete dag (#4406)
- [AIRFLOW-4737] Increase and document celery queue name limit (#5383)
- [AIRFLOW-4505] Correct Tag ALL for PY3 (#5275)
- [AIRFLOW-4743] Add environment variables support to SSHOperator (#5385)
- [AIRFLOW-4725] Fix setup.py PEP440 & Sphinx-PyPI-upload dependency (#5363)
- [AIRFLOW-3370] Add stdout output options to Elasticsearch task log handler (#5048)
- [AIRFLOW-4396] Provide a link to external Elasticsearch logs in UI. (#5164)
- [AIRFLOW-1381] Allow setting host temporary directory in DockerOperator (#5369)
- [AIRFLOW-4598] Task retries are not exhausted for K8s executor (#5347)
- [AIRFLOW-4218] Support to Provide http args to K8executor while calling k8 Python client lib apis (#5060)
- [AIRFLOW-4159] Add support for additional static pod labels for K8sExecutor (#5134)
- [AIRFLOW-4720] Allow comments in .airflowignore files. (#5355)
- [AIRFLOW-4486] Add AWS IAM authentication in MySqlHook (#5334)
- [AIRFLOW-4417] Add AWS IAM authentication for PostgresHook (#5223)
- [AIRFLOW-3990] Compile regular expressions. (#4813)
- [AIRFLOW-4572] Rename prepare_classpath() to prepare_syspath() (#5328)
- [AIRFLOW-3869] Raise consistent exception in AirflowConfigParser.getboolean (#4692)
- [AIRFLOW-4571] Add headers to templated field for SimpleHttpOperator (#5326)
- [AIRFLOW-3867] Rename GCP's subpackage (#4690)
- [AIRFLOW-3725] Add private_key to bigquery_hook get_pandas_df (#4549)
- [AIRFLOW-4546] Upgrade google-cloud-bigtable. (#5307)
- [AIRFLOW-4519] Optimise operator classname sorting in views (#5282)
- [AIRFLOW-4503] Support fully pig options (#5271)
- [AIRFLOW-4468] add sql_alchemy_max_overflow parameter (#5249)
- [AIRFLOW-4467] Add dataproc_jars to templated fields in Dataproc oper… (#5248)
- [AIRFLOW-4381] Use get_direct_relative_ids get task relatives (#5147)
- [AIRFLOW-3624] Add masterType parameter to MLEngineTrainingOperator (#4428)
- [AIRFLOW-3143] Support Auto-Zone in DataprocClusterCreateOperator (#5169)
- [AIRFLOW-3874] Improve BigQueryHook.run_with_configuration's location support (#4695)
- [AIRFLOW-4399] Avoid duplicated os.path.isfile() check in models.dagbag (#5165)
- [AIRFLOW-4031] Allow for key pair auth in snowflake hook (#4875)
- [AIRFLOW-3901] add role as optional config parameter for SnowflakeHook (#4721)
- [AIRFLOW-3455] add region in snowflake connector (#4285)
- [AIRFLOW-4073] add template_ext for AWS Athena operator (#4907)
- [AIRFLOW-4093] AWSAthenaOperator: Throw exception if job failed/cancelled/reach max retries (#4919)
- [AIRFLOW-4356] Add extra RuntimeEnvironment keys to DataFlowHook (#5149)
- [AIRFLOW-4337] Fix docker-compose deprecation warning in CI (#5119)
- [AIRFLOW-3603] QuboleOperator: Remove SQLCommand from SparkCmd documentation (#4411)
- [AIRFLOW-4328] Fix link to task instances from Pool page (#5124)
- [AIRFLOW-4255] Make GCS Hook Backwards compatible (#5089)
- [AIRFLOW-4103] Allow uppercase letters in dataflow job names (#4925)
- [AIRFLOW-4255] Replace Discovery based api with client based for GCS (#5054)
- [AIRFLOW-4311] Remove sleep in localexecutor (#5096)
- [AIRFLOW-2836] Minor improvement-contrib.sensors.FileSensor (#3674)
- [AIRFLOW-4104] Add type annotations to common classes. (#4926)
- [AIRFLOW-3910] Raise exception explicitly in Connection.get_hook() (#4728)
- [AIRFLOW-3322] Update QuboleHook to fetch args dynamically from qds_sdk (#4165)
- [AIRFLOW-4565] instrument celery executor (#5321)
- [AIRFLOW-4573] Import airflow_local_settings after prepare_classpath (#5330)
- [AIRFLOW-4448] Don't bake ENV and _cmd into tmp config for non-sudo (#4050)
- [AIRFLOW-4295] Make ``method`` attribute case insensitive in HttpHook (#5313)
- [AIRFLOW-3703] Add dnsPolicy option for KubernetesPodOperator (#4520)
- [AIRFLOW-3057] add prev_*_date_success to template context (#5372)
- [AIRFLOW-4336] Stop showing entire GCS files bytes in log for gcs_download_operator (#5151)
- [AIRFLOW-4528] Cancel DataProc task on timeout (#5293)

Bug fixes
^^^^^^^^^

- [AIRFLOW-5089] Change version requirement on google-cloud-spanner to work around version incompatibility (#5703)
- [AIRFLOW-4289] fix spark_binary argument being ignored in SparkSubmitHook (#5564)
- [AIRFLOW-5075] Let HttpHook handle connections with empty host fields (#5686)
- [AIRFLOW-4822] Fix bug where parent-dag task instances are wrongly cleared when using subdags (#5444)
- [AIRFLOW-5050] Correctly delete FAB permission m2m objects in ``airflow sync_perms`` (#5679)
- [AIRFLOW-5030] fix env var expansion for config key contains __ (#5650)
- [AIRFLOW-4590] changing log level to be proper library to suppress warning in WinRM (#5337)
- [AIRFLOW-4451] Allow named tuples to be templated (#5673)
- [AIRFLOW-XXX] Fix bug where Kube pod limits were not applied (requests were, but not limits) (#5657)
- [AIRFLOW-4775] Fix incorrect parameter order in GceHook (#5613)
- [AIRFLOW-4995] Fix DB initialisation on MySQL >=8.0.16 (#5614)
- [AIRFLOW-4934] Fix ProxyFix due to Werkzeug upgrade (#5563) (#5571)
- [AIRFLOW-4136] fix key_file of hook is overwritten by SSHHook connection (#5558)
- [AIRFLOW-4587] Replace self.conn with self.get_conn() in AWSAthenaHook (#5545)
- [AIRFLOW-1740] Fix xcom creation and update via UI (#5530) (#5531)
- [AIRFLOW-4900] Resolve incompatible version of Werkzeug (#5535)
- [AIRFLOW-4510] Don't mutate default_args during DAG initialization (#5277)
- [AIRFLOW-3360] Make the DAGs search respect other querystring parameters with url-search-params-polyfill for IE support (#5503)
- [AIRFLOW-4896] Make KubernetesExecutorConfig's default args immutable (#5534)
- [AIRFLOW-4494] Remove ``shell=True`` in DaskExecutor (#5273)
- [AIRFLOW-4890] Fix Log link in TaskInstance's View for Non-RBAC (#5525)
- [AIRFLOW-4892] Fix connection creation via UIs (#5527)
- [AIRFLOW-4406] Fix a method name typo: NullFernet.decrpyt to decrypt (#5509)
- [AIRFLOW-4849] Add gcp_conn_id to cloudsqldatabehook class to use correctly CloudSqlProxyRunner class (#5478)
- [AIRFLOW-4769] Pass gcp_conn_id to BigtableHook (#5445)
- [AIRFLOW-4524] Fix incorrect field names in view for Mark Success/Failure (#5486)
- [AIRFLOW-3671] Remove arg ``replace`` of MongoToS3Operator from ``kwargs`` (#4480)
- [AIRFLOW-4845] Fix bug where runAsUser 0 doesn't get set in k8s security context (#5474)
- [AIRFLOW-4354] Fix exception in "between" date filter in classic UI (#5480)
- [AIRFLOW-4587] Replace self.conn with self.get_conn() in AWSAthenaHook (#5462)
- [AIRFLOW-4516] K8s runAsUser and fsGroup cannot be strings (#5429)
- [AIRFLOW-4298] Stop Scheduler repeatedly warning "connection invalidated" (#5470)
- [AIRFLOW-4559] JenkinsJobTriggerOperator bugfix (#5318)
- [AIRFLOW-4841] Pin Sphinx AutoApi to 1.0.0 (#5468)
- [AIRFLOW-4479] Include s3_overwrite kwarg in load_bytes method (#5312)
- [AIRFLOW-3746] Fix DockerOperator missing container exit (#4583)
- [AIRFLOW-4233] Remove Template Extension from Bq to GCS Operator (#5456)
- [AIRFLOW-2141][AIRFLOW-3157][AIRFLOW-4170] Serialize non-str value by JSON when importing Variables (#4991)
- [AIRFLOW-4826] Remove warning from ``airflow resetdb`` command (#5447)
- [AIRFLOW-4148] Fix editing DagRuns when clicking state column (#5436)
- [AIRFLOW-4455] dag_details broken for subdags in RBAC UI (#5234)
- [AIRFLOW-2955] Fix kubernetes pod operator to set requests and limits on task pods (#4551)
- [AIRFLOW-4459] Fix wrong DAG count in /home page when DAG count is zero (#5235)
- [AIRFLOW-3876] AttributeError: module ``distutils`` has no attribute 'util'
- [AIRFLOW-4146] Fix CgroupTaskRunner errors (#5224)
- [AIRFLOW-4524] Fix bug with "Ignore \*" toggles in RBAC mode (#5378)
- [AIRFLOW-4765] Fix DataProcPigOperator execute method (#5426)
- [AIRFLOW-4798] Obviate interdependencies for dagbag and TI tests (#5422)
- [AIRFLOW-4800] Fix GKEClusterHook ctor calls (#5424)
- [AIRFLOW-4799] Don't mutate self.env in BashOperator execute method (#5421)
- [AIRFLOW-4393] Add retry logic when fetching pod status and/or logs in KubernetesPodOperator (#5284)
- [AIRFLOW-4174] Fix HttpHook run with backoff (#5213)
- [AIRFLOW-4463] Handle divide-by-zero errors in short retry intervals (#5243)
- [AIRFLOW-2614] Speed up trigger_dag API call when lots of DAGs in system
- [AIRFLOW-4756] add ti.state to ti.start_date as criteria for gantt (#5399)
- [AIRFLOW-4760] Fix zip-packaged DAGs disappearing from DagBag when reloaded (#5404)
- [AIRFLOW-4731] Fix GCS hook with google-storage-client 1.16 (#5368)
- [AIRFLOW-3506] use match_phrase to query log_id in elasticsearch (#4342)
- [AIRFLOW-4084] fix ElasticSearch log download (#5177)
- [AIRFLOW-4501] Register pendulum datetime converter for sqla+pymysql (#5190)
- [AIRFLOW-986] HiveCliHook ignores 'proxy_user' value in a connection's extra parameter (#5305)
- [AIRFLOW-4442] fix hive_tblproperties in HiveToDruidTransfer (#5211)
- [AIRFLOW-4557] Add gcp_conn_id parameter to get_sqlproxy_runner() of CloudSqlDatabaseHook (#5314)
- [AIRFLOW-4545] Upgrade FAB to latest version (#4955)
- [AIRFLOW-4492] Change Dataproc Cluster operators to poll Operations (#5269)
- [AIRFLOW-4452] Webserver and Scheduler keep crashing because of slackclient update (#5225)
- [AIRFLOW-4450] Fix request arguments in has_dag_access (#5220)
- [AIRFLOW-4434] Support Impala with the HiveServer2Hook (#5206)
- [AIRFLOW-3449] Write local dag parsing logs when remote logging enabled. (#5175)
- [AIRFLOW-4300] Fix graph modal call when DAG has not yet run (#5185)
- [AIRFLOW-4401] Use managers for Queue synchronization (#5200)
- [AIRFLOW-3626] Fixed triggering DAGs contained within zip files (#4439)
- [AIRFLOW-3720] Fix mismatch while comparing GCS and S3 files (#4766)
- [AIRFLOW-4403] search by ``dag_id`` or ``owners`` in UI (#5184)
- [AIRFLOW-4308] Fix TZ-loop around DST on Python 3.6+  (#5095)
- [AIRFLOW-4324] fix DAG fuzzy search in RBAC UI (#5131)
- [AIRFLOW-4297] Temporary hot fix on manage_slas() for 1.10.4 release (#5150)
- [AIRFLOW-4299] Upgrade to Celery 4.3.0 to fix crashing workers (#5116)
- [AIRFLOW-4291] Correctly render doc_md in DAG graph page (#5121)
- [AIRFLOW-4310] Fix incorrect link on Dag Details page (#5122)
- [AIRFLOW-4331] Correct filter for Null-state runs from Dag Detail page (#5123)
- [AIRFLOW-4294] Fix missing dag & task runs in UI dag_id contains a dot (#5111)
- [AIRFLOW-4332] Upgrade sqlalchemy to remove security Vulnerability (#5113)
- [AIRFLOW-4312] Add template_fields & template_ext to BigQueryCheckO… (#5097)
- [AIRFLOW-4293] Fix downgrade in d4ecb8fbee3_add_schedule_interval_to_dag.py (#5086)
- [AIRFLOW-4267] Fix TI duration in Graph View (#5071)
- [AIRFLOW-4163] IntervalCheckOperator supports relative diff and not ignore 0 (#4983)
- [AIRFLOW-3938] QuboleOperator Fixes and Support for SqlCommand (#4832)
- [AIRFLOW-2903] Change default owner to ``airflow`` (#4151)
- [AIRFLOW-4136] Fix overwrite of key_file by constructor (#5155)
- [AIRFLOW-3241] Remove Invalid template ext in GCS Sensors (#4076)

Misc/Internal
^^^^^^^^^^^^^

- [AIRFLOW-4338] Change k8s pod_request_factory to use yaml safe_load (#5120)
- [AIRFLOW-4869] Reorganize sql to gcs operators. (#5504)
- [AIRFLOW-5021] move gitpython into setup_requires (#5640)
- [AIRFLOW-4583] Fixes type error in GKEPodOperator (#5612)
- [AIRFLOW-4116] Dockerfile now supports CI image build on DockerHub (#4937)
- [AIRFLOW-4115] Multi-staging Airflow Docker image (#4936)
- [AIRFLOW-4963] Avoid recreating task context (#5596)
- [AIRFLOW-4865] Add context manager to set temporary config values in tests. (#5569)
- [AIRFLOW-4937] Fix lodash security issue with version below 4.17.13 (#5572) (used only in build-pipeline, not runtime)
- [AIRFLOW-4868] Fix typo in kubernetes/docker/build.sh (#5505)
- [AIRFLOW-4211] Add tests for WebHDFSHook (#5015)
- [AIRFLOW-4320] Add tests for SegmentTrackEventOperator (#5104)
- [AIRFLOW-4319] Add tests for Bigquery related Operators (#5101)
- [AIRFLOW-4014] Change DatastoreHook and add tests (#4842)
- [AIRFLOW-4322] Add test for VerticaOperator (#5107)
- [AIRFLOW-4323] Add 2 tests for WinRMOperator (#5108)
- [AIRFLOW-3677] Improve CheckOperator test coverage (#4756)
- [AIRFLOW-4659] Fix pylint problems for api module (#5398)
- [AIRFLOW-4358] Speed up test_jobs by not running tasks (#5162)
- [AIRFLOW-4394] Don't test behaviour of BackfillJob from CLI tests (#5160)
- [AIRFLOW-3471] Move XCom out of models.py (#4629)
- [AIRFLOW-4379] Remove duplicate code & Add validation in gcs_to_gcs.py (#5145)
- [AIRFLOW-4259] Move models out of models.py (#5056)
- [AIRFLOW-XXX] Speed up building of Cassanda module on Travis (#5233)
- [AIRFLOW-4535] Break jobs.py into multiple files (#5303)
- [AIRFLOW-1464] Batch update task_instance state (#5323)
- [AIRFLOW-4554] Test for sudo command, add some other test docs (#5310)
- [AIRFLOW-4419] Refine concurrency check in scheduler (#5194)
- [AIRFLOW-4269] Minor acceleration of jobs._process_task_instances() (#5076)
- [AIRFLOW-4341] Remove ``View.render()`` already exists in fab.BaseView (#5125)
- [AIRFLOW-4342] Use @cached_property instead of re-implementing it each time (#5126)
- [AIRFLOW-4256] Remove noqa from migrations (#5055)
- [AIRFLOW-4034] Remove unnecessary string formatting with ``**locals()`` (#4861)
- [AIRFLOW-3944] Remove code smells (#4762)

Doc-only changes
^^^^^^^^^^^^^^^^

- [AIRFLOW-XXX] Add missing doc for annotations param of KubernetesPodOperator (#5666)
- [AIRFLOW-XXX] Fix typos in CONTRIBUTING.md (#5626)
- [AIRFLOW-XXX] Correct BaseSensorOperator docs (#5562)
- [AIRFLOW-4926] Fix example dags where its start_date is datetime.utcnow() (#5553)
- [AIRFLOW-4860] Remove Redundant Information in Example Dags (#5497)
- [AIRFLOW-4767] Fix errors in the documentation of Dataproc Operator (#5487)
- [AIRFLOW-1684] Branching based on XCom variable (Docs) (#4365)
- [AIRFLOW-3341] FAQ return DAG object example (#4605)
- [AIRFLOW-4433] Add missing type in DockerOperator doc string (#5205)
- [AIRFLOW-4321] Replace incorrect info of Max Size limit of GCS Object Size (#5106)
- [AIRFLOW-XXX] Add information about user list (#5341)
- [AIRFLOW-XXX] Clarify documentation related to autodetect parameter in GCS_to_BQ Op (#5294)
- [AIRFLOW-XXX] Remove mention of pytz compatibility from timezone documentation (#5316)
- [AIRFLOW-XXX] Add missing docs for GoogleCloudStorageDeleteOperator (#5274)
- [AIRFLOW-XXX] Remove incorrect note about Scopes of GCP connection (#5242)
- [AIRFLOW-XXX] Fix mistakes in docs of Dataproc operators (#5192)
- [AIRFLOW-XXX] Link to correct class for timedelta in macros.rst (#5226)
- [AIRFLOW-XXX] Add Kamil as committer (#5216)
- [AIRFLOW-XXX] Add Joshua and Kevin as committer (#5207)
- [AIRFLOW-XXX] Reduce log spam in tests (#5174)
- [AIRFLOW-XXX] Speed up tests for PythonSensor (#5158)
- [AIRFLOW-XXX] Add Bas Harenslak to committer list (#5157)
- [AIRFLOW-XXX] Add Jarek Potiuk to committer list (#5132)
- [AIRFLOW-XXX] Update docstring for SchedulerJob (#5105)
- [AIRFLOW-XXX] Fix docstrings for CassandraToGoogleCloudStorageOperator (#5103)
- [AIRFLOW-XXX] update SlackWebhookHook and SlackWebhookOperator docstring (#5074)
- [AIRFLOW-XXX] Ignore Python files under node_modules in docs (#5063)
- [AIRFLOW-XXX] Build a universal wheel with LICNESE files (#5052)
- [AIRFLOW-XXX] Fix docstrings of SQSHook (#5099)
- [AIRFLOW-XXX] Use Py3.7 on readthedocs
- [AIRFLOW-4446] Fix typos (#5217)



Airflow 1.10.3 (2019-04-09)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

New ``dag_discovery_safe_mode`` config option
"""""""""""""""""""""""""""""""""""""""""""""""""

If ``dag_discovery_safe_mode`` is enabled, only check files for DAGs if
they contain the strings "airflow" and "DAG". For backwards
compatibility, this option is enabled by default.

RedisPy dependency updated to v3 series
"""""""""""""""""""""""""""""""""""""""

If you are using the Redis Sensor or Hook you may have to update your code. See
`redis-py porting instructions <https://github.com/andymccurdy/redis-py/tree/3.2.0#upgrading-from-redis-py-2x-to-30>`_ to check if your code might be affected (MSET,
MSETNX, ZADD, and ZINCRBY all were, but read the full doc).

SLUGIFY_USES_TEXT_UNIDECODE or AIRFLOW_GPL_UNIDECODE no longer required
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

It is no longer required to set one of the environment variables to avoid
a GPL dependency. Airflow will now always use ``text-unidecode`` if ``unidecode``
was not installed before.

New ``sync_parallelism`` config option in ``[celery]`` section
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The new ``sync_parallelism`` config option will control how many processes CeleryExecutor will use to
fetch celery task state in parallel. Default value is max(1, number of cores - 1)

Rename of BashTaskRunner to StandardTaskRunner
""""""""""""""""""""""""""""""""""""""""""""""

BashTaskRunner has been renamed to StandardTaskRunner. It is the default task runner
so you might need to update your config.

``task_runner = StandardTaskRunner``

Modification to config file discovery
"""""""""""""""""""""""""""""""""""""

If the ``AIRFLOW_CONFIG`` environment variable was not set and the
``~/airflow/airflow.cfg`` file existed, airflow previously used
``~/airflow/airflow.cfg`` instead of ``$AIRFLOW_HOME/airflow.cfg``. Now airflow
will discover its config file using the ``$AIRFLOW_CONFIG`` and ``$AIRFLOW_HOME``
environment variables rather than checking for the presence of a file.

Changes in Google Cloud related operators
"""""""""""""""""""""""""""""""""""""""""

Most GCP-related operators have now optional ``PROJECT_ID`` parameter. In case you do not specify it,
the project id configured in
`GCP Connection <https://airflow.apache.org/howto/manage-connections.html#connection-type-gcp>`_ is used.
There will be an ``AirflowException`` thrown in case ``PROJECT_ID`` parameter is not specified and the
connection used has no project id defined. This change should be  backwards compatible as earlier version
of the operators had ``PROJECT_ID`` mandatory.

Operators involved:


* GCP Compute Operators

  * GceInstanceStartOperator
  * GceInstanceStopOperator
  * GceSetMachineTypeOperator

* GCP Function Operators

  * GcfFunctionDeployOperator

* GCP Cloud SQL Operators

  * CloudSqlInstanceCreateOperator
  * CloudSqlInstancePatchOperator
  * CloudSqlInstanceDeleteOperator
  * CloudSqlInstanceDatabaseCreateOperator
  * CloudSqlInstanceDatabasePatchOperator
  * CloudSqlInstanceDatabaseDeleteOperator

Other GCP operators are unaffected.

Changes in Google Cloud related hooks
"""""""""""""""""""""""""""""""""""""

The change in GCP operators implies that GCP Hooks for those operators require now keyword parameters rather
than positional ones in all methods where ``project_id`` is used. The methods throw an explanatory exception
in case they are called using positional parameters.

Hooks involved:


* GceHook
* GcfHook
* CloudSqlHook

Other GCP hooks are unaffected.

Changed behaviour of using default value when accessing variables
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

It's now possible to use ``None`` as a default value with the ``default_var`` parameter when getting a variable, e.g.

.. code-block:: python

   foo = Variable.get("foo", default_var=None)
   if foo is None:
       handle_missing_foo()

(Note: there is already ``Variable.setdefault()`` which me be helpful in some cases.)

This changes the behaviour if you previously explicitly provided ``None`` as a default value. If your code expects a ``KeyError`` to be thrown, then don't pass the ``default_var`` argument.

Removal of ``airflow_home`` config setting
""""""""""""""""""""""""""""""""""""""""""""""

There were previously two ways of specifying the Airflow "home" directory
(\ ``~/airflow`` by default): the ``AIRFLOW_HOME`` environment variable, and the
``airflow_home`` config setting in the ``[core]`` section.

If they had two different values different parts of the code base would end up
with different values. The config setting has been deprecated, and you should
remove the value from the config file and set ``AIRFLOW_HOME`` environment
variable if you need to use a non default value for this.

(Since this setting is used to calculate what config file to load, it is not
possible to keep just the config option)

Change of two methods signatures in ``GCPTransferServiceHook``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The signature of the ``create_transfer_job`` method in ``GCPTransferServiceHook``
class has changed. The change does not change the behavior of the method.

Old signature:

.. code-block:: python

   def create_transfer_job(self, description, schedule, transfer_spec, project_id=None): ...

New signature:

.. code-block:: python

   def create_transfer_job(self, body): ...

It is necessary to rewrite calls to method. The new call looks like this:

.. code-block:: python

   body = {
       "status": "ENABLED",
       "projectId": project_id,
       "description": description,
       "transferSpec": transfer_spec,
       "schedule": schedule,
   }
   gct_hook.create_transfer_job(body)

The change results from the unification of all hooks and adjust to
`the official recommendations <https://lists.apache.org/thread.html/e8534d82be611ae7bcb21ba371546a4278aad117d5e50361fd8f14fe@%3Cdev.airflow.apache.org%3E>`_
for the Google Cloud.

The signature of ``wait_for_transfer_job`` method in ``GCPTransferServiceHook`` has changed.

Old signature:

.. code-block:: python

   def wait_for_transfer_job(self, job): ...

New signature:

.. code-block:: python

   def wait_for_transfer_job(self, job, expected_statuses=(GcpTransferOperationStatus.SUCCESS,)): ...

The behavior of ``wait_for_transfer_job`` has changed:

Old behavior:

``wait_for_transfer_job`` would wait for the SUCCESS status in specified jobs operations.

New behavior:

You can now specify an array of expected statuses. ``wait_for_transfer_job`` now waits for any of them.

The default value of ``expected_statuses`` is SUCCESS so that change is backwards compatible.

Moved two classes to different modules
""""""""""""""""""""""""""""""""""""""

The class ``GoogleCloudStorageToGoogleCloudStorageTransferOperator`` has been moved from
``airflow.contrib.operators.gcs_to_gcs_transfer_operator`` to ``airflow.contrib.operators.gcp_transfer_operator``

the class ``S3ToGoogleCloudStorageTransferOperator`` has been moved from
``airflow.contrib.operators.s3_to_gcs_transfer_operator`` to ``airflow.contrib.operators.gcp_transfer_operator``

The change was made to keep all the operators related to GCS Transfer Services in one file.

The previous imports will continue to work until Airflow 2.0

Fixed typo in --driver-class-path in SparkSubmitHook
""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``driver_classapth`` argument  to SparkSubmit Hook and Operator was
generating ``--driver-classpath`` on the spark command line, but this isn't a
valid option to spark.

The argument has been renamed to ``driver_class_path``  and  the option it
generates has been fixed.

New Feature
^^^^^^^^^^^

- [AIRFLOW-4232] Add ``none_skipped`` trigger rule (#5032)
- [AIRFLOW-3971] Add Google Cloud Natural Language operators (#4980)
- [AIRFLOW-4069] Add Opsgenie Alert Hook and Operator (#4903)
- [AIRFLOW-3552] Fix encoding issue in ImapAttachmentToS3Operator (#5040)
- [AIRFLOW-3552] Add ImapAttachmentToS3Operator (#4476)
- [AIRFLOW-1526] Add dingding hook and operator (#4895)
- [AIRFLOW-3490] Add BigQueryHook's Ability to Patch Table/View (#4299)
- [AIRFLOW-3918] Add SSH private-key support to git-sync for KubernetesExecutor (#4777)
- [AIRFLOW-3659] Create Google Cloud Transfer Service Operators (#4792)
- [AIRFLOW-3939] Add Google Cloud Translate operator (#4755)
- [AIRFLOW-3541] Add Avro logical type conversion to bigquery hook (#4553)
- [AIRFLOW-4106] instrument staving tasks in pool (#4927)
- [AIRFLOW-2568] Azure Container Instances operator (#4121)
- [AIRFLOW-4107] instrument executor (#4928)
- [AIRFLOW-4033] record stats of task duration (#4858)
- [AIRFLOW-3892] Create Redis pub sub sensor (#4712)
- [AIRFLOW-4124] add get_table and get_table_location in aws_glue_hook and tests (#4942)
- [AIRFLOW-1262] Adds missing docs for email configuration (#4557)
- [AIRFLOW-3701] Add Google Cloud Vision Product Search operators (#4665)
- [AIRFLOW-3766] Add support for kubernetes annotations (#4589)
- [AIRFLOW-3741] Add extra config to Oracle hook (#4584)
- [AIRFLOW-1262] Allow configuration of email alert subject and body (#2338)
- [AIRFLOW-2985] Operators for S3 object copying/deleting (#3823)
- [AIRFLOW-2993] s3_to_sftp and sftp_to_s3 operators (#3828)
- [AIRFLOW-3799] Add compose method to GoogleCloudStorageHook (#4641)
- [AIRFLOW-3218] add support for poking a whole DAG (#4058)
- [AIRFLOW-3315] Add ImapAttachmentSensor (#4161)
- [AIRFLOW-3556] Add cross join set dependency function (#4356)

Improvement
^^^^^^^^^^^

- [AIRFLOW-3823] Exclude branch's downstream tasks from the tasks to skip (#4666)
- [AIRFLOW-3274] Add run_as_user and fs_group options for Kubernetes (#4648)
- [AIRFLOW-4247] Template Region on the DataprocOperators (#5046)
- [AIRFLOW-4008] Add envFrom for Kubernetes Executor (#4952)
- [AIRFLOW-3947] Flash msg for no DAG-level access error (#4767)
- [AIRFLOW-3287] Moving database clean-up code into the CoreTest.tearDown() (#4122)
- [AIRFLOW-4058] Name models test file to get automatically picked up (#4901)
- [AIRFLOW-3830] Remove DagBag from /dag_details (#4831)
- [AIRFLOW-3596] Clean up undefined template variables. (#4401)
- [AIRFLOW-3573] Remove DagStat table (#4378)
- [AIRFLOW-3623] Fix bugs in Download task logs (#5005)
- [AIRFLOW-4173] Improve SchedulerJob.process_file() (#4993)
- [AIRFLOW-3540] Warn if old airflow.cfg file is found (#5006)
- [AIRFLOW-4000] Return response when no file (#4822)
- [AIRFLOW-3383] Rotate fernet keys. (#4225)
- [AIRFLOW-3003] Pull the krb5 image instead of building (#3844)
- [AIRFLOW-3862] Check types with mypy. (#4685)
- [AIRFLOW-251] Add option SQL_ALCHEMY_SCHEMA parameter to specify schema for metadata (#4199)
- [AIRFLOW-1814] Temple PythonOperator {op_args,op_kwargs} fields (#4691)
- [AIRFLOW-3730] Standardization use of logs mechanisms (#4556)
- [AIRFLOW-3770] Validation of documentation on CI] (#4593)
- [AIRFLOW-3866] Run docker-compose pull silently in CI (#4688)
- [AIRFLOW-3685] Move licence header check (#4497)
- [AIRFLOW-3670] Add stages to Travis build (#4477)
- [AIRFLOW-3937] KubernetesPodOperator support for envFrom configMapRef and secretRef (#4772)
- [AIRFLOW-3408] Remove outdated info from Systemd Instructions (#4269)
- [AIRFLOW-3202] add missing documentation for AWS hooks/operator (#4048)
- [AIRFLOW-3908] Add more Google Cloud Vision operators (#4791)
- [AIRFLOW-2915] Add example DAG for GoogleCloudStorageToBigQueryOperator (#3763)
- [AIRFLOW-3062] Add Qubole in integration docs (#3946)
- [AIRFLOW-3288] Add SNS integration (#4123)
- [AIRFLOW-3148] Remove unnecessary arg "parameters" in RedshiftToS3Transfer (#3995)
- [AIRFLOW-3049] Add extra operations for Mongo hook (#3890)
- [AIRFLOW-3559] Add missing options to DatadogHook. (#4362)
- [AIRFLOW-1191] Simplify override of spark submit command. (#4360)
- [AIRFLOW-3155] Add ability to filter by a last modified time in GCS Operator (#4008)
- [AIRFLOW-2864] Fix docstrings for SubDagOperator (#3712)
- [AIRFLOW-4062] Improve docs on install extra package commands (#4966)
- [AIRFLOW-3743] Unify different methods of working out AIRFLOW_HOME (#4705)
- [AIRFLOW-4002] Option to open debugger on errors in ``airflow test``. (#4828)
- [AIRFLOW-3997] Extend Variable.get so it can return None when var not found (#4819)
- [AIRFLOW-4009] Fix docstring issue in GCSToBQOperator (#4836)
- [AIRFLOW-3980] Unify logger (#4804)
- [AIRFLOW-4076] Correct port type of beeline_default in init_db (#4908)
- [AIRFLOW-4046] Add validations for poke_interval & timeout for Sensor (#4878)
- [AIRFLOW-3744] Abandon the use of obsolete aliases of methods (#4568)
- [AIRFLOW-3865] Add API endpoint to get Python code of dag by id (#4687)
- [AIRFLOW-3516] Support to create k8 worker pods in batches (#4434)
- [AIRFLOW-2843] Add flag in ExternalTaskSensor to check if external DAG/task exists (#4547)
- [AIRFLOW-2224] Add support CSV files in MySqlToGoogleCloudStorageOperator (#4738)
- [AIRFLOW-3895] GoogleCloudStorageHook/Op create_bucket takes optional resource params (#4717)
- [AIRFLOW-3950] Improve AirflowSecurityManager.update_admin_perm_view (#4774)
- [AIRFLOW-4006] Make better use of Set in AirflowSecurityManager (#4833)
- [AIRFLOW-3917] Specify alternate kube config file/context when running out of cluster (#4859)
- [AIRFLOW-3911] Change Harvesting DAG parsing results to DEBUG log level (#4729)
- [AIRFLOW-3584] Use ORM DAGs for index view. (#4390)
- [AIRFLOW-2821] Refine Doc "Plugins" (#3664)
- [AIRFLOW-3561] Improve queries (#4368)
- [AIRFLOW-3600] Remove dagbag from trigger (#4407)
- [AIRFLOW-3713] Updated documentation for GCP optional project_id (#4541)
- [AIRFLOW-2767] Upgrade gunicorn to 19.5.0 to avoid moderate-severity CVE (#4795)
- [AIRFLOW-3795] provide_context param is now used (#4735)
- [AIRFLOW-4012] Upgrade tabulate to 0.8.3 (#4838)
- [AIRFLOW-3623] Support download logs by attempts from UI (#4425)
- [AIRFLOW-2715] Use region setting when launching Dataflow templates (#4139)
- [AIRFLOW-3932] Update unit tests and documentation for safe mode flag. (#4760)
- [AIRFLOW-3932] Optionally skip dag discovery heuristic. (#4746)
- [AIRFLOW-3258] K8S executor environment variables section. (#4627)
- [AIRFLOW-3931] set network, subnetwork when launching dataflow template (#4744)
- [AIRFLOW-4095] Add template_fields for S3CopyObjectOperator & S3DeleteObjectsOperator (#4920)
- [AIRFLOW-2798] Remove needless code from models.py
- [AIRFLOW-3731] Constrain mysqlclient to <1.4 (#4558)
- [AIRFLOW-3139] include parameters into log.info in SQL operators, if any (#3986)
- [AIRFLOW-3174] Refine Docstring for SQL Operators & Hooks (#4043)
- [AIRFLOW-3933] Fix various typos (#4747)
- [AIRFLOW-3905] Allow using "parameters" in SqlSensor (#4723)
- [AIRFLOW-2761] Parallelize enqueue in celery executor (#4234)
- [AIRFLOW-3540] Respect environment config when looking up config file. (#4340)
- [AIRFLOW-2156] Parallelize Celery Executor task state fetching (#3830)
- [AIRFLOW-3702] Add backfill option to run backwards (#4676)
- [AIRFLOW-3821] Add replicas logic to GCP SQL example DAG (#4662)
- [AIRFLOW-3547] Fixed Jinja templating in SparkSubmitOperator (#4347)
- [AIRFLOW-3647] Add archives config option to SparkSubmitOperator (#4467)
- [AIRFLOW-3802] Updated documentation for HiveServer2Hook (#4647)
- [AIRFLOW-3817] Corrected task ids returned by BranchPythonOperator to match the dummy operator ids (#4659)
- [AIRFLOW-3782] Clarify docs around celery worker_autoscale in default_airflow.cfg (#4609)
- [AIRFLOW-1945] Add Autoscale config for Celery workers (#3989)
- [AIRFLOW-3590] Change log message of executor exit status (#4616)
- [AIRFLOW-3591] Fix start date, end date, duration for rescheduled tasks (#4502)
- [AIRFLOW-3709] Validate ``allowed_states`` for ExternalTaskSensor (#4536)
- [AIRFLOW-3522] Add support for sending Slack attachments (#4332)
- [AIRFLOW-3569] Add "Trigger DAG" button in DAG page (#4373)
- [AIRFLOW-3044] Dataflow operators accept templated job_name param (#3887)
- [AIRFLOW-3023] Fix docstring datatypes
- [AIRFLOW-2928] Use uuid4 instead of uuid1 (#3779)
- [AIRFLOW-2988] Run specifically python2 for dataflow (#3826)
- [AIRFLOW-3697] Vendorize nvd3 and slugify (#4513)
- [AIRFLOW-3692] Remove ENV variables to avoid GPL (#4506)
- [AIRFLOW-3907] Upgrade flask and set cookie security flags. (#4725)
- [AIRFLOW-3698] Add documentation for AWS Connection (#4514)
- [AIRFLOW-3616][AIRFLOW-1215] Add aliases for schema with underscore (#4523)
- [AIRFLOW-3375] Support returning multiple tasks with BranchPythonOperator (#4215)
- [AIRFLOW-3742] Fix handling of "fallback" for AirflowConfigParsxer.getint/boolean (#4674)
- [AIRFLOW-3742] Respect the ``fallback`` arg in airflow.configuration.get (#4567)
- [AIRFLOW-3789] Fix flake8 3.7 errors. (#4617)
- [AIRFLOW-3602] Improve ImapHook handling of retrieving no attachments (#4475)
- [AIRFLOW-3631] Update flake8 and fix lint. (#4436)

Bug fixes
^^^^^^^^^

- [AIRFLOW-4248] Fix ``FileExistsError`` makedirs race in file_processor_handler (#5047)
- [AIRFLOW-4240] State-changing actions should be POST requests (#5039)
- [AIRFLOW-4246] Flask-Oauthlib needs downstream dependencies pinning due to breaking changes (#5045)
- [AIRFLOW-3887] Downgrade dagre-d3 to 0.4.18 (#4713)
- [AIRFLOW-3419] Fix S3Hook.select_key on Python3 (#4970)
- [AIRFLOW-4127] Correct AzureContainerInstanceHook._get_instance_view's return (#4945)
- [AIRFLOW-4172] Fix changes for driver class path option in Spark Submit (#4992)
- [AIRFLOW-3615] Preserve case of UNIX socket paths in Connections (#4591)
- [AIRFLOW-3417] ECSOperator: pass platformVersion only for FARGATE launch type (#4256)
- [AIRFLOW-3884] Fixing doc checker, no warnings allowed anymore and fixed the current… (#4702)
- [AIRFLOW-2652] implement / enhance baseOperator deepcopy
- [AIRFLOW-4001] Update docs about how to run tests (#4826)
- [AIRFLOW-3699] Speed up Flake8 (#4515)
- [AIRFLOW-4160] Fix redirecting of 'Trigger Dag' Button in DAG Page (#4982)
- [AIRFLOW-3650] Skip running on mysql for the flaky test (#4457)
- [AIRFLOW-3423] Fix mongo hook to work with anonymous access (#4258)
- [AIRFLOW-3982] Fix race condition in CI test (#4968)
- [AIRFLOW-3982] Update DagRun state based on its own tasks (#4808)
- [AIRFLOW-3737] Kubernetes executor cannot handle long dag/task names (#4636)
- [AIRFLOW-3945] Stop inserting row when permission views unchanged (#4764)
- [AIRFLOW-4123] Add Exception handling for _change_state method in K8 Executor (#4941)
- [AIRFLOW-3771] Minor refactor securityManager (#4594)
- [AIRFLOW-987] Pass kerberos cli args keytab and principal to kerberos.run() (#4238)
- [AIRFLOW-3736] Allow int value in SqoopOperator.extra_import_options(#4906)
- [AIRFLOW-4063] Fix exception string in BigQueryHook [2/2] (#4902)
- [AIRFLOW-4063] Fix exception string in BigQueryHook (#4899)
- [AIRFLOW-4037] Log response in SimpleHttpOperator even if the response check fails
- [AIRFLOW-4044] The documentation of ``query_params`` in ``BigQueryOperator`` is wrong.  (#4876)
- [AIRFLOW-4015] Make missing API endpoints available in classic mode
- [AIRFLOW-3153] Send DAG processing stats to StatsD (#4748)
- [AIRFLOW-2966] Catch ApiException in the Kubernetes Executor (#4209)
- [AIRFLOW-4129] Escape HTML in generated tooltips (#4950)
- [AIRFLOW-4070] AirflowException -> log.warning for duplicate task dependencies (#4904)
- [AIRFLOW-4054] Fix assertEqualIgnoreMultipleSpaces util & add tests (#4886)
- [AIRFLOW-3239] Fix test recovery further (#4074)
- [AIRFLOW-4053] Fix KubePodOperator Xcom on Kube 1.13.0 (#4883)
- [AIRFLOW-2961] Refactor tests.BackfillJobTest.test_backfill_examples test (#3811)
- [AIRFLOW-3606] Fix Flake8 test & fix the Flake8 errors introduced since Flake8 test was broken (#4415)
- [AIRFLOW-3543] Fix deletion of DAG with rescheduled tasks (#4646)
- [AIRFLOW-2548] Output plugin import errors to web UI (#3930)
- [AIRFLOW-4019] Fix AWS Athena Sensor object has no attribute 'mode' (#4844)
- [AIRFLOW-3758] Fix circular import in WasbTaskHandler (#4601)
- [AIRFLOW-3706] Fix tooltip max-width by correcting ordering of CSS files (#4947)
- [AIRFLOW-4100] Correctly JSON escape data for tree/graph views (#4921)
- [AIRFLOW-3636] Fix a test introduced in #4425 (#4446)
- [AIRFLOW-3977] Add examples of trigger rules in doc (#4805)
- [AIRFLOW-2511] Fix improper failed session commit handling causing deadlocks (#4769)
- [AIRFLOW-3962] Added graceful handling for creation of dag_run of a dag which doesn't have any task (#4781)
- [AIRFLOW-3881] Correct to_csv row number (#4699)
- [AIRFLOW-3875] Simplify SlackWebhookHook code and change docstring (#4696)
- [AIRFLOW-3733] Don't raise NameError in HQL hook to_csv when no rows returned (#4560)
- [AIRFLOW-3734] Fix hql not run when partition is None (#4561)
- [AIRFLOW-3767] Correct bulk insert function (#4773)
- [AIRFLOW-4087] remove sudo in basetaskrunner on_finish (#4916)
- [AIRFLOW-3768] Escape search parameter in pagination controls (#4911)
- [AIRFLOW-4045] Fix hard-coded URLs in FAB-based UI (#4914)
- [AIRFLOW-3123] Use a stack for DAG context management (#3956)
- [AIRFLOW-3060] DAG context manager fails to exit properly in certain circumstances
- [AIRFLOW-3924] Fix try number in alert emails (#4741)
- [AIRFLOW-4083] Add tests for link generation utils (#4912)
- [AIRFLOW-2190] Send correct HTTP status for base_url not found (#4910)
- [AIRFLOW-4015] Add get_dag_runs GET endpoint to "classic" API (#4884)
- [AIRFLOW-3239] Enable existing CI tests (#4131)
- [AIRFLOW-1390] Update Alembic to 0.9 (#3935)
- [AIRFLOW-3885] Fix race condition in scheduler test (#4737)
- [AIRFLOW-3885] ~10x speed-up of SchedulerJobTest suite (#4730)
- [AIRFLOW-3780] Fix some incorrect when base_url is used (#4643)
- [AIRFLOW-3807] Fix Graph View Highlighting of Tasks (#4653)
- [AIRFLOW-3009] Import Hashable from collection.abc to fix Python 3.7 deprecation warning (#3849)
- [AIRFLOW-2231] Fix relativedelta DAG schedule_interval (#3174)
- [AIRFLOW-2641] Fix MySqlToHiveTransfer to handle MySQL DECIMAL correctly
- [AIRFLOW-3751] Option to allow malformed schemas for LDAP authentication (#4574)
- [AIRFLOW-2888] Add deprecation path for task_runner config change (#4851)
- [AIRFLOW-2930] Fix celery executor scheduler crash (#3784)
- [AIRFLOW-2888] Remove shell=True and bash from task launch (#3740)
- [AIRFLOW-3885] ~2.5x speed-up for backfill tests (#4731)
- [AIRFLOW-3885] ~20x speed-up of slowest unit test (#4726)
- [AIRFLOW-2508] Handle non string types in Operators templatized fields (#4292)
- [AIRFLOW-3792] Fix validation in BQ for useLegacySQL & queryParameters (#4626)
- [AIRFLOW-3749] Fix Edit Dag Run page when using RBAC (#4613)
- [AIRFLOW-3801] Fix DagBag collect dags invocation to prevent examples to be loaded (#4677)
- [AIRFLOW-3774] Register blueprints with RBAC web app (#4598)
- [AIRFLOW-3719] Handle StopIteration in CloudWatch logs retrieval (#4516)
- [AIRFLOW-3108] Define get_autocommit method for MsSqlHook (#4525)
- [AIRFLOW-3074] Add relevant ECS options to ECS operator. (#3908)
- [AIRFLOW-3353] Upgrade Redis client (#4834)
- [AIRFLOW-3250] Fix for Redis Hook for not authorised connection calls (#4090)
- [AIRFLOW-2009] Fix dataflow hook connection-id (#4563)
- [AIRFLOW-2190] Fix TypeError when returning 404 (#4596)
- [AIRFLOW-2876] Update Tenacity to 4.12 (#3723)
- [AIRFLOW-3923] Update flask-admin dependency to 1.5.3 to resolve security vulnerabilities from safety (#4739)
- [AIRFLOW-3683] Fix formatting of error message for invalid TriggerRule (#4490)
- [AIRFLOW-2787] Allow is_backfill to handle NULL DagRun.run_id (#3629)
- [AIRFLOW-3639] Fix request creation in Jenkins Operator (#4450)
- [AIRFLOW-3779] Don't install enum34 backport when not needed (#4620)
- [AIRFLOW-3079] Improve migration scripts to support MSSQL Server (#3964)
- [AIRFLOW-2735] Use equality, not identity, check for detecting AWS Batch failures[]
- [AIRFLOW-2706] AWS Batch Operator should use top-level job state to determine status
- [AIRFLOW-XXX] Fix typo in http_operator.py
- [AIRFLOW-XXX] Solve lodash security warning (#4820)
- [AIRFLOW-XXX] Pin version of tornado pulled in by celery. (#4815)
- [AIRFLOW-XXX] Upgrade FAB to 1.12.3 (#4694)
- [AIRFLOW-XXX] Pin pinodb dependency (#4704)
- [AIRFLOW-XXX] Pin version of Pip in tests to work around pypa/pip#6163 (#4576)
- [AIRFLOW-XXX] Fix spark submit hook KeyError (#4578)
- [AIRFLOW-XXX] Pin psycopg2 due to breaking change (#5036)
- [AIRFLOW-XXX] Pin Sendgrid dep. (#5031)
- [AIRFLOW-XXX] Fix flaky test - test_execution_unlimited_parallelism (#4988)

Misc/Internal
^^^^^^^^^^^^^

- [AIRFLOW-4144] Add description of is_delete_operator_pod (#4943)
- [AIRFLOW-3476][AIRFLOW-3477] Move Kube classes out of models.py (#4443)
- [AIRFLOW-3464] Move SkipMixin out of models.py (#4386)
- [AIRFLOW-3463] Move Log out of models.py (#4639)
- [AIRFLOW-3458] Move connection tests (#4680)
- [AIRFLOW-3461] Move TaskFail out of models.py (#4630)
- [AIRFLOW-3462] Move TaskReschedule out of models.py (#4618)
- [AIRFLOW-3474] Move SlaMiss out of models.py (#4608)
- [AIRFLOW-3475] Move ImportError out of models.py (#4383)
- [AIRFLOW-3459] Move DagPickle to separate file (#4374)
- [AIRFLOW-3925] Don't pull docker-images on pretest (#4740)
- [AIRFLOW-4154] Correct string formatting in jobs.py (#4972)
- [AIRFLOW-3458] Deprecation path for moving models.Connection
- [AIRFLOW-3458] Move models.Connection into separate file (#4335)
- [AIRFLOW-XXX] Remove old/non-test files that nose ignores (#4930)

Doc-only changes
^^^^^^^^^^^^^^^^

- [AIRFLOW-3996] Add view source link to included fragments
- [AIRFLOW-3811] Automatic generation of API Reference in docs (#4788)
- [AIRFLOW-3810] Remove duplicate autoclass directive (#4656)
- [AIRFLOW-XXX] Mention that StatsD must be installed to gather metrics (#5038)
- [AIRFLOW-XXX] Add contents to cli (#4825)
- [AIRFLOW-XXX] fix check docs failure on CI (#4998)
- [AIRFLOW-XXX] Fix syntax docs errors (#4789)
- [AIRFLOW-XXX] Docs rendering improvement (#4684)
- [AIRFLOW-XXX] Automatically link Jira/GH on doc's changelog page (#4587)
- [AIRFLOW-XXX] Mention Oracle in the Extra Packages documentation (#4987)
- [AIRFLOW-XXX] Drop deprecated sudo option; use default docker compose on Travis. (#4732)
- [AIRFLOW-XXX] Update kubernetes.rst docs (#3875)
- [AIRFLOW-XXX] Improvements to formatted content in documentation (#4835)
- [AIRFLOW-XXX] Add Daniel to committer list (#4961)
- [AIRFLOW-XXX] Add Xiaodong Deng to committers list
- [AIRFLOW-XXX] Add history become ASF top level project (#4757)
- [AIRFLOW-XXX] Move out the examples from integration.rst (#4672)
- [AIRFLOW-XXX] Extract reverse proxy info to a separate file (#4657)
- [AIRFLOW-XXX] Reduction of the number of warnings in the documentation (#4585)
- [AIRFLOW-XXX] Fix GCS Operator docstrings (#4054)
- [AIRFLOW-XXX] Fix Docstrings in Hooks, Sensors & Operators (#4137)
- [AIRFLOW-XXX] Split guide for operators to multiple files (#4814)
- [AIRFLOW-XXX] Split connection guide to multiple files (#4824)
- [AIRFLOW-XXX] Remove almost all warnings from building docs (#4588)
- [AIRFLOW-XXX] Add backreference in docs between operator and integration (#4671)
- [AIRFLOW-XXX] Improve linking to classes (#4655)
- [AIRFLOW-XXX] Mock optional modules when building docs (#4586)
- [AIRFLOW-XXX] Update plugin macros documentation (#4971)
- [AIRFLOW-XXX] Add missing docstring for 'autodetect' in GCS to BQ Operator (#4979)
- [AIRFLOW-XXX] Add missing GCP operators to Docs (#4260)
- [AIRFLOW-XXX] Fixing the issue in Documentation (#3756)
- [AIRFLOW-XXX] Add Hint at user defined macros (#4885)
- [AIRFLOW-XXX] Correct schedule_interval in Scheduler docs (#4157)
- [AIRFLOW-XXX] Improve airflow-jira script to make RelManager's life easier (#4857)
- [AIRFLOW-XXX] Add missing class references to docs (#4644)
- [AIRFLOW-XXX] Fix typo (#4564)
- [AIRFLOW-XXX] Add a doc about fab security (#4595)
- [AIRFLOW-XXX] Speed up DagBagTest cases (#3974)


Airflow 1.10.2 (2019-01-19)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

New ``dag_processor_manager_log_location`` config option
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The DAG parsing manager log now by default will be log into a file, where its location is
controlled by the new ``dag_processor_manager_log_location`` config option in core section.

DAG level Access Control for new RBAC UI
""""""""""""""""""""""""""""""""""""""""

Extend and enhance new Airflow RBAC UI to support DAG level ACL. Each dag now has two permissions(one for write, one for read) associated('can_dag_edit', 'can_dag_read').
The admin will create new role, associate the dag permission with the target dag and assign that role to users. That user can only access / view the certain dags on the UI
that he has permissions on. If a new role wants to access all the dags, the admin could associate dag permissions on an artificial view(\ ``all_dags``\ ) with that role.

We also provide a new cli command(\ ``sync_perm``\ ) to allow admin to auto sync permissions.

Modification to ``ts_nodash`` macro
"""""""""""""""""""""""""""""""""""""""

``ts_nodash`` previously contained TimeZone information along with execution date. For Example: ``20150101T000000+0000``. This is not user-friendly for file or folder names which was a popular use case for ``ts_nodash``. Hence this behavior has been changed and using ``ts_nodash`` will no longer contain TimeZone information, restoring the pre-1.10 behavior of this macro. And a new macro ``ts_nodash_with_tz`` has been added which can be used to get a string with execution date and timezone info without dashes.

Examples:


* ``ts_nodash``\ : ``20150101T000000``
* ``ts_nodash_with_tz``\ : ``20150101T000000+0000``

Semantics of next_ds/prev_ds changed for manually triggered runs
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

next_ds/prev_ds now map to execution_date instead of the next/previous schedule-aligned execution date for DAGs triggered in the UI.

User model changes
""""""""""""""""""

This patch changes the ``User.superuser`` field from a hard-coded boolean to a ``Boolean()`` database column. ``User.superuser`` will default to ``False``\ , which means that this privilege will have to be granted manually to any users that may require it.

For example, open a Python shell and

.. code-block:: python

   from airflow import models, settings

   session = settings.Session()
   users = session.query(models.User).all()  # [admin, regular_user]

   users[1].superuser  # False

   admin = users[0]
   admin.superuser = True
   session.add(admin)
   session.commit()

Custom auth backends interface change
"""""""""""""""""""""""""""""""""""""

We have updated the version of flask-login we depend upon, and as a result any
custom auth backends might need a small change: ``is_active``\ ,
``is_authenticated``\ , and ``is_anonymous`` should now be properties. What this means is if
previously you had this in your user class

.. code-block:: python

   def is_active(self):
       return self.active

then you need to change it like this

.. code-block:: python

   @property
   def is_active(self):
       return self.active

Support autodetected schemas to GoogleCloudStorageToBigQueryOperator
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

GoogleCloudStorageToBigQueryOperator is now support schema auto-detection is available when you load data into BigQuery. Unfortunately, changes can be required.

If BigQuery tables are created outside of airflow and the schema is not defined in the task, multiple options are available:

define a schema_fields:

.. code-block:: python

   gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
       # ...
       schema_fields={...}
   )

or define a schema_object:

.. code-block:: python

   gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
       # ...
       schema_object="path/to/schema/object"
   )

or enabled autodetect of schema:

.. code-block:: python

   gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
       # ...
       autodetect=True
   )

New features
^^^^^^^^^^^^

- [AIRFLOW-2658] Add GCP specific k8s pod operator (#3532)
- [AIRFLOW-2440] Google Cloud SQL import/export operator (#4251)
- [AIRFLOW-3212] Add AwsGlueCatalogPartitionSensor (#4112)
- [AIRFLOW-2750] Add subcommands to delete and list users
- [AIRFLOW-3480] Add GCP Spanner Database Operators (#4353)
- [AIRFLOW-3560] Add DayOfWeek Sensor (#4363)
- [AIRFLOW-3371] BigQueryHook's Ability to Create View (#4213)
- [AIRFLOW-3332] Add method to allow inserting rows into BQ table (#4179)
- [AIRFLOW-3055] add get_dataset and get_datasets_list to bigquery_hook (#3894)
- [AIRFLOW-2887] Added BigQueryCreateEmptyDatasetOperator and create_empty_dataset to bigquery_hook (#3876)
- [AIRFLOW-2758] Add a sensor for MongoDB
- [AIRFLOW-2640] Add Cassandra table sensor
- [AIRFLOW-3398] Google Cloud Spanner instance database query operator (#4314)
- [AIRFLOW-3310] Google Cloud Spanner deploy / delete operators (#4286)
- [AIRFLOW-3406] Implement an Azure CosmosDB operator (#4265)
- [AIRFLOW-3434] Allows creating intermediate dirs in SFTPOperator (#4270)
- [AIRFLOW-3345] Add Google Cloud Storage (GCS) operators for ACL (#4192)
- [AIRFLOW-3266] Add AWS Athena Hook and Operator (#4111)
- [AIRFLOW-3346] Add hook and operator for GCP transfer service (#4189)
- [AIRFLOW-2983] Add prev_ds_nodash and next_ds_nodash macro (#3821)
- [AIRFLOW-3403] Add AWS Athena Sensor (#4244)
- [AIRFLOW-3323] Support HTTP basic authentication for Airflow Flower (#4166)
- [AIRFLOW-3410] Add feature to allow Host Key Change for SSH Op (#4249)
- [AIRFLOW-3275] Add Google Cloud SQL Query operator (#4170)
- [AIRFLOW-2691] Manage JS dependencies via npm
- [AIRFLOW-2795] Oracle to Oracle Transfer Operator (#3639)
- [AIRFLOW-2596] Add Oracle to Azure Datalake Transfer Operator
- [AIRFLOW-3220] Add Instance Group Manager Operators for GCE (#4167)
- [AIRFLOW-2882] Add import and export for pool cli using JSON
- [AIRFLOW-2965] CLI tool to show the next execution datetime (#3834)
- [AIRFLOW-2874] Enables FAB's theme support (#3719)
- [AIRFLOW-3336] Add new TriggerRule for 0 upstream failures (#4182)

Improvements
^^^^^^^^^^^^

- [AIRFLOW-3680] Consistency update in tests for All GCP-related operators (#4493)
- [AIRFLOW-3675] Use googleapiclient for google apis (#4484)
- [AIRFLOW-3205] Support multipart uploads to GCS (#4084)
- [AIRFLOW-2826] Add GoogleCloudKMSHook (#3677)
- [AIRFLOW-3676] Add required permission to CloudSQL export/import example (#4489)
- [AIRFLOW-3679] Added Google Cloud Base Hook to documentation (#4487)
- [AIRFLOW-3594] Unify different License Header
- [AIRFLOW-3197] Remove invalid parameter KeepJobFlowAliveWhenNoSteps in example DAG (#4404)
- [AIRFLOW-3504] Refine the functionality of "/health" endpoint (#4309)
- [AIRFLOW-3103][AIRFLOW-3147] Update flask-appbuilder (#3937)
- [AIRFLOW-3168] More resilient database use in CI (#4014)
- [AIRFLOW-3076] Remove preloading of MySQL testdata (#3911)
- [AIRFLOW-3035] Allow custom 'job_error_states' in dataproc ops (#3884)
- [AIRFLOW-3246] Make hmsclient optional in airflow.hooks.hive_hooks (#4080)
- [AIRFLOW-3059] Log how many rows are read from Postgres (#3905)
- [AIRFLOW-2463] Make task instance context available for hive queries
- [AIRFLOW-3190] Make flake8 compliant (#4035)
- [AIRFLOW-1998] Implemented DatabricksRunNowOperator for jobs/run-now … (#3813)
- [AIRFLOW-2267] Airflow DAG level access (#3197)
- [AIRFLOW-2359] Add set failed for DagRun and task in tree view (#3255)
- [AIRFLOW-3008] Move Kubernetes example DAGs to contrib
- [AIRFLOW-3402] Support global k8s affinity and toleration configs (#4247)
- [AIRFLOW-3610] Add region param for EMR jobflow creation (#4418)
- [AIRFLOW-3531] Fix test for GCS to GCS Transfer Hook (#4452)
- [AIRFLOW-3531] Add gcs to gcs transfer operator. (#4331)
- [AIRFLOW-3034] Readme updates : Add Slack & Twitter, remove Gitter
- [AIRFLOW-3028] Update Text & Images in Readme.md
- [AIRFLOW-208] Add badge to show supported Python versions (#3839)
- [AIRFLOW-2238] Update PR tool to push directly to GitHub
- [AIRFLOW-2238] Flake8 fixes on dev/airflow-pr
- [AIRFLOW-2238] Update PR tool to remove outdated info (#3978)
- [AIRFLOW-3005] Replace 'Airbnb Airflow' with 'Apache Airflow' (#3845)
- [AIRFLOW-3150] Make execution_date templated in TriggerDagRunOperator (#4359)
- [AIRFLOW-1196][AIRFLOW-2399] Add templated field in TriggerDagRunOperator (#4228)
- [AIRFLOW-3340] Placeholder support in connections form (#4185)
- [AIRFLOW-3446] Add Google Cloud BigTable operators (#4354)
- [AIRFLOW-1921] Add support for https and user auth (#2879)
- [AIRFLOW-2770] Read ``dags_in_image`` config value as a boolean (#4319)
- [AIRFLOW-3022] Add volume mount to KubernetesExecutorConfig (#3855)
- [AIRFLOW-2917] Set AIRFLOW__CORE__SQL_ALCHEMY_CONN only when needed (#3766)
- [AIRFLOW-2712] Pass annotations to KubernetesExecutorConfig
- [AIRFLOW-461] Support autodetected schemas in BigQuery run_load (#3880)
- [AIRFLOW-2997] Support cluster fields in bigquery (#3838)
- [AIRFLOW-2916] Arg ``verify`` for AwsHook() & S3 sensors/operators (#3764)
- [AIRFLOW-491] Add feature to pass extra api configs to BQ Hook (#3733)
- [AIRFLOW-2889] Fix typos detected by github.com/client9/misspell (#3732)
- [AIRFLOW-850] Add a PythonSensor (#4349)
- [AIRFLOW-2747] Explicit re-schedule of sensors (#3596)
- [AIRFLOW-3392] Add index on dag_id in sla_miss table (#4235)
- [AIRFLOW-3001] Add index 'ti_dag_date' to taskinstance (#3885)
- [AIRFLOW-2861] Add index on log table (#3709)
- [AIRFLOW-3518] Performance fixes for topological_sort of Tasks (#4322)
- [AIRFLOW-3521] Fetch more than 50 items in ``airflow-jira compare`` script (#4300)
- [AIRFLOW-1919] Add option to query for DAG runs given a DAG ID
- [AIRFLOW-3444] Explicitly set transfer operator description. (#4279)
- [AIRFLOW-3411]  Add OpenFaaS hook (#4267)
- [AIRFLOW-2785] Add context manager entry points to mongoHook
- [AIRFLOW-2524] Add SageMaker doc to AWS integration section (#4278)
- [AIRFLOW-3479] Keeps records in Log Table when DAG is deleted (#4287)
- [AIRFLOW-2948] Arg check & better doc - SSHOperator & SFTPOperator (#3793)
- [AIRFLOW-2245] Add remote_host of SSH/SFTP operator as templated field (#3765)
- [AIRFLOW-2670] Update SSH Operator's Hook to respect timeout (#3666)
- [AIRFLOW-3380] Add metrics documentation (#4219)
- [AIRFLOW-3361] Log the task_id in the PendingDeprecationWarning from BaseOperator (#4030)
- [AIRFLOW-3213] Create ADLS to GCS operator (#4134)
- [AIRFLOW-3395] added the REST API endpoints to the doc (#4236)
- [AIRFLOW-3294] Update connections form and integration docs (#4129)
- [AIRFLOW-3236] Create AzureDataLakeStorageListOperator (#4094)
- [AIRFLOW-3062] Add Qubole in integration docs (#3946)
- [AIRFLOW-3306] Disable flask-sqlalchemy modification tracking. (#4146)
- [AIRFLOW-2867] Refactor Code to conform standards (#3714)
- [AIRFLOW-2753] Add dataproc_job_id instance var holding actual DP jobId
- [AIRFLOW-3132] Enable specifying auto_remove option for DockerOperator (#3977)
- [AIRFLOW-2731] Raise psutil restriction to <6.0.0
- [AIRFLOW-3384] Allow higher versions of Sqlalchemy and Jinja2 (#4227)
- [Airflow-2760] Decouple DAG parsing loop from scheduler loop (#3873)
- [AIRFLOW-3004] Add config disabling scheduler cron (#3899)
- [AIRFLOW-3175] Fix docstring format in airflow/jobs.py (#4025)
- [AIRFLOW-3589] Visualize reschedule state in all views (#4408)
- [AIRFLOW-2698] Simplify Kerberos code (#3563)
- [AIRFLOW-2499] Dockerise CI pipeline (#3393)
- [AIRFLOW-3432] Add test for feature "Delete DAG in UI" (#4266)
- [AIRFLOW-3301] Update DockerOperator CI test for PR #3977 (#4138)
- [AIRFLOW-3478] Make sure that the session is closed
- [AIRFLOW-3687] Add missing @apply_defaults decorators (#4498)
- [AIRFLOW-3691] Update notice to 2019 (#4503)
- [AIRFLOW-3689] Update pop-up message when deleting DAG in RBAC UI (#4505)
- [AIRFLOW-2801] Skip test_mark_success_no_kill in PostgreSQL on CI (#3642)
- [AIRFLOW-3693] Replace psycopg2-binary by psycopg2 (#4508)
- [AIRFLOW-3700] Change the lowest allowed version of "requests" (#4517)
- [AIRFLOW-3704] Support SSL Protection When Redis is Used as Broker for CeleryExecutor (#4521)
- [AIRFLOW-3681] All GCP operators have now optional GCP Project ID (#4500)
- [Airflow 2782] Upgrades Dagre D3 version to latest possible
- [Airflow 2783] Implement eslint for JS code check (#3641)
- [AIRFLOW-2805] Display multiple timezones on UI (#3687)
- [AIRFLOW-3302] Small CSS fixes (#4140)
- [Airflow-2766] Respect shared datetime across tabs
- [AIRFLOW-2776] Compress tree view JSON
- [AIRFLOW-2407] Use feature detection for reload() (#3298)
- [AIRFLOW-3452] Removed an unused/dangerous display-none (#4295)
- [AIRFLOW-3348] Update run statistics on dag refresh (#4197)
- [AIRFLOW-3125] Monitor Task Instances creation rates (#3966)


Bug fixes
^^^^^^^^^

- [AIRFLOW-3191] Fix not being able to specify execution_date when creating dagrun (#4037)
- [AIRFLOW-3657] Fix zendesk integration (#4466)
- [AIRFLOW-3605] Load plugins from entry_points (#4412)
- [AIRFLOW-3646] Rename plugins_manager.py to test_xx to trigger tests (#4464)
- [AIRFLOW-3655] Escape links generated in model views (#4463)
- [AIRFLOW-3662] Add dependency for Enum (#4468)
- [AIRFLOW-3630] Cleanup of GCP Cloud SQL Connection (#4451)
- [AIRFLOW-1837] Respect task start_date when different from dag's (#4010)
- [AIRFLOW-2829] Brush up the CI script for minikube
- [AIRFLOW-3519] Fix example http operator (#4455)
- [AIRFLOW-2811] Fix scheduler_ops_metrics.py to work (#3653)
- [AIRFLOW-2751] add job properties update in hive to druid operator.
- [AIRFLOW-2918] Remove unused imports
- [AIRFLOW-2918] Fix Flake8 violations (#3931)
- [AIRFLOW-2771] Add except type to broad S3Hook try catch clauses
- [AIRFLOW-2918] Fix Flake8 violations (#3772)
- [AIRFLOW-2099] Handle getsource() calls gracefully
- [AIRFLOW-3397] Fix integrity error in rbac AirflowSecurityManager (#4305)
- [AIRFLOW-3281] Fix Kubernetes operator with git-sync (#3770)
- [AIRFLOW-2615] Limit DAGs parsing to once only
- [AIRFLOW-2952] Fix Kubernetes CI (#3922)
- [AIRFLOW-2933] Enable Codecov on Docker-CI Build (#3780)
- [AIRFLOW-2082] Resolve a bug in adding password_auth to api as auth method (#4343)
- [AIRFLOW-3612] Remove incubation/incubator mention (#4419)
- [AIRFLOW-3581] Fix next_ds/prev_ds semantics for manual runs (#4385)
- [AIRFLOW-3527] Update Cloud SQL Proxy to have shorter path for UNIX socket (#4350)
- [AIRFLOW-3316] For gcs_to_bq: add missing init of schema_fields var (#4430)
- [AIRFLOW-3583] Fix AirflowException import (#4389)
- [AIRFLOW-3578] Fix Type Error for BigQueryOperator (#4384)
- [AIRFLOW-2755] Added ``kubernetes.worker_dags_folder`` configuration (#3612)
- [AIRFLOW-2655] Fix inconsistency of default config of kubernetes worker
- [AIRFLOW-2645][AIRFLOW-2617] Add worker_container_image_pull_policy
- [AIRFLOW-2661] fix config dags_volume_subpath and logs_volume_subpath
- [AIRFLOW-3550] Standardize GKE hook (#4364)
- [AIRFLOW-2863] Fix GKEClusterHook catching wrong exception (#3711)
- [AIRFLOW-2939][AIRFLOW-3568] Fix TypeError in GCSToS3Op & S3ToGCSOp (#4371)
- [AIRFLOW-3327] Add support for location in BigQueryHook (#4324)
- [AIRFLOW-3438] Fix default values in BigQuery Hook & BigQueryOperator (…
- [AIRFLOW-3355] Fix BigQueryCursor.execute to work with Python3 (#4198)
- [AIRFLOW-3447] Add 2 options for ts_nodash Macro (#4323)
- [AIRFLOW-1552] Airflow Filter_by_owner not working with password_auth (#4276)
- [AIRFLOW-3484] Fix Over-logging in the k8s executor (#4296)
- [AIRFLOW-3309] Add MongoDB connection (#4154)
- [AIRFLOW-3414] Fix reload_module in DagFileProcessorAgent (#4253)
- [AIRFLOW-1252] API accept JSON when invoking a trigger dag (#2334)
- [AIRFLOW-3425] Fix setting default scope in hook (#4261)
- [AIRFLOW-3416] Fixes Python 3 compatibility with CloudSqlQueryOperator (#4254)
- [AIRFLOW-3263] Ignore exception when 'run' kills already killed job (#4108)
- [AIRFLOW-3264] URL decoding when parsing URI for connection (#4109)
- [AIRFLOW-3365][AIRFLOW-3366] Allow celery_broker_transport_options to be set with environment variables (#4211)
- [AIRFLOW-2642] fix wrong value git-sync initcontainer env GIT_SYNC_ROOT (#3519)
- [AIRFLOW-3353] Pin redis version (#4195)
- [AIRFLOW-3251] KubernetesPodOperator now uses 'image_pull_secrets' argument when creating Pods (#4188)
- [AIRFLOW-2705] Move class-level moto decorator to method-level
- [AIRFLOW-3233] Fix deletion of DAGs in the UI (#4069)
- [AIRFLOW-2908] Allow retries with KubernetesExecutor. (#3758)
- [AIRFLOW-1561] Fix scheduler to pick up example DAGs without other DAGs (#2635)
- [AIRFLOW-3352] Fix expose_config not honoured on RBAC UI (#4194)
- [AIRFLOW-3592] Fix logs when task is in rescheduled state (#4492)
- [AIRFLOW-3634] Fix GCP Spanner Test (#4440)
- [AIRFLOW-XXX] Fix PythonVirtualenvOperator tests (#3968)
- [AIRFLOW-3239] Fix/refine tests for api/common/experimental/ (#4255)
- [AIRFLOW-2951] Update dag_run table end_date when state change (#3798)
- [AIRFLOW-2756] Fix bug in set DAG run state workflow (#3606)
- [AIRFLOW-3690] Fix bug to set state of a task for manually-triggered DAGs (#4504)
- [AIRFLOW-3319] KubernetsExecutor: Need in try_number in labels if getting them later (#4163)
- [AIRFLOW-3724] Fix the broken refresh button on Graph View in RBAC UI
- [AIRFLOW-3732] Fix issue when trying to edit connection in RBAC UI
- [AIRFLOW-2866] Fix missing CSRF token head when using RBAC UI (#3804)
- [AIRFLOW-3259] Fix internal server error when displaying charts (#4114)
- [AIRFLOW-3271] Fix issue with persistence of RBAC Permissions modified via UI (#4118)
- [AIRFLOW-3141] Handle duration View for missing dag (#3984)
- [AIRFLOW-2766] Respect shared datetime across tabs
- [AIRFLOW-1413] Fix FTPSensor failing on error message with unexpected (#2450)
- [AIRFLOW-3378] KubernetesPodOperator does not delete on timeout failure (#4218)
- [AIRFLOW-3245] Fix list processing in resolve_template_files (#4086)
- [AIRFLOW-2703] Catch transient DB exceptions from scheduler's heartbeat it does not crash (#3650)
- [AIRFLOW-1298] Clear UPSTREAM_FAILED using the clean cli (#3886)

Doc-only changes
^^^^^^^^^^^^^^^^

- [AIRFLOW-XXX] GCP operators documentation clarifications (#4273)
- [AIRFLOW-XXX] Docs: Fix paths to GCS transfer operator (#4479)
- [AIRFLOW-XXX] Add missing GCP operators to Docs (#4260)
- [AIRFLOW-XXX] Fix Docstrings for Operators (#3820)
- [AIRFLOW-XXX] Fix inconsistent comment in example_python_operator.py (#4337)
- [AIRFLOW-XXX] Fix incorrect parameter in SFTPOperator example (#4344)
- [AIRFLOW-XXX] Add missing remote logging field (#4333)
- [AIRFLOW-XXX] Revise template variables documentation (#4172)
- [AIRFLOW-XXX] Fix typo in docstring of gcs_to_bq (#3833)
- [AIRFLOW-XXX] Fix display of SageMaker operators/hook docs (#4263)
- [AIRFLOW-XXX] Better instructions for Airflow flower (#4214)
- [AIRFLOW-XXX] Make pip install commands consistent (#3752)
- [AIRFLOW-XXX] Add ``BigQueryGetDataOperator`` to Integration Docs (#4063)
- [AIRFLOW-XXX] Don't spam test logs with "bad cron expression" messages (#3973)
- [AIRFLOW-XXX] Update committer list based on latest TLP discussion (#4427)
- [AIRFLOW-XXX] Fix incorrect statement in contributing guide (#4104)
- [AIRFLOW-XXX] Fix Broken Link in CONTRIBUTING.md
- [AIRFLOW-XXX] Update Contributing Guide - Git Hooks (#4120)
- [AIRFLOW-3426] Correct Python Version Documentation Reference (#4259)
- [AIRFLOW-2663] Add instructions to install SSH dependencies
- [AIRFLOW-XXX] Clean up installation extra packages table (#3750)
- [AIRFLOW-XXX] Remove redundant space in Kerberos (#3866)
- [AIRFLOW-3086] Add extras group for google auth to setup.py (#3917)
- [AIRFLOW-XXX] Add Kubernetes Dependency in Extra Packages Doc (#4281)
- [AIRFLOW-3696] Add Version info to Airflow Documentation (#4512)
- [AIRFLOW-XXX] Correct Typo in sensor's exception (#4545)
- [AIRFLOW-XXX] Fix a typo of config (#4544)
- [AIRFLOW-XXX] Fix BashOperator Docstring (#4052)
- [AIRFLOW-3018] Fix Minor issues in Documentation
- [AIRFLOW-XXX] Fix Minor issues with Azure Cosmos Operator (#4289)
- [AIRFLOW-3382] Fix incorrect docstring in DatastoreHook (#4222)
- [AIRFLOW-XXX] Fix copy&paste mistake (#4212)
- [AIRFLOW-3260] Correct misleading BigQuery error (#4098)
- [AIRFLOW-XXX] Fix Typo in SFTPOperator docstring (#4016)
- [AIRFLOW-XXX] Fixing the issue in Documentation (#3998)
- [AIRFLOW-XXX] Fix undocumented params in S3_hook
- [AIRFLOW-XXX] Fix SlackWebhookOperator execute method comment (#3963)
- [AIRFLOW-3070] Refine web UI authentication-related docs (#3863)

Airflow 1.10.1 (2018-11-13)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

min_file_parsing_loop_time config option temporarily disabled
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The scheduler.min_file_parsing_loop_time config option has been temporarily removed due to
some bugs.

StatsD Metrics
""""""""""""""

The ``scheduler_heartbeat`` metric has been changed from a gauge to a counter. Each loop of the scheduler will increment the counter by 1. This provides a higher degree of visibility and allows for better integration with Prometheus using the `StatsD Exporter <https://github.com/prometheus/statsd_exporter>`_. The scheduler's activity status can be determined by graphing and alerting using a rate of change of the counter. If the scheduler goes down, the rate will drop to 0.

EMRHook now passes all of connection's extra to CreateJobFlow API
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

EMRHook.create_job_flow has been changed to pass all keys to the create_job_flow API, rather than
just specific known keys for greater flexibility.

However prior to this release the "emr_default" sample connection that was created had invalid
configuration, so creating EMR clusters might fail until your connection is updated. (Ec2KeyName,
Ec2SubnetId, TerminationProtection and KeepJobFlowAliveWhenNoSteps were all top-level keys when they
should be inside the "Instances" dict)

LDAP Auth Backend now requires TLS
""""""""""""""""""""""""""""""""""

Connecting to an LDAP server over plain text is not supported anymore. The
certificate presented by the LDAP server must be signed by a trusted
certificate, or you must provide the ``cacert`` option under ``[ldap]`` in the
config file.

If you want to use LDAP auth backend without TLS then you will have to create a
custom-auth backend based on
https://github.com/apache/airflow/blob/1.10.0/airflow/contrib/auth/backends/ldap_auth.py

New features
^^^^^^^^^^^^

- [AIRFLOW-2524] Airflow integration with AWS Sagemaker
- [AIRFLOW-2657] Add ability to delete DAG from web ui
- [AIRFLOW-2780] Adds IMAP Hook to interact with a mail server
- [AIRFLOW-2794] Add delete support for Azure blob
- [AIRFLOW-2912] Add operators for Google Cloud Functions
- [AIRFLOW-2974] Add Start/Restart/Terminate methods Databricks Hook
- [AIRFLOW-2989] No Parameter to change bootDiskType for DataprocClusterCreateOperator
- [AIRFLOW-3078] Basic operators for Google Compute Engine
- [AIRFLOW-3147] Update Flask-AppBuilder version
- [AIRFLOW-3231] Basic operators for Google Cloud SQL (deploy / patch / delete)
- [AIRFLOW-3276] Google Cloud SQL database create / patch / delete operators

Improvements
^^^^^^^^^^^^

- [AIRFLOW-393] Add progress callbacks for FTP downloads
- [AIRFLOW-520] Show Airflow version on web page
- [AIRFLOW-843] Exceptions now available in context during on_failure_callback
- [AIRFLOW-2476] Update tabulate dependency to v0.8.2
- [AIRFLOW-2592] Bump Bleach dependency
- [AIRFLOW-2622] Add "confirm=False" option to SFTPOperator
- [AIRFLOW-2662] support affinity & nodeSelector policies for kubernetes executor/operator
- [AIRFLOW-2709] Improve error handling in Databricks hook
- [AIRFLOW-2723] Update lxml dependency to >= 4.0.
- [AIRFLOW-2763] No precheck mechanism in place during worker initialisation for the connection to metadata database
- [AIRFLOW-2789] Add ability to create single node cluster to DataprocClusterCreateOperator
- [AIRFLOW-2797] Add ability to create Google Dataproc cluster with custom image
- [AIRFLOW-2854] kubernetes_pod_operator add more configuration items
- [AIRFLOW-2855] Need to Check Validity of Cron Expression When Process DAG File/Zip File
- [AIRFLOW-2904] Clean an unnecessary line in airflow/executors/celery_executor.py
- [AIRFLOW-2921] A trivial incorrectness in CeleryExecutor()
- [AIRFLOW-2922] Potential deal-lock bug in CeleryExecutor()
- [AIRFLOW-2932] GoogleCloudStorageHook - allow compression of file
- [AIRFLOW-2949] Syntax Highlight for Single Quote
- [AIRFLOW-2951] dag_run end_date Null after a dag is finished
- [AIRFLOW-2956] Kubernetes tolerations for pod operator
- [AIRFLOW-2997] Support for clustered tables in Bigquery hooks/operators
- [AIRFLOW-3006] Fix error when schedule_interval="None"
- [AIRFLOW-3008] Move Kubernetes related example DAGs to contrib/example_dags
- [AIRFLOW-3025] Allow to specify dns and dns-search parameters for DockerOperator
- [AIRFLOW-3067] (www_rbac) Flask flash messages are not displayed properly (no background color)
- [AIRFLOW-3069] Decode output of S3 file transform operator
- [AIRFLOW-3072] Assign permission get_logs_with_metadata to viewer role
- [AIRFLOW-3090] INFO logs are too verbose
- [AIRFLOW-3103] Update flask-login
- [AIRFLOW-3112] Align SFTP hook with SSH hook
- [AIRFLOW-3119] Enable loglevel on celery worker and inherit from airflow.cfg
- [AIRFLOW-3137] Make ProxyFix middleware optional
- [AIRFLOW-3173] Add _cmd options for more password config options
- [AIRFLOW-3177] Change scheduler_heartbeat metric from gauge to counter
- [AIRFLOW-3193] Pin docker requirement version to v3
- [AIRFLOW-3195] Druid Hook: Log ingestion spec and task id
- [AIRFLOW-3197] EMR Hook is missing some parameters to valid on the AWS API
- [AIRFLOW-3232] Make documentation for GCF Functions operator more readable
- [AIRFLOW-3262] Can't get log containing Response when using SimpleHttpOperator
- [AIRFLOW-3265] Add support for "unix_socket" in connection extra for Mysql Hook

Doc-only changes
^^^^^^^^^^^^^^^^

- [AIRFLOW-1441] Tutorial Inconsistencies Between Example Pipeline Definition and Recap
- [AIRFLOW-2682] Add how-to guide(s) for how to use basic operators like BashOperator and PythonOperator
- [AIRFLOW-3104] .airflowignore feature is not mentioned at all in documentation
- [AIRFLOW-3237] Refactor example DAGs
- [AIRFLOW-3187] Update Airflow.gif file with a slower version
- [AIRFLOW-3159] Update Airflow documentation on GCP Logging
- [AIRFLOW-3030] Command Line docs incorrect subdir
- [AIRFLOW-2990] Docstrings for Hooks/Operators are in incorrect format
- [AIRFLOW-3127] Celery SSL Documentation is out-dated
- [AIRFLOW-2779] Add license headers to doc files
- [AIRFLOW-2779] Add project version to license

Bug fixes
^^^^^^^^^

- [AIRFLOW-839] docker_operator.py attempts to log status key without first checking existence
- [AIRFLOW-1104] Concurrency check in scheduler should count queued tasks as well as running
- [AIRFLOW-1163] Add support for x-forwarded-* headers to support access behind AWS ELB
- [AIRFLOW-1195] Cleared tasks in SubDagOperator do not trigger Parent dag_runs
- [AIRFLOW-1508] Skipped state not part of State.task_states
- [AIRFLOW-1762] Use key_file in SSHHook.create_tunnel()
- [AIRFLOW-1837] Differing start_dates on tasks not respected by scheduler.
- [AIRFLOW-1874] Support standard SQL in Check, ValueCheck and IntervalCheck BigQuery operators
- [AIRFLOW-1917] print() from Python operators end up with extra new line
- [AIRFLOW-1970] Database cannot be initialized if an invalid fernet key is provided
- [AIRFLOW-2145] Deadlock after clearing a running task
- [AIRFLOW-2216] Cannot specify a profile for AWS Hook to load with s3 config file
- [AIRFLOW-2574] initdb fails when mysql password contains percent sign
- [AIRFLOW-2707] Error accessing log files from web UI
- [AIRFLOW-2716] Replace new Python 3.7 keywords
- [AIRFLOW-2744] RBAC app doesn't integrate plugins (blueprints etc)
- [AIRFLOW-2772] BigQuery hook does not allow specifying both the partition field name and table name at the same time
- [AIRFLOW-2778] Bad Import in collect_dag in DagBag
- [AIRFLOW-2786] Variables view fails to render if a variable has an empty key
- [AIRFLOW-2799] Filtering UI objects by datetime is broken
- [AIRFLOW-2800] Remove airflow/ low-hanging linting errors
- [AIRFLOW-2825] S3ToHiveTransfer operator may not may able to handle GZIP file with uppercase ext in S3
- [AIRFLOW-2848] dag_id is missing in metadata table "job" for LocalTaskJob
- [AIRFLOW-2860] DruidHook: time variable is not updated correctly when checking for timeout
- [AIRFLOW-2865] Race condition between on_success_callback and LocalTaskJob's cleanup
- [AIRFLOW-2893] Stuck dataflow job due to jobName mismatch.
- [AIRFLOW-2895] Prevent scheduler from spamming heartbeats/logs
- [AIRFLOW-2900] Code not visible for Packaged DAGs
- [AIRFLOW-2905] Switch to regional dataflow job service.
- [AIRFLOW-2907] Sendgrid - Attachments - ERROR - Object of type 'bytes' is not JSON serializable
- [AIRFLOW-2938] Invalid 'extra' field in connection can raise an AttributeError when attempting to edit
- [AIRFLOW-2979] Deprecated Celery Option not in Options list
- [AIRFLOW-2981] TypeError in dataflow operators when using GCS jar or py_file
- [AIRFLOW-2984] Cannot convert naive_datetime when task has a naive start_date/end_date
- [AIRFLOW-2994] flatten_results in BigQueryOperator/BigQueryHook should default to None
- [AIRFLOW-3002] ValueError in dataflow operators when using GCS jar or py_file
- [AIRFLOW-3012] Email on sla miss is send only to first address on the list
- [AIRFLOW-3046] ECS Operator mistakenly reports success when task is killed due to EC2 host termination
- [AIRFLOW-3064] No output from ``airflow test`` due to default logging config
- [AIRFLOW-3072] Only admin can view logs in RBAC UI
- [AIRFLOW-3079] Improve initdb to support MSSQL Server
- [AIRFLOW-3089] Google auth doesn't work under http
- [AIRFLOW-3099] Errors raised when some blocks are missing in airflow.cfg
- [AIRFLOW-3109] Default user permission should contain 'can_clear'
- [AIRFLOW-3111] Confusing comments and instructions for log templates in UPDATING.md and default_airflow.cfg
- [AIRFLOW-3124] Broken webserver debug mode (RBAC)
- [AIRFLOW-3136] Scheduler Failing the Task retries run while processing Executor Events
- [AIRFLOW-3138] Migration cc1e65623dc7 creates issues with postgres
- [AIRFLOW-3161] Log Url link does not link to task instance logs in RBAC UI
- [AIRFLOW-3162] HttpHook fails to parse URL when port is specified
- [AIRFLOW-3183] Potential Bug in utils/dag_processing/DagFileProcessorManager.max_runs_reached()
- [AIRFLOW-3203] Bugs in DockerOperator & Some operator test scripts were named incorrectly
- [AIRFLOW-3238] Dags, removed from the filesystem, are not deactivated on initdb
- [AIRFLOW-3268] Cannot pass SSL dictionary to mysql connection via URL
- [AIRFLOW-3277] Invalid timezone transition handling for cron schedules
- [AIRFLOW-3295] Require encryption in DaskExecutor when certificates are configured.
- [AIRFLOW-3297] EmrStepSensor marks cancelled step as successful

Airflow 1.10.0 (2018-08-03)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Installation and upgrading requires setting ``SLUGIFY_USES_TEXT_UNIDECODE=yes`` in your environment or
``AIRFLOW_GPL_UNIDECODE=yes``. In case of the latter a GPL runtime dependency will be installed due to a
dependency ``(python-nvd3 -> python-slugify -> unidecode)``.

Replace DataProcHook.await calls to DataProcHook.wait
"""""""""""""""""""""""""""""""""""""""""""""""""""""

The method name was changed to be compatible with the Python 3.7 async/await keywords

Setting UTF-8 as default mime_charset in email utils
""""""""""""""""""""""""""""""""""""""""""""""""""""

Add a configuration variable(default_dag_run_display_number) to control numbers of dag run for display
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Add a configuration variable(default_dag_run_display_number) under webserver section to control the number of dag runs to show in UI.

Default executor for SubDagOperator is changed to SequentialExecutor
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

New Webserver UI with Role-Based Access Control
"""""""""""""""""""""""""""""""""""""""""""""""

The current webserver UI uses the Flask-Admin extension. The new webserver UI uses the `Flask-AppBuilder (FAB) <https://github.com/dpgaspar/Flask-AppBuilder>`_ extension. FAB has built-in authentication support and Role-Based Access Control (RBAC), which provides configurable roles and permissions for individual users.

To turn on this feature, in your airflow.cfg file (under [webserver]), set the configuration variable ``rbac = True``\ , and then run ``airflow`` command, which will generate the ``webserver_config.py`` file in your $AIRFLOW_HOME.

Setting up Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~

FAB has built-in authentication support for DB, OAuth, OpenID, LDAP, and REMOTE_USER. The default auth type is ``AUTH_DB``.

For any other authentication type (OAuth, OpenID, LDAP, REMOTE_USER), see the `Authentication section of FAB docs <http://flask-appbuilder.readthedocs.io/en/latest/security.html#authentication-methods>`_ for how to configure variables in webserver_config.py file.

Once you modify your config file, run ``airflow db init`` to generate new tables for RBAC support (these tables will have the prefix ``ab_``\ ).

Creating an Admin Account
~~~~~~~~~~~~~~~~~~~~~~~~~

Once configuration settings have been updated and new tables have been generated, create an admin account with ``airflow create_user`` command.

Using your new UI
~~~~~~~~~~~~~~~~~

Run ``airflow webserver`` to start the new UI. This will bring up a log in page, enter the recently created admin username and password.

There are five roles created for Airflow by default: Admin, User, Op, Viewer, and Public. To configure roles/permissions, go to the ``Security`` tab and click ``List Roles`` in the new UI.

Breaking changes
~~~~~~~~~~~~~~~~

* AWS Batch Operator renamed property queue to job_queue to prevent conflict with the internal queue from CeleryExecutor - AIRFLOW-2542
* Users created and stored in the old users table will not be migrated automatically. FAB's built-in authentication support must be reconfigured.
* Airflow dag home page is now ``/home`` (instead of ``/admin``\ ).
* All ModelViews in Flask-AppBuilder follow a different pattern from Flask-Admin. The ``/admin`` part of the URL path will no longer exist. For example: ``/admin/connection`` becomes ``/connection/list``\ , ``/admin/connection/new`` becomes ``/connection/add``\ , ``/admin/connection/edit`` becomes ``/connection/edit``\ , etc.
* Due to security concerns, the new webserver will no longer support the features in the ``Data Profiling`` menu of old UI, including ``Ad Hoc Query``\ , ``Charts``\ , and ``Known Events``.
* HiveServer2Hook.get_results() always returns a list of tuples, even when a single column is queried, as per Python API 2.
* **UTC is now the default timezone**\ : Either reconfigure your workflows scheduling in UTC or set ``default_timezone`` as explained in https://airflow.apache.org/timezone.html#default-time-zone

airflow.contrib.sensors.hdfs_sensors renamed to airflow.contrib.sensors.hdfs_sensor
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

We now rename airflow.contrib.sensors.hdfs_sensors to airflow.contrib.sensors.hdfs_sensor for consistency purpose.

MySQL setting required
""""""""""""""""""""""

We now rely on more strict ANSI SQL settings for MySQL in order to have sane defaults. Make sure
to have specified ``explicit_defaults_for_timestamp=1`` in your my.cnf under ``[mysqld]``

Celery config
"""""""""""""

To make the config of Airflow compatible with Celery, some properties have been renamed:

.. code-block::

   celeryd_concurrency -> worker_concurrency
   celery_result_backend -> result_backend
   celery_ssl_active -> ssl_active
   celery_ssl_cert -> ssl_cert
   celery_ssl_key -> ssl_key

Resulting in the same config parameters as Celery 4, with more transparency.

GCP Dataflow Operators
""""""""""""""""""""""

Dataflow job labeling is now supported in Dataflow{Java,Python}Operator with a default
"airflow-version" label, please upgrade your google-cloud-dataflow or apache-beam version
to 2.2.0 or greater.

BigQuery Hooks and Operator
"""""""""""""""""""""""""""

The ``bql`` parameter passed to ``BigQueryOperator`` and ``BigQueryBaseCursor.run_query`` has been deprecated and renamed to ``sql`` for consistency purposes. Using ``bql`` will still work (and raise a ``DeprecationWarning``\ ), but is no longer
supported and will be removed entirely in Airflow 2.0

Redshift to S3 Operator
"""""""""""""""""""""""

With Airflow 1.9 or lower, Unload operation always included header row. In order to include header row,
we need to turn off parallel unload. It is preferred to perform unload operation using all nodes so that it is
faster for larger tables. So, parameter called ``include_header`` is added and default is set to False.
Header row will be added only if this parameter is set True and also in that case parallel will be automatically turned off (\ ``PARALLEL OFF``\ )

Google cloud connection string
""""""""""""""""""""""""""""""

With Airflow 1.9 or lower, there were two connection strings for the Google Cloud operators, both ``google_cloud_storage_default`` and ``google_cloud_default``. This can be confusing and therefore the ``google_cloud_storage_default`` connection id has been replaced with ``google_cloud_default`` to make the connection id consistent across Airflow.

Logging Configuration
"""""""""""""""""""""

With Airflow 1.9 or lower, ``FILENAME_TEMPLATE``\ , ``PROCESSOR_FILENAME_TEMPLATE``\ , ``LOG_ID_TEMPLATE``\ , ``END_OF_LOG_MARK`` were configured in ``airflow_local_settings.py``. These have been moved into the configuration file, and hence if you were using a custom configuration file the following defaults need to be added.

.. code-block::

   [core]
   fab_logging_level = WARN
   log_filename_template = {{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{{{{ ts }}}}/{{{{ try_number }}}}.log
   log_processor_filename_template = {{{{ filename }}}}.log

   [elasticsearch]
   elasticsearch_log_id_template = {{dag_id}}-{{task_id}}-{{execution_date}}-{{try_number}}
   elasticsearch_end_of_log_mark = end_of_log

The previous setting of ``log_task_reader`` is not needed in many cases now when using the default logging config with remote storage. (Previously it needed to be set to ``s3.task`` or similar. This is not needed with the default config anymore)

Change of per-task log path
~~~~~~~~~~~~~~~~~~~~~~~~~~~

With the change to Airflow core to be timezone aware the default log path for task instances will now include timezone information. This will by default mean all previous task logs won't be found. You can get the old behaviour back by setting the following config options:

.. code-block::

   [core]
   log_filename_template = {{ ti.dag_id }}/{{ ti.task_id }}/{{ execution_date.strftime("%%Y-%%m-%%dT%%H:%%M:%%S") }}/{{ try_number }}.log

Changelog
^^^^^^^^^

- [AIRFLOW-2870] Use abstract TaskInstance for migration
- [AIRFLOW-2859] Implement own UtcDateTime (#3708)
- [AIRFLOW-2140] Don't require kubernetes for the SparkSubmit hook
- [AIRFLOW-2869] Remove smart quote from default config
- [AIRFLOW-2857] Fix Read the Docs env
- [AIRFLOW-2817] Force explicit choice on GPL dependency
- [AIRFLOW-2716] Replace async and await py3.7 keywords
- [AIRFLOW-2810] Fix typo in Xcom model timestamp
- [AIRFLOW-2710] Clarify fernet key value in documentation
- [AIRFLOW-2606] Fix DB schema and SQLAlchemy model
- [AIRFLOW-2646] Fix setup.py not to install snakebite on Python3
- [AIRFLOW-2604] Add index to task_fail
- [AIRFLOW-2650] Mark SchedulerJob as succeed when hitting Ctrl-c
- [AIRFLOW-2678] Fix db schema unit test to remove checking fab models
- [AIRFLOW-2624] Fix webserver login as anonymous
- [AIRFLOW-2654] Fix incorrect URL on refresh in Graph View of FAB UI
- [AIRFLOW-2668] Handle missing optional cryptography dependency
- [AIRFLOW-2681] Include last dag run of externally triggered DAGs in UI.
- [AIRFLOW-1840] Support back-compat on old celery config
- [AIRFLOW-2612][AIRFLOW-2534] Clean up Hive-related tests
- [AIRFLOW-2608] Implements/Standardize custom exceptions for experimental APIs
- [AIRFLOW-2607] Fix failing TestLocalClient
- [AIRFLOW-2638] dbapi_hook: support REPLACE INTO
- [AIRFLOW-2542][AIRFLOW-1790] Rename AWS Batch Operator queue to job_queue
- [AIRFLOW-2567] Extract result from the kubernetes pod as Xcom
- [AIRFLOW-XXX] Adding REA Group to readme
- [AIRFLOW-2601] Allow user to specify k8s config
- [AIRFLOW-2559] Azure Fileshare hook
- [AIRFLOW-1786] Enforce correct behavior for soft-fail sensors
- [AIRFLOW-2355] Airflow trigger tag parameters in subdag
- [AIRFLOW-2613] Fix Airflow searching .zip bug
- [AIRFLOW-2627] Add a sensor for Cassandra
- [AIRFLOW-2634][AIRFLOW-2534] Remove dependency for impyla
- [AIRFLOW-2611] Fix wrong dag volume mount path for kubernetes executor
- [AIRFLOW-2562] Add Google Kubernetes Engine Operators
- [AIRFLOW-2630] Fix classname in test_sql_sensor.py
- [AIRFLOW-2534] Fix bug in HiveServer2Hook
- [AIRFLOW-2586] Stop getting AIRFLOW_HOME value from config file in bash operator
- [AIRFLOW-2605] Fix autocommit for MySqlHook
- [AIRFLOW-2539][AIRFLOW-2359] Move remaining log config to configuration file
- [AIRFLOW-1656] Tree view dags query changed
- [AIRFLOW-2617] add imagePullPolicy config for kubernetes executor
- [AIRFLOW-2429] Fix security/task/sensors/ti_deps folders flake8 error
- [AIRFLOW-2550] Implements API endpoint to list DAG runs
- [AIRFLOW-2512][AIRFLOW-2522] Use google-auth instead of oauth2client
- [AIRFLOW-2429] Fix operators folder flake8 error
- [AIRFLOW-2585] Fix several bugs in CassandraHook and CassandraToGCSOperator
- [AIRFLOW-2597] Restore original dbapi.run() behavior
- [AIRFLOW-2590] Fix commit in DbApiHook.run() for no-autocommit DB
- [AIRFLOW-1115] fix github oauth api URL
- [AIRFLOW-2587] Add TIMESTAMP type mapping to MySqlToHiveTransfer
- [AIRFLOW-2591][AIRFLOW-2581] Set default value of autocommit to False in DbApiHook.run()
- [AIRFLOW-59] Implement bulk_dump and bulk_load for the Postgres hook
- [AIRFLOW-2533] Fix path to DAG's on kubernetes executor workers
- [AIRFLOW-2581] RFLOW-2581] Fix DbApiHook autocommit
- [AIRFLOW-2578] Add option to use proxies in JiraHook
- [AIRFLOW-2575] Make gcs to gcs operator work with large files
- [AIRFLOW-437] Send TI context in kill zombies
- [AIRFLOW-2566] Change backfill to rerun failed tasks
- [AIRFLOW-1021] Fix double login for new users with LDAP
- [AIRFLOW-XXX] Typo fix
- [AIRFLOW-2561] Fix typo in EmailOperator
- [AIRFLOW-2573] Cast BigQuery TIMESTAMP field to float
- [AIRFLOW-2560] Adding support for internalIpOnly to DataprocClusterCreateOperator
- [AIRFLOW-2565] templatize cluster_label
- [AIRFLOW-83] add mongo hook and operator
- [AIRFLOW-2558] Clear task/dag is clearing all executions
- [AIRFLOW-XXX] Fix doc typos
- [AIRFLOW-2513] Change ``bql`` to ``sql`` for BigQuery Hooks & Ops
- [AIRFLOW-2557] Fix pagination for s3
- [AIRFLOW-2545] Eliminate DeprecationWarning
- [AIRFLOW-2500] Fix MySqlToHiveTransfer to transfer unsigned type properly
- [AIRFLOW-2462] Change PasswordUser setter to correct syntax
- [AIRFLOW-2525] Fix a bug introduced by commit ``dabf1b9``
- [AIRFLOW-2553] Add webserver.pid to .gitignore
- [AIRFLOW-1863][AIRFLOW-2529] Add dag run selection widgets to gantt view
- [AIRFLOW-2504] Log username correctly and add extra to search columns
- [AIRFLOW-2551] Encode binary data with base64 standard rather than base64 url
- [AIRFLOW-2537] Add reset-dagrun option to backfill command
- [AIRFLOW-2526] dag_run.conf can override params
- [AIRFLOW-2544][AIRFLOW-1967] Guard against next major release of Celery, Flower
- [AIRFLOW-XXX] Add Yieldr to who is using airflow
- [AIRFLOW-2547] Describe how to run tests using Docker
- [AIRFLOW-2538] Update faq doc on how to reduce Airflow scheduler latency
- [AIRFLOW-2529] Improve graph view performance and usability
- [AIRFLOW-2517] backfill support passing key values through CLI
- [AIRFLOW-2532] Support logs_volume_subpath for KubernetesExecutor
- [AIRFLOW-2466] consider task_id in _change_state_for_tis_without_dagrun
- [AIRFLOW-2519] Fix CeleryExecutor with SQLAlchemy
- [AIRFLOW-2402] Fix RBAC task log
- [AIRFLOW-XXX] Add M4U to user list
- [AIRFLOW-2536] docs about how to deal with Airflow initdb failure
- [AIRFLOW-2530] KubernetesOperator supports multiple clusters
- [AIRFLOW-1499] Eliminate duplicate and unneeded code
- [AIRFLOW-2521] backfill - make variable name and logging messages more accurate
- [AIRFLOW-2429] Fix hook, macros folder flake8 error
- [Airflow-XXX] add Prime to company list
- [AIRFLOW-2525] Fix PostgresHook.copy_expert to work with "COPY FROM"
- [AIRFLOW-2515] Add dependency on thrift_sasl to hive extra
- [AIRFLOW-2523] Add how-to for managing GCP connections
- [AIRFLOW-2510] Introduce new macros: prev_ds and next_ds
- [AIRFLOW-1730] Unpickle value of XCom queried from DB
- [AIRFLOW-2518] Fix broken ToC links in integration.rst
- [AIRFLOW-1472] Fix SLA misses triggering on skipped tasks.
- [AIRFLOW-2520] CLI - make backfill less verbose
- [AIRFLOW-2107] add time_partitioning to run_query on BigQueryBaseCursor
- [AIRFLOW-1057][AIRFLOW-1380][AIRFLOW-2362][2362] AIRFLOW Update DockerOperator to new API
- [AIRFLOW-2415] Make Airflow DAG templating render numbers
- [AIRFLOW-2473] Fix wrong skip condition for TransferTests
- [AIRFLOW-2472] Implement MySqlHook.bulk_dump
- [AIRFLOW-2419] Use default view for subdag operator
- [AIRFLOW-2498] Fix Unexpected argument in SFTP Sensor
- [AIRFLOW-2509] Separate config docs into how-to guides
- [AIRFLOW-2429] Add BaseExecutor back
- [AIRFLOW-2429] Fix dag, example_dags, executors flake8 error
- [AIRFLOW-2502] Change Single triple quotes to double for docstrings
- [AIRFLOW-2503] Fix broken links in CONTRIBUTING.md
- [AIRFLOW-2501] Refer to devel instructions in docs contrib guide
- [AIRFLOW-2429] Fix contrib folder's flake8 errors
- [AIRFLOW-2471] Fix HiveCliHook.load_df to use unused parameters
- [AIRFLOW-2495] Update celery to 4.1.1
- [AIRFLOW-2429] Fix api, bin, config_templates folders flake8 error
- [AIRFLOW-2493] Mark template_fields of all Operators in the API document as "templated"
- [AIRFLOW-2489] Update FlaskAppBuilder to 1.11.1
- [AIRFLOW-2448] Enhance HiveCliHook.load_df to work with datetime
- [AIRFLOW-2487] Enhance druid ingestion hook
- [AIRFLOW-2397] Support affinity policies for Kubernetes executor/operator
- [AIRFLOW-2482] Add test for rewrite method in GCS Hook
- [AIRFLOW-2481] Fix flaky Kubernetes test
- [AIRFLOW-2479] Improve doc FAQ section
- [AIRFLOW-2485] Fix Incorrect logging for Qubole Sensor
- [AIRFLOW-2486] Remove unnecessary slash after port
- [AIRFLOW-2429] Make Airflow flake8 compliant
- [AIRFLOW-2491] Resolve flask version conflict
- [AIRFLOW-2484] Remove duplicate key in MySQL to GCS Op
- [AIRFLOW-2458] Add cassandra-to-gcs operator
- [AIRFLOW-2477] Improve time units for task duration and landing times charts for RBAC UI
- [AIRFLOW-2474] Only import snakebite if using py2
- [AIRFLOW-48] Parse connection uri querystring
- [AIRFLOW-2467][AIRFLOW-2] Update import direct warn message to use the module name
- [AIRFLOW-XXX] Fix order of companies
- [AIRFLOW-2452] Document field_dict must be OrderedDict
- [AIRFLOW-2420] Azure Data Lake Hook
- [AIRFLOW-2213] Add Qubole check operator
- [AIRFLOW-2465] Fix wrong module names in the doc
- [AIRFLOW-1929] Modifying TriggerDagRunOperator to accept execution_date
- [AIRFLOW-2460] Users can now use volume mounts and volumes
- [AIRFLOW-2110][AIRFLOW-2122] Enhance Http Hook
- [AIRFLOW-XXX] Updated contributors list
- [AIRFLOW-2435] Add launch_type to ECSOperator to allow FARGATE
- [AIRFLOW-2451] Remove extra slash ('/') char when using wildcard in gcs_to_gcs operator
- [AIRFLOW-2461] Add support for cluster scaling on dataproc operator
- [AIRFLOW-2376] Fix no hive section error
- [AIRFLOW-2425] Add lineage support
- [AIRFLOW-2430] Extend query batching to additional slow queries
- [AIRFLOW-2453] Add default nil value for kubernetes/git_subpath
- [AIRFLOW-2396] Add support for resources in kubernetes operator
- [AIRFLOW-2169] Encode binary data with base64 before importing to BigQuery
- [AIRFLOW-XXX] Add spotahome in user list
- [AIRFLOW-2457] Update FAB version requirement
- [AIRFLOW-2454][Airflow 2454] Support imagePullPolicy for k8s
- [AIRFLOW-2450] update supported k8s versions to 1.9 and 1.10
- [AIRFLOW-2333] Add Segment Hook and TrackEventOperator
- [AIRFLOW-2442][AIRFLOW-2] Airflow run command leaves database connections open
- [AIRFLOW-2016] assign template_fields for Dataproc Workflow Template sub-classes, not base class
- [AIRFLOW-2446] Add S3ToRedshiftTransfer into the "Integration" doc
- [AIRFLOW-2449] Fix operators.py to run all test cases
- [AIRFLOW-2424] Add dagrun status endpoint and increased k8s test coverage
- [AIRFLOW-2441] Fix bugs in HiveCliHook.load_df
- [AIRFLOW-2358][AIRFLOW-201804] Make the Kubernetes example optional
- [AIRFLOW-2436] Remove cli_logger in initdb
- [AIRFLOW-2444] Remove unused option(include_adhoc) in cli backfill command
- [AIRFLOW-2447] Fix TestHiveMetastoreHook to run all cases
- [AIRFLOW-2445] Allow templating in kubernetes operator
- [AIRFLOW-2086][AIRFLOW-2393] Customize default dagrun number in tree view
- [AIRFLOW-2437] Add PubNub to list of current Airflow users
- [AIRFLOW-XXX] Add Quantopian to list of Airflow users
- [AIRFLOW-1978] Add WinRM windows operator and hook
- [AIRFLOW-2427] Add tests to named hive sensor
- [AIRFLOW-2412] Fix HiveCliHook.load_file to address HIVE-10541
- [AIRFLOW-2431] Add the navigation bar color parameter for RBAC UI
- [AIRFLOW-2407] Resolve Python undefined names
- [AIRFLOW-1952] Add the navigation bar color parameter
- [AIRFLOW-2222] Implement GoogleCloudStorageHook.rewrite
- [AIRFLOW-2426] Add Google Cloud Storage Hook tests
- [AIRFLOW-2418] Bump Flask-WTF
- [AIRFLOW-2417] Wait for pod is not running to end task
- [AIRFLOW-1914] Add other charset support to email utils
- [AIRFLOW-XXX] Update README.md with Craig@Work
- [AIRFLOW-1899] Fix Kubernetes tests
- [AIRFLOW-1812] Update logging example
- [AIRFLOW-2313] Add TTL parameters for Dataproc
- [AIRFLOW-2411] add dataproc_jars to templated_fields
- [AIRFLOW-XXX] Add Reddit to Airflow users
- [AIRFLOW-XXX] Fix wrong table header in scheduler.rst
- [AIRFLOW-2409] Supply password as a parameter
- [AIRFLOW-2410][AIRFLOW-75] Set the timezone in the RBAC Web UI
- [AIRFLOW-2394] default cmds and arguments in kubernetes operator
- [AIRFLOW-2406] Add Apache2 License Shield to Readme
- [AIRFLOW-2404] Add additional documentation for unqueued task
- [AIRFLOW-2400] Add Ability to set Environment Variables for K8s
- [AIRFLOW-XXX] Add Twine Labs as an Airflow user
- [AIRFLOW-1853] Show only the desired number of runs in tree view
- [AIRFLOW-2401] Document the use of variables in Jinja template
- [AIRFLOW-2403] Fix License Headers
- [AIRFLOW-1313] Fix license header
- [AIRFLOW-2398] Add BounceX to list of current Airflow users
- [AIRFLOW-2363] Fix return type bug in TaskHandler
- [AIRFLOW-2389] Create a pinot db api hook
- [AIRFLOW-2390] Resolve FlaskWTFDeprecationWarning
- [AIRFLOW-1933] Fix some typos
- [AIRFLOW-1960] Add support for secrets in kubernetes operator
- [AIRFLOW-1313] Add vertica_to_mysql operator
- [AIRFLOW-1575] Add AWS Kinesis Firehose Hook for inserting batch records
- [AIRFLOW-2266][AIRFLOW-2343] Remove google-cloud-dataflow dependency
- [AIRFLOW-2370] Implement --use_random_password in create_user
- [AIRFLOW-2348] Strip path prefix from the destination_object when source_object contains a wildcard[]
- [AIRFLOW-2391] Fix to Flask 0.12.2
- [AIRFLOW-2381] Fix the flaky ApiPasswordTests test
- [AIRFLOW-2378] Add Groupon to list of current users
- [AIRFLOW-2382] Fix wrong description for delimiter
- [AIRFLOW-2380] Add support for environment variables in Spark submit operator.
- [AIRFLOW-2377] Improve Sendgrid sender support
- [AIRFLOW-2331] Support init action timeout on dataproc cluster create
- [AIRFLOW-1835] Update docs: Variable file is json
- [AIRFLOW-1781] Make search case-insensitive in LDAP group
- [AIRFLOW-2042] Fix browser menu appearing over the autocomplete menu
- [AIRFLOW-XXX] Remove wheelhouse files from travis not owned by travis
- [AIRFLOW-2336] Use hmsclient in hive_hook
- [AIRFLOW-2041] Correct Syntax in Python examples
- [AIRFLOW-74] SubdagOperators can consume all celeryd worker processes
- [AIRFLOW-2369] Fix gcs tests
- [AIRFLOW-2365] Fix autocommit attribute check
- [AIRFLOW-2068] MesosExecutor allows optional Docker image
- [AIRFLOW-1652] Push DatabricksRunSubmitOperator metadata into XCOM
- [AIRFLOW-2234] Enable insert_rows for PrestoHook
- [AIRFLOW-2208][Airflow-22208] Link to same DagRun graph from TaskInstance view
- [AIRFLOW-1153] Allow HiveOperators to take hiveconfs
- [AIRFLOW-775] Fix autocommit settings with Jdbc hook
- [AIRFLOW-2364] Warn when setting autocommit on a connection which does not support it
- [AIRFLOW-2357] Add persistent volume for the logs
- [AIRFLOW-766] Skip conn.commit() when in Auto-commit
- [AIRFLOW-2351] Check for valid default_args start_date
- [AIRFLOW-1433] Set default rbac to initdb
- [AIRFLOW-2270] Handle removed tasks in backfill
- [AIRFLOW-2344] Fix ``connections -l`` to work with pipe/redirect
- [AIRFLOW-2300] Add S3 Select functionality to S3ToHiveTransfer
- [AIRFLOW-1314] Cleanup the config
- [AIRFLOW-1314] Polish some of the Kubernetes docs/config
- [AIRFLOW-1314] Improve error handling
- [AIRFLOW-1999] Add per-task GCP service account support
- [AIRFLOW-1314] Rebasing against master
- [AIRFLOW-1314] Small cleanup to address PR comments (#24)
- [AIRFLOW-1314] Add executor_config and tests
- [AIRFLOW-1314] Improve k8s support
- [AIRFLOW-1314] Use VolumeClaim for transporting DAGs
- [AIRFLOW-1314] Create integration testing environment
- [AIRFLOW-1314] Git Mode to pull in DAGs for Kubernetes Executor
- [AIRFLOW-1314] Add support for volume mounts & Secrets in Kubernetes Executor
- [AIRFLOW=1314] Basic Kubernetes Mode
- [AIRFLOW-2326][AIRFLOW-2222] remove contrib.gcs_copy_operator
- [AIRFLOW-2328] Fix empty GCS blob in S3ToGoogleCloudStorageOperator
- [AIRFLOW-2350] Fix grammar in UPDATING.md
- [AIRFLOW-2302] Fix documentation
- [AIRFLOW-2345] pip is not used in this setup.py
- [AIRFLOW-2347] Add Banco de Formaturas to Readme
- [AIRFLOW-2346] Add Investorise as official user of Airflow
- [AIRFLOW-2330] Do not append destination prefix if not given
- [AIRFLOW-2240][DASK] Added TLS/SSL support for the dask-distributed scheduler.
- [AIRFLOW-2309] Fix duration calculation on TaskFail
- [AIRFLOW-2335] fix issue with jdk8 download for ci
- [AIRFLOW-2184] Add druid_checker_operator
- [AIRFLOW-2299] Add S3 Select functionality to S3FileTransformOperator
- [AIRFLOW-2254] Put header as first row in unload
- [AIRFLOW-610] Respect _cmd option in config before defaults
- [AIRFLOW-2287] Fix incorrect ASF headers
- [AIRFLOW-XXX] Add Zego as an Apache Airflow user
- [AIRFLOW-952] fix save empty extra field in UI
- [AIRFLOW-1325] Add ElasticSearch log handler and reader
- [AIRFLOW-2301] Sync files of an S3 key with a GCS path
- [AIRFLOW-2293] Fix S3FileTransformOperator to work with boto3
- [AIRFLOW-3212][AIRFLOW-2314] Remove only leading slash in GCS path
- [AIRFLOW-1509][AIRFLOW-442] SFTP Sensor
- [AIRFLOW-2291] Add optional params to ML Engine
- [AIRFLOW-1774] Allow consistent templating of arguments in MLEngineBatchPredictionOperator
- [AIRFLOW-2302] Add missing operators and hooks
- [AIRFLOW-2312] Docs Typo Correction: Corresponding
- [AIRFLOW-1623] Trigger on_kill method in operators
- [AIRFLOW-2162] When impersonating another user, pass env variables to sudo
- [AIRFLOW-2304] Update quickstart doc to mention scheduler part
- [AIRFLOW-1633] docker_operator needs a way to set shm_size
- [AIRFLOW-1340] Add S3 to Redshift transfer operator
- [AIRFLOW-2303] Lists the keys inside an S3 bucket
- [AIRFLOW-2209] restore flask_login imports
- [AIRFLOW-2306] Add Bonnier Broadcasting to list of current users
- [AIRFLOW-2305][AIRFLOW-2027] Fix CI failure caused by []
- [AIRFLOW-2281] Add support for Sendgrid categories
- [AIRFLOW-2027] Only trigger sleep in scheduler after all files have parsed
- [AIRFLOW-2256] SparkOperator: Add Client Standalone mode and retry mechanism
- [AIRFLOW-2284] GCS to S3 operator
- [AIRFLOW-2287] Update license notices
- [AIRFLOW-2296] Add Cinimex DataLab to Readme
- [AIRFLOW-2298] Add Kalibrr to who uses Airflow
- [AIRFLOW-2292] Fix docstring for S3Hook.get_wildcard_key
- [AIRFLOW-XXX] Update PR template
- [AIRFLOW-XXX] Remove outdated migrations.sql
- [AIRFLOW-2287] Add license header to docs/Makefile
- [AIRFLOW-2286] Add tokopedia to the readme
- [AIRFLOW-2273] Add Discord webhook operator/hook
- [AIRFLOW-2282] Fix grammar in UPDATING.md
- [AIRFLOW-2200] Add snowflake operator with tests
- [AIRFLOW-2178] Add handling on SLA miss errors
- [AIRFLOW-2169] Fix type 'bytes' is not JSON serializable in python3
- [AIRFLOW-2215] Pass environment to subprocess.Popen in base_task_runner
- [AIRFLOW-2253] Add Airflow CLI instrumentation
- [AIRFLOW-2274] Fix Dataflow tests
- [AIRFLOW-2269] Add Custom Ink as an Airflow user
- [AIRFLOW-2259] Dataflow Hook Index out of range
- [AIRFLOW-2233] Update updating.md to include the info of hdfs_sensors renaming
- [AIRFLOW-2217] Add Slack webhook operator
- [AIRFLOW-1729] improve DagBag time
- [AIRFLOW-2264] Improve create_user cli help message
- [AIRFLOW-2260][AIRFLOW-2260] SSHOperator add command template .sh files
- [AIRFLOW-2261] Check config/env for remote base log folder
- [AIRFLOW-2258] Allow import of Parquet-format files into BigQuery
- [AIRFLOW-1430] Include INSTALL instructions to avoid GPL
- [AIRFLOW-1430] Solve GPL dependency
- [AIRFLOW-2251] Add Thinknear as an Airflow user
- [AIRFLOW-2244] bugfix: remove legacy LongText code from models.py
- [AIRFLOW-2247] Fix RedshiftToS3Transfer not to fail with ValueError
- [AIRFLOW-2249] Add side-loading support for Zendesk Hook
- [AIRFLOW-XXX] Add Qplum to Airflow users
- [AIRFLOW-2228] Enhancements in ValueCheckOperator
- [AIRFLOW-1206] Typos
- [AIRFLOW-2060] Update pendulum version to 1.4.4
- [AIRFLOW-2248] Fix wrong param name in RedshiftToS3Transfer doc
- [AIRFLOW-1433][AIRFLOW-85] New Airflow Webserver UI with RBAC support
- [AIRFLOW-1235] Fix webserver's odd behaviour
- [AIRFLOW-1460] Allow restoration of REMOVED TI's
- [AIRFLOW-2235] Fix wrong docstrings in two operators
- [AIRFLOW-XXX] Fix chronological order for companies using Airflow
- [AIRFLOW-2124] Upload Python file to a bucket for Dataproc
- [AIRFLOW-2212] Fix ungenerated sensor API reference
- [AIRFLOW-2226] Rename google_cloud_storage_default to google_cloud_default
- [AIRFLOW-2211] Rename hdfs_sensors.py to hdfs_sensor.py for consistency
- [AIRFLOW-2225] Update document to include DruidDbApiHook
- [Airflow-2202] Add filter support in HiveMetastoreHook().max_partition()
- [AIRFLOW-2220] Remove duplicate numeric list entry in security.rst
- [AIRFLOW-XXX] Update tutorial documentation
- [AIRFLOW-2215] Update celery task to preserve environment variables and improve logging on exception
- [AIRFLOW-2185] Use state instead of query param
- [AIRFLOW-2183] Refactor DruidHook to enable sql
- [AIRFLOW-2203] Defer cycle detection
- [AIRFLOW-2203] Remove Useless Commands.
- [AIRFLOW-2203] Cache signature in apply_defaults
- [AIRFLOW-2203] Speed up Operator Resources
- [AIRFLOW-2203] Cache static rules (trigger/weight)
- [AIRFLOW-2203] Store task ids as sets not lists
- [AIRFLOW-2205] Remove unsupported args from JdbcHook doc
- [AIRFLOW-2207] Fix flaky test that uses app.cached_app()
- [AIRFLOW-2206] Remove unsupported args from JdbcOperator doc
- [AIRFLOW-2140] Add Kubernetes scheduler to SparkSubmitOperator
- [AIRFLOW-XXX] Add Xero to list of users
- [AIRFLOW-2204] Fix webserver debug mode
- [AIRFLOW-102] Fix test_complex_template always succeeds
- [AIRFLOW-442] Add SFTPHook
- [AIRFLOW-2169] Add schema to MySqlToGoogleCloudStorageOperator
- [AIRFLOW-2184][AIRFLOW-2138] Google Cloud Storage allow wildcards
- [AIRFLOW-1588] Cast Variable value to string
- [AIRFLOW-2199] Fix invalid reference to logger
- [AIRFLOW-2191] Change scheduler heartbeat logs from info to debug
- [AIRFLOW-2106] SalesForce hook sandbox option
- [AIRFLOW-2197] Silence hostname_callable config error message
- [AIRFLOW-2150] Use lighter call in HiveMetastoreHook().max_partition()
- [AIRFLOW-2186] Change the way logging is carried out in few ops
- [AIRFLOW-2181] Convert password_auth and test_password_endpoints from DOS to UNIX
- [AIRFLOW-2187] Fix Broken Travis CI due to AIRFLOW-2123
- [AIRFLOW-2175] Check that filepath is not None
- [AIRFLOW-2173] Don't check task IDs for concurrency reached check
- [AIRFLOW-2168] Remote logging for Azure Blob Storage
- [AIRFLOW-XXX] Add DocuTAP to list of users
- [AIRFLOW-2176] Change the way logging is carried out in BQ Get Data Operator
- [AIRFLOW-2177] Add mock test for GCS Download op
- [AIRFLOW-2123] Install CI dependencies from setup.py
- [AIRFLOW-2129] Presto hook calls _parse_exception_message but defines _get_pretty_exception_message
- [AIRFLOW-2174] Fix typos and wrongly rendered documents
- [AIRFLOW-2171] Store delegated credentials
- [AIRFLOW-2166] Restore BQ run_query dialect param
- [AIRFLOW-2163] Add HBC Digital to users of Airflow
- [AIRFLOW-2065] Fix race-conditions when creating loggers
- [AIRFLOW-2147] Plugin manager: added 'sensors' attribute
- [AIRFLOW-2059] taskinstance query is awful, un-indexed, and does not scale
- [AIRFLOW-2159] Fix a few typos in salesforce_hook
- [AIRFLOW-2132] Add step to initialize database
- [AIRFLOW-2160] Fix bad rowid deserialization
- [AIRFLOW-2161] Add Vevo to list of companies using Airflow
- [AIRFLOW-2149] Add link to apache Beam documentation to create self executing Jar
- [AIRFLOW-2151] Allow getting the session from AwsHook
- [AIRFLOW-2097] tz referenced before assignment
- [AIRFLOW-2152] Add Multiply to list of companies using Airflow
- [AIRFLOW-1551] Add operator to trigger Jenkins job
- [AIRFLOW-2034] Fix mixup between %s and {} when using str.format
- [AIRFLOW-2102] Add custom_args to Sendgrid personalizations
- [AIRFLOW-1035][AIRFLOW-1053] import unicode_literals to parse Unicode in HQL
- [AIRFLOW-2127] Keep loggers during DB migrations
- [AIRFLOW-2146] Resolve issues with BQ using DbApiHook methods
- [AIRFLOW-2087] Scheduler Report shows incorrect Total task number
- [AIRFLOW-2139] Remove unnecessary boilerplate to get DataFrame using pandas_gbq
- [AIRFLOW-2125] Using binary package psycopg2-binary
- [AIRFLOW-2142] Include message on mkdir failure
- [AIRFLOW-1615] SSHHook: use port specified by Connection
- [AIRFLOW-2122] Handle boolean values in sshHook
- [AIRFLOW-XXX] Add Tile to the list of users
- [AIRFLOW-2130] Add missing Operators to API Reference docs
- [AIRFLOW-XXX] Add timeout units (seconds)
- [AIRFLOW-2134] Add Alan to the list of companies that use Airflow
- [AIRFLOW-2133] Remove references to GitHub issues in CONTRIBUTING
- [AIRFLOW-2131] Remove confusing AirflowImport docs
- [AIRFLOW-1852] Allow hostname to be overridable.
- [AIRFLOW-2126] Add Bluecore to active users
- [AIRFLOW-1618] Add feature to create GCS bucket
- [AIRFLOW-2108] Fix log indentation in BashOperator
- [AIRFLOW-2115] Fix doc links to PythonHosted
- [AIRFLOW-XXX] Add contributor from Easy company
- [AIRFLOW-1882] Add ignoreUnknownValues option to gcs_to_bq operator
- [AIRFLOW-2089] Add on kill for SparkSubmit in Standalone Cluster
- [AIRFLOW-2113] Address missing DagRun callbacks Given that the handle_callback method belongs to the DAG object, we are able to get the list of task directly with get_task and reduce the communication with the database, making Airflow more lightweight.
- [AIRFLOW-2112] Fix svg width for Recent Tasks on UI.
- [AIRFLOW-2116] Set CI Cloudant version to <2.0
- [AIRFLOW-XXX] Add PMC to list of companies using Airflow
- [AIRFLOW-2100] Fix Broken Documentation Links
- [AIRFLOW-1404] Add 'flatten_results' & 'maximum_bytes_billed' to BQ Operator
- [AIRFLOW-800] Initialize valid Google BigQuery Connection
- [AIRFLOW-1319] Fix misleading SparkSubmitOperator and SparkSubmitHook docstring
- [AIRFLOW-1983] Parse environment parameter as template
- [AIRFLOW-2095] Add operator to create External BigQuery Table
- [AIRFLOW-2085] Add SparkJdbc operator
- [AIRFLOW-1002] Add ability to clean all dependencies of removed DAG
- [AIRFLOW-2094] Jinjafied project_id, region & zone in DataProc{*} Operators
- [AIRFLOW-2092] Fixed incorrect parameter in docstring for FTPHook
- [AIRFLOW-XXX] Add SocialCops to Airflow users
- [AIRFLOW-2088] Fix duplicate keys in MySQL to GCS Helper function
- [AIRFLOW-2091] Fix incorrect docstring parameter in BigQuery Hook
- [AIRFLOW-2090] Fix typo in DataStore Hook
- [AIRFLOW-1157] Fix missing pools crashing the scheduler
- [AIRFLOW-713] Jinjafy {EmrCreateJobFlow,EmrAddSteps}Operator attributes
- [AIRFLOW-2083] Docs: Use "its" instead of "it's" where appropriate
- [AIRFLOW-2066] Add operator to create empty BQ table
- [AIRFLOW-XXX] add Karmic to list of companies
- [AIRFLOW-2073] Make FileSensor fail when the file doesn't exist
- [AIRFLOW-2078] Improve task_stats and dag_stats performance
- [AIRFLOW-2080] Use a log-out icon instead of a power button
- [AIRFLOW-2077] Fetch all pages of list_objects_v2 response
- [AIRFLOW-XXX] Add TM to list of companies
- [AIRFLOW-1985] Impersonation fixes for using ``run_as_user``
- [AIRFLOW-2018][AIRFLOW-2] Make Sensors backward compatible
- [AIRFLOW-XXX] Fix typo in concepts doc (dag_md)
- [AIRFLOW-2069] Allow Bytes to be uploaded to S3
- [AIRFLOW-2074] Fix log var name in GHE auth
- [AIRFLOW-1927] Convert naive datetimes for TaskInstances
- [AIRFLOW-1760] Password auth for experimental API
- [AIRFLOW-2038] Add missing kubernetes dependency for dev
- [AIRFLOW-2040] Escape special chars in task instance logs URL
- [AIRFLOW-1968][AIRFLOW-1520] Add role_arn and aws_account_id/aws_iam_role support back to aws hook
- [AIRFLOW-2048] Fix task instance failure string formatting
- [AIRFLOW-2046] Fix kerberos error to work with Python 3.x
- [AIRFLOW-2063] Add missing docs for GCP
- [AIRFLOW-XXX] Fix typo in docs
- [AIRFLOW-1793] Use docker_url instead of invalid base_url
- [AIRFLOW-2055] Elaborate on slightly ambiguous documentation
- [AIRFLOW-2039] BigQueryOperator supports priority property
- [AIRFLOW-2053] Fix quote character bug in BQ hook
- [AIRFLOW-2057] Add Overstock to list of companies
- [AIRFLOW-XXX] Add Plaid to Airflow users
- [AIRFLOW-2044] Add SparkSubmitOperator to documentation
- [AIRFLOW-2037] Add methods to get Hash values of a GCS object
- [AIRFLOW-2050] Fix Travis permission problem
- [AIRFLOW-2043] Add Intercom to list of companies
- [AIRFLOW-2023] Add debug logging around number of queued files
- [AIRFLOW-XXX] Add Pernod-ricard as an Airflow user
- [AIRFLOW-1453] Add 'steps' into template_fields in EmrAddSteps
- [AIRFLOW-2015] Add flag for interactive runs
- [AIRFLOW-1895] Fix primary key integrity for mysql
- [AIRFLOW-2030] Fix ``KeyError: 'i'`` in DbApiHook for insert
- [AIRFLOW-1943] Add External BigQuery Table feature
- [AIRFLOW-2033] Add Google Cloud Storage List Operator
- [AIRFLOW-2006] Add local log catching to kubernetes operator
- [AIRFLOW-2031] Add missing gcp_conn_id in the example in DataFlow docstrings
- [AIRFLOW-2029] Fix AttributeError in BigQueryPandasConnector
- [AIRFLOW-2028] Add JobTeaser to official users list
- [AIRFLOW-2016] Add support for Dataproc Workflow Templates
- [AIRFLOW-2025] Reduced Logging verbosity
- [AIRFLOW-1267][AIRFLOW-1874] Add dialect parameter to BigQueryHook
- [AIRFLOW-XXX] Fixed a typo
- [AIRFLOW-XXX] Typo node to nodes
- [AIRFLOW-2019] Update DataflowHook for updating Streaming type job
- [AIRFLOW-2017][Airflow 2017] adding query output to PostgresOperator
- [AIRFLOW-1889] Split sensors into separate files
- [AIRFLOW-1950] Optionally pass xcom_pull task_ids
- [AIRFLOW-1755] Allow mount below root
- [AIRFLOW-511][Airflow 511] add success/failure callbacks on dag level
- [AIRFLOW-192] Add weight_rule param to BaseOperator
- [AIRFLOW-2008] Use callable for Python column defaults
- [AIRFLOW-1984] Fix to AWS Batch operator
- [AIRFLOW-2000] Support non-main dataflow job class
- [AIRFLOW-2003] Use flask-caching instead of flask-cache
- [AIRFLOW-2002] Do not swallow exception on logging import
- [AIRFLOW-2004] Import flash from flask not flask.login
- [AIRFLOW-1997] Fix GCP operator doc strings
- [AIRFLOW-1996] Update DataflowHook wait_for_done for Streaming type job
- [AIRFLOW-1995][Airflow 1995] add on_kill method to SqoopOperator
- [AIRFLOW-1770] Allow HiveOperator to take in a file
- [AIRFLOW-1994] Change background color of Scheduled state Task Instances
- [AIRFLOW-1436][AIRFLOW-1475] EmrJobFlowSensor considers Cancelled step as Successful
- [AIRFLOW-1517] Kubernetes operator PR fixes
- [AIRFLOW-1517] Addressed PR comments
- [AIRFLOW-1517] Started documentation of k8s operator
- [AIRFLOW-1517] Restore authorship of resources
- [AIRFLOW-1517] Remove authorship of resources
- [AIRFLOW-1517] Add minikube for kubernetes integration tests
- [AIRFLOW-1517] Restore authorship of resources
- [AIRFLOW-1517] fixed license issues
- [AIRFLOW-1517] Created more accurate failures for kube cluster issues
- [AIRFLOW-1517] Remove authorship of resources
- [AIRFLOW-1517] Add minikube for kubernetes integration tests
- [AIRFLOW-1988] Change BG color of None state TIs
- [AIRFLOW-790] Clean up TaskInstances without DagRuns
- [AIRFLOW-1949] Fix var upload, str() produces "b'...'" which is not json
- [AIRFLOW-1930] Convert func.now() to timezone.utcnow()
- [AIRFLOW-1688] Support load.time_partitioning in bigquery_hook
- [AIRFLOW-1975] Make TriggerDagRunOperator callback optional
- [AIRFLOW-1480] Render template attributes for ExternalTaskSensor fields
- [AIRFLOW-1958] Add kwargs to send_email
- [AIRFLOW-1976] Fix for missing log/logger attribute FileProcessHandler
- [AIRFLOW-1982] Fix Executor event log formatting
- [AIRFLOW-1971] Propagate hive config on impersonation
- [AIRFLOW-1969] Always use HTTPS URIs for Google OAuth2
- [AIRFLOW-1954] Add DataFlowTemplateOperator
- [AIRFLOW-1963] Add config for HiveOperator mapred_queue
- [AIRFLOW-1946][AIRFLOW-1855] Create a BigQuery Get Data Operator
- [AIRFLOW-1953] Add labels to dataflow operators
- [AIRFLOW-1967] Update celery to 4.0.2
- [AIRFLOW-1964] Add Upsight to list of Airflow users
- [AIRFLOW-XXX] Changelog for 1.9.0
- [AIRFLOW-1470] Implement BashSensor operator
- [AIRFLOW-XXX] Pin sqlalchemy dependency
- [AIRFLOW-1955] Do not reference unassigned variable
- [AIRFLOW-1957] Add contributor to BalanceHero in Readme
- [AIRFLOW-1517] Restore authorship of secrets and init container
- [AIRFLOW-1517] Remove authorship of secrets and init container
- [AIRFLOW-1935] Add BalanceHero to readme
- [AIRFLOW-1939] add astronomer contributors
- [AIRFLOW-1517] Kubernetes Operator
- [AIRFLOW-1928] Fix @once with catchup=False
- [AIRFLOW-1937] Speed up scheduling by committing in batch
- [AIRFLOW-1821] Enhance default logging config by removing extra loggers
- [AIRFLOW-1904] Correct DAG fileloc to the right filepath
- [AIRFLOW-1909] Update docs with supported versions of MySQL server
- [AIRFLOW-1915] Relax flask-wtf dependency specification
- [AIRFLOW-1920] Update CONTRIBUTING.md to reflect enforced linting rules
- [AIRFLOW-1942] Update Sphinx docs to remove deprecated import structure
- [AIRFLOW-1846][AIRFLOW-1697] Hide Ad Hoc Query behind secure_mode config
- [AIRFLOW-1948] Include details for on_kill failure
- [AIRFLOW-1938] Clean up unused exception
- [AIRFLOW-1932] Add GCP Pub/Sub Pull and Ack
- [AIRFLOW-XXX] Purge coveralls
- [AIRFLOW-XXX] Remove unused coveralls token
- [AIRFLOW-1938] Remove tag version check in setup.py
- [AIRFLOW-1916] Don't upload logs to remote from ``run --raw``
- [AIRFLOW-XXX] Fix failing PubSub tests on Python3
- [AIRFLOW-XXX] Upgrade to Python 3.5 and disable dask tests
- [AIRFLOW-1913] Add new GCP PubSub operators
- [AIRFLOW-1525] Fix minor LICENSE and NOTICE issues
- [AIRFLOW-1687] fix fernet error without encryption
- [AIRFLOW-1912] airflow.processor should not propagate logging
- [AIRFLOW-1911] Rename celeryd_concurrency
- [AIRFLOW-1885] Fix IndexError in ready_prefix_on_cmdline
- [AIRFLOW-1854] Improve Spark Submit operator for standalone cluster mode
- [AIRFLOW-1908] Fix celery broker options config load
- [AIRFLOW-1907] Pass max_ingestion_time to Druid hook
- [AIRFLOW-1909] Add away to list of users
- [AIRFLOW-1893][AIRFLOW-1901] Propagate PYTHONPATH when using impersonation
- [AIRFLOW-1892] Modify BQ hook to extract data filtered by column
- [AIRFLOW-1829] Support for schema updates in query jobs
- [AIRFLOW-1840] Make celery configuration congruent with Celery 4
- [AIRFLOW-1878] Fix stderr/stdout redirection for tasks
- [AIRFLOW-1897][AIRFLOW-1873] Task Logs for running instance not visible in WebUI
- [AIRFLOW-1896] FIX bleach <> html5lib incompatibility
- [AIRFLOW-1884][AIRFLOW-1059] Reset orphaned task state for external dagruns
- [AIRFLOW-XXX] Fix typo in comment
- [AIRFLOW-1869] Do not emit spurious warning on missing logs
- [AIRFLOW-1888] Add AWS Redshift Cluster Sensor
- [AIRFLOW-1887] Renamed endpoint url variable
- [AIRFLOW-1873] Set TI.try_number to right value depending TI state
- [AIRFLOW-1891] Fix non-ascii typo in default configuration template
- [AIRFLOW-1879] Handle ti log entirely within ti
- [AIRFLOW-1869] Write more error messages into gcs and file logs
- [AIRFLOW-1876] Write subtask id to task log header
- [AIRFLOW-1554] Fix wrong DagFileProcessor termination method call
- [AIRFLOW-342] Do not use amqp, rpc as result backend
- [AIRFLOW-966] Make celery broker_transport_options configurable
- [AIRFLOW-1881] Make operator log in task log
- [AIRFLOW-XXX] Added DataReply to the list of Airflow Users
- [AIRFLOW-1883] Get File Size for objects in Google Cloud Storage
- [AIRFLOW-1872] Set context for all handlers including parents
- [AIRFLOW-1855][AIRFLOW-1866] Add GCS Copy Operator to copy multiple files
- [AIRFLOW-1870] Enable flake8 tests
- [AIRFLOW-1785] Enable Python 3 tests
- [AIRFLOW-1850] Copy cmd before masking
- [AIRFLOW-1665] Reconnect on database errors
- [AIRFLOW-1559] Dispose SQLAlchemy engines on exit
- [AIRFLOW-1559] Close file handles in subprocesses
- [AIRFLOW-1559] Make database pooling optional
- [AIRFLOW-1848][Airflow-1848] Fix DataFlowPythonOperator py_file extension doc comment
- [AIRFLOW-1843] Add Google Cloud Storage Sensor with prefix
- [AIRFLOW-1803] Time zone documentation
- [AIRFLOW-1826] Update views to use timezone aware objects
- [AIRFLOW-1827] Fix api endpoint date parsing
- [AIRFLOW-1806] Use naive datetime when using cron
- [AIRFLOW-1809] Update tests to use timezone aware objects
- [AIRFLOW-1806] Use naive datetime for cron scheduling
- [AIRFLOW-1807] Force use of time zone aware db fields
- [AIRFLOW-1808] Convert all utcnow() to time zone aware
- [AIRFLOW-1804] Add time zone configuration options
- [AIRFLOW-1802] Convert database fields to timezone aware
- [AIRFLOW-XXX] Add dask lock files to excludes
- [AIRFLOW-1790] Add support for AWS Batch operator
- [AIRFLOW-XXX] Update README.md
- [AIRFLOW-1820] Remove timestamp from metric name
- [AIRFLOW-1810] Remove unused mysql import in migrations.
- [AIRFLOW-1838] Properly log collect_dags exception
- [AIRFLOW-1842] Fixed Super class name for the gcs to gcs copy operator
- [AIRFLOW-1845] Modal background now covers long or tall pages
- [AIRFLOW-1229] Add link to Run Id, incl execution_date
- [AIRFLOW-1842] Add gcs to gcs copy operator with renaming if required
- [AIRFLOW-1841] change False to None in operator and hook
- [AIRFLOW-1839] Fix more bugs in S3Hook boto -> boto3 migration
- [AIRFLOW-1830] Support multiple domains in Google authentication backend
- [AIRFLOW-1831] Add driver-classpath spark submit
- [AIRFLOW-1795] Correctly call S3Hook after migration to boto3
- [AIRFLOW-1811] Fix render Druid operator
- [AIRFLOW-1819] Fix slack operator unittest bug
- [AIRFLOW-1805] Allow Slack token to be passed through connection
- [AIRFLOW-1816] Add region param to Dataproc operators
- [AIRFLOW-868] Add postgres_to_gcs operator and unittests
- [AIRFLOW-1613] make mysql_to_gcs_operator py3 compatible
- [AIRFLOW-1817] use boto3 for s3 dependency
- [AIRFLOW-1813] Bug SSH Operator empty buffer
- [AIRFLOW-1801][AIRFLOW-288] Url encode execution dates
- [AIRFLOW-1563] Catch OSError while symlinking the latest log directory
- [AIRFLOW-1794] Remove uses of Exception.message for Python 3
- [AIRFLOW-1799] Fix logging line which raises errors
- [AIRFLOW-1102] Upgrade gunicorn >=19.4.0
- [AIRFLOW-1756] Fix S3TaskHandler to work with Boto3-based S3Hook
- [AIRFLOW-1797] S3Hook.load_string did not work on Python3
- [AIRFLOW-646] Add docutils to setup_requires
- [AIRFLOW-1792] Missing intervals DruidOperator
- [AIRFLOW-1789][AIRFLOW-1712] Log SSHOperator stderr to log.warning
- [AIRFLOW-1787] Fix task instance batch clear and set state bugs
- [AIRFLOW-1780] Fix long output lines with unicode from hanging parent
- [AIRFLOW-387] Close SQLAlchemy sessions properly
- [AIRFLOW-1779] Add keepalive packets to ssh hook
- [AIRFLOW-1669] Fix Docker and pin moto to 1.1.19
- [AIRFLOW-71] Add support for private Docker images
- [AIRFLOW-XXX] Give a clue what the 'ds' variable is
- [AIRFLOW-XXX] Correct typos in the faq docs page
- [AIRFLOW-1571] Add AWS Lambda Hook
- [AIRFLOW-1675] Fix docstrings for API docs
- [AIRFLOW-1712][AIRFLOW-756][AIRFLOW-751] Log SSHOperator output
- [AIRFLOW-1776] Capture stdout and stderr for logging
- [AIRFLOW-1765] Make experimental API securable without needing Kerberos.
- [AIRFLOW-1764] The web interface should not use the experimental API
- [AIRFLOW-1771] Rename heartbeat to avoid confusion
- [AIRFLOW-1769] Add support for templates in VirtualenvOperator
- [AIRFLOW-1763] Fix S3TaskHandler unit tests
- [AIRFLOW-1315] Add Qubole File & Partition Sensors
- [AIRFLOW-1018] Make processor use logging framework
- [AIRFLOW-1695] Add RedshiftHook using boto3
- [AIRFLOW-1706] Fix query error for MSSQL backend
- [AIRFLOW-1711] Use ldap3 dict for group membership
- [AIRFLOW-1723] Make sendgrid a plugin
- [AIRFLOW-1757] Add missing options to SparkSubmitOperator
- [AIRFLOW-1734][Airflow 1734] Sqoop hook/operator enhancements
- [AIRFLOW-1761] Fix type in scheduler.rst
- [AIRFLOW-1731] Set pythonpath for logging
- [AIRFLOW-1641] Handle executor events in the scheduler
- [AIRFLOW-1744] Make sure max_tries can be set
- [AIRFLOW-1732] Improve dataflow hook logging
- [AIRFLOW-1736] Add HotelQuickly to Who Uses Airflow
- [AIRFLOW-1657] Handle failing qubole operator
- [AIRFLOW-1677] Fix typo in example_qubole_operator
- [AIRFLOW-926] Fix JDBC Hook
- [AIRFLOW-1520] Boto3 S3Hook, S3Log
- [AIRFLOW-1716] Fix multiple __init__ def in SimpleDag
- [AIRFLOW-XXX] Fix DateTime in Tree View
- [AIRFLOW-1719] Fix small typo
- [AIRFLOW-1432] Charts label for Y axis not visible
- [AIRFLOW-1743] Verify ldap filters correctly
- [AIRFLOW-1745] Restore default signal disposition
- [AIRFLOW-1741] Correctly hide second chart on task duration page
- [AIRFLOW-1728] Add networkUri, subnet, tags to Dataproc operator
- [AIRFLOW-1726] Add copy_expert psycopg2 method to PostgresHook
- [AIRFLOW-1330] Add conn_type argument to CLI when adding connection
- [AIRFLOW-1698] Remove SCHEDULER_RUNS env var in systemd
- [AIRFLOW-1694] Stop using itertools.izip
- [AIRFLOW-1692] Change test_views filename to support Windows
- [AIRFLOW-1722] Fix typo in scheduler autorestart output filename
- [AIRFLOW-1723] Support sendgrid in email backend
- [AIRFLOW-1718] Set num_retries on Dataproc job request execution
- [AIRFLOW-1727] Add unit tests for DataProcHook
- [AIRFLOW-1631] Fix timing issue in unit test
- [AIRFLOW-1631] Fix local executor unbound parallelism
- [AIRFLOW-1724] Add Fundera to Who uses Airflow?
- [AIRFLOW-1683] Cancel BigQuery job on timeout.
- [AIRFLOW-1714] Fix misspelling: s/separate/separate/
- [AIRFLOW-1681] Add batch clear in task instance view
- [AIRFLOW-1696] Fix dataproc version label error
- [AIRFLOW-1613] Handle binary field in MySqlToGoogleCloudStorageOperator
- [AIRFLOW-1697] Mode to disable charts endpoint
- [AIRFLOW-1691] Add better Google cloud logging documentation
- [AIRFLOW-1690] Add detail to gcs error messages
- [AIRFLOW-1682] Make S3TaskHandler write to S3 on close
- [AIRFLOW-1634] Adds task_concurrency feature
- [AIRFLOW-1676] Make GCSTaskHandler write to GCS on close
- [AIRFLOW-1678] Fix erroneously repeated word in function docstrings
- [AIRFLOW-1323] Made Dataproc operator parameter names consistent
- [AIRFLOW-1590] fix unused module and variable
- [AIRFLOW-1671] Add @apply_defaults back to gcs download operator
- [AIRFLOW-988] Fix repeating SLA miss callbacks
- [AIRFLOW-1611] Customize logging
- [AIRFLOW-1668] Expose keepalives_idle for Postgres connections
- [AIRFLOW-1658] Kill Druid task on timeout
- [AIRFLOW-1669][AIRFLOW-1368] Fix Docker import
- [AIRFLOW-891] Make webserver clock include date
- [AIRFLOW-1560] Add AWS DynamoDB hook and operator for inserting batch items
- [AIRFLOW-1654] Show tooltips for link icons in DAGs view
- [AIRFLOW-1660] Change webpage width to full-width
- [AIRFLOW-1664] write file as binary instead of str
- [AIRFLOW-1659] Fix invalid obj attribute bug in file_task_handler.py
- [AIRFLOW-1635] Allow creating GCP connection without requiring a JSON file
- [AIRFLOW-1650] Fix custom celery config loading
- [AIRFLOW-1647] Fix Spark-sql hook
- [AIRFLOW-1587] Fix CeleryExecutor import error
- [Airflow-1640][AIRFLOW-1640] Add qubole default connection
- [AIRFLOW-1576] Added region param to Dataproc{*}Operators
- [AIRFLOW-1643] Add healthjump to officially using list
- [AIRFLOW-1626] Add Azri Solutions to Airflow users
- [AIRFLOW-1636] Add AWS and EMR connection type
- [AIRFLOW-1527] Refactor celery config
- [AIRFLOW-1639] Fix Fernet error handling
- [AIRFLOW-1637] Fix Travis CI build status link
- [AIRFLOW-1628] Fix docstring of sqlsensor
- [AIRFLOW-1331] add SparkSubmitOperator option
- [AIRFLOW-1627] Only query pool in SubDAG init when necessary
- [AIRFLOW-1629] Make extra a textarea in edit connections form
- [AIRFLOW-1368] Automatically remove Docker container on exit
- [AIRFLOW-289] Make Airflow timezone independent
- [AIRFLOW-1356] Add ``--celery_hostname`` to ``airflow worker``
- [AIRFLOW-1247] Fix ignore_all_dependencies argument ignored
- [AIRFLOW-1621] Add tests for server side paging
- [AIRFLOW-1591] Avoid attribute error when rendering logging filename
- [AIRFLOW-1031] Replace hard-code to DagRun.ID_PREFIX
- [AIRFLOW-1604] Rename logger to log
- [AIRFLOW-1512] Add PythonVirtualenvOperator
- [AIRFLOW-1617] Fix XSS vulnerability in Variable endpoint
- [AIRFLOW-1497] Reset hidden fields when changing connection type
- [AIRFLOW-1619] Add poll_sleep parameter to GCP dataflow operator
- [AIRFLOW-XXX] Remove landscape.io config
- [AIRFLOW-XXX] Remove non working service badges
- [AIRFLOW-1177] Fix Variable.setdefault w/existing JSON
- [AIRFLOW-1600] Fix exception handling in get_fernet
- [AIRFLOW-1614] Replace inspect.stack() with sys._getframe()
- [AIRFLOW-1519] Add server side paging in DAGs list
- [AIRFLOW-1309] Allow hive_to_druid to take tblproperties
- [AIRFLOW-1613] Make MySqlToGoogleCloudStorageOperator compatible with python3
- [AIRFLOW-1603] add PAYMILL to companies list
- [AIRFLOW-1609] Fix gitignore to ignore all venvs
- [AIRFLOW-1601] Add configurable task cleanup time

Airflow 1.9.0 (2018-01-02)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

SSH Hook updates, along with new SSH Operator & SFTP Operator
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

SSH Hook now uses the Paramiko library to create an ssh client connection, instead of the sub-process based ssh command execution previously (<1.9.0), so this is backward incompatible.


* update SSHHook constructor
* use SSHOperator class in place of SSHExecuteOperator which is removed now. Refer to test_ssh_operator.py for usage info.
* SFTPOperator is added to perform secure file transfer from server A to server B. Refer to test_sftp_operator.py for usage info.
* No updates are required if you are using ``ftpHook``, it will continue to work as is.

S3Hook switched to use Boto3
""""""""""""""""""""""""""""

The airflow.hooks.S3_hook.S3Hook has been switched to use boto3 instead of the older boto (a.k.a. boto2). This results in a few backwards incompatible changes to the following classes: S3Hook:


* the constructors no longer accepts ``s3_conn_id``. It is now called ``aws_conn_id``.
* the default connection is now "aws_default" instead of "s3_default"
* the return type of objects returned by ``get_bucket`` is now boto3.s3.Bucket
* the return type of ``get_key``\ , and ``get_wildcard_key`` is now an boto3.S3.Object.

If you are using any of these in your DAGs and specify a connection ID you will need to update the parameter name for the connection to "aws_conn_id": S3ToHiveTransfer, S3PrefixSensor, S3KeySensor, RedshiftToS3Transfer.

Logging update
""""""""""""""

The logging structure of Airflow has been rewritten to make configuration easier and the logging system more transparent.

A quick recap about logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A logger is the entry point into the logging system. Each logger is a named bucket to which messages can be written for processing. A logger is configured to have a log level. This log level describes the severity of the messages that the logger will handle. Python defines the following log levels: DEBUG, INFO, WARNING, ERROR or CRITICAL.

Each message that is written to the logger is a Log Record. Each log record contains a log level indicating the severity of that specific message. A log record can also contain useful metadata that describes the event that is being logged. This can include details such as a stack trace or an error code.

When a message is given to the logger, the log level of the message is compared to the log level of the logger. If the log level of the message meets or exceeds the log level of the logger itself, the message will undergo further processing. If it doesn't, the message will be ignored.

Once a logger has determined that a message needs to be processed, it is passed to a Handler. This configuration is now more flexible and can be easily be maintained in a single file.

Changes in Airflow Logging
~~~~~~~~~~~~~~~~~~~~~~~~~~

Airflow's logging mechanism has been refactored to use Python's built-in ``logging`` module to perform logging of the application. By extending classes with the existing ``LoggingMixin``\ , all the logging will go through a central logger. Also the ``BaseHook`` and ``BaseOperator`` already extend this class, so it is easily available to do logging.

The main benefit is easier configuration of the logging by setting a single centralized python file. Disclaimer; there is still some inline configuration, but this will be removed eventually. The new logging class is defined by setting the dotted classpath in your ``~/airflow/airflow.cfg`` file:

.. code-block::

   # Logging class
   # Specify the class that will specify the logging configuration
   # This class has to be on the python classpath
   logging_config_class = my.path.default_local_settings.LOGGING_CONFIG

The logging configuration file needs to be on the ``PYTHONPATH``\ , for example ``$AIRFLOW_HOME/config``. This directory is loaded by default. Any directory may be added to the ``PYTHONPATH``\ , this might be handy when the config is in another directory or a volume is mounted in case of Docker.

The config can be taken from ``airflow/config_templates/airflow_local_settings.py`` as a starting point. Copy the contents to ``${AIRFLOW_HOME}/config/airflow_local_settings.py``\ ,  and alter the config as is preferred.

.. code-block::

   #
   # Licensed to the Apache Software Foundation (ASF) under one
   # or more contributor license agreements.  See the NOTICE file
   # distributed with this work for additional information
   # regarding copyright ownership.  The ASF licenses this file
   # to you under the Apache License, Version 2.0 (the
   # "License"); you may not use this file except in compliance
   # with the License.  You may obtain a copy of the License at
   #
   #   http://www.apache.org/licenses/LICENSE-2.0
   #
   # Unless required by applicable law or agreed to in writing,
   # software distributed under the License is distributed on an
   # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   # KIND, either express or implied.  See the License for the
   # specific language governing permissions and limitations
   # under the License.

   import os

   from airflow import configuration as conf

   # TODO: Logging format and level should be configured
   # in this file instead of from airflow.cfg. Currently
   # there are other log format and level configurations in
   # settings.py and cli.py. Please see AIRFLOW-1455.

   LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()
   LOG_FORMAT = conf.get('core', 'log_format')

   BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')
   PROCESSOR_LOG_FOLDER = conf.get('scheduler', 'child_process_log_directory')

   FILENAME_TEMPLATE = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log'
   PROCESSOR_FILENAME_TEMPLATE = '{{ filename }}.log'

   DEFAULT_LOGGING_CONFIG = {
       'version': 1,
       'disable_existing_loggers': False,
       'formatters': {
           'airflow.task': {
               'format': LOG_FORMAT,
           },
           'airflow.processor': {
               'format': LOG_FORMAT,
           },
       },
       'handlers': {
           'console': {
               'class': 'logging.StreamHandler',
               'formatter': 'airflow.task',
               'stream': 'ext://sys.stdout'
           },
           'file.task': {
               'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
               'formatter': 'airflow.task',
               'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
               'filename_template': FILENAME_TEMPLATE,
           },
           'file.processor': {
               'class': 'airflow.utils.log.file_processor_handler.FileProcessorHandler',
               'formatter': 'airflow.processor',
               'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
               'filename_template': PROCESSOR_FILENAME_TEMPLATE,
           }
           # When using s3 or gcs, provide a customized LOGGING_CONFIG
           # in airflow_local_settings within your PYTHONPATH, see UPDATING.md
           # for details
           # 's3.task': {
           #     'class': 'airflow.utils.log.s3_task_handler.S3TaskHandler',
           #     'formatter': 'airflow.task',
           #     'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
           #     's3_log_folder': S3_LOG_FOLDER,
           #     'filename_template': FILENAME_TEMPLATE,
           # },
           # 'gcs.task': {
           #     'class': 'airflow.utils.log.gcs_task_handler.GCSTaskHandler',
           #     'formatter': 'airflow.task',
           #     'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
           #     'gcs_log_folder': GCS_LOG_FOLDER,
           #     'filename_template': FILENAME_TEMPLATE,
           # },
       },
       'loggers': {
           '': {
               'handlers': ['console'],
               'level': LOG_LEVEL
           },
           'airflow': {
               'handlers': ['console'],
               'level': LOG_LEVEL,
               'propagate': False,
           },
           'airflow.processor': {
               'handlers': ['file.processor'],
               'level': LOG_LEVEL,
               'propagate': True,
           },
           'airflow.task': {
               'handlers': ['file.task'],
               'level': LOG_LEVEL,
               'propagate': False,
           },
           'airflow.task_runner': {
               'handlers': ['file.task'],
               'level': LOG_LEVEL,
               'propagate': True,
           },
       }
   }

To customize the logging (for example, use logging rotate), define one or more of the logging handles that `Python has to offer <https://docs.python.org/3/library/logging.handlers.html>`_. For more details about the Python logging, please refer to the `official logging documentation <https://docs.python.org/3/library/logging.html>`_.

Furthermore, this change also simplifies logging within the DAG itself:

.. code-block::

   root@ae1bc863e815:/airflow# python
   Python 3.6.2 (default, Sep 13 2017, 14:26:54)
   [GCC 4.9.2] on linux
   Type "help", "copyright", "credits" or "license" for more information.
   >>> from airflow.settings import *
   >>>
   >>> from datetime import datetime
   >>> from airflow.models.dag import DAG
   >>> from airflow.operators.dummy import DummyOperator
   >>>
   >>> dag = DAG('simple_dag', start_date=datetime(2017, 9, 1))
   >>>
   >>> task = DummyOperator(task_id='task_1', dag=dag)
   >>>
   >>> task.log.error('I want to say something..')
   [2017-09-25 20:17:04,927] {<stdin>:1} ERROR - I want to say something..

Template path of the file_task_handler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``file_task_handler`` logger has been made more flexible. The default format can be changed, ``{dag_id}/{task_id}/{execution_date}/{try_number}.log`` by supplying Jinja templating in the ``FILENAME_TEMPLATE`` configuration variable. See the ``file_task_handler`` for more information.

I'm using S3Log or GCSLogs, what do I do!?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are logging to Google cloud storage, please see the `Google cloud platform documentation <https://airflow.apache.org/integration.html#gcp-google-cloud-platform>`_ for logging instructions.

If you are using S3, the instructions should be largely the same as the Google cloud platform instructions above. You will need a custom logging config. The ``REMOTE_BASE_LOG_FOLDER`` configuration key in your airflow config has been removed, therefore you will need to take the following steps:


* Copy the logging configuration from `airflow/config_templates/airflow_logging_settings.py <https://github.com/apache/airflow/blob/main/airflow/config_templates/airflow_local_settings.py>`_.
* Place it in a directory inside the Python import path ``PYTHONPATH``. If you are using Python 2.7, ensuring that any ``__init__.py`` files exist so that it is importable.
* Update the config by setting the path of ``REMOTE_BASE_LOG_FOLDER`` explicitly in the config. The ``REMOTE_BASE_LOG_FOLDER`` key is not used anymore.
* Set the ``logging_config_class`` to the filename and dict. For example, if you place ``custom_logging_config.py`` on the base of your ``PYTHONPATH``\ , you will need to set ``logging_config_class = custom_logging_config.LOGGING_CONFIG`` in your config as Airflow 1.8.

New Features
""""""""""""

Dask Executor
~~~~~~~~~~~~~

A new DaskExecutor allows Airflow tasks to be run in Dask Distributed clusters.

Deprecated Features
"""""""""""""""""""

These features are marked for deprecation. They may still work (and raise a ``DeprecationWarning``\ ), but are no longer
supported and will be removed entirely in Airflow 2.0


*
  If you're using the ``google_cloud_conn_id`` or ``dataproc_cluster`` argument names explicitly in ``contrib.operators.Dataproc{*}Operator``\ (s), be sure to rename them to ``gcp_conn_id`` or ``cluster_name``\ , respectively. We've renamed these arguments for consistency. (AIRFLOW-1323)

*
  ``post_execute()`` hooks now take two arguments, ``context`` and ``result``
  (AIRFLOW-886)

  Previously, post_execute() only took one argument, ``context``.

*
  ``contrib.hooks.gcp_dataflow_hook.DataFlowHook`` starts to use ``--runner=DataflowRunner`` instead of ``DataflowPipelineRunner``\ , which is removed from the package ``google-cloud-dataflow-0.6.0``.

*
  The pickle type for XCom messages has been replaced by json to prevent RCE attacks.
  Note that JSON serialization is stricter than pickling, so if you want to e.g. pass
  raw bytes through XCom you must encode them using an encoding like base64.
  By default pickling is still enabled until Airflow 2.0. To disable it
  set enable_xcom_pickling = False in your Airflow config.

Changelog
^^^^^^^^^

- [AIRFLOW-1525] Fix minor LICENSE and NOTICE issues
- [AIRFLOW-XXX] Bump version to 1.9.0
- [AIRFLOW-1897][AIRFLOW-1873] Task Logs for running instance not visible in WebUI
- [AIRFLOW-XXX] Make sure session is committed
- [AIRFLOW-1896] FIX bleach <> html5lib incompatibility
- [AIRFLOW-XXX] Fix log handler test
- [AIRFLOW-1873] Set TI.try_number to right value depending TI state
- [AIRFLOW-1554] Fix wrong DagFileProcessor termination method call
- [AIRFLOW-1872] Set context for all handlers including parents
- [AIRFLOW-XXX] Add dask lock files to excludes
- [AIRFLOW-1839] Fix more bugs in S3Hook boto -> boto3 migration
- [AIRFLOW-1795] Correctly call S3Hook after migration to boto3
- [AIRFLOW-1813] Bug SSH Operator empty buffer
- [AIRFLOW-1794] Remove uses of Exception.message for Python 3
- [AIRFLOW-1799] Fix logging line which raises errors
- [AIRFLOW-1102] Upgrade gunicorn >=19.4.0
- [AIRFLOW-1756] Fix S3TaskHandler to work with Boto3-based S3Hook
- [AIRFLOW-1797] S3Hook.load_string did not work on Python3
- [AIRFLOW-1792] Missing intervals DruidOperator
- [AIRFLOW-1789][AIRFLOW-1712] Log SSHOperator stderr to log.warning
- [AIRFLOW-1669] Fix Docker and pin Moto to 1.1.19
- [AIRFLOW-71] Add support for private Docker images
- [AIRFLOW-1779] Add keepalive packets to ssh hook
- [AIRFLOW-XXX] Give a clue what the 'ds' variable is
- [AIRFLOW-XXX] Correct typos in the faq docs page
- [AIRFLOW-1571] Add AWS Lambda Hook
- [AIRFLOW-1675] Fix docstrings for API docs
- [AIRFLOW-1712][AIRFLOW-756][AIRFLOW-751] Log SSHOperator output
- [AIRFLOW-1776] Capture stdout and stderr for logging
- [AIRFLOW-1765] Make experimental API securable without needing Kerberos.
- [AIRFLOW-1764] The web interface should not use the experimental API
- [AIRFLOW-1634] Adds task_concurrency feature
- [AIRFLOW-1018] Make processor use logging framework
- [AIRFLOW-1695] Add RedshiftHook using boto3
- [AIRFLOW-1706] Fix query error for MSSQL backend
- [AIRFLOW-1711] Use ldap3 dict for group membership
- [AIRFLOW-1757] Add missing options to SparkSubmitOperator
- [AIRFLOW-1734][Airflow 1734] Sqoop hook/operator enhancements
- [AIRFLOW-1731] Set pythonpath for logging
- [AIRFLOW-1641] Handle executor events in the scheduler
- [AIRFLOW-1744] Make sure max_tries can be set
- [AIRFLOW-1330] Add conn_type argument to CLI when adding connection
- [AIRFLOW-926] Fix JDBC Hook
- [AIRFLOW-1520] Boto3 S3Hook, S3Log
- [AIRFLOW-XXX] Fix DateTime in Tree View
- [AIRFLOW-1432] Charts label for Y axis not visible
- [AIRFLOW-1743] Verify ldap filters correctly
- [AIRFLOW-1745] Restore default signal disposition
- [AIRFLOW-1741] Correctly hide second chart on task duration page
- [AIRFLOW-1726] Add copy_expert psycopg2 method to PostgresHook
- [AIRFLOW-1698] Remove SCHEDULER_RUNS env var in systemd
- [AIRFLOW-1694] Stop using itertools.izip
- [AIRFLOW-1692] Change test_views filename to support Windows
- [AIRFLOW-1722] Fix typo in scheduler autorestart output filename
- [AIRFLOW-1691] Add better Google Cloud logging documentation
- [AIRFLOW-1690] Add detail to gcs error messages
- [AIRFLOW-1682] Make S3TaskHandler write to S3 on close
- [AIRFLOW-1676] Make GCSTaskHandler write to GCS on close
- [AIRFLOW-1635] Allow creating GCP connection without requiring a JSON file
- [AIRFLOW-1323] Made Dataproc operator parameter names consistent
- [AIRFLOW-1590] fix unused module and variable
- [AIRFLOW-988] Fix repeating SLA miss callbacks
- [AIRFLOW-1611] Customize logging
- [AIRFLOW-1668] Expose keepalives_idle for Postgres connections
- [AIRFLOW-1658] Kill Druid task on timeout
- [AIRFLOW-1669][AIRFLOW-1368] Fix Docker import
- [AIRFLOW-1560] Add AWS DynamoDB hook and operator for inserting batch items
- [AIRFLOW-1654] Show tooltips for link icons in DAGs view
- [AIRFLOW-1660] Change webpage width to full-width
- [AIRFLOW-1664] write file as binary instead of str
- [AIRFLOW-1659] Fix invalid obj attribute bug in file_task_handler.py
- [AIRFLOW-1650] Fix custom celery config loading
- [AIRFLOW-1647] Fix Spark-sql hook
- [AIRFLOW-1587] Fix CeleryExecutor import error
- [AIRFLOW-1636] Add AWS and EMR connection type
- [AIRFLOW-1527] Refactor celery config
- [AIRFLOW-1639] Fix Fernet error handling
- [AIRFLOW-1628] Fix docstring of sqlsensor
- [AIRFLOW-1331] add SparkSubmitOperator option
- [AIRFLOW-1627] Only query pool in SubDAG init when necessary
- [AIRFLOW-1629] Make extra a textarea in edit connections form
- [AIRFLOW-1621] Add tests for server side paging
- [AIRFLOW-1519] Add server side paging in DAGs list
- [AIRFLOW-289] Make Airflow timezone independent
- [AIRFLOW-1356] Add ``--celery_hostname`` to ``airflow worker``
- [AIRFLOW-1591] Avoid attribute error when rendering logging filename
- [AIRFLOW-1031] Replace hard-code to DagRun.ID_PREFIX
- [AIRFLOW-1604] Rename logger to log
- [AIRFLOW-1512] Add PythonVirtualenvOperator
- [AIRFLOW-1617] Fix XSS vulnerability in Variable endpoint
- [AIRFLOW-1497] Reset hidden fields when changing connection type
- [AIRFLOW-1177] Fix Variable.setdefault w/existing JSON
- [AIRFLOW-1600] Fix exception handling in get_fernet
- [AIRFLOW-1614] Replace inspect.stack() with sys._getframe()
- [AIRFLOW-1613] Make MySqlToGoogleCloudStorageOperator compatible with python3
- [AIRFLOW-1609] Fix gitignore to ignore all venvs
- [AIRFLOW-1601] Add configurable task cleanup time
- [AIRFLOW-XXX] Bumping airflow 1.9.0alpha0 version
- [AIRFLOW-1608] Handle pending job state in GCP Dataflow hook
- [AIRFLOW-1606] Use non static DAG.sync_to_db
- [AIRFLOW-1606][Airflow-1606][AIRFLOW-1605][AIRFLOW-160] DAG.sync_to_db is now a normal method
- [AIRFLOW-1602] LoggingMixin in DAG class
- [AIRFLOW-1593] expose load_string in WasbHook
- [AIRFLOW-1597] Add GameWisp as Airflow user
- [AIRFLOW-1594] Don't install test packages into Python root.[]
- [AIRFLOW-1582] Improve logging within Airflow
- [AIRFLOW-1476] add INSTALL instruction for source releases
- [AIRFLOW-XXX] Save username and password in airflow-pr
- [AIRFLOW-1522] Increase text size for var field in variables for MySQL
- [AIRFLOW-950] Missing AWS integrations on documentation::integrations
- [AIRFLOW-XXX] 1.8.2 release notes
- [AIRFLOW-1573] Remove ``thrift < 0.10.0`` requirement
- [AIRFLOW-1584] Remove insecure /headers endpoint
- [AIRFLOW-1586] Add mapping for date type to mysql_to_gcs operator
- [AIRFLOW-1579] Adds support for jagged rows in Bigquery hook for BQ load jobs
- [AIRFLOW-1577] Add token support to DatabricksHook
- [AIRFLOW-1580] Error in string formatting
- [AIRFLOW-1567] Updated docs for Google ML Engine operators/hooks
- [AIRFLOW-1574] add 'to' attribute to templated vars of email operator
- [AIRFLOW-1572] add carbonite to company list
- [AIRFLOW-1568] Fix typo in BigQueryHook
- [AIRFLOW-1493][AIRFLOW-XXXX][WIP] fixed dumb thing
- [AIRFLOW-1567][Airflow-1567] Renamed cloudml hook and operator to mlengine
- [AIRFLOW-1568] Add datastore export/import operators
- [AIRFLOW-1564] Use Jinja2 to render logging filename
- [AIRFLOW-1562] Spark-sql logging contains deadlock
- [AIRFLOW-1556][Airflow 1556] Add support for SQL parameters in BigQueryBaseCursor
- [AIRFLOW-108] Add CreditCards.com to companies list
- [AIRFLOW-1541] Add channel to template fields of slack_operator
- [AIRFLOW-1535] Add service account/scopes in dataproc
- [AIRFLOW-1384] Add to README.md CaDC/ARGO
- [AIRFLOW-1546] add Zymergen 80to org list in README
- [AIRFLOW-1545] Add Nextdoor to companies list
- [AIRFLOW-1544] Add DataFox to companies list
- [AIRFLOW-1529] Add logic supporting quoted newlines in Google BigQuery load jobs
- [AIRFLOW-1521] Fix template rendering for BigqueryTableDeleteOperator
- [AIRFLOW-1324] Generalize Druid operator and hook
- [AIRFLOW-1516] Fix error handling getting fernet
- [AIRFLOW-1420][AIRFLOW-1473] Fix deadlock check
- [AIRFLOW-1495] Fix migration on index on job_id
- [AIRFLOW-1483] Making page size consistent in list
- [AIRFLOW-1495] Add TaskInstance index on job_id
- [AIRFLOW-855] Replace PickleType with LargeBinary in XCom
- [AIRFLOW-1505] Document when Jinja substitution occurs
- [AIRFLOW-1504] Log dataproc cluster name
- [AIRFLOW-1239] Fix unicode error for logs in base_task_runner
- [AIRFLOW-1280] Fix Gantt chart height
- [AIRFLOW-1507] Template parameters in file_to_gcs operator
- [AIRFLOW-1452] workaround lock on method
- [AIRFLOW-1385] Make Airflow task logging configurable
- [AIRFLOW-940] Handle error on variable decrypt
- [AIRFLOW-1492] Add gauge for task successes/failures
- [AIRFLOW-1443] Update Airflow configuration documentation
- [AIRFLOW-1486] Unexpected S3 writing log error
- [AIRFLOW-1487] Added links to all companies officially using Airflow
- [AIRFLOW-1489] Fix typo in BigQueryCheckOperator
- [AIRFLOW-1349] Fix backfill to respect limits
- [AIRFLOW-1478] Chart owner column should be sortable
- [AIRFLOW-1397][AIRFLOW-1] No Last Run column data displayed in airflow UI 1.8.1
- [AIRFLOW-1474] Add dag_id regex feature for ``airflow clear`` command
- [AIRFLOW-1445] Changing HivePartitionSensor UI color to lighter shade
- [AIRFLOW-1359] Use default_args in Cloud ML eval
- [AIRFLOW-1389] Support createDisposition in BigQueryOperator
- [AIRFLOW-1349] Refactor BackfillJob _execute
- [AIRFLOW-1459] Fixed broken integration .rst formatting
- [AIRFLOW-1448] Revert "Fix cli reading logfile in memory"
- [AIRFLOW-1398] Allow ExternalTaskSensor to wait on multiple runs of a task
- [AIRFLOW-1399] Fix cli reading logfile in memory
- [AIRFLOW-1442] Remove extra space from ignore_all_deps generated command
- [AIRFLOW-1438] Change batch size per query in scheduler
- [AIRFLOW-1439] Add max billing tier for the BQ Hook and Operator
- [AIRFLOW-1437] Modify BigQueryTableDeleteOperator
- [Airflow 1332] Split logs based on try number
- [AIRFLOW-1385] Create abstraction for Airflow task logging
- [AIRFLOW-756][AIRFLOW-751] Replace ssh hook, operator & sftp operator with paramiko based
- [AIRFLOW-1393][[AIRFLOW-1393] Enable Py3 tests in contrib/spark_submit_hook[
- [AIRFLOW-1345] Dont expire TIs on each scheduler loop
- [AIRFLOW-1059] Reset orphaned tasks in batch for scheduler
- [AIRFLOW-1255] Fix SparkSubmitHook output deadlock
- [AIRFLOW-1359] Add Google CloudML utils for model evaluation
- [AIRFLOW-1247] Fix ignore all dependencies argument ignored
- [AIRFLOW-1401] Standardize cloud ml operator arguments
- [AIRFLOW-1394] Add quote_character param to GCS hook and operator
- [AIRFLOW-1402] Cleanup SafeConfigParser DeprecationWarning
- [AIRFLOW-1326][[AIRFLOW-1326][AIRFLOW-1184] Don't split argument array -- it's already an array.[
- [AIRFLOW-1384] Add ARGO/CaDC as a Airflow user
- [AIRFLOW-1357] Fix scheduler zip file support
- [AIRFLOW-1382] Add working dir option to DockerOperator
- [AIRFLOW-1388] Add Cloud ML Engine operators to integration doc
- [AIRFLOW-1387] Add unicode string prefix
- [AIRFLOW-1366] Add max_tries to task instance
- [AIRFLOW-1300] Enable table creation with TBLPROPERTIES
- [AIRFLOW-1271] Add Google CloudML Training Operator
- [AIRFLOW-300] Add Google Pubsub hook and operator
- [AIRFLOW-1343] Fix dataproc label format
- [AIRFLOW-1367] Pass Content-ID To reference inline images in an email, we need to be able to add <img src="cid:{}"/> to the HTML. However currently the Content-ID (cid) is not passed, so we need to add it
- [AIRFLOW-1265] Fix celery executor parsing CELERY_SSL_ACTIVE
- [AIRFLOW-1272] Google Cloud ML Batch Prediction Operator
- [AIRFLOW-1352][AIRFLOW-1335] Revert MemoryHandler change ()[]
- [AIRFLOW-1350] Add query_uri param to Hive/SparkSQL DataProc operator
- [AIRFLOW-1334] Check if tasks are backfill on scheduler in a join
- [AIRFLOW-1343] Add Airflow default label to the dataproc operator
- [AIRFLOW-1273] Add Google Cloud ML version and model operators
- [AIRFLOW-1273]AIRFLOW-1273] Add Google Cloud ML version and model operators
- [AIRFLOW-1321] Fix hidden field key to ignore case
- [AIRFLOW-1337] Make log_format key names lowercase
- [AIRFLOW-1338][AIRFLOW-782] Add GCP dataflow hook runner change to UPDATING.md
- [AIRFLOW-801] Remove outdated docstring on BaseOperator
- [AIRFLOW-1344] Fix text encoding bug when reading logs for Python 3.5
- [AIRFLOW-1338] Fix incompatible GCP dataflow hook
- [AIRFLOW-1333] Enable copy function for Google Cloud Storage Hook
- [AIRFLOW-1337] Allow log format customization via airflow.cfg
- [AIRFLOW-1320] Update LetsBonus users in README
- [AIRFLOW-1335] Use MemoryHandler for buffered logging
- [AIRFLOW-1339] Add Drivy to the list of users
- [AIRFLOW-1275] Put 'airflow pool' into API
- [AIRFLOW-1296] Propagate SKIPPED to all downstream tasks
- [AIRFLOW-1317] Fix minor issues in API reference
- [AIRFLOW-1308] Disable nanny usage for Dask
- [AIRFLOW-1172] Support nth weekday of the month cron expression
- [AIRFLOW-936] Add clear/mark success for DAG in the UI
- [AIRFLOW-1294] Backfills can loose tasks to execute
- [AIRFLOW-1299] Support imageVersion in Google Dataproc cluster
- [AIRFLOW-1291] Update NOTICE and LICENSE files to match ASF requirements
- [AIRFLOW-1301] Add New Relic to list of companies
- [AIRFLOW-1289] Removes restriction on number of scheduler threads
- [AIRFLOW-1024] Ignore celery executor errors (#49)
- [AIRFLOW-1265] Fix exception while loading celery configurations
- [AIRFLOW-1290] set docs author to 'Apache Airflow'
- [AIRFLOW-1242] Allowing project_id to have a colon in it.
- [AIRFLOW-1282] Fix known event column sorting
- [AIRFLOW-1166] Speed up _change_state_for_tis_without_dagrun
- [AIRFLOW-1208] Speed-up cli tests
- [AIRFLOW-1192] Some enhancements to qubole_operator
- [AIRFLOW-1281] Sort variables by key field by default
- [AIRFLOW-1277] Forbid KE creation with empty fields
- [AIRFLOW-1276] Forbid event creation with end_data earlier than start_date
- [AIRFLOW-1263] Dynamic height for charts
- [AIRFLOW-1266] Increase width of gantt y axis
- [AIRFLOW-1244] Forbid creation of a pool with empty name
- [AIRFLOW-1274][HTTPSENSOR] Rename parameter params to data
- [AIRFLOW-654] Add SSL Config Option for CeleryExecutor w/ RabbitMQ - Add BROKER_USE_SSL config to give option to send AMQP messages over SSL - Can be set using usual Airflow options (e.g. airflow.cfg, env vars, etc.)
- [AIRFLOW-1256] Add United Airlines to readme
- [AIRFLOW-1251] Add eRevalue to Airflow users
- [AIRFLOW-908] Print hostname at the start of cli run
- [AIRFLOW-1237] Fix IN-predicate sqlalchemy warning
- [AIRFLOW-1243] DAGs table has no default entries to show
- [AIRFLOW-1245] Fix random failure in test_trigger_dag_for_date
- [AIRFLOW-1248] Fix wrong conf name for worker timeout
- [AIRFLOW-1197] : SparkSubmitHook on_kill error
- [AIRFLOW-1191] : SparkSubmitHook custom cmd
- [AIRFLOW-1234] Cover utils.operator_helpers with UTs
- [AIRFLOW-1217] Enable Sqoop logging
- [AIRFLOW-645] Support HTTPS connections in HttpHook
- [AIRFLOW-1231] Use flask_wtf.CSRFProtect
- [AIRFLOW-1232] Remove deprecated readfp warning
- [AIRFLOW-1233] Cover utils.json with unit tests
- [AIRFLOW-1227] Remove empty column on the Logs view
- [AIRFLOW-1226] Remove empty column on the Jobs view
- [AIRFLOW-1221] Fix templating bug with DatabricksSubmitRunOperator
- [AIRFLOW-1210] Enable DbApiHook unit tests
- [AIRFLOW-1199] Fix create modal
- [AIRFLOW-1200] Forbid creation of a variable with an empty key
- [AIRFLOW-1207] Enable utils.helpers unit tests
- [AIRFLOW-1213] Add hcatalog parameters to sqoop
- [AIRFLOW-1201] Update deprecated 'nose-parameterized'
- [AIRFLOW-1186] Sort dag.get_task_instances by execution_date
- [AIRFLOW-1203] Pin Google API client version to fix OAuth issue
- [AIRFLOW-1145] Fix closest_date_partition function with before set to True If we're looking for the closest date before, we should take the latest date in the list of date before.
- [AIRFLOW-1180] Fix flask-wtf version for test_csrf_rejection
- [AIRFLOW-993] Update date inference logic
- [AIRFLOW-1170] DbApiHook insert_rows inserts parameters separately
- [AIRFLOW-1041] Do not shadow xcom_push method[]
- [AIRFLOW-860][AIRFLOW-935] Fix plugin executor import cycle and executor selection
- [AIRFLOW-1189] Fix get a DataFrame using BigQueryHook failing
- [AIRFLOW-1184] SparkSubmitHook does not split args
- [AIRFLOW-1182] SparkSubmitOperator template field
- [AIRFLOW-823] Allow specifying execution date in task_info API
- [AIRFLOW-1175] Add Pronto Tools to Airflow user list
- [AIRFLOW-1150] Fix scripts execution in sparksql hook[]
- [AIRFLOW-1141] remove crawl_for_tasks
- [AIRFLOW-1193] Add Checkr to company using Airflow
- [AIRFLOW-1168] Add closing() to all connections and cursors
- [AIRFLOW-1188] Add max_bad_records param to GoogleCloudStorageToBigQueryOperator
- [AIRFLOW-1187][AIRFLOW-1185] Fix PyPi package names in documents
- [AIRFLOW-1185] Fix PyPi URL in templates
- [AIRFLOW-XXX] Updating CHANGELOG, README, and UPDATING after 1.8.1 release
- [AIRFLOW-1181] Add delete and list functionality to gcs_hook
- [AIRFLOW-1179] Fix Pandas 0.2x breaking Google BigQuery change
- [AIRFLOW-1167] Support microseconds in FTPHook modification time
- [AIRFLOW-1173] Add Robinhood to who uses Airflow
- [AIRFLOW-945][AIRFLOW-941] Remove psycopg2 connection workaround
- [AIRFLOW-1140] DatabricksSubmitRunOperator should template the "json" field.
- [AIRFLOW-1160] Update Spark parameters for Mesos
- [AIRFLOW 1149][AIRFLOW-1149] Allow for custom filters in Jinja2 templates
- [AIRFLOW-1036] Randomize exponential backoff
- [AIRFLOW-1155] Add Tails.com to community
- [AIRFLOW-1142] Do not reset orphaned state for backfills
- [AIRFLOW-492] Make sure stat updates cannot fail a task
- [AIRFLOW-1119] Fix unload query so headers are on first row[]
- [AIRFLOW-1089] Add Spark application arguments
- [AIRFLOW-1125] Document encrypted connections
- [AIRFLOW-1122] Increase stroke width in UI
- [AIRFLOW-1138] Add missing licenses to files in scripts directory
- [AIRFLOW-11-38][AIRFLOW-1136] Capture invalid arguments for Sqoop
- [AIRFLOW-1127] Move license notices to LICENSE
- [AIRFLOW-1118] Add evo.company to Airflow users
- [AIRFLOW-1121][AIRFLOW-1004] Fix ``airflow webserver --pid`` to write out pid file
- [AIRFLOW-1124] Do not set all tasks to scheduled in backfill
- [AIRFLOW-1120] Update version view to include Apache prefix
- [AIRFLOW-1091] Add script that can compare Jira target against merges
- [AIRFLOW-1107] Add support for ftps non-default port
- [AIRFLOW-1000] Rebrand distribution to Apache Airflow
- [AIRFLOW-1094] Run unit tests under contrib in Travis
- [AIRFLOW-1112] Log which pool when pool is full in scheduler
- [AIRFLOW-1106] Add Groupalia/Letsbonus to the ReadMe
- [AIRFLOW-1109] Use kill signal to kill processes and log results
- [AIRFLOW-1074] Don't count queued tasks for concurrency limits
- [AIRFLOW-1095] Make ldap_auth memberOf come from configuration
- [AIRFLOW-1090] Add HBO
- [AIRFLOW-1035] Use binary exponential backoff
- [AIRFLOW-1081] Improve performance of duration chart
- [AIRFLOW-1078] Fix latest_runs endpoint for old flask versions
- [AIRFLOW-1085] Enhance the SparkSubmitOperator
- [AIRFLOW-1050] Do not count up_for_retry as not ready
- [AIRFLOW-1028] Databricks Operator for Airflow
- [AIRFLOW-1075] Security docs cleanup
- [AIRFLOW-1033][AIFRLOW-1033] Fix ti_deps for no schedule dags
- [AIRFLOW-1016] Allow HTTP HEAD request method on HTTPSensor
- [AIRFLOW-970] Load latest_runs on homepage async
- [AIRFLOW-111] Include queued tasks in scheduler concurrency check
- [AIRFLOW-1001] Fix landing times if there is no following schedule
- [AIRFLOW-1065] Add functionality for Azure Blob Storage over wasb://
- [AIRFLOW-947] Improve exceptions for unavailable Presto cluster
- [AIRFLOW-1067] use example.com in examples
- [AIRFLOW-1064] Change default sort to job_id for TaskInstanceModelView
- [AIRFLOW-1030][AIRFLOW-1] Fix hook import for HttpSensor
- [AIRFLOW-1051] Add a test for resetdb to CliTests
- [AIRFLOW-1004][AIRFLOW-276] Fix ``airflow webserver -D`` to run in background
- [AIRFLOW-1062] Fix DagRun#find to return correct result
- [AIRFLOW-1011] Fix bug in BackfillJob._execute() for SubDAGs
- [AIRFLOW-1038] Specify celery serialization options explicitly
- [AIRFLOW-1054] Fix broken import in test_dag
- [AIRFLOW-1007] Use Jinja sandbox for chart_data endpoint
- [AIRFLOW-719] Fix race condition in ShortCircuit, Branch and LatestOnly
- [AIRFLOW-1043] Fix doc strings of operators
- [AIRFLOW-840] Make ticket renewer python3 compatible
- [AIRFLOW-985] Extend the sqoop operator and hook
- [AIRFLOW-1034] Make it possible to connect to S3 in sigv4 regions
- [AIRFLOW-1045] Make log level configurable via airflow.cfg
- [AIRFLOW-1047] Sanitize strings passed to Markup
- [AIRFLOW-1040] Fix some small typos in comments and docstrings
- [AIRFLOW-1017] get_task_instance should not throw exception when no TI
- [AIRFLOW-1006] Add config_templates to MANIFEST
- [AIRFLOW-999] Add support for Redis database
- [AIRFLOW-1009] Remove SQLOperator from Concepts page
- [AIRFLOW-1006] Move config templates to separate files
- [AIRFLOW-1005] Improve Airflow startup time
- [AIRFLOW-1010] Add convenience script for signing releases
- [AIRFLOW-995] Remove reference to actual Airflow issue
- [AIRFLOW-681] homepage doc link should pointing to apache repo not airbnb repo
- [AIRFLOW-705][AIRFLOW-706] Fix run_command bugs
- [AIRFLOW-990] Fix Py27 unicode logging in DockerOperator
- [AIRFLOW-963] Fix non-rendered code examples
- [AIRFLOW-969] Catch bad python_callable argument
- [AIRFLOW-984] Enable subclassing of SubDagOperator
- [AIRFLOW-997] Update setup.cfg to point to Apache
- [AIRFLOW-994] Add MiNODES to the official Airflow user list
- [AIRFLOW-995][AIRFLOW-1] Update GitHub PR Template
- [AIRFLOW-989] Do not mark dag run successful if unfinished tasks
- [AIRFLOW-903] New configuration setting for the default dag view
- [AIRFLOW-979] Add GovTech GDS
- [AIRFLOW-933] Replace eval with literal_eval to prevent RCE
- [AIRFLOW-974] Fix mkdirs race condition
- [AIRFLOW-917] Fix formatting of error message
- [AIRFLOW-770] Refactor BaseHook so env vars are always read
- [AIRFLOW-900] Double trigger should not kill original task instance
- [AIRFLOW-900] Fixes bugs in LocalTaskJob for double run protection
- [AIRFLOW-932][AIRFLOW-932][AIRFLOW-921][AIRFLOW-910] Do not mark tasks removed when backfilling
- [AIRFLOW-961] run onkill when SIGTERMed
- [AIRFLOW-910] Use parallel task execution for backfills
- [AIRFLOW-967] Wrap strings in native for py2 ldap compatibility
- [AIRFLOW-958] Improve tooltip readability
- AIRFLOW-959 Cleanup and reorganize .gitignore
- AIRFLOW-960 Add .editorconfig file
- [AIRFLOW-931] Do not set QUEUED in TaskInstances
- [AIRFLOW-956] Get docs working on readthedocs.org
- [AIRFLOW-954] Fix configparser ImportError
- [AIRFLOW-941] Use defined parameters for psycopg2
- [AIRFLOW-943] Update Digital First Media in users list
- [AIRFLOW-942] Add mytaxi to Airflow users
- [AIRFLOW-939] add .swp to gitignore
- [AIRFLOW-719] Prevent DAGs from ending prematurely
- [AIRFLOW-938] Use test for True in task_stats queries
- [AIRFLOW-937] Improve performance of task_stats
- [AIRFLOW-933] use ast.literal_eval rather eval because ast.literal_eval does not execute input.
- [AIRFLOW-925] Revert airflow.hooks change that cherry-pick picked
- [AIRFLOW-919] Running tasks with no start date should not break a DAGs UI
- [AIRFLOW-802][AIRFLOW-1] Add spark-submit operator/hook
- [AIRFLOW-725] Use keyring to store credentials for JIRA
- [AIRFLOW-916] Remove deprecated readfp function
- [AIRFLOW-911] Add coloring and timing to tests
- [AIRFLOW-906] Update Code icon from lightning bolt to file
- [AIRFLOW-897] Prevent dagruns from failing with unfinished tasks
- [AIRFLOW-896] Remove unicode to 8-bit conversion in BigQueryOperator
- [AIRFLOW-899] Tasks in SCHEDULED state should be white in the UI instead of black
- [AIRFLOW-895] Address Apache release incompliancies
- [AIRFLOW-893][AIRFLOW-510] Fix crashing webservers when a dagrun has no start date
- [AIRFLOW-880] Make webserver serve logs in a sane way for remote logs
- [AIRFLOW-889] Fix minor error in the docstrings for BaseOperator
- [AIRFLOW-809][AIRFLOW-1] Use __eq__ ColumnOperator When Testing Booleans
- [AIRFLOW-875] Add template to HttpSensor params
- [AIRFLOW-866] Add FTPSensor
- [AIRFLOW-881] Check if SubDagOperator is in DAG context manager
- [AIRFLOW-885] Add change.org to the users list
- [AIRFLOW-836] Use POST and CSRF for state changing endpoints
- [AIRFLOW-862] Fix Unit Tests for DaskExecutor
- [AIRFLOW-887] Support future v0.16
- [AIRFLOW-886] Pass result to post_execute() hook
- [AIRFLOW-871] change logging.warn() into warning()
- [AIRFLOW-882] Remove unnecessary dag>>op assignment in docs
- [AIRFLOW-861] Make pickle_info endpoint be login_required
- [AIRFLOW-869] Refactor mark success functionality
- [AIRFLOW-877] Remove .sql template extension from GCS download operator
- [AIRFLOW-826] Add Zendesk hook
- [AIRFLOW-842] Do not query the DB with an empty IN clause
- [AIRFLOW-834] Change raise StopIteration into return
- [AIRFLOW-832] Let debug server run without SSL
- [AIRFLOW-862] Add DaskExecutor
- [AIRFLOW-858] Configurable database name for DB operators
- [AIRFLOW-863] Example DAGs should have recent start dates
- [AIRFLOW-853] Use utf8 encoding for stdout line decode
- [AIRFLOW-857] Use library assert statements instead of conditionals
- [AIRFLOW-856] Make sure execution date is set for local client
- [AIRFLOW-854] Add OKI as Airflow user
- [AIRFLOW-830][AIRFLOW-829][AIRFLOW-88] Reduce Travis log verbosity
- [AIRFLOW-814] Fix Presto*CheckOperator.__init__
- [AIRFLOW-793] Enable compressed loading in S3ToHiveTransfer
- [AIRFLOW-844] Fix cgroups directory creation
- [AIRFLOW-831] Restore import to fix broken tests
- [AIRFLOW-794] Access DAGS_FOLDER and SQL_ALCHEMY_CONN exclusively from settings
- [AIRFLOW-694] Fix config behaviour for empty envvar
- [AIRFLOW-365] Set dag.fileloc explicitly and use for Code view
- [AIRFLOW-781] Allow DataFlowOperators to accept jobs stored in GCS

Airflow 1.8.2 (2017-09-04)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Changelog
^^^^^^^^^

- [AIRFLOW-809][AIRFLOW-1] Use __eq__ ColumnOperator When Testing Booleans
- [AIRFLOW-1296] Propagate SKIPPED to all downstream tasks
- Re-enable caching for hadoop components
- Pin Hive and Hadoop to a specific version and create writable warehouse dir
- [AIRFLOW-1308] Disable nanny usage for Dask
- Updating CHANGELOG for 1.8.2rc1
- [AIRFLOW-1294] Backfills can loose tasks to execute
- [AIRFLOW-1291] Update NOTICE and LICENSE files to match ASF requirements
- [AIRFLOW-XXX] Set version to 1.8.2rc1
- [AIRFLOW-1160] Update Spark parameters for Mesos
- [AIRFLOW 1149][AIRFLOW-1149] Allow for custom filters in Jinja2 templates
- [AIRFLOW-1119] Fix unload query so headers are on first row[]
- [AIRFLOW-1089] Add Spark application arguments
- [AIRFLOW-1078] Fix latest_runs endpoint for old flask versions
- [AIRFLOW-1074] Don't count queued tasks for concurrency limits
- [AIRFLOW-1064] Change default sort to job_id for TaskInstanceModelView
- [AIRFLOW-1038] Specify celery serialization options explicitly
- [AIRFLOW-1036] Randomize exponential backoff
- [AIRFLOW-993] Update date inference logic
- [AIRFLOW-1167] Support microseconds in FTPHook modification time
- [AIRFLOW-1179] Fix pandas 0.2x breaking Google BigQuery change
- [AIRFLOW-1263] Dynamic height for charts
- [AIRFLOW-1266] Increase width of gantt y axis
- [AIRFLOW-1290] set docs author to 'Apache Airflow'
- [AIRFLOW-1282] Fix known event column sorting
- [AIRFLOW-1166] Speed up _change_state_for_tis_without_dagrun
- [AIRFLOW-1192] Some enhancements to qubole_operator
- [AIRFLOW-1281] Sort variables by key field by default
- [AIRFLOW-1244] Forbid creation of a pool with empty name
- [AIRFLOW-1243] DAGs table has no default entries to show
- [AIRFLOW-1227] Remove empty column on the Logs view
- [AIRFLOW-1226] Remove empty column on the Jobs view
- [AIRFLOW-1199] Fix create modal
- [AIRFLOW-1200] Forbid creation of a variable with an empty key
- [AIRFLOW-1186] Sort dag.get_task_instances by execution_date
- [AIRFLOW-1145] Fix closest_date_partition function with before set to True If we're looking for the closest date before, we should take the latest date in the list of date before.
- [AIRFLOW-1180] Fix flask-wtf version for test_csrf_rejection
- [AIRFLOW-1170] DbApiHook insert_rows inserts parameters separately
- [AIRFLOW-1150] Fix scripts execution in sparksql hook[]
- [AIRFLOW-1168] Add closing() to all connections and cursors
- [AIRFLOW-XXX] Updating CHANGELOG, README, and UPDATING after 1.8.1 release

Airflow 1.8.1 (2017-05-09)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

The Airflow package name was changed from ``airflow`` to ``apache-airflow`` during this release. You must uninstall
a previously installed version of Airflow before installing 1.8.1.

Changelog
^^^^^^^^^

- [AIRFLOW-1142] SubDAG Tasks Not Executed Even Though All Dependencies Met
- [AIRFLOW-1138] Add licenses to files in scripts directory
- [AIRFLOW-1127] Move license notices to LICENSE instead of NOTICE
- [AIRFLOW-1124] Do not set all task instances to scheduled on backfill
- [AIRFLOW-1120] Update version view to include Apache prefix
- [AIRFLOW-1062] DagRun#find returns wrong result if external_trigger=False is specified
- [AIRFLOW-1054] Fix broken import on test_dag
- [AIRFLOW-1050] Retries ignored - regression
- [AIRFLOW-1033] TypeError: can't compare datetime.datetime to None
- [AIRFLOW-1017] get_task_instance should return None instead of throw an exception for non-existent TIs
- [AIRFLOW-1011] Fix bug in BackfillJob._execute() for SubDAGs
- [AIRFLOW-1004] ``airflow webserver -D`` runs in foreground
- [AIRFLOW-1001] Landing Time shows ``unsupported operand type(s) for -: 'datetime.datetime' and 'NoneType'`` on example_subdag_operator
- [AIRFLOW-1000] Rebrand to Apache Airflow instead of Airflow
- [AIRFLOW-989] Clear Task Regression
- [AIRFLOW-974] airflow.util.file mkdir has a race condition
- [AIRFLOW-906] Update Code icon from lightning bolt to file
- [AIRFLOW-858] Configurable database name for DB operators
- [AIRFLOW-853] ssh_execute_operator.py stdout decode default to ASCII
- [AIRFLOW-832] Fix debug server
- [AIRFLOW-817] Trigger dag fails when using CLI + API
- [AIRFLOW-816] Make sure to pull nvd3 from local resources
- [AIRFLOW-815] Add previous/next execution dates to available default variables.
- [AIRFLOW-813] Fix unterminated unit tests in tests.job (tests/job.py)
- [AIRFLOW-812] Scheduler job terminates when there is no dag file
- [AIRFLOW-806] UI should properly ignore DAG doc when it is None
- [AIRFLOW-794] Consistent access to DAGS_FOLDER and SQL_ALCHEMY_CONN
- [AIRFLOW-785] ImportError if cgroupspy is not installed
- [AIRFLOW-784] Cannot install with ``funcsigs`` > 1.0.0
- [AIRFLOW-780] The UI no longer shows broken DAGs
- [AIRFLOW-777] dag_is_running is initialized to True instead of False
- [AIRFLOW-719] Skipped operations make DAG finish prematurely
- [AIRFLOW-694] Empty env vars do not overwrite non-empty config values
- [AIRFLOW-492] Insert into dag_stats table results into failed task while task itself succeeded
- [AIRFLOW-139] Executing VACUUM with PostgresOperator
- [AIRFLOW-111] DAG concurrency is not honored
- [AIRFLOW-88] Improve clarity Travis CI reports

Airflow 1.8.0 (2017-03-12)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Database
""""""""

The database schema needs to be upgraded. Make sure to shutdown Airflow and make a backup of your database. To
upgrade the schema issue ``airflow upgradedb``.

Upgrade systemd unit files
""""""""""""""""""""""""""

Systemd unit files have been updated. If you use systemd please make sure to update these.

..

   Please note that the webserver does not detach properly, this will be fixed in a future version.


Tasks not starting although dependencies are met due to stricter pool checking
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Airflow 1.7.1 has issues with being able to over subscribe to a pool, ie. more slots could be used than were
available. This is fixed in Airflow 1.8.0, but due to past issue jobs may fail to start although their
dependencies are met after an upgrade. To workaround either temporarily increase the amount of slots above
the amount of queued tasks or use a new pool.

Less forgiving scheduler on dynamic start_date
""""""""""""""""""""""""""""""""""""""""""""""

Using a dynamic start_date (e.g. ``start_date = datetime.now()``\ ) is not considered a best practice. The 1.8.0 scheduler
is less forgiving in this area. If you encounter DAGs not being scheduled you can try using a fixed start_date and
renaming your DAG. The last step is required to make sure you start with a clean slate, otherwise the old schedule can
interfere.

New and updated scheduler options
"""""""""""""""""""""""""""""""""

Please read through the new scheduler options, defaults have changed since 1.7.1.

child_process_log_directory
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to increase the robustness of the scheduler, DAGs are now processed in their own process. Therefore each
DAG has its own log file for the scheduler. These log files are placed in ``child_process_log_directory`` which defaults to
``<AIRFLOW_HOME>/scheduler/latest``. You will need to make sure these log files are removed.

..

   DAG logs or processor logs ignore and command line settings for log file locations.


run_duration
~~~~~~~~~~~~

Previously the command line option ``num_runs`` was used to let the scheduler terminate after a certain amount of
loops. This is now time bound and defaults to ``-1``\ , which means run continuously. See also num_runs.

num_runs
~~~~~~~~

Previously ``num_runs`` was used to let the scheduler terminate after a certain amount of loops. Now num_runs specifies
the number of times to try to schedule each DAG file within ``run_duration`` time. Defaults to ``-1``\ , which means try
indefinitely. This is only available on the command line.

min_file_process_interval
~~~~~~~~~~~~~~~~~~~~~~~~~

After how much time should an updated DAG be picked up from the filesystem.

min_file_parsing_loop_time
~~~~~~~~~~~~~~~~~~~~~~~~~~

CURRENTLY DISABLED DUE TO A BUG
How many seconds to wait between file-parsing loops to prevent the logs from being spammed.

dag_dir_list_interval
~~~~~~~~~~~~~~~~~~~~~

The frequency with which the scheduler should relist the contents of the DAG directory. If while developing +dags, they are not being picked up, have a look at this number and decrease it when necessary.

catchup_by_default
~~~~~~~~~~~~~~~~~~

By default the scheduler will fill any missing interval DAG Runs between the last execution date and the current date.
This setting changes that behavior to only execute the latest interval. This can also be specified per DAG as
``catchup = False / True``. Command line backfills will still work.

Faulty DAGs do not show an error in the Web UI
""""""""""""""""""""""""""""""""""""""""""""""

Due to changes in the way Airflow processes DAGs the Web UI does not show an error when processing a faulty DAG. To
find processing errors go the ``child_process_log_directory`` which defaults to ``<AIRFLOW_HOME>/scheduler/latest``.

New DAGs are paused by default
""""""""""""""""""""""""""""""

Previously, new DAGs would be scheduled immediately. To retain the old behavior, add this to airflow.cfg:

.. code-block::

   [core]
   dags_are_paused_at_creation = False

Airflow Context variable are passed to Hive config if conf is specified
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If you specify a hive conf to the run_cli command of the HiveHook, Airflow add some
convenience variables to the config. In case you run a secure Hadoop setup it might be
required to allow these variables by adjusting you hive configuration to add ``airflow\.ctx\..*`` to the regex
of user-editable configuration properties. See
`the Hive docs on Configuration Properties <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=82903061#ConfigurationProperties-SQLStandardBasedAuthorization.1>`_ for more info.

Google Cloud Operator and Hook alignment
""""""""""""""""""""""""""""""""""""""""

All Google Cloud Operators and Hooks are aligned and use the same client library. Now you have a single connection
type for all kinds of Google Cloud Operators.

If you experience problems connecting with your operator make sure you set the connection type "Google Cloud".

Also the old P12 key file type is not supported anymore and only the new JSON key files are supported as a service
account.

Deprecated Features
"""""""""""""""""""

These features are marked for deprecation. They may still work (and raise a ``DeprecationWarning``\ ), but are no longer
supported and will be removed entirely in Airflow 2.0


*
  Hooks and operators must be imported from their respective submodules

  ``airflow.operators.PigOperator`` is no longer supported; ``from airflow.operators.pig_operator import PigOperator`` is.
  (AIRFLOW-31, AIRFLOW-200)

*
  Operators no longer accept arbitrary arguments

  Previously, ``Operator.__init__()`` accepted any arguments (either positional ``*args`` or keyword ``**kwargs``\ ) without
  complaint. Now, invalid arguments will be rejected. (https://github.com/apache/airflow/pull/1285)

*
  The config value secure_mode will default to True which will disable some insecure endpoints/features

Known Issues
""""""""""""

There is a report that the default of "-1" for num_runs creates an issue where errors are reported while parsing tasks.
It was not confirmed, but a workaround was found by changing the default back to ``None``.

To do this edit ``cli.py``\ , find the following:

.. code-block::

           'num_runs': Arg(
               ("-n", "--num_runs"),
               default=-1, type=int,
               help="Set the number of runs to execute before exiting"),

and change ``default=-1`` to ``default=None``. If you have this issue please report it on the mailing list.

Changelog
^^^^^^^^^

- [AIRFLOW-900] Double trigger should not kill original task instance
- [AIRFLOW-900] Fixes bugs in LocalTaskJob for double run protection
- [AIRFLOW-932] Do not mark tasks removed when backfilling
- [AIRFLOW-961] run onkill when SIGTERMed
- [AIRFLOW-910] Use parallel task execution for backfills
- [AIRFLOW-967] Wrap strings in native for py2 ldap compatibility
- [AIRFLOW-941] Use defined parameters for psycopg2
- [AIRFLOW-719] Prevent DAGs from ending prematurely
- [AIRFLOW-938] Use test for True in task_stats queries
- [AIRFLOW-937] Improve performance of task_stats
- [AIRFLOW-933] use ast.literal_eval rather eval because ast.literal_eval does not execute input.
- [AIRFLOW-925] Revert airflow.hooks change that cherry-pick picked
- [AIRFLOW-919] Running tasks with no start date should not break a DAGs UI
- [AIRFLOW-802] Add spark-submit operator/hook
- [AIRFLOW-897] Prevent dagruns from failing with unfinished tasks
- [AIRFLOW-861] make pickle_info endpoint be login_required
- [AIRFLOW-853] use utf8 encoding for stdout line decode
- [AIRFLOW-856] Make sure execution date is set for local client
- [AIRFLOW-830][AIRFLOW-829][AIRFLOW-88] Reduce Travis log verbosity
- [AIRFLOW-831] Restore import to fix broken tests
- [AIRFLOW-794] Access DAGS_FOLDER and SQL_ALCHEMY_CONN exclusively from settings
- [AIRFLOW-694] Fix config behaviour for empty envvar
- [AIRFLOW-365] Set dag.fileloc explicitly and use for Code view
- [AIRFLOW-931] Do not set QUEUED in TaskInstances
- [AIRFLOW-899] Tasks in SCHEDULED state should be white in the UI instead of black
- [AIRFLOW-895] Address Apache release incompliancies
- [AIRFLOW-893][AIRFLOW-510] Fix crashing webservers when a dagrun has no start date
- [AIRFLOW-793] Enable compressed loading in S3ToHiveTransfer
- [AIRFLOW-863] Example DAGs should have recent start dates
- [AIRFLOW-869] Refactor mark success functionality
- [AIRFLOW-856] Make sure execution date is set for local client
- [AIRFLOW-814] Fix Presto*CheckOperator.__init__
- [AIRFLOW-844] Fix cgroups directory creation
- [AIRFLOW-816] Use static nvd3 and d3
- [AIRFLOW-821] Fix py3 compatibility
- [AIRFLOW-817] Check for None value of execution_date in endpoint
- [AIRFLOW-822] Close db before exception
- [AIRFLOW-815] Add prev/next execution dates to template variables
- [AIRFLOW-813] Fix unterminated unit tests in SchedulerJobTest
- [AIRFLOW-813] Fix unterminated scheduler unit tests
- [AIRFLOW-806] UI should properly ignore DAG doc when it is None
- [AIRFLOW-812] Fix the scheduler termination bug.
- [AIRFLOW-780] Fix dag import errors no longer working
- [AIRFLOW-783] Fix py3 incompatibility in BaseTaskRunner
- [AIRFLOW-810] Correct down_revision dag_id/state index creation
- [AIRFLOW-807] Improve scheduler performance for large DAGs
- [AIRFLOW-798] Check return_code before forcing termination
- [AIRFLOW-139] Let psycopg2 handle autocommit for PostgresHook
- [AIRFLOW-776] Add missing cgroups devel dependency
- [AIRFLOW-777] Fix expression to check if a DagRun is in running state
- [AIRFLOW-785] Don't import CgroupTaskRunner at global scope
- [AIRFLOW-784] Pin ``funcsigs`` to 1.0.0
- [AIRFLOW-624] Fix setup.py to not import airflow.version as version
- [AIRFLOW-779] Task should fail with specific message when deleted
- [AIRFLOW-778] Fix completely broken MetastorePartitionSensor
- [AIRFLOW-739] Set pickle_info log to debug
- [AIRFLOW-771] Make S3 logs append instead of clobber
- [AIRFLOW-773] Fix flaky datetime addition in api test
- [AIRFLOW-219][AIRFLOW-398] Cgroups + impersonation
- [AIRFLOW-683] Add Jira hook, operator and sensor
- [AIRFLOW-762] Add Google DataProc delete operator
- [AIRFLOW-760] Update systemd config
- [AIRFLOW-759] Use previous dag_run to verify depend_on_past
- [AIRFLOW-757] Set child_process_log_directory default more sensible
- [AIRFLOW-692] Open XCom page to super-admins only
- [AIRFLOW-737] Fix HDFS Sensor directory.
- [AIRFLOW-747] Fix retry_delay not honoured
- [AIRFLOW-558] Add Support for dag.catchup=(True|False) Option
- [AIRFLOW-489] Allow specifying execution date in trigger_dag API
- [AIRFLOW-738] Commit deleted xcom items before insert
- [AIRFLOW-729] Add Google Cloud Dataproc cluster creation operator
- [AIRFLOW-728] Add Google BigQuery table sensor
- [AIRFLOW-741] Log to debug instead of info for app.py
- [AIRFLOW-731] Fix period bug for NamedHivePartitionSensor
- [AIRFLOW-740] Pin jinja2 to < 2.9.0
- [AIRFLOW-663] Improve time units for task performance charts
- [AIRFLOW-665] Fix email attachments
- [AIRFLOW-734] Fix SMTP auth regression when not using user/pass
- [AIRFLOW-702] Fix LDAP Regex Bug
- [AIRFLOW-717] Add Cloud Storage updated sensor
- [AIRFLOW-695] Retries do not execute because dagrun is in FAILED state
- [AIRFLOW-673] Add operational metrics test for SchedulerJob
- [AIRFLOW-727] try_number is not increased
- [AIRFLOW-715] A more efficient HDFS Sensor:
- [AIRFLOW-716] Allow AVRO BigQuery load-job without schema
- [AIRFLOW-718] Allow the query URI for DataProc Pig
- Log needs to be part of try/catch block
- [AIRFLOW-721] Descendant process can disappear before termination
- [AIRFLOW-403] Bash operator's kill method leaves underlying processes running
- [AIRFLOW-657] Add AutoCommit Parameter for MSSQL
- [AIRFLOW-641] Improve pull request instructions
- [AIRFLOW-685] Add test for MySqlHook.bulk_load()
- [AIRFLOW-686] Match auth backend config section
- [AIRFLOW-691] Add SSH KeepAlive option to SSH_hook
- [AIRFLOW-709] Use same engine for migrations and reflection
- [AIRFLOW-700] Update to reference to web authentication documentation
- [AIRFLOW-649] Support non-sched DAGs in LatestOnlyOp
- [AIRFLOW-712] Fix AIRFLOW-667 to use proper HTTP error properties
- [AIRFLOW-710] Add OneFineStay as official user
- [AIRFLOW-703][AIRFLOW-1] Stop Xcom being cleared too early
- [AIRFLOW-679] Stop concurrent task instances from running
- [AIRFLOW-704][AIRFLOW-1] Fix invalid syntax in BQ hook
- [AIRFLOW-667] Handle BigQuery 503 error
- [AIRFLOW-680] Disable connection pool for commands
- [AIRFLOW-678] Prevent scheduler from double triggering TIs
- [AIRFLOW-677] Kill task if it fails to heartbeat
- [AIRFLOW-674] Ability to add descriptions for DAGs
- [AIRFLOW-682] Bump MAX_PERIODS to make mark_success work for large DAGs
- Use jdk selector to set required jdk
- [AIRFLOW-647] Restore dag.get_active_runs
- [AIRFLOW-662] Change seasons to months in project description
- [AIRFLOW-656] Add dag/task/date index to xcom table
- [AIRFLOW-658] Improve schema_update_options in GCP
- [AIRFLOW-41] Fix pool oversubscription
- [AIRFLOW-489] Add API Framework
- [AIRFLOW-653] Add some missing endpoint tests
- [AIRFLOW-652] Remove obsolete endpoint
- [AIRFLOW-345] Add contrib ECSOperator
- [AIRFLOW-650] Adding Celect to user list
- [AIRFLOW-510] Filter Paused Dags, show Last Run & Trigger Dag
- [AIRFLOW-643] Improve date handling for sf_hook
- [AIRFLOW-638] Add schema_update_options to GCP ops
- [AIRFLOW-640] Install and enable nose-ignore-docstring
- [AIRFLOW-639]AIRFLOW-639] Alphasort package names
- [AIRFLOW-375] Fix pylint errors
- [AIRFLOW-347] Show empty DAG runs in tree view
- [AIRFLOW-628] Adding SalesforceHook to contrib/hooks
- [AIRFLOW-514] hive hook loads data from pandas DataFrame into hive and infers types
- [AIRFLOW-565] Fixes DockerOperator on Python3.x
- [AIRFLOW-635] Encryption option for S3 hook
- [AIRFLOW-137] Fix max_active_runs on clearing tasks
- [AIRFLOW-343] Fix schema plumbing in HiveServer2Hook
- [AIRFLOW-130] Fix ssh operator macosx
- [AIRFLOW-633] Show TI attributes in TI view
- [AIRFLOW-626][AIRFLOW-1] HTML Content does not show up when sending email with attachment
- [AIRFLOW-533] Set autocommit via set_autocommit
- [AIRFLOW-629] stop pinning lxml
- [AIRFLOW-464] Add setdefault method to Variable
- [AIRFLOW-626][AIRFLOW-1] HTML Content does not show up when sending email with attachment
- [AIRFLOW-591] Add datadog hook & sensor
- [AIRFLOW-561] Add RedshiftToS3Transfer operator
- [AIRFLOW-570] Pass root to date form on gantt
- [AIRFLOW-504] Store fractional seconds in MySQL tables
- [AIRFLOW-623] LDAP attributes not always a list
- [AIRFLOW-611] source_format in BigQueryBaseCursor
- [AIRFLOW-619] Fix exception in Gantt chart
- [AIRFLOW-618] Cast DateTimes to avoid sqlite errors
- [AIRFLOW-422] Add JSON endpoint for task info
- [AIRFLOW-616][AIRFLOW-617] Minor fixes to PR tool UX
- [AIRFLOW-179] Fix DbApiHook with non-ASCII chars
- [AIRFLOW-566] Add timeout while fetching logs
- [AIRFLOW-615] Set graph glyphicon first
- [AIRFLOW-609] Add application_name to PostgresHook
- [AIRFLOW-604] Revert .first() to .one()
- [AIRFLOW-370] Create AirflowConfigException in exceptions.py
- [AIRFLOW-582] Fixes TI.get_dagrun filter (removes start_date)
- [AIRFLOW-568] Fix double task_stats count if a DagRun is active
- [AIRFLOW-585] Fix race condition in backfill execution loop
- [AIRFLOW-580] Prevent landscape warning on .format
- [AIRFLOW-597] Check if content is None, not false-equivalent
- [AIRFLOW-586] test_dag_v1 fails from 0 to 3 a.m.
- [AIRFLOW-453] Add XCom Admin Page
- [AIRFLOW-588] Add Google Cloud Storage Object sensor[]
- [AIRFLOW-592] example_xcom import Error
- [AIRFLOW-587] Fix incorrect scope for Google Auth[]
- [AIRFLOW-589] Add templatable job_name[]
- [AIRFLOW-227] Show running config in config view
- [AIRFLOW-319]AIRFLOW-319] xcom push response in HTTP Operator
- [AIRFLOW-385] Add symlink to latest scheduler log directory
- [AIRFLOW-583] Fix decode error in gcs_to_bq
- [AIRFLOW-96] s3_conn_id using environment variable
- [AIRFLOW-575] Clarify tutorial and FAQ about ``schedule_interval`` always inheriting from DAG object
- [AIRFLOW-577] Output BigQuery job for improved debugging
- [AIRFLOW-560] Get URI & SQLA engine from Connection
- [AIRFLOW-518] Require DataProfilingMixin for Variables CRUD
- [AIRFLOW-553] Fix load path for filters.js
- [AIRFLOW-554] Add Jinja support to Spark-sql
- [AIRFLOW-550] Make ssl config check empty string safe
- [AIRFLOW-500] Use id for github allowed teams
- [AIRFLOW-556] Add UI PR guidelines
- [AIRFLOW-358][AIRFLOW-430] Add ``connections`` cli
- [AIRFLOW-548] Load DAGs immediately & continually
- [AIRFLOW-539] Updated BQ hook and BQ operator to support Standard SQL.
- [AIRFLOW-378] Add string casting to params of spark-sql operator
- [AIRFLOW-544] Add Pause/Resume toggle button
- [AIRFLOW-333][AIRFLOW-258] Fix non-module plugin components
- [AIRFLOW-542] Add tooltip to DAGs links icons
- [AIRFLOW-530] Update docs to reflect connection environment var has to be in uppercase
- [AIRFLOW-525] Update template_fields in Qubole Op
- [AIRFLOW-480] Support binary file download from GCS
- [AIRFLOW-198] Implement latest_only_operator
- [AIRFLOW-91] Add SSL config option for the webserver
- [AIRFLOW-191] Fix connection leak with PostgreSQL backend
- [AIRFLOW-512] Fix 'bellow' typo in docs & comments
- [AIRFLOW-509][AIRFLOW-1] Create operator to delete tables in BigQuery
- [AIRFLOW-498] Remove hard-coded gcp project id
- [AIRFLOW-505] Support unicode characters in authors' names
- [AIRFLOW-494] Add per-operator success/failure metrics
- [AIRFLOW-488] Fix test_simple fail
- [AIRFLOW-468] Update panda requirement to 0.17.1
- [AIRFLOW-159] Add cloud integration section + GCP documentation
- [AIRFLOW-477][AIRFLOW-478] Restructure security section for clarity
- [AIRFLOW-467] Allow defining of project_id in BigQueryHook
- [AIRFLOW-483] Change print to logging statement
- [AIRFLOW-475] make the segment granularity in Druid hook configurable


Airflow 1.7.1.2 (2016-05-20)
----------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Changes to Configuration
""""""""""""""""""""""""

Email configuration change
~~~~~~~~~~~~~~~~~~~~~~~~~~

To continue using the default smtp email backend, change the email_backend line in your config file from:

.. code-block::

   [email]
   email_backend = airflow.utils.send_email_smtp

to:

.. code-block::

   [email]
   email_backend = airflow.utils.email.send_email_smtp

S3 configuration change
~~~~~~~~~~~~~~~~~~~~~~~

To continue using S3 logging, update your config file so:

.. code-block::

   s3_log_folder = s3://my-airflow-log-bucket/logs

becomes:

.. code-block::

   remote_base_log_folder = s3://my-airflow-log-bucket/logs
   remote_log_conn_id = <your desired s3 connection>

Changelog
^^^^^^^^^

- [AIRFLOW-463] Link Airflow icon to landing page
- [AIRFLOW-149] Task Dependency Engine + Why Isn't My Task Running View
- [AIRFLOW-361] Add default failure handler for the Qubole Operator
- [AIRFLOW-353] Fix dag run status update failure
- [AIRFLOW-447] Store source URIs in Python 3 compatible list
- [AIRFLOW-443] Make module names unique when importing
- [AIRFLOW-444] Add Google authentication backend
- [AIRFLOW-446][AIRFLOW-445] Adds missing dataproc submit options
- [AIRFLOW-431] Add CLI for CRUD operations on pools
- [AIRFLOW-329] Update Dag Overview Page with Better Status Columns
- [AIRFLOW-360] Fix style warnings in models.py
- [AIRFLOW-425] Add white fill for null state tasks in tree view.
- [AIRFLOW-69] Use dag runs in backfill jobs
- [AIRFLOW-415] Make dag_id not found error clearer
- [AIRFLOW-416] Use ordinals in README's company list
- [AIRFLOW-369] Allow setting default DAG orientation
- [AIRFLOW-410] Add 2 Q/A to the FAQ in the docs
- [AIRFLOW-407] Add different colors for some sensors
- [AIRFLOW-414] Improve error message for missing FERNET_KEY
- [AIRFLOW-406] Sphinx/rst fixes
- [AIRFLOW-412] Fix lxml dependency
- [AIRFLOW-413] Fix unset path bug when backfilling via pickle
- [AIRFLOW-78] Airflow clear leaves dag_runs
- [AIRFLOW-402] Remove NamedHivePartitionSensor static check, add docs
- [AIRFLOW-394] Add an option to the Task Duration graph to show cumulative times
- [AIRFLOW-404] Retry download if unpacking fails for hive
- [AIRFLOW-276] Gunicorn rolling restart
- [AIRFLOW-399] Remove dags/testdruid.py
- [AIRFLOW-400] models.py/DAG.set_dag_runs_state() does not correctly set state
- [AIRFLOW-395] Fix colon/equal signs typo for resources in default config
- [AIRFLOW-397] Documentation: Fix typo in the word "instantiating"
- [AIRFLOW-395] Remove trailing commas from resources in config
- [AIRFLOW-388] Add a new chart for Task_Tries for each DAG
- [AIRFLOW-322] Fix typo in FAQ section
- [AIRFLOW-375] Pylint fixes
- limit scope to user email only AIRFLOW-386
- [AIRFLOW-383] Cleanup example qubole operator dag
- [AIRFLOW-160] Parse DAG files through child processes
- [AIRFLOW-381] Manual UI Dag Run creation: require dag_id field
- [AIRFLOW-373] Enhance CLI variables functionality
- [AIRFLOW-379] Enhance Variables page functionality: import/export variables
- [AIRFLOW-331] modify the LDAP authentication config lines in  'Security' sample codes
- [AIRFLOW-356][AIRFLOW-355][AIRFLOW-354] Replace nobr, enable DAG only exists locally message, change edit DAG icon
- [AIRFLOW-362] Import __future__ division
- [AIRFLOW-359] Pin flask-login to 0.2.11
- [AIRFLOW-261] Add bcc and cc fields to EmailOperator
- [AIRFLOW-348] Fix code style warnings
- [AIRFLOW-349] Add metric for number of zombies killed
- [AIRFLOW-340] Remove unused dependency on Babel
- [AIRFLOW-339]: Ability to pass a flower conf file
- [AIRFLOW-341][operators] Add resource requirement attributes to operators
- [AIRFLOW-335] Fix simple style errors/warnings
- [AIRFLOW-337] Add __repr__ to VariableAccessor and VariableJsonAccessor
- [AIRFLOW-334] Fix using undefined variable
- [AIRFLOW-315] Fix blank lines code style warnings
- [AIRFLOW-306] Add Spark-sql Hook and Operator
- [AIRFLOW-327] Add rename method to the FTPHook
- [AIRFLOW-321] Fix a wrong code example about tests/dags
- [AIRFLOW-316] Always check DB state for Backfill Job execution
- [AIRFLOW-264] Adding workload management for Hive
- [AIRFLOW-297] support exponential backoff option for retry delay
- [AIRFLOW-31][AIRFLOW-200] Add note to updating.md
- [AIRFLOW-307] There is no __neq__ Python magic method.
- [AIRFLOW-309] Add requirements of develop dependencies to docs
- [AIRFLOW-307] Rename __neq__ to __ne__ Python magic method.
- [AIRFLOW-313] Fix code style for sqoop_hook.py
- [AIRFLOW-311] Fix wrong path in CONTRIBUTING.md
- [AIRFLOW-24] DataFlow Java Operator
- [AIRFLOW-308] Add link to refresh DAG within DAG view header
- [AIRFLOW-314] Fix BigQuery cursor run_table_upsert method
- [AIRFLOW-298] fix incubator disclaimer in docs
- [AIRFLOW-284] HiveServer2Hook fix for cursor scope for get_results
- [AIRFLOW-260] More graceful exit when issues can't be closed
- [AIRFLOW-260] Handle case when no version is found
- [AIRFLOW-228] Handle empty version list in PR tool
- [AIRFLOW-302] Improve default squash commit message
- [AIRFLOW-187] Improve prompt styling
- [AIRFLOW-187] Fix typo in argument name
- [AIRFLOW-187] Move "Close XXX" message to end of squash commit
- [AIRFLOW-247] Add EMR hook, operators and sensors. Add AWS base hook
- [AIRFLOW-301] Fix broken unit test
- [AIRFLOW-100] Add execution_date_fn to ExternalTaskSensor
- [AIRFLOW-282] Remove PR Tool logic that depends on version formatting
- [AIRFLOW-291] Add index for state in TI table
- [AIRFLOW-269] Add some unit tests for PostgreSQL
- [AIRFLOW-296] template_ext is being treated as a string rather than a tuple in qubole operator
- [AIRFLOW-286] Improve FTPHook to implement context manager interface
- [AIRFLOW-243] Create NamedHivePartitionSensor
- [AIRFLOW-246] Improve dag_stats endpoint query
- [AIRFLOW-189] Highlighting of Parent/Child nodes in Graphs
- [ARFLOW-255] Check dagrun timeout when comparing active runs
- [AIRFLOW-281] Add port to mssql_hook
- [AIRFLOW-285] Use Airflow 2.0 style imports for all remaining hooks/operators
- [AIRFLOW-40] Add LDAP group filtering feature.
- [AIRFLOW-277] Multiple deletions does not work in Task Instances view if using SQLite backend
- [AIRFLOW-200] Make hook/operator imports lazy, and print proper exceptions
- [AIRFLOW-283] Make store_to_xcom_key a templated field in GoogleCloudStorageDownloadOperator
- [AIRFLOW-278] Support utf-8 encoding for SQL
- [AIRFLOW-280] clean up tmp druid table no matter if an ingestion job succeeds or not
- [AIRFLOW-274] Add XCom functionality to GoogleCloudStorageDownloadOperator
- [AIRFLOW-273] Create an svg version of the Airflow logo.
- [AIRFLOW-275] Update contributing guidelines
- [AIRFLOW-244] Modify hive operator to inject analysis data
- [AIRFLOW-162] Allow variable to be accessible into templates
- [AIRFLOW-248] Add Apache license header to all files
- [AIRFLOW-263] Remove temp backtick file
- [AIRFLOW-252] Raise Sqlite exceptions when deleting tasks instance in WebUI
- [AIRFLOW-180] Fix timeout behavior for sensors
- [AIRFLOW-262] Simplify commands in MANIFEST.in
- [AIRFLOW-31] Add zope dependency
- [AIRFLOW-6] Remove dependency on Highcharts
- [AIRFLOW-234] make task that are not ``running`` self-terminate
- [AIRFLOW-256] Fix test_scheduler_reschedule heartrate
- Add Python 3 compatibility fix
- [AIRFLOW-31] Use standard imports for hooks/operators
- [AIRFLOW-173] Initial implementation of FileSensor
- [AIRFLOW-224] Collect orphaned tasks and reschedule them
- [AIRFLOW-239] Fix tests indentation
- [AIRFLOW-225] Better units for task duration graph
- [AIRFLOW-241] Add testing done section to PR template
- [AIRFLOW-222] Show duration of task instances in ui
- [AIRFLOW-231] Do not eval user input in PrestoHook
- [AIRFLOW-216] Add Sqoop Hook and Operator
- [AIRFLOW-171] Add upgrade notes on email and S3 to 1.7.1.2
- [AIRFLOW-238] Make compatible with flask-admin 1.4.1
- [AIRFLOW-230][HiveServer2Hook] adding multi statements support
- [AIRFLOW-142] setup_env.sh doesn't download hive tarball if hdp is specified as distro
- [AIRFLOW-223] Make parametable the IP on which Flower binds to
- [AIRFLOW-218] Added option to enable webserver gunicorn access/err logs
- [AIRFLOW-213] Add "Closes #X" phrase to commit messages
- [AIRFLOW-68] Align start_date with the schedule_interval
- [AIRFLOW-9] Improving docs to meet Apache's standards
- [AIRFLOW-131] Make XCom.clear more selective
- [AIRFLOW-214] Fix occasion of detached taskinstance
- [AIRFLOW-206] Add commit to close PR
- [AIRFLOW-206] Always load local log files if they exist
- [AIRFLOW-211] Fix JIRA "resolve" vs "close" behavior
- [AIRFLOW-64] Add note about relative DAGS_FOLDER
- [AIRFLOW-114] Sort plugins dropdown
- [AIRFLOW-209] Add scheduler tests and improve lineage handling
- [AIRFLOW-207] Improve JIRA auth workflow
- [AIRFLOW-187] Improve PR tool UX
- [AIRFLOW-155] Documentation of Qubole Operator
- Optimize and refactor process_dag
- [AIRFLOW-185] Handle empty versions list
- [AIRFLOW-201] Fix for HiveMetastoreHook + kerberos
- [AIRFLOW-202]: Fixes stray print line
- [AIRFLOW-196] Fix bug that exception is not handled in HttpSensor
- [AIRFLOW-195] : Add toggle support to subdag clearing in the CLI
- [AIRFLOW-23] Support for Google Cloud DataProc
- [AIRFLOW-25] Configuration for Celery always required
- [AIRFLOW-190] Add codecov and remove download count
- [AIRFLOW-168] Correct evaluation of @once schedule
- [AIRFLOW-183] Fetch log from remote when worker returns 4xx/5xx response
- [AIRFLOW-181] Fix failing unpacking of hadoop by redownloading
- [AIRFLOW-176] remove unused formatting key
- [AIRFLOW-167]: Add dag_state option in cli
- [AIRFLOW-178] Fix bug so that zip file is detected in DAG folder
- [AIRFLOW-176] Improve PR Tool JIRA workflow
- AIRFLOW-45: Support Hidden Airflow Variables
- [AIRFLOW-175] Run git-reset before checkout in PR tool
- [AIRFLOW-157] Make PR tool Py3-compat; add JIRA command
- [AIRFLOW-170] Add missing @apply_defaults

Airflow 1.7.1 (2016-05-19)
--------------------------

- Fix : Don't treat premature tasks as could_not_run tasks
- AIRFLOW-92 Avoid unneeded upstream_failed session closes apache/airflow#1485
- Add logic to lock DB and avoid race condition
- Handle queued tasks from multiple jobs/executors
- AIRFLOW-52 Warn about overwriting tasks in a DAG
- Fix corner case with joining processes/queues (#1473)
- [AIRFLOW-52] Fix bottlenecks when working with many tasks
- Add columns to toggle extra detail in the connection list view.
- Log the number of errors when importing DAGs
- Log dagbag metrics duplicate messages in queue into StatsD (#1406)
- Clean up issue template (#1419)
- correct missed arg.foreground to arg.daemon in cli
- Reinstate imports for github enterprise auth
- Use os.execvp instead of subprocess.Popen for the webserver
- Revert from using "--foreground" to "--daemon"
- Implement a Cloudant hook
- Add missing args to ``airflow clear``
- Fixed a bug in the scheduler: num_runs used where runs intended
- Add multiprocessing support to the scheduler
- Partial fix to make sure next_run_date cannot be None
- Support list/get/set variables in the CLI
- Properly handle BigQuery booleans in BigQuery hook.
- Added the ability to view XCom variables in webserver
- Change DAG.tasks from a list to a dict
- Add support for zipped dags
- Stop creating hook on instantiating of S3 operator
- User subquery in views to find running DAGs
- Prevent DAGs from being reloaded on every scheduler iteration
- Add a missing word to docs
- Document the parameters of ``DbApiHook``
- added oracle operator with existing oracle hook
- Add PyOpenSSL to Google Cloud gcp_api.
- Remove executor error unit test
- Add DAG inference, deferral, and context manager
- Don't return error when writing files to Google cloud storage.
- Fix GCS logging for gcp_api.
- Ensure attr is in scope for error message
- Fixing misnamed PULL_REQUEST_TEMPLATE
- Extract non_pooled_task_slot_count into a configuration param
- Update plugins.rst for clarity on the example (#1309)
- Fix s3 logging issue
- Add twitter feed example dag
- GitHub ISSUE_TEMPLATE & PR_TEMPLATE cleanup
- Reduce logger verbosity
- Adding a PR Template
- Add Lucid to list of users
- Fix usage of asciiart
- Use session instead of outdated main_session for are_dependencies_met
- Fix celery flower port allocation
- Fix for missing edit actions due to flask-admin upgrade
- Fix typo in comment in prioritize_queued method
- Add HipchatOperator
- Include all example dags in backfill unit test
- Make sure skipped jobs are actually skipped
- Fixing a broken example dag, example_skip_dag.py
- Add consistent and thorough signal handling and logging
- Allow Operators to specify SKIPPED status internally
- Update docstring for executor trap unit test
- Doc: explain the usage of Jinja templating for templated params
- Don't schedule runs before the DAG's start_date
- Fix infinite retries with pools, with test
- Fix handling of deadlocked jobs
- Show only Airflow's deprecation warnings
- Set DAG_FOLDER for unit tests
- Missing comma in setup.py
- Deprecate args and kwargs in BaseOperator
- Raise deep scheduler exceptions to force a process restart.
- Change inconsistent example DAG owners
- Fix module path of send_email_smtp in configuration
- added Gentner Lab to list of users
- Increase timeout time for unit test
- Fix reading strings from conf
- CHORE - Remove Trailing Spaces
- Fix SSHExecuteOperator crash when using a custom ssh port
- Add note about Airflow components to template
- Rewrite BackfillJob logic for clarity
- Add unit tests
- Fix miscellaneous bugs and clean up code
- Fix logic for determining DagRun states
- Make SchedulerJob not run EVERY queued task
- Improve BackfillJob handling of queued/deadlocked tasks
- Introduce ignore_depends_on_past parameters
- Use Popen with CeleryExecutor
- Rename user table to users to avoid conflict with postgres
- Beware of negative pool slots.
- Add support for calling_format from boto to S3_Hook
- Add PyPI meta data and sync version number
- Set dags_are_paused_at_creation's default value to True
- Resurface S3Log class eaten by rebase/push -f
- Add missing session.commit() at end of initdb
- Validate that subdag tasks have pool slots available, and test
- Use urlparse for remote GCS logs, and add unit tests
- Make webserver worker timeout configurable
- Fixed scheduling for @once interval
- Use psycopg2's API for serializing postgres cell values
- Make the provide_session decorator more robust
- update link to Lyft's website
- use num_shards instead of partitions to be consistent with batch ingestion
- Add documentation links to README
- Update docs with separate configuration section
- Fix airflow.utils deprecation warning code being Python 3 incompatible
- Extract dbapi cell serialization into its own method
- Set Postgres autocommit as supported only if server version is < 7.4
- Use refactored utils module in unit test imports
- Add changelog for 1.7.0
- Use LocalExecutor on Travis if possible
- remove unused logging,errno, MiniHiveCluster imports
- remove extra import of logging lib
- Fix required gcloud version
- Refactoring utils into smaller submodules
- Properly measure number of task retry attempts
- Add function to get configuration as dict, plus unit tests
- Merge branch 'master' into hivemeta_sasl
- Add wiki link to README.md
- [hotfix] make email.Utils > email.utils for py3
- Add the missing "Date" header to the warning e-mails
- Add the missing "Date" header to the warning e-mails
- Check name of SubDag class instead of class itself
- [hotfix] removing repo_token from .coveralls.yml
- Set the service_name in coverals.yml
- Fixes #1223
- Update Airflow docs for remote logging
- Add unit tests for trapping Executor errors
- Make sure Executors properly trap errors
- Fix HttpOpSensorTest to use fake request session
- Linting
- Add an example on pool usage in the documentation
- Add two methods to bigquery hook's base cursor: run_table_upsert, which adds a table or updates an existing table; and run_grant_dataset_view_access, which grants view access to a given dataset for a given table.
- Tasks references upstream and downstream tasks using strings instead of references
- Fix typos in models.py
- Fix broken links in documentation
- [hotfix] fixing the Scheduler CLI to make dag_id optional
- Update link to Common Pitfalls wiki page in README
- Allow disabling periodic committing when inserting rows with DbApiHook
- added Glassdoor to "who uses airflow"
- Fix typo preventing from launching webserver
- Documentation badge
- Fixing ISSUE_TEMPLATE name to include .md suffix
- Adding an ISSUE_TEMPLATE to ensure that issues are adequately defined
- Linting & debugging
- Refactoring the CLI to be data-driven
- Updating the Bug Reporting protocol in the Contributing.md file
- Fixing the docs
- Clean up references to old session
- remove session reference
- resolve conflict
- clear xcom data when task instance starts
- replace main_session with @provide_session
- Add extras to installation.rst
- Changes to Contributing to reflect more closely the current state of development.
- Modifying README to link to the wiki committer list
- docs: fixes a spelling mistake in default config
- Set killMode to 'control-group' for webservice.service
- Set KillMode to 'control-group' for worker.service
- Linting
- Fix WebHdfsSensor
- Adding more licenses to pass checks
- fixing landscape's config
- [hotfix] typo that made it in master
- [hotfix] fixing landscape requirement detection
- Make testing on hive conditional
- Merge remote-tracking branch 'upstream/master' into minicluster
- Update README.md
- Throwing in a few license to pass the build
- Adding a reqs.txt for landscape.io
- Pointing to a reqs file
- Some linting
- Adding a .landscape.yml file
- badge for PyPI version
- Add license and ignore for sql and csv
- Use correct connection id
- Use correct table name
- Provide data for ci tests
- New badge for showing staleness of reqs
- Removing requirements.txt as it is uni-dimensional
- Make it work on py3
- Remove decode for logging
- Also keep py2 compatible
- More py3 fixes
- Convert to bytes for py3 compat
- Make sure to be py3 compatible
- Use unicodecsv to make it py3 compatible
- Replace tab with spaces Remove unused import
- Merge remote-tracking branch 'upstream/master'
- Support decimal types in MySQL to GCS
- Make sure to write binary as string can be unicode
- Ignore metastore
- More impyla fixes
- Test HivemetaStore if Python 2
- Allow users to set hdfs_namenode_principal in HDFSHook config
- Add tests for Hiveserver2 and fix some issues from impyla
- Merge branch 'impyla' into minicluster
- This patch allows for testing of hive operators and hooks. Sasl is used (NoSasl in connection string is not possible). Tests have been adjusted.
- Treat SKIPPED and SUCCESS the same way when evaluating depends_on_past=True
- fix bigquery hook
- Version cap for gcp_api
- Fix typo when returning VerticaHook
- Adding fernet key to use it as part of stdout commands
- Adding support for ssl parameters.  (picking up from jthomas123)
- More detail in error message.
- Make sure paths don't conflict bc of trailing /
- Change gcs_hook to self.hook
- Refactor remote log read/write and add GCS support
- Only use multipart upload in S3Hook if file is large enough
- Merge branch 'airbnb/master'
- Add GSSAPI SASL to HiveMetaStoreHook.
- Add warning for deprecated setting
- Use kerberos_service_name = 'hive' as standard instead of 'impala'.
- Use GSSAPI instead of KERBEROS and provide backwards compatibility
- ISSUE-1123 Use impyla instead of pyhs2
- Set celery_executor to use queue name as exchange
