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

.. note::
   Release notes for older versions can be found in the versioned documentation.

.. towncrier release notes start

Airflow 3.1.5 (2025-12-12)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- Handle invalid token in JWTRefreshMiddleware (#56904)
- Fix inconsistent Dag hashes when template fields contain unordered dicts (#59091) (#59175)
- Fix assets used only as inlets being incorrectly orphaned (#58986)
- Fix exception when logging stdout with a custom %-format string (#58963)
- Fix backfill max_active_runs race condition with concurrent schedulers (#58935)
- Fix LocalExecutor memory spike by applying ``gc.freeze`` (#58934)
- Fix string to datetime pydantic conversion (#58916)
- Fix deadlines being incorrectly pruned for DAG runs with the same run_id (#58910)
- Fix handling of ``pre-AIP-39`` DAG runs (#58773)
- Mask secrets properly when using deprecated import path (#58726)
- Preserve Asset.extra when using AssetAlias (#58712)
- Fix timeout_after in run_trigger method of TriggerRunner (#58703)
- Fix connection retrieval from secrets backend without conn_type (#58664)
- Fix task retry logic to respect retries for all exit codes (#58478)
- Respect default_args in DAG when set to a "falsy" value (#58396)
- Fix airflow config list output for multi-line values (#58378)
- Fix TriggerDagRunOperator stuck in deferred state with reset_dag_run=True (#58333)
- Fix HITLTrigger params serialization (#58297)
- Fix atomicity issue in SerializedDagModel.write_dag preventing orphaned DAG versions (#58281)
- Mask kwargs when illegal arguments are passed (#58283)
- Fix supervisor communications not reconnecting when using ``dag.test()`` (#58266)
- Fix supervisor communications and logs not reconnecting in task subprocesses (#58263)
- Make pool description optional when patching pools (#58169)
- Fix check_files.py script after source tarball was renamed (#58192)
- Fix db cleanup logging behavior and docstrings (#58523)
- Fix Asset URI normalization for user info without password (#58485)
- UI: Fix object rendering in Human-in-the-Loop (HITL) interface (#58611)
- UI: Fix "Consuming Tasks" section not in asset header (#58060)
- UI: Fix timezone string parsing to use ``dayjs`` correctly (#57880)
- UI: Ensure task instance ``endDate`` is not null (#58435)
- UI: Fix trigger parameter field showing as dict when param.value is null (#58899)
- UI: Remove unnecessary refresh state consumption for DAG header (#58692)
- UI: Fix mobile responsiveness of Dashboard sections (#58853)
- UI: Fix incorrect backfill duration calculation in Grid view (#58816)
- UI: Redact secrets in rendered templates to not expose them in UI (#58772)
- UI: Add fallback value of 1 for number of DAG runs in Grid view (#58735)
- UI: Update refresh token flow (#58649)
- UI: Fix 404 handling with fallback route for invalid URLs (#58629)
- UI: Fix excessive database queries in UI grid endpoint by adding query count guard (#57977, #58632)
- UI: Fix DAG documentation markdown display issue (#58627)
- UI: Fix duration chart duration format (#58564)
- UI: Fix TaskGroup nodes not being properly highlighted when selected in Graph view (#58559)
- UI: Fix tag filter with special characters (#58558)
- UI: Fix group task instance tab memory leak (#58557)
- UI: Fix popup automatically closing when DAG run completes (#58538)
- UI: Fix operator extra links not appearing on failed tasks (#58508)
- UI: Fix TypeError in ``parseStreamingLogContent`` for non-string data (#58399)
- UI: Fix Dag tag order (#58904)

Miscellaneous
^^^^^^^^^^^^^

- Do not remove ``.pyc`` and ``.pyo`` files after building Python (#58947)
- Improve cross-distribution dependency management (#58472)
- Bump glob from 10.4.5 to 10.5.0 in simple auth manager UI (#58463)
- Bump glob in React core UI (#58461)

Doc Only Changes
^^^^^^^^^^^^^^^^
- Fix Chinese (Traditional) translations for trigger-related terminology (#58989)
- Close translation gaps in German (#58971)
- Add missing Polish translations (#58939)
- Clarify that Connection extra JSON masking is keyword-dependent (#58587)
- Add migration guide for Airflow 2 users accessing database in tasks (#57479)
- Update UIAlert import path and usage for v3 (#58891)
- Add clarifying documentation for TaskGroup parameters (#58880)
- Enhance asset extra field documentation (#58830)
- Update mask_secret documentation to use the latest import path (#58534)
- Improve disable_bundle_versioning configuration documentation (#58405)
- Fix documentation for installing from sources (#58373)
- Fix broken link on installing-from-sources page (#58324)
- Add missing DAG run table translations (#58572)


Airflow 3.1.3 (2025-11-13)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Fix Connection & Variable access in API server contexts (plugins, log handlers)(#56583)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously, hooks used in API server contexts (plugins, middlewares, log handlers) would fail with an ``ImportError``
for ``SUPERVISOR_COMMS``, because ``SUPERVISOR_COMMS`` only exists in task runner child processes.

This has been fixed by implementing automatic context detection with three separate secrets backend chains:

**Context Detection:**

1. **Client contexts** (task runner in worker): Detected via ``SUPERVISOR_COMMS`` presence
2. **Server contexts** (API server, scheduler): Explicitly marked with ``_AIRFLOW_PROCESS_CONTEXT=server`` environment variable
3. **Fallback contexts** (supervisor, unknown contexts): Neither marker present, uses minimal safe chain

**Backend Chains:**

- **Client**: ``EnvironmentVariablesBackend`` → ``ExecutionAPISecretsBackend`` (routes to Execution API via SUPERVISOR_COMMS)
- **Server**: ``EnvironmentVariablesBackend`` → ``MetastoreBackend`` (direct database access)
- **Fallback**: ``EnvironmentVariablesBackend`` only (+ external backends from config like AWS Secrets Manager, Vault)

The fallback chain is crucial for supervisor processes (worker-side, before task runner starts) which need to access
external secrets for remote logging setup but should not use ``MetastoreBackend`` (to maintain worker isolation).

**Architecture Benefits:**

- Workers (supervisor + task runner) never use ``MetastoreBackend``, maintaining strict isolation
- External secrets backends (AWS Secrets Manager, Vault, etc.) work in all three contexts
- Supervisor falls back to Execution API client for connections not found in external backends
- API server and scheduler have direct database access for optimal performance

**Impact:**

- Hooks like ``GCSHook``, ``S3Hook`` now work correctly in log handlers and plugins
- No code changes required for existing plugins or hooks
- Workers remain isolated from direct database access (network-level DB blocking fully supported)
- External secrets work everywhere (workers, supervisor, API server)
- Robust handling of unknown contexts with safe minimal chain

See: `#56120 <https://github.com/apache/airflow/issues/56120>`__, `#56583 <https://github.com/apache/airflow/issues/56583>`__, `#51816 <https://github.com/apache/airflow/issues/51816>`__

Remove insecure dag reports API endpoint that executed user code in API server (#56609)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

  The ``/api/v2/dagReports`` endpoint has been removed because it loaded user DAG files directly in the API server process,
  violating Airflow's security architecture. This endpoint was not used in the UI and had no known consumers.
  Use the ``airflow dags report`` CLI command instead for DAG loading reports.

Bug Fixes
^^^^^^^^^
- Fix HITL tasks not properly validating params (#57547) (#58144)
- Fix secrets being exposed in Jinja template rendering error messages (#57467) (#57962)
- UI: Fix slow loading on next run assets page (#58052) (#58064)
- Fix logout not working in airflow-core (#57990) (#58043)
- Fix slow loading on UI [(#57820) (#57856), (#57956) (#57973), (#57957) (#57972),(#57869) (#57882), (#57868) (#57918),(#57624) (#57757)]
- UI: Fix log download to include .txt file extension (#57991) (#58040)
- Fix scheduler using incorrect max_active_runs value from cached DAG (#57619) (#57959)
- Fix database migration failures when XCom contains NaN values (#57866) (#57893)
- Fix incorrect task context in trigger rule scenarios (#57884) (#57892)
- UI: Fix test connection not working (#57811) (#57852)
- Fix worker ``healthcheck`` timeout not respecting worker-timeout CLI option (#57731) (#57854)
- Fix provider hooks not loading when FAB provider is not installed (#57717) (#57830)
- Fix slow API responses for task instances list [(#57645) (#57794), (#57646) (#57664),(#57500) (#57735), (#57549) (#57738), (#57450) (#57736),(#57647) (#57732)]
- Fix task instance errors when tasks are triggered by trigger rules (#57474) (#57786)
- Fix type consistency for extra field in Asset, AssetAlias, and AssetEvent (#57352) (#57728)
- Fix upgrade failures when XCom contains NaN in string values (#57614)

Miscellaneous
^^^^^^^^^^^^^

- UI: Add resize functionality to DAG run and task instance notes (#57897) (#58068)
- Add Taiwan translation for UI (#58121)
- UI: Shorten German translation of Asset in navigation (#57671) (#57690)
- Fix code formatting via ruff preview (#57641) (#57670)
- Remove remnants from unlimited parallelism in local executor (#57579) (#57644)

Doc Only Changes
^^^^^^^^^^^^^^^^

- Add learning from Airflow 3 migration guide (#57989) (#58083)
- Fix duplicate mention of 'DAGs' and 'tasks' in overview documentation (#57524) (#57793)
- Document asset event extra storage behavior (#57727) (#57734)


Airflow 3.1.2 (2025-11-05)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- Fix import error when upgrading structlog to 25.5.0+ (#57335)
- Fix connection retrieval in ``DagProcessorManager`` for bundle initialization (#57459)
- Fix incorrect task instance counts displayed in task group headers (#55670)
- Fix task retry execution after tasks are killed by external signals (#55767)
- Fix triggerer errors after Airflow 2 to 3 migration (#55884)
- Fix tasks unable to access ``triggering_user_name`` context variable (#56193)
- Fix outlet event extra data being empty in task instance success listener callbacks (#57031)
- UI: Fix panel button spacing and alignment issues (#57062)
- UI: Fix broken grid view links for tasks with retries (#57097)
- Fix DAG processor crash when renaming DAG tag case on MySQL (#57113)
- Fix iteration errors when using ``ObjectStoragePath`` (#57156)
- Fix auto-refresh not working on Required Actions page (#57207)
- Fix DAG processor crash by ignoring callbacks from other bundles (#57330)
- Fix asset name text overflow in DAG list view (#57363)
- Fix memory leak caused by repeated SSL context creation in API client (#57374)
- Fix performance issues loading DAG list page with many DAGs (#57444)
- Fix text selection jumping unexpectedly in log viewer (#57453)
- Fix DAG documentation pane not scroll-able when content is too long (#57518)
- Fix incorrect macro listings in template reference documentation (#57529)
- Fix Human-In-The-Loop operators failing when using notifiers (#57551)
- Fix n+1 query issues in XCom API endpoints (#57554)
- Fix n+1 query issues in Event Logs API endpoint (#57558)
- Fix n+1 query to fetch tags in the dags list page (#57570)
- Optimize database query to prevent "Out of sort memory" errors with many DAG versions (#57042)
- Optimize DAG list query for users with limited access (#57460)
- Optimize dynamic DAG updates to avoid loading large serialized DAGs (#57592)
- Reduce serialized DAG size by optimizing callback serialization in ``default_args`` (#57397)

Miscellaneous
^^^^^^^^^^^^^

- UI: Improve global navigation visual design, interaction, and accessibility (#57565)
- UI: Add button to download all task logs at once (#56771)
- UI: Add timestamp column to ``XCom`` viewer and standardize task instance columns (#57447)
- UI: Improve highlighting of selected task instances and edges in grid view (#57560)
- Improve retry logic by migrating from ``retryhttp`` to ``tenacity`` library (#56762)
- Improve exception logging for task instance heartbeat failures (#57179)
- Add ``Content-Type`` header to Task SDK API requests (#57386)
- Log execution API server URL at task startup (#57409)
- Reduce log noise by changing "Connection not found" from error to debug level (#57548)
- Add ``task_display_name`` alias in event log API responses (#57609)
- Improve Pydantic model validation strictness in serialization (#57616)
- Fix systemd service files issues (#57231)

Doc Only Changes
^^^^^^^^^^^^^^^^
- Improve plugin system documentation for clarity and completeness (#57068)
- Improve clarity on api workers recommendation in docs (#57404)
- Fix ``instance_name`` in UI docs (#57523)
- Fix airflow macros list in template document (#57529)

Airflow 3.1.1 (2025-10-27)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
^^^^^^^^^

- Fix execution failures with NULL ``dag_run.conf`` during upgrades from earlier versions (#56729)
- Fix memory leak in remote logging connection cache (#56695)
- Fix DAG processor crash with pre-import module optimization enabled (#56779)
- Fix scheduler crash with email notifications (#56431)
- Fix scheduler crash during 3.0 to 3.1 migration when ``retry_delay`` is ``None`` (#56236)
- Fix task retries executing wrong method after deferred state (#56737)
- Fix retry callbacks not executing for externally killed tasks (#56607)
- Fix custom timetable ``generate_run_id`` not called for manual triggers (#56699)
- Fix ``KeyError`` when accessing ``retry_delay`` on ``MappedOperator`` without explicit value (#56605)
- Fix ``task-sdk`` connection error handling to match ``airflow-core`` behavior (#56653)
- Fix topological sort for Grid View (#56963)
- Fix ``get_ti_count`` and ``get_task_states`` access in callback requests (#56860)
- Fix ``Connection`` or ``Variable`` access in Server context (#56602)
- Fix ``.airflowignore`` order precedence (#56832)
- Fix migration errors for Pydantic 2.12.0 compatibility (#56581)
- Fix: Correctly parse JSON for ``--dag_run_conf`` in ``airflow dags backfill`` CLI (#56599)
- UI: Fix note modal does not change markdown text after change (#56092)
- UI: Fix Grid for cleared runs when tasks were removed (#56297)
- UI: Fix log text selection contrast in light mode (#56893)
- UI: Fix Advanced Search button overlap in DAG List View (#56777)
- UI: Fix view for many DAG tags (#55604)
- UI: Fix asset name text overflow in DAGs list view (#55914)
- UI: Fix auto refresh when only 1 dag run is running (#56649)
- UI: Fix UI keeps poking pools API when no permission (#56626)
- UI: Fix multi-line drag selection in task log view (#56300)
- UI: Fix task named ``'root'`` causes blue screen on hover (#56926)
- UI: Fix cron expression display for ``Day-of-Month`` and ``Day-of-Week`` conflicts (#56255)
- UI: Fix Grid view performance issues with ``SerializedDagModel`` query optimization (#56938)
- Fix: Gracefully handle FastAPI plugins with empty ``url_prefix`` (#55262)
- Fix: Allow mapped tasks to accept zero-length inputs on rerun (#56162)
- Fix: Enable API to clear task instances by specifying map indexes (#56945)
- Fix: Add ``max_retry_delay`` to ``MappedOperator`` model (#56951)
- Fix: Use name passed to ``@asset`` decorator when fetching the asset (#56611)
- UI: Add English as a fallback locale (#56934)

Miscellaneous
^^^^^^^^^^^^^

- Add Greek UI translation (#56724)
- Add Thai UI translation (#56946)
- Add Polish translations (#56825)
- Close German translation gaps for full UI translation (#56981)
- Fix Hebrew typo in translations (#56168)
- Improve DAG and task missing error handling in callbacks (#56725)
- Improve UI retry strategy on client errors (#56638)
- Improve get dag grid structure endpoint speed (#56937)
- Optimize grid structure query with ``DISTINCT`` for ``dag_version_id`` lookup (#56565)
- Add configurable timeout for Execution API requests (#56969)
- Prevent unnecessary kubernetes client imports in workers (#56692)
- Lazy import ``PodGenerator`` for deserialization (#56733)
- Serialize pydantic models in json mode for JSON serialization compatibility (#56939)
- Update authentication to handle JWT token in backend (#56677)
- Update bulk API permission check to handle ``action_on_existence`` (#56672)
- Migrate ``CreateAssetEventsBody`` to Pydantic v2 ``ConfigDict`` (#56772)
- Restore timetable ``active_runs_limit`` check (#56922)
- Add ``is_favorite`` to UI dags list (#56341)
- Add ``executor``, ``hostname``, and ``queue`` columns to ``TaskInstances`` page (#55922)
- Add resize function for DAG Documentation (#56344)
- Add optional pending dag runs check to auto refresh (#56648)
- Add auto refresh to backfill banner (#56774)
- UI: Add Expand/Collapse all to ``XComs`` page (#56285)
- UI: Update recent runs bar chart and improve responsiveness (#56314)
- UI: Update duration format to show milliseconds (#56961)
- UI: Modify min width for task names in grid view (#56952)
- UI: Use Task Display Name in Graph if existing (#56511)
- UI: Use Task Display Name in Grid if existing (#56410)
- UI: Use TI duration from database instead of UI calculated (#56329)
- UI: Make DAG Run ID visible in DAG Header Card (#56409)
- UI: Modify calendar cell colors (#56161)
- UI: Modify log highlight color (#56894)
- UI: Fix show appropriate time units in grid view (#56414)
- UI: Reduce default columns of DAG Run and Task Instance lists (#55968)
- UI: Add expand and collapse functionality for task groups (#56334)
- UI: Avoid using rem for icons for Safari compatibility (#56304)
- UI: Add ANSI support to log viewer (#56721)
- UI: Support Dynamic UI Alerts (#56259)
- UI: Disable Gantt view by default (#56242)
- UI: Use welcome on dashboard instead of airflow (#56074)
- UI: Improve clipboard button visibility with hover effect (#56484)
- UI: Allow sub-pages in React UI plugins (#56485)
- Include task instance id in log printed by supervisor (#56383)
- Emit log stream stopped warning as ``ndjson`` (#56480)
- Detect interactive terminal to set colored logging with override env variable support (#56157)
- Add back deprecation warning for ``sla_miss_callback`` (#56127)
- Move ``natsort`` dependency to ``airflow-core`` (#56582)
- Added missing ``babel`` dependency in Task SDK (#56592)
- Remove unused ``dagReports`` API endpoint (#56621)

Doc Only Changes
^^^^^^^^^^^^^^^^

- Improve API sort documentation (#56617)
- Improve API doc for ordering query param (#55988)
- Add Audit Logs detailed documentation (#56719)
- Update serializer document to reflect latest changes in codebase (#56857)
- Update ASF logos in documentation to the new Oak logo (#56601)
- Enhance ``triggering_asset_event`` retrieval documentation in DAGs (#56957)
- Remove self-reference in best practices documentation (#56111)
- Fix supported Python versions in README (#56734)

Airflow 3.1.0 (2025-09-25)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Human in the Loop (HITL)
""""""""""""""""""""""""

Airflow 3.1 introduces :doc:`Human-in-the-Loop (HITL) </tutorial/hitl>` functionality that enables
workflows to pause and wait for human decision-making. This powerful feature is particularly valuable for
AI/ML workflows, content moderation, and approval processes where human judgment is essential.

HITL tasks pause execution in a ``deferred`` state while waiting for human input via the Airflow UI. Users
with appropriate roles can see pending tasks, review context (including ``XCom`` data and ``DAG`` parameters), and
complete actions through intuitive web forms. The feature also supports API-driven interactions for custom
UIs and notification integration.

For detailed usage instructions, see :doc:`/tutorial/hitl`.

**Note**: HITL operators require ``apache-airflow-providers-standard`` package and Airflow 3.1+.

Task SDK Decoupling for Independent Upgrades
"""""""""""""""""""""""""""""""""""""""""""""

Airflow 3.1 advances the decoupling of the Task SDK from Airflow Core through
improved DAG serialization with versioned contracts. While complete code separation is planned for Airflow 3.2.0,
the serialization foundation enables independent upgrades when components are deployed separately.

**For DAG Authors**: Import constructs from ``airflow.sdk`` namespace:

- ``from airflow.sdk import DAG, task, asset``
- Access to latest authoring features with forward compatibility
- Reduced dependency on server-side Airflow versions

**For Platform Teams**: Foundation for independent upgrades:

- Schema compliance ensures compatibility across versions
- Deployment flexibility when components are separated
- Reduced coordination overhead between development and operations teams

For technical details on the serialization contract, see :doc:`/administration-and-deployment/dag-serialization`.

Deadline Alerts
"""""""""""""""

Deadline Alerts provide proactive monitoring for DAG execution by automatically triggering notifications
when time thresholds are exceeded. This helps ensure SLA compliance and timely completion of critical workflows.

Configure deadline monitoring by specifying:

- **Reference point**: Choose from DAG run queued time, logical date, or fixed datetime
- **Interval**: Time threshold relative to the reference point (positive or negative)
- **Callback**: Response action using Airflow Notifiers or custom functions

Example use cases:

- Alert if a daily ETL hasn't completed 1 hour after its scheduled time
- Notify stakeholders 30 minutes before a critical deadline
- Escalate when resource-constrained DAGs remain queued too long

**Current Limitations**: Deadline Alerts currently support only asynchronous callbacks (``AsyncCallback``).
Support for synchronous callbacks (``SyncCallback``) is planned for a future release.

For configuration details and examples, see :doc:`/howto/deadline-alerts`.

.. warning::

  Deadline Alerts are experimental in 3.1 and may change in future versions based on user feedback.

UI Internationalization
"""""""""""""""""""""""

Airflow 3.1 delivers comprehensive internationalization (``i18n``) support, making the web interface
accessible to users worldwide. The React-based UI now supports 17 languages with robust translation infrastructure.

**Supported Languages**:

- Arabic
- Catalan
- Dutch
- English
- French
- German
- Hebrew
- Hindi
- Hungarian
- Italian
- Korean
- Polish
- Portuguese
- Simplified Chinese
- Spanish
- Traditional Chinese
- Turkish

The translation system includes automated completeness checking and clear contribution guidelines for community translators.

React Plugin System (AIP-68)
"""""""""""""""""""""""""""""

Airflow 3.1 introduces a modern plugin architecture enabling rich integrations through React components and
external views. This extensibility framework allows organizations to embed custom dashboards,
monitoring tools, and domain-specific interfaces directly within the Airflow UI.

**New Plugin Capabilities**:

- **React Apps**: Full-featured React applications integrated into Airflow navigation
- **External Views**: Embed external web applications via iframe with seamless authentication
- **Dashboard Integration**: Custom widgets and panels for operational dashboards
- **Menu Integration**: Add custom navigation items and organize tools logically

**Developer Experience**:

- Hot reloading during development with ``airflow-react-plugin`` dev tools
- TypeScript support and modern React patterns
- Standardized plugin loading and validation
- Comprehensive documentation and boilerplate generation

This plugin system replaces legacy Flask-based approaches with modern web standards, improving performance,
maintainability, and user experience.

For more details and examples, see :doc:`/howto/custom-view-plugin`.

Enhanced UI Views and Filtering
""""""""""""""""""""""""""""""""

Airflow 3.1 brings significant UI improvements including rebuilt Calendar and Gantt chart views for the modern React UI,
comprehensive filtering capabilities, and a refreshed visual design system.

**Visual Design Improvements**

The UI now features an updated color palette leveraging Chakra UI semantic tokens, providing better consistency,
accessibility, and theme support across the interface. This modernization improves readability and creates
a more cohesive visual experience throughout Airflow.

**Rebuilt Views and Enhanced Filtering**

The Calendar and Gantt views from Airflow 2.x have been rebuilt for the modern React UI, along with enhanced
filtering capabilities across all views. These improvements provide better performance and a more consistent
user experience with the rest of the modern Airflow interface.

**DAG Dashboard Organization**

Users can now pin and favorite DAGs for better dashboard organization, making it easier to find and prioritize
frequently used workflows. This feature is particularly valuable for teams managing large numbers of DAGs,
providing quick access to critical workflows without searching through extensive DAG lists.

Inference Execution (Synchronous DAGs)
""""""""""""""""""""""""""""""""""""""

Airflow 3.1 introduces a new streaming API endpoint that allows applications to watch DAG runs until completion,
enabling more responsive integration patterns for real-time and inference workflows.

**New Streaming Endpoint**:
The ``/dags/{dag_id}/dagRuns/{dag_run_id}/wait`` endpoint repeatedly emits JSON updates at specified intervals until the DAG run reaches a finished state.

.. code-block:: bash

    # Watch a DAG run with 2-second polling interval, including XCom results
    curl -X GET "http://localhost:8080/api/v2/dags/ml_pipeline/dagRuns/manual_2024_01_15/wait?result=inference_task" \
         -H "Accept: application/x-ndjson"

This enables use cases like:

- **ML Inference Monitoring**: Trigger inference DAGs and wait for completion before returning results
- **Real-time Processing**: Monitor event-driven workflows with immediate response requirements
- **API Integration**: Build responsive services that react to DAG completion without polling
- **Synchronous Workflows**: Create quasi-synchronous behavior for workflows that need immediate feedback

New Trigger Rule: ``ALL_DONE_MIN_ONE_SUCCESS``
""""""""""""""""""""""""""""""""""""""""""""""

``ALL_DONE_MIN_ONE_SUCCESS``: This rule triggers when all upstream tasks are done (success, failed, or skipped) and
at least one has succeeded, filling a gap between existing trigger rules for complex workflow patterns.

Enhanced DAG Processing Visibility
"""""""""""""""""""""""""""""""""""

DAG parsing duration is now exposed in the UI, providing better visibility into DAG processing
performance and helping identify parsing bottlenecks. This information is displayed alongside
other DAG metadata to assist with performance optimization.

Python 3.13 support added & 3.9 support removed
"""""""""""""""""""""""""""""""""""""""""""""""

Support for Python 3.9 has been removed, as it has reached end-of-life.
Airflow 3.1.0 requires Python 3.10, 3.11, 3.12 or 3.13.

Configuration Changes and Cleanup
""""""""""""""""""""""""""""""""""

**Webserver Configuration Reorganization**

Several webserver configuration options have been moved to the ``api`` section for better organization:

- ``[webserver] log_fetch_timeout_sec`` → ``[api] log_fetch_timeout_sec``
- ``[webserver] hide_paused_dags_by_default`` → ``[api] hide_paused_dags_by_default``
- ``[webserver] page_size`` → ``[api] page_size``
- ``[webserver] default_wrap`` → ``[api] default_wrap``
- ``[webserver] require_confirmation_dag_change`` → ``[api] require_confirmation_dag_change``
- ``[webserver] auto_refresh_interval`` → ``[api] auto_refresh_interval``

Unused configuration options have been removed:

- ``[webserver] instance_name_has_markup``
- ``[webserver] warn_deployment_exposure``

**API Server Logging Configuration**

The API server configuration option ``[api] access_logfile`` has been replaced with ``[api] log_config`` to align with uvicorn's logging configuration. The new option accepts a path to a logging configuration file compatible with ``logging.config.fileConfig``, providing more flexible logging configuration.

**Security Improvement: XCom Deserialization**

The ``enable_xcom_deserialize_support`` configuration option has been removed as a security improvement. This option previously allowed deserializing unknown objects in the API, which posed a security risk due to potential remote code execution vulnerabilities when deserializing arbitrary Python objects.

The XCom display improvements now handle showing non-native XComs (like custom objects, Assets, datetime objects) in a human-readable way through safer methods that don't require deserializing unknown objects in the API server. This provides better user experience when viewing XCom data in the Airflow UI while eliminating the security risk.

API Changes
"""""""""""

**Asset API Key Rename**

The ``consuming_dags`` key in asset API responses has been renamed to ``scheduled_dags`` to better reflect its purpose. This key contains only DAGs that use the asset in their ``schedule`` argument, not all DAGs that technically use the asset.

Task SDK Interface Changes
""""""""""""""""""""""""""

**Removed Functions**

The following functions have been removed from the task-sdk (``airflow.sdk.definitions.taskgroup``) and moved to server-side API services:

- ``get_task_group_children_getter``
- ``task_group_to_dict``

These functions are now internal to Airflow's API layer and should not be imported directly by users.

Reduce default API server workers to 1
""""""""""""""""""""""""""""""""""""""

The default number of API server workers (``[api] workers``) has been reduced from ``4`` to ``1``.

With FastAPI, sync code runs in external thread pools, making multiple workers within a single
process less necessary. Additionally, with uvicorn's spawn behavior instead of fork, there is
no shared copy-on-write memory between workers, so horizontal scaling with multiple API server
instances is now the recommended approach for better resource utilization and fault isolation.

A good starting point for the number of workers is to set it to the number of CPU cores available.
If you do have multiple CPU cores available for the API server, consider deploying multiple API
server instances instead of increasing the number of workers.

Airflow now uses `structlog <https://www.structlog.org/en/stable/>`_ everywhere
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Most users should not notice the difference, but it is now possible to emit structured log key/value pairs from tasks.

If your class subclasses ``LoggingMixin`` (which all ``BaseHook`` and ``BaseOperator`` do -- i.e. all hooks and
operators) then ``self.log`` is now a `<structlog logger>`_.

The advantage of using structured logging is that it is much easier to find specific information about log
message, especially when using a central store such as ``OpenSearch``/``Elastic``/``Splunk`` etc.
You don't have to make any changes, but you can now take advantage of this.

.. code-block:: python

    # Inside a Task/Hook etc.

    # Before:
    # self.log.info("Registering adapter %r", item.name)
    # Now:
    self.log.info("Registering adapter", name=item.name)

This will produce a log that (in the UI) will look something like this::

    [2025-09-16 10:36:13] INFO - Registering adapter  name="adapter1"

or in JSON (i.e. the log files on disk)::

    {"timestamp": "2025-09-16T10:36:13Z", "log_level": "info", "event": "Registering adapter", "name": "adapter1"}

You can also use ``structlog`` loggers at the top level of modules etc, and ``stdlib`` both continue to work:

.. code-block:: python

    import logging
    import structlog

    log1 = logging.getLogger(__name__)
    log2 = strcutlog.get_logger(__name__)

    log1.info("Loading something from %s", __name__)
    log2.info("Loading something", source=__name__)

(You can't add arbitrary key/value pairs to ``stdlib``, but the normal ``percent-formatter`` approaches still work fine.)

Serialization Interface Changes
"""""""""""""""""""""""""""""""

The deserializer interface in ``airflow.serialization.serializers`` has changed for improved security.

**Before 3.1.0:**

``def deserialize(classname: str, version: int, data: Any)``

**Starting with 3.1.0:**

``def deserialize(cls: type, version: int, data: Any)``

The class loading is now handled in ``serde.py``, and the deserializer receives the loaded class directly rather than a ``classname`` string.
This update avoids the use of ``import_string`` in the deserializer, making deserialization more secure.

New Features
^^^^^^^^^^^^

- Add Calendar and Gantt chart views to modern React UI with enhanced filtering (#54252, #51667)
- Add Python 3.13 support for Airflow runtime and dependencies (#46891)
- Add ``SQLAlchemy 2.0`` support with various compatibility fixes for ``Python 3.13`` (#52233, #52518, #54940)
- Add support for the ``psycopg3`` postgres driver (#52976)
- Add ability to track & display user who triggers DAG runs (#51738, #53510, #54164, #55112)
- Add toggle for log grouping in task log viewer for better organization (#51146)
- Add tag filtering improvements with Any/All selection options (#51162)
- Add comprehensive filtering for DAG runs, task instances, and audit logs (#53652, #54210, #55082)
- Add ``XCom`` browsing with filtering and improved navigation (#54049)
- Add bulk task instance actions and deletion endpoints (#50443, #50165, #50235)
- Add DAG run deletion functionality through UI (#50368)
- Add test connection button for connection validation (#51055)
- Add hyperlink support for URLs in XCom values (#54288)
- Add pool column to task instances list and improve pool integration (#51185, #51031)
- Add drag-and-drop log grouping and improved log visualization (#51146)
- Add color support for XCom JSON display (#51323)
- Add configuration column to DAG runs page (#51270)
- Add enhanced note visibility and management in task headers (#51764, #54163)
- Introduce React plugin system (AIP-68) for modern UI extensions (#52255)
- Add support for external view plugins via iframe integration (#51003, #51889)
- Add dashboard integration capabilities for custom React apps (#54131, #54144)
- Add comprehensive plugin development tools and documentation (#53643)
- Implement complete HITL operator suite (``HITLOperator``, ``ApprovalOperator``, ``HITLEntryOperator``) for human decision workflows (#52868)
- Add HITL UI integration with role-based access and form handling (#53035)
- Add HITL API endpoints with filtering and query support (#53376, #53923)
- Add HITL utility functions for generating URLs to required actions page (#54827)
- Improve HITL user experience with bug fixes, UI enhancements, and data model consistency (#55463, #55539, #55575, #55546, #55543, #55536, #55535)
- Add ordering and filtering support for HITL details endpoints (#55217)
- Add "No Response Received" required action state (#55149)
- Add operator filter for HITL task instances (#54773)
- Implement deadline alert system for proactive DAG monitoring (AIP-86) (#53951, #53903, #53201, #55086)
- Add configurable reference points and notification callbacks (#50677, #50093)
- Add deadline calculation and tracking in DAG execution lifecycle (#51638, #50925)
- Add comprehensive UI translation support for 16 languages (#51266, #51038, #51219, #50929, #50981, #51793 and more)
- Add right-to-left (RTL) layout support for Arabic and Hebrew (#51376)
- Add language selection interface and browser preference detection (#51369)
- Add translation completeness validation and automated checks (#51166, #51131)
- Add calendar data API endpoints for DAG execution visualization (#52748)
- Add endpoint to watch DAG runs until completion (#51920, #53346)
- Add DAG run ID pattern search functionality (#52437)
- Add multi-sorting capabilities for improved data navigation (#53408)
- Add bulk connection deletion API and UI (#51201)
- Add task group detail pages across DAG runs (#50412, #50309)
- Add asset event tracking with last event timestamps (#50060, #50279)
- Add ``has_import_errors`` filter to Core API GET ``/dags`` endpoint (#54563)
- Add dag_version filter to get_dag_runs endpoint (#54882)
- Add pattern search for event log endpoint (#55114)
- Add dry_run support with consistent audit log handling (#55116)
- Add utility functions for generic filter counting (#54817)
- Add keyboard navigation for Grid view interface (#51784)
- Add improved error handling for plugin import failures (#49643)
- Add plugin validation in ``/plugins`` API with warnings for invalid plugins (#55673)
- Improve accessibility for screen readers and assistive technologies with proper language detection (#55839)
- Add enhanced variable management with upsert operations (#48547)
- Add favorites/pinning support for DAG dashboard organization (#51264)
- Add system theme support with automatic OS preference detection (#52649)
- Add hotkey shortcut to toggle between Grid and Graph views (#54667)
- Add queued DAGs filter button to DAGs page (#55052)
- Add DAG parsing duration visibility in UI (#54752)
- Add owner links support in DAG Header UI for better navigation (#50627)
- Add ``dag_display_name`` aliases for improved API consistency (#50332, #50065, #50014, #49933, #49641)
- Add enhanced search capabilities with SearchParamsKeys constants (#55218)
- Add ALL_DONE_MIN_ONE_SUCCESS trigger rule for flexible task dependencies (#53959)
- Add fail_when_dag_is_paused parameter to TriggerDagRunOperator for better control (#48214)
- Add ``XCom`` validation to prevent empty keys in ``XCom.set()`` and ``XCom.get()`` operations (#46929)
- Add collapsible plugin menu when multiple plugins are present (#55265)
- Add external view plugin categories (admin, browse, docs, user) (#52737)
- Add iframe plugins integration to DAG pages (#52795)
- Add plugin error display in UI with comprehensive error handling (#49643, #49436)
- Add collapsible failed task logs to prevent React error overflow (#54377)
- Add dynamic legend system for calendar view (#55155)
- Add React UI for Edge functionality (#53563)
- Add pending actions display to DAG UI (#55041)
- Add description field for filter parameters (#54903)
- Add Catalan language support to Airflow UI (#55013)
- Add Hungarian language support to Airflow UI (#54716)
- Add map_index validation in categorize_task_instances (#54791)
- Add Grid view UX improvements (#54846)
- Add HITL UX improvements for better user experience (#54990)
- Add async support for Notifiers (AIP-86) (#53831)
- Add filtering capabilities for tasks view (#54484)
- Add asset-based filtering support to DAG API endpoint (#54263)
- Add iframe plugins to navigation (#51706)
- Add RTL (right-to-left) layout support for Arabic and Hebrew (#51376)
- Add test connection button to UI (#51055)
- Add task instance bulk actions endpoint (#50443)
- Add connection bulk deletion functionality (#51201)
- Add pool column to task instances list (#51185)
- Add ``iframe_views`` to backend plugin support (#51003)
- Add keyboard shortcuts to clear and mark state for task instances and DAG runs (#50885)
- Add deadline relationship to DAG runs and deadline model (#50925, #50093)
- Add DAG run deletion UI (#50368)
- Add task instance deletion UI and endpoint (#50235, #50165)
- Switch all airflow logging to structlog (#52651, #55434, #55431, #55638)
- Add Filter Bar to Audit Log (#55487)
- Add Filters UI for Asset View (#54640)
- Update color palette and leverage Chakra semantic tokens (#53981, #55739)
- Improve calendar view UI with enhanced tooltips and visual fixes (#55476)

Bug Fixes
^^^^^^^^^

- Fix DAG list filtering to include ``QUEUED`` runs with null ``start_date`` (#52668)
- Fix XCom deletion failure for mapped task instances through bulk deletion API (#51850)
- Fix XCom deletion failure for mapped task instances (#54954)
- Fix task timeout handling within task SDK (#54089)
- Fix task instance tries API duplicate entries (#50597)
- Fix connection validation and type checking during construction (#54759)
- Fix mapped task instance index display in Task Instances tab (#55363)
- Fix Gantt chart state mismatch with Grid view (#55300)
- Fix Gantt chart status color display issues (#55296)
- Fix XCom mapping for dynamically-mapped task groups (#51556)
- Fix missing ``ti_successes`` and related metrics in Airflow 3.0 Task SDK (#55322)
- Fix bulk operation permissions for connection, pool and variable (#55278)
- Fix ``clearTaskInstances`` API: Restore ``include_past``/``future`` support on UI (#54416)
- Fix migration when XCom has NaN values (#53812)
- Fix HITL related UI schema generated by prek hooks (#55204)
- Fix consistent no-log handling for tasks with try_number=0 in API and UI (#55035)
- Fix timezone conversion in datetime trigger parameters (#54593)
- Fix audit log payload for DAG pause/unpause actions (#55091)
- Fix pushing None as an XCom value (#55080)
- Fix scheduler processing of cleared running tasks stuck in RESTARTING state (#55084)
- Fix XCom deletion failure for mapped task instances (#54954)
- Fix outgoing graph edges should exit opposite of incoming edges (#54789)
- Fix external links in Navigation buttons (#52220)
- Fix Error when viewing DAG details of a no longer configured bundle (#52086)
- Fix compatibility with new numpy and pandas versions (#52071)
- Fix connection recovery from URI when host has protocol (#51953)
- Fix last DAG run not showing on DAG listing (#51115)
- Fix task instance tries API returning duplicate entries (#50597)
- Fix Graph view vanishing and loading issues (#53886, #54756)
- Fix rendered template display formatting for better readability (#53657)
- Fix Grid view expand/collapse button functionality (#54257)
- Fix tooltip visibility and positioning issues (#53913)
- Fix grid keyboard navigation focus management (#54271)
- Fix plugin registration for invalid objects and middleware registration (#55264, #55399)
- Fix external links for plugins with undefined URL routes (#55221)
- Fix language display consistency and flag representation (#51560, #51177)
- Fix RTL layout rendering for Arabic and Hebrew interfaces (#51853)
- Fix graph export cropping when view is partial (#55012)
- Fix log viewer "Toggle Source" to hide only source fields, not all structured log fields (#55474)
- Output on stdout/stderr from within tasks is now filterable in the Sources list in the UI log view (#55508)
- Redact JWT tokens in task logs (#55499)
- Fix grid view to handle long task name (#55332)
- Allow slash characters in Variable keys similar to Airflow 2.x (#55324)
- Fix Grid cache invalidation for multi-run task operations (#55504)
- Fix Gantt chart rendering issues (#55554)
- Fix ``XCom`` access in DAG processor callbacks for notifiers (#55542)
- Fix alignment of arrows in RTL mode for right-to-left languages (#55619)
- Fix connection form extras not inferring correct type in UI (#55492)
- Fix incorrect log timestamps in UI when ``default_timezone`` is not UTC (#54431)
- Fix handling of priority_weight for DAG processor callbacks (#55436)
- Fix pointless requests from Gantt view when there is no Run ID (#55668)
- Ensure filename and ``lineno`` of logger calls are present in Task Logs (#55581)
- Fix DAG disappearing after callback execution in stale detection (#55698)
- Fix DB downgrade to Airflow 2 when fab tables exists (#55738)
- Fix UI stats endpoint causing dashboard loading issues (#55733)
- Fix unintended console output when DAG not found in ``serialized_dag`` table (#54972)
- Fix scheduler handling of orphaned tasks from Airflow 2 during upgrade (#55848)
- Fix logging format to respect existing configuration during upgrade to prevent unexpected log format changes (#55824)
- Fix Grid view crashes when DAG version information is missing (#55771)
- Fix compatibility for custom triggers migrating from Airflow 2.x that use synchronous connection calls (#55799)
- Fix DAG runs triggered from UI incorrectly marked as REST API triggers instead of UI triggers (#54650)
- Fix XCom API responses failing when encountering non-serializable objects by falling back to string representation (#55880)
- Fix asset queue display in UI showing incorrect timestamps for deleted queue events (#54652)
- Fix SQLite database migrations failing due to foreign key constraint handling (#55883)
- Fix DAG deserialization failure when using non-default weight_rule values like 'absolute' (#55906)
- Fix async connection retrieval in triggerer context preventing event loop blocking (#55812)
- Fix Airflow downgrade compatibility by handling serialized DAG format conversion from v3 to v2 (#55975)
- Fix 'All Log Levels' filter not working in task log viewer (#55851)
- Fix Grid view scrollbar overlapping issues on Firefox browser (#55960)
- Fix Gantt chart misalignment with Grid view layout (#55995)
- Fix Grid view task names being extremely collapsed and unreadable when displaying many DAG runs (#55997)
- Fix ``LocalExecutor`` race condition where tasks could start before database state was committed (#56010)

Miscellaneous
^^^^^^^^^^^^^

- Move secrets masker to shared distribution for better modularity (#54449)
- Move email notifications from scheduler to DAG processor for better architecture (#55238)
- Add graph UI load optimization with latest run info endpoint (#53429)
- Optimize UI bundle size by moving translations to dynamic loading (#51735)
- Relocate Task SDK components for improved separation (#55174, #54795)
- Refactor trigger rule utilities and weight rule consolidation (#54797, #53393)
- Remove deprecated Airflow 2.x modules and legacy imports (#50482)
- Clean up unused code and improve module organization (#52176, #52173, #53031)
- Add SQLAlchemy 2.0 CI support for future compatibility (#52233)
- Improve test fixtures and SDK communication testing (#54795, #50603)
- Add translation completeness linting and validation tools (#51166)
- Upgrade to latest versions of important dependencies (#55350)
- Move webserver configuration options to API section (#50693, #50656)
- Improve DAG bundle handling and versioning support (#47592)
- Add database management CLI tools for external database operations (#50657)
- Add comprehensive HITL operator documentation and examples (#54618)
- Add guards for registering middlewares from plugins (#55399)
- Optimize Gantt group expansion with de-bouncing and deferred rendering (#55334)
- Differentiate between triggers and watchers currently running for better visibility (#55376)
- Removed unused config: ``dag_stale_not_seen_duration`` (#55601, #55684)
- Update UI's query client strategy for improved performance (#55528)
- Unify datetime format across the UI for consistency (#55572)
- Mark React Apps as Experimental for Airflow 3.1 release (#55478)
- Improve OOM error messaging for clearer task failure diagnosis (#55602)
- Display responder username for better audit trail in HITL workflows (#55509)
- The constraint file do not contain developer dependencies anymore (#53631)
- Add hyperlinks to ``dag_id`` column in DAG Runs and Task Instances pages for better navigation (#55648)
- Add responsive web design (RWD) support to Grid view (#55745)

Doc Only Changes
^^^^^^^^^^^^^^^^

- Add comprehensive Human-in-the-Loop operator tutorial and examples (#54618)
- Add deadline alerts configuration and usage documentation (#53727)
- Make term Dag consistent in docs task-sdk (#55100)
- Add migration guide for upgrading from legacy SLA functionality to deadline alerts (#55743)
- Add DAG bundles triggerer limitation documentation (#55232)
- Add deadline alerts usage guides and best practices (#53727)
- Remove ``--preview`` flag from ``ruff check`` instructions for Airflow 3 upgrade path (#55516)
- Add documentation for context parameter (#55377)

Airflow 3.0.6 (2025-08-29)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""

- Fix Connection extra field masking errors when connections use masked values (#54780)
- UI: Fix ``TriggerDagRunLink`` broken page when clicking "Triggered DAG" button (#54760)
- Fix remote logging connection availability in Task SDK supervisor when connections are created via UI (#54720)
- Fix ``task_queued_timeout`` not working after first DAG run by properly resetting ``queued_by_job_id`` (#54604)
- Fix DAG version determination to use bundle path and relative fileloc instead of absolute fileloc (#54483)
- Remove Kerberos replay cache (``KRB5CCNAME`` env) when running tasks with user impersonation (#54672)
- Skip additional span-related database queries when tracing is disabled (#54626)
- Fix ``max_active_tasks`` persisting after removal from DAG code (#54639)
- UI: Automatically switch to the triggered DAG run in Graph/Grid view when manually triggering a DAG run (#54336)
- UI: Fix "Maximum update depth exceeded" errors in Task Log Preview by filtering out empty log entries (#54628)
- Fix custom logging configuration failures preventing triggerer and scheduler startup with simple module paths (#54686)
- Fix MySQL UUID generation in task_instance migration (#54814)
- Only redirect on the dag detail page (#54921)
- Fix local executor task execution (#54922)

Miscellaneous
"""""""""""""

- Add logging when triggerer reaches maximum trigger capacity for better observability (#54549)
- Point deprecation warning in Variable methods to specific alternatives (#54871)
- Point deprecation warning in Connection method to specific alternatives (#54872)
- Bump ``axios`` UI dependency from ``1.8.0`` to ``1.11.0`` (#54733)
- Bump ``pluggy`` to ``1.6.0`` (#54728, #54730)

Doc Only Changes
""""""""""""""""

- Fix broken link for Listener spec (#54535)
- Remove experimental status from ``get_parsing_context`` function (#54802)
- Correct Trigger-Form UI documentation for current Airflow 3 features (#54806)
- Add backfill through UI to docs (#54910)

Airflow 3.0.5 (2025-08-20)
--------------------------

This release has been yanked.

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""

- UI: Fix JSON field display in dark mode when using system OS theme detection (#54260)
- Restore proper DAG callback execution context (#53684)
- Restore ``get_previous_dagrun`` functionality for task context (#53655)
- Fix scheduler crashes with ``DetachedInstanceError`` when processing executor events (#54334)
- Fix ``DetachedInstanceError`` when accessing ``DagRun.created_dag_version`` (#54362)
- Fix Task SDK to respect custom default queue configuration from config settings (#52786)
- Fix: Cannot edit or delete pools with ``"/"`` in the name in the UI  (#54268)
- Fix: Validate and handle invalid ``extra`` field in connections UI and API (#53963, #54034, #54235)
- Fix: Apply DAG permission filter to dashboard (#54215)
- Fix API validation error when DAG runs have bundle_version but no created_dag_version (#54010)
- Fix task configuration defaults not being read from configuration settings (#52871)
- Fix duplicate task group prefixes in task IDs when unmapping ``MappedOperators`` within ``TaskGroups`` (#53532)
- Fix custom ``XCom`` backends not being used when ``BaseXCom.get_all()`` is called (#53814)
- Fix ``xcom_pull`` ignoring ``include_prior_dates`` parameter when ``map_indexes`` is not specified (#53809)
- Allow setting and deleting Variables and XComs from triggers (#53514)
- Fix ``AttributeError`` when reading logs for previous task attempts with ``TaskInstanceHistory`` (#54114)
- Skip database queries for spans and metrics when tracing/metrics are disabled (#54404)
- UI: Fix Graph view edge rendering issues for nested task groups with excessive bends and misalignment (#54412)
- Allow database downgrade from Airflow 3.x to 2.11 (#54399, #54508)
- Reduce excessive warning logs when multiple deferred tasks are queued in triggerer (#54441)
- Fix log retrieval failures for in-progress tasks by properly configuring JWT authentication (#54444)
- Fix DAG import errors for invalid access control roles to persist consistently in UI (#54432)
- Fix task failure callbacks missing ``end_date`` and ``duration`` by populating ``TaskInstance`` data before invoking callbacks (#54458)
- Fix task retry overflow errors when calculating next retry datetime by capping delay to maximum configured value (#54460)
- Add missing ordering to ``AssetEvent`` queries in scheduler to maintain consistent event processing order (#52231)
- Fix XCom lookup failures in nested mapped task groups by correctly resolving ``map_index`` for upstream tasks (#54249)
- UI: Fix task name indentation in Graph view for deeply nested task groups beyond 5 levels (#54419)
- Run failure callbacks for task instances that get stuck in queued state and fail after requeue attempts (#54401)
- Make secrets masking work when connections are loaded from secrets backends (#54574, #54612)

Miscellaneous
"""""""""""""

- Set minimum version for ``common.messaging`` to ``1.0.3`` (#54176)
- Add IP validation to example_dag_decorator DAG (#54208)

Doc Only Changes
""""""""""""""""

- Fix doc redirects for operators moved to the standard provider (#54251)
- Add FAQ entry about testing connections and "Canary" Dag (#54151)
- Add note about ruff rules and preview flag (#53331)
- Fix broken link in advanced logging config docs (#53460)
- Update dag bundles docs; add s3, fix git classpath (#53473)
- Fix example to use proper task context and logging instead of ``dag.log`` (#54463)
- Improve documentation navigation by hiding Public Interface subsections from sidebar while preserving page links (#54465)

Airflow 3.0.4 (2025-08-08)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""

- Fix scheduler heartbeat timeout failures with intermittent ``DetachedInstanceError`` crashes (#53838) (#53858)
- Fix connection editing where sensitive fields like passwords and extras were lost when updating connections (#53943) (#53973)
- Fix BaseOperator ``on_kill`` functionality not working when tasks are killed externally in TaskSDK (#53718) (#53832)
- Fix TaskInstance notes not refreshing automatically without manual page refresh (#53307) (#54025)
- Fix invalid execution API URLs causing failures in task supervisor (#53082) (#53518)
- Fix task failure callbacks not running on DAG Processor when tasks are externally killed (#53058) (#53143)
- Fix ``task_success_overtime`` configuration option not being configurable (#53342) (#53351)
- Fix CSS warning for nth-child selector (#53982) (#54000)
- Fix DAG filtering where "all" option did not show all DAGs as expected (#53656) (#53672)
- Fix accordion child contents not being visible when content overflows (#53595) (#53602)
- Fix navbar positioning for anchor calculations (#52016) (#53581)
- Fix DagBag safe mode configuration resolution in DAG processor (#52694) (#53507)
- Fix large log reading causing out-of-memory issues in API server (#49470) (#53167)
- Fix connection exceptions consistency between Airflow 2.x and 3.x (#52968) (#53093)
- Remove unnecessary ``group_by`` clause in event logs query for performance (#53733) (#53807)
- Allow remote logging providers to load connections from API Server (#53719) (#53761)
- Add certificate support for API server client communication with self-signed certificates (#53574) (#53793)
- Respect ``apps`` flags for API server command configuration (#52929) (#53775)
- Skip empty DAG run configuration rows and set statement timeout (#50788) (#53619)
- Remove incorrect warning for ``BaseOperator.executor`` attribute (#53496) (#53519)
- Add back DAG parsing pre-import optimization for improved performance (#50371) (#52698)
- Flexible form use ReactMarkdown instead of default Markdown component (#54032) (#54040)
- Unconditionally disable ``start_from_trigger`` functionality (#53744) (#53750)
- Serialize NaN and infinity values to string (#53835) (#53844)
- Make log redaction safer in edge case when redaction has an error (#54046) (#54048)
- Flexible form use ReactMarkdown instead of default Markdown component (#54032) (#54040)
- Fix inconsistent casing in UI of decorated tasks (#54056) (#54092)

Miscellaneous
"""""""""""""

- Fix AIRFLOW_API_APPS constant in API server command (#54007) (#54012)
- Add deprecation notice for using Connection from models in favor of SDK approach (#53594) (#53621)
- Remove remnants of ``~=`` used in requires-python configuration (#52985) (#52987)
- Remove upper-binding for "python-requires" specification (#52980) (#52984)
- Update GitPython from 3.1.44 to 3.1.45 (#53725) (#53731)(#53724) (#53732)

Doc Only Changes
""""""""""""""""

- Update DAG author documentation to use "DAG author" terminology (#53857) (#53950)
- Update architecture diagrams labels from "Webserver(s)" to "API Server(s)" (#53917) (#54020)
- Remove bold formatting for Public Interface documentation in Airflow 3.0+ (#53955) (#53964)
- Add user-facing documentation for running separate Task Execution API server (#53789) (#53794)
- Add documentation for self-signed certificate configuration (#53788) (#53792)
- Update systemd unit files and documentation for Airflow 3.0 compatibility (#52294) (#53609)
- Update public interface documentation to reflect airflow.sdk and AIP-72 changes (#52197) (#53117)
- Update BaseOperator documentation string for clarity (#53403) (#53404)
- Remove extra slash from endpoint URL formatting (#53755) (#53764)
- Clarify our security model for sensitive connection information (#54088) (#54100)

Airflow 3.0.3 (2025-07-14)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""

- Fix task execution failures with large data by improving internal communication protocol (#51924, #53194)
- Fix reschedule sensors failing after multiple re-queue attempts over long periods (#52706)
- Improve ``xcom_pull`` to cover different scenarios for mapped tasks (#51568)
- Fix connection retrieval failures in triggerer when schema field is used (#52691)
- Add back user impersonation (``run_as_user``) support for task execution (#51780)
- Fix DAG version not updating when bundle name changes without DAG structure changes (#51939)
- Add back ``exception`` to context for task callbacks (#52066)
- Fix task log retrieval for retry attempts showing incorrect logs (#51592)
- Fix data interval handling for DAGs created before AIP-39 during serialization (#51913)
- Fix lingering task supervisors when ``EOF`` is missed (#51180) (#51970)
- Persist ``EventsTimetable``'s description during serialization (#51926)
- Delete import error when a dag bundle becomes inactive (#51921)
- Cleanup import errors during DB migration (#51919)
- Fix ``EOF`` detection of subprocesses in Dag Processor (#51895)
- Stop streaming task logs if end of log mark is missing (#51482)
- Allow more empty loops before stopping log streaming (#52624)
- Fix Jinja2 Template deep copy error with ``dag.test`` (#51673)
- Explicitly close log file descriptor in the supervise function (#51654)
- Improve structured logging format and layout (#51567) (#51626)
- Use Connection Hook Names for Dropdown instead of connection IDs (#51613)
- Add back config setting to control exposing stacktrace (#51617)
- Fix task level alias resolution in structure endpoint (#51579)
- Fix backfill creation to include DAG run configuration from form (#51584)
- Fix structure edges in API responses (#51489)
- Make ``dag.test`` consistent with ``airflow dags test`` CLI command (#51476)
- Fix downstream asset attachment at task level in structure endpoint (#51425)
- Fix Task Instance ``No Status`` Filter (#52154)
- UI: Fix backfill creation to respect run backwards setting from form (#52168)
- UI: Set downstream option to default on task instance clear (#52246)
- UI: Enable iframe script execution (#52568)
- UI: Fix DAG tags filter not showing all tags in UI when tags are greater than 50 (#52714)
- UI: Add real-time clock updates to timezone selector (#52414)
- Improve Grid view performance and responsiveness with optimized data loading (#52718,#52822,#52919)
- Fix editing connection with sensitive extra field (#52445)
- Fix archival for cascading deletes by archiving dependent tables first (#51952)
- Fix whitespace handling in DAG owners parsing for multiple owners (#52221)
- Fix SQLite migration from 2.7.0 to 3.0.0 (#51431)
- Fix http exception when ti not found for extra links API (#51465)
- Fix Starting from Trigger when using ``MappedOperator`` (#52681)
- Add ti information to re-queue logs (#49995)
- Task SDK: Fix ``AssetEventOperations.get`` to use ``alias_name`` when specified (#52324)
- Ensure trigger kwargs are properly deserialized during trigger execution (#52721)
- Fixing bad cadwyn migration for upstream map indexes (#52797)
- Run trigger expansion logic only when ``start_from_trigger`` is True (#52873)
- Fix example dag ``example_external_task_parent_deferrable.py`` imports (#52957)
- Fixes pagination in DAG run lists (#52989)
- Fix db downgrade check condition (#53005)
- Fix log viewing for skipped task (#53028,#53101)
- Fixes Grid view refresh after user actions (#53086)
- Fix ``no_status`` and ``duration`` for grid summaries (#53092)
- Fix ``ti.log_url`` not in Task Context (#50376)
- Fix XCom data deserialization when using ``XCom.get_all()`` method (#53102)

Miscellaneous
"""""""""""""

- Update ``connections_test`` CLI to use Connection instead of BaseHook (#51834) (#51917)
- Fix table pagination when DAG filtering changes (#51795)
- UI: Move asset events to its own tab (#51655)
- Exclude ``libcst`` 1.8.1 for Python 3.9 (#51609)
- UI: Implement navigation on bar click (#50416)
- Reduce unnecessary logging when retrieving connections and variables (#51826)

Doc Only Changes
""""""""""""""""

- Add note about payload size considerations in API docs (#51768)
- Enhance ENV vars and conns visibility docs (#52026)
- Add http-only warning when running behind proxy in documentation (#52699)
- Publish separate docs for Task SDK (#52682)
- Streamline Taskflow examples and link to core tutorial (#52709)
- Refresh Public Interface & align how-to guides for Airflow 3.0+ (#53011)

Airflow 3.0.2 (2025-06-10)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""

- Fix memory leak in dag-processor (#50558)
- Add back invalid inlet and outlet check before running tasks (#50773)
- Implement slice on LazyXComSequence to allow filtering items from a mapped task(#50117)
- Fix execution API server URL handling for relative paths in KE (#51183)
- Add log lookup exception for Empty operator subtypes (#50325)
- Increase the max zoom on the graph view to make it easier to see small dags on big monitor screens (#50772)
- Fix timezone selection and dashboard layout (#50463)
- Creating backfill for a dag is affecting other dags (#50577)
- Fix next asset schedule and dag card UX (#50271)
- Add bundle path to ``sys.path`` in task runner (#51318)
- Add bundle path to ``sys.path`` in dag processor (#50385)
- Prevent CPU spike in task supervisor when heartbeat timeout exceeded (#51023)
- Fix Airflow Connection Form widget error (#51168)
- Add backwards compatibility shim and deprecation warning for EmailOperator (#51004)
- Handle ``SIGSEGV`` signals during DAG file imports (#51171)
- Fix deferred task resumption in ``dag.test()`` (#51182)
- Fix get dags query to not have join explosion (#50984)
- Ensure Logical date is populated correctly in Context vars (#50898)
- Mask variable values in task logs only if the variable key is sensitive (#50775)
- Mask secrets when retrieving variables from secrets backend (#50895)
- Deserialize should work while retrieving variables with secrets backend (#50889)
- Fix XCom deserialization for mapped tasks with custom backend (#50687)
- Support macros defined via plugins in Airflow 3 (#50642)
- Fix Pydantic ``ForwardRef`` error by reordering discriminated union definitions (#50688)
- Adding backwards compatibility shim for ``BaseNotifier`` (#50340)
- Use latest bundle version when clearing / re-running dag (#50040)
- Handle ``upstream_mapped_index`` when xcom access is needed (#50641)
- Remove unnecessary breaking flag in config command (#50781)
- Do not flood worker logs with secrets backend loading logs (#50581)
- Persist table sorting preferences across sessions using local storage (#50720)
- Fixed patch_task_instance API endpoint to support task instance summaries and task groups (#50550)
- Fixed bulk API schemas to improve OpenAPI compatibility and client generation (#50852)
- Fixed variable API endpoints to support keys containing slashes (#50841)
- Restored backward compatibility for the ``/run`` API endpoint for older Task SDK clients
- Fixed dropdown overflow and error text styling in ``FlexibleForm`` component (#50845)
- Corrected DAG tag rendering to display ``+1 more`` when tags exceed the display limit by one (#50669)
- Fix permission check on the ui config endpoint (#50564)
- Fix ``default_args`` handling in operator ``.partial()`` to prevent ``TypeError`` when unused keys are present (#50525)
- DAG Processor: Fix index to sort by last parsing duration (#50388)
- UI: Fix border overlap issue in the Events page (#50453)
- Fix ``airflow tasks clear`` command (#49631)
- Restored support for ``--local`` flag in ``dag list`` and ``dag list-import-errors`` CLI commands (#49380)
- CLI: Exclude example dags when a bundle is passed (#50401)
- Fix CLI export to handle stdout without file descriptors (#50328)
- Fix ``DagProcessor`` stats log to show the correct parse duration (#50316)
- Fix OpenAPI schema for ``get_log`` API (#50547)
- Remove ``logical_date`` check when validating inlets and outlets (#51464)
- Guard ``ti`` update state and set task to fail if exception encountered (#51295)
- Fix task log URL generation with various ``base_url`` formats (#55699)

Miscellaneous
"""""""""""""

- UI: Implement navigation on bar click (#50416)
- UI: Always Show Trends count in Dag Overview (#50183)
- UI: Add basic json check to variable value
- Remove filtering by last dag run state in patch dags endpoint (#51347)
- Ensure that both public and ui dags endpoints map to DagService (#51226)
- Refresh Dag details page on new run (#51173)
- Log fallback to None when no XCom value is found (#51285)
- Move ``example_dags`` in standard provider to ``example_dags`` in sources (#51275)
- Bring back "standard" example dags to the ``airflow-core`` package (#51192)
- Faster note on grid endpoint (#51247)
- Port ``task.test`` to Task SDK (#50827)
- Port ``dag.test`` to Task SDK (#50300,#50419)
- Port ``ti.run`` to Task SDK execution path (#50141)
- Support running ``airflow dags test`` from local files (#50420)
- Move macros to task SDK ``execution_time`` module (#50940)
- Add a link to the Airflow logo in Nav (#50304)
- UI: Bump minor and patch package json dependencies (#50298)
- Added a direct link to the latest DAG run in the DAG header (#51119,#51148)
- Fetch only the most recent ``dagrun`` value for list display (#50834)
- Move ``secret_key`` config to ``api`` section (#50839)
- Move various ``webserver`` configs to ``fab`` provider (#50774,#50269,#50208,#50896)
- Make ``dag_run`` nullable in Details page (#50719)
- Rename Operation IDs for task instance endpoints to include map indexes (#49608)
- Update default sort for connections and dags (#50600)
- Raise exception if downgrade can't proceed due to no ``ab_user`` table (#50343)
- Enable JSON serialization for variables created via the bulk API (#51057)
- Always display the backfill option in the UI; enable it only for DAGs with a defined schedule (#50969)
- Optimized DAG header to fetch only the most recent DAG run for improved performance (#50767)
- Add ``owner_links`` field to ``DAGDetailsResponse`` for enhanced owner metadata in the API (#50557)
- UI: Move map index column to be in line with other columns when viewing a summary mapped tasks (#50302)
- Separate configurations for colorized and json logs in Task SDK / Celery Executor (#51082)
- Enhanced task log viewer with virtualized rendering for improved performance on large logs (#50746)

Doc Only Changes
""""""""""""""""

- Add dates for Limited Maintenance & EOL for Airflow 2.x (#50794)
- Add Apache Airflow setup instructions for Apple Silicon (#50179)
- Update recommendation for upgrade path to airflow 3 (#50318)
- Add "disappearing DAGs" section on FAQ doc (#49987)
- Update Airflow 3 migration guide with step about custom operators (#50871) (#50948)
- Use ``AssetAlias`` for alias in Asset ``Metadata`` example (#50768)
- Do not use outdated ``schedule_interval`` in tutorial dags (#50947)
- Add Airflow Version in Page Title (#50358)
- Fix callbacks docs (#50377)
- Updating operator extra links doc (#50197)
- Prune old Airflow versions from release notes (#50860)
- Fix types in config templates reference (#50792)
- Fix wrong import for ``PythonOperator`` in tutorial dag (#50962)
- Better structure of extras documentation (#50495)

Airflow 3.0.1 (2025-05-12)
--------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

No significant changes.

Bug Fixes
"""""""""

- Improves the handling of value masking when setting Airflow variables for enhanced security (#43123)
- Make entire task box clickable to select the task (#49299)
- Vertically align task log header components in full screen mode (#49569)
- Remove ``dag_code`` records with no serialized dag (#49478)
- Clear out the ``dag_code`` and ``serialized_dag`` tables on 3.0 upgrade (#49563)
- Remove extra slash so that the runs tab is selected (#49600)
- Null out the ``scheduler_interval`` field on downgrade (#49583)
- Logout functionality should respect ``base_url`` in api server (#49545)
- Fix bug with showing invalid credentials on Login UI (#49556)
- Fix Dag Code text selection when dark mode is enabled (#49649)
- Bugfix: ``max_active_tis_per_dag`` is not respected by dynamically mapped tasks (#49708)
- Fix infinite redirect caused by mistakenly setting token cookie as secure (#49721)
- Better handle safe url redirects in login form for ``SimpleAuthManager`` (#49697)(#49866)
- API: Add missing ``bundle_version`` to DagRun response (#49726)
- Display bundle version in Dag details tab (#49787)
- Fix gcp remote log module import in airflow local settings (#49788)
- Bugfix: Grid view stops loading when there is a pending task to be expanded (#49772)
- Treat single ``task_ids`` in ``xcom_pull`` the same as multiple when provided as part of a list (#49692)
- UI: Auto refresh Home page stats (#49830)
- UI: Error alert overflows out of the alert box (#49880)
- Show backfill banner after creating a new backfill (#49666)
- Mark ``DAGModel`` stale and associate bundle on import errors to aid migration from 2.10.5 (#49769)
- Improve detection and handling of timed out DAG processor processes (#49868)
- Fix editing port for connections (#50002)
- Improve & Fix grid endpoint response time (#49969)
- Update time duration format (#49914)
- Fix Dashboard overflow and handle no status tasks (#49964)
- Fix timezone setting for logical date input on Trigger Run form (#49662)
- Help ``pip`` with avoiding resolution too deep issues in Python 3.12 (#49853)
- Bugfix: backfill dry run does not use same timezone as create backfill (#49911)
- Fix Edit Connection when connection is imported (#49989)
- Bugfix: Filtering items from a mapped task is broken (#50011)
- Fix Dashboard for queued DagRuns (#49961)
- Fix backwards-compat import path for ``BashSensor`` (#49935)
- Apply task group sorting based on webserver config in grid structure response (#49418)
- Render custom ``map_index_template`` on task completion (#49809)
- Fix ``ContinuousTimetable`` false triggering when last run ends in future (#45175)
- Make Trigger Dag form warning more obvious (#49981)
- Restore task hover and selection indicators in the Grid view (#50050)
- Fix datetime validation for backfills (#50116)
- Fix duration charts (#50094)
- Fix DAG node selections (#50095)
- UI: Fix date range field alignment (#50086)
- Add auto-refresh for ``Stats`` (#50088)
- UI: Fixes validation error and adds error indicator for Params form (#50127)
- fix: wrap overflowing texts of asset events (#50173)
- Add audit log extra to table and improve UX (#50100)
- Handle map indexes for Mapped ``TaskGroup`` (#49996)
- Do not use introspection in migration to fix offline SQL generation (#49873)
- Fix operator extra links for mapped tasks (#50238)
- Fix backfill form (#50249)(#50243)
- UI: Fix operator overflow in graph (#50252)
- UI: Pass ``mapIndex`` to clear the relevant task instances. (#50256)
- Fix markdown rendering on dag docs (#50142)

Miscellaneous
"""""""""""""

- Add ``STRAIGHT_JOIN`` prefix for MySQL query optimization in ``get_sorted_triggers`` (#46303)
- Ensure ``sqlalchemy[asyncio]`` extra is in core deps (#49452)
- Remove unused constant ``HANDLER_SUPPORTS_TRIGGERER`` (#49370)
- Remove sort indicators on XCom table to avoid confusion (#49547)
- Remove ``gitpython`` as a core dependency (#49537)
- Bump ``@babel/runtime`` from ``7.26.0`` to ``7.27.0`` (#49479)
- Add backwards compatibility shim for ``get_current_context`` (#49630)
- AIP-38: enhance layout for ``RunBackfillForm`` (#49609)
- AIP-38: merge Backfill and Trigger Dag Run (#49490)
- Add count to Stats Cards in Dashboard (#49519)
- Add auto-refresh to health section for live updates. (#49645)
- Tweak Execution API OpenAPI spec to improve code Generation (#49700)
- Stricter validation for ``backfill_id`` (#49691)(#49716)
- Add ``SimpleAllAdminMiddleware`` to allow api usage without auth header in request (#49599)
- Bump ``react-router`` and ``react-router-dom`` from 7.4.0 to 7.5.2 (#49742)
- Remove reference to ``root_dag_id`` in dagbag and restore logic (#49668)
- Fix a few SqlAlchemy deprecation warnings (#49477)
- Make default execution server URL be relative to API Base URL (#49747)(#49782)
- Common ``airflow.cfg`` files across all containers in default ``docker-compose.yaml`` (#49681)
- Add redirects for old operators location to standard provider (#49776)
- Bump packaging from 24.2 to 25.0 in ``/airflow-core`` (#49512)
- Move some non-core dependencies to the ``apache-airflow`` meta package (#49846)
- Add more lower-bind limits to address resolution too deep (#49860)
- UI: Add counts to pool bar (#49894)
- Add type hints for ``@task.kuberenetes_cmd``  (#46913)
- Bump ``vite`` from ``5.4.17`` to ``5.4.19`` for Airflow UI (#49162)(#50074)
- Add ``map_index`` filter option to ``GetTICount`` and ``GetTaskStates`` (#49818)
- Add ``stats`` ui endpoint (#49985)
- Add link to tag to filter dags associated with the tag (#49680)
- Add keyboard shortcut for full screen and wrap in logs. (#50008)
- Update graph node styling to decrease border width on tasks in UI (#50047) (#50073)
- Allow non-string valid JSON values in Variable import. (#49844)
- Bump min versions of crucial providers (#50076)
- Add ``state`` attribute to ``RuntimeTaskInstance`` for easier ``ti.state`` access in Task Context (#50031)
- Move SQS message queue to Amazon provider (#50057)
- Execution API: Improve task instance logging with structlog context (#50120)
- Add ``dag_run_conf`` to ``RunBackfillForm`` (#49763)
- Refactor Dashboard to enhance layout (#50026)
- Add the download button on the assets page (#50045)
- Add ``dateInterval`` validation and error handling (#50072)
- Add ``Task Instances [{map_index}]`` tab to mapped task details (#50085)
- Add focus view on grid and graph on second click (#50125)
- Add formatted extra to asset events (#50124)
- Move webserver expose config to api section (#50209)

Doc Only Changes
""""""""""""""""

- Remove flask application configuration from docs for AF3 (#49393)
- Docker compose: airflow-cli to depend on airflow common services (#49318)
- Better upgrade docs about flask/fab plugins in Airflow 3 (#49632)(#49614)(#49628)
- Various Airflow 3.0 Release notes & Updating guide docs updates (#49623)(#49401)(#49654)(#49663)(#49988)(#49954)(#49840)(#50195)(#50264)
- Add using the rest api by referring to ``security/api.rst`` (#49675)
- Add correct redirects for rest api and upgrade docs (#49764)
- Update ``max_consecutive_failed_dag_runs`` default value to zero in TaskSDK dag (#49795) (#49803)
- Fix spacing issues in params example dag (``example_params_ui_tutorial``) (#49905)
- Doc: Fix Kubernetes duplicated version in maintenance policy (#50030)
- Fix links to source examples in Airflow docs (#50082)
- Update ruff instructions for migration checks (#50232)
- Fix example of backfill command (#50222)
- Update docs for running behind proxy for Content-Security-Policy (#50236)

Airflow 3.0.0 (2025-04-22)
--------------------------
We are proud to announce the General Availability of Apache Airflow 3.0 — the most significant release in the project's
history. This version introduces a service-oriented architecture, a stable DAG authoring interface, expanded support for
event-driven and ML workflows, and a fully modernized UI built on React. Airflow 3.0 reflects years of community
investment and lays the foundation for the next era of scalable, modular orchestration.

Highlights
^^^^^^^^^^

- **Service-Oriented Architecture**: A new Task Execution API and ``airflow api-server`` enable task execution in remote environments with improved isolation and flexibility (AIP-72).

- **Edge Executor**: A new executor that supports distributed, event-driven, and edge-compute workflows (AIP-69), now generally available.

- **Stable Authoring Interface**: DAG authors should now use the new ``airflow.sdk`` namespace to import core DAG constructs like ``@dag``, ``@task``, and ``DAG``.

- **Scheduler-Managed Backfills**: Backfills are now scheduled and tracked like regular DAG runs, with native UI and API support (AIP-78).

- **DAG Versioning**: Airflow now tracks structural changes to DAGs over time, enabling inspection of historical DAG definitions via the UI and API (AIP-66).

- **Asset-Based Scheduling**: The dataset model has been renamed and redesigned as assets, with a new ``@asset`` decorator and cleaner event-driven DAG definition (AIP-74, AIP-75).

- **Support for ML and AI Workflows**: DAGs can now run with ``logical_date=None``, enabling use cases such as model inference, hyperparameter tuning, and non-interval workflows (AIP-83).

- **Removal of Legacy Features**: SLAs, SubDAGs, DAG and Xcom pickling, and several internal context variables have been removed. Use the upgrade tools to detect deprecated usage.

- **Split CLI and API Changes**: The CLI has been split into ``airflow`` and ``airflowctl`` (AIP-81), and REST API now defaults to ``logical_date=None`` when triggering a new DAG run.

- **Modern React UI**: A complete UI overhaul built on React and FastAPI includes version-aware views, backfill management, and improved DAG and task introspection (AIP-38, AIP-84).

- **Migration Tooling**: Use **ruff** and **airflow config update** to validate DAGs and configurations. Upgrade requires Airflow 2.7 or later and Python 3.9–3.12.

Significant Changes
^^^^^^^^^^^^^^^^^^^

Airflow 3.0 introduces the most significant set of changes since the 2.0 release, including architectural shifts, new
execution models, and improvements to DAG authoring and scheduling.

Task Execution API & Task SDK (AIP-72)
""""""""""""""""""""""""""""""""""""""

Airflow now supports a service-oriented architecture, enabling tasks to be executed remotely via a new Task Execution
API. This API decouples task execution from the scheduler and introduces a stable contract for running tasks outside of
Airflow's traditional runtime environment.

To support this, Airflow introduces the Task SDK — a lightweight runtime environment for running Airflow tasks in
external systems such as containers, edge environments, or other runtimes. This lays the groundwork for
language-agnostic task execution and brings improved isolation, portability, and extensibility to Airflow-based
workflows.

Airflow 3.0 also introduces a new ``airflow.sdk`` namespace that exposes the core authoring interfaces for defining DAGs
and tasks. DAG authors should now import objects like ``DAG``, ``@dag``, and ``@task`` from ``airflow.sdk`` rather than
internal modules. This new namespace provides a stable, forward-compatible interface for DAG authoring across future
versions of Airflow.

Edge Executor (AIP-69)
""""""""""""""""""""""

Airflow 3.0 introduces the **Edge Executor** as a generally available feature, enabling execution of tasks in
distributed or remote compute environments. Designed for event-driven and edge-compute use cases, the Edge Executor
integrates with the Task Execution API to support task orchestration beyond the traditional Airflow runtime. This
advancement facilitates hybrid and cross-environment orchestration patterns, allowing task workers to operate closer to
data or application layers.

Scheduler-Managed Backfills (AIP-78)
""""""""""""""""""""""""""""""""""""

Backfills are now fully managed by the scheduler, rather than being launched as separate command-line jobs. This change
unifies backfill logic with regular DAG execution and ensures that backfill runs follow the same scheduling, versioning,
and observability models as other DAG runs.

Airflow 3.0 also introduces native UI and REST API support for initiating and monitoring backfills, making them more
accessible and easier to integrate into automated workflows. These improvements lay the foundation for smarter, safer
historical reprocessing — now available directly through the Airflow UI and API.

DAG Versioning (AIP-66)
"""""""""""""""""""""""

Airflow 3.0 introduces native DAG versioning. DAG structure changes (e.g., renamed tasks, dependency shifts) are now
tracked directly in the metadata database. This allows users to inspect historical DAG structures through the UI and API,
and lays the foundation for safer backfills, improved observability, and runtime-determined DAG logic.

**Note**: DAG bundles are not initialized in the triggerer. In practice, this means that triggers cannot come from a
DAG bundle. This is because the triggerer does not deal with changes in trigger code over time, as everything happens
in the main process. Triggers can come from anywhere else on ``sys.path`` instead.

React UI Rewrite (AIP-38, AIP-84)
"""""""""""""""""""""""""""""""""

Airflow 3.0 ships with a completely redesigned user interface built on React and FastAPI. This modern architecture
improves responsiveness, enables more consistent navigation across views, and unlocks new UI capabilities — including
support for DAG versioning, asset-centric DAG definitions, and more intuitive filtering and search.

The new UI replaces the legacy Flask-based frontend and introduces a foundation for future extensibility and community
contributions.

Asset-Based Scheduling & Terminology Alignment (AIP-74, AIP-75)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The concept of **Datasets** has been renamed to **Assets**, unifying terminology with common practices in the modern
data ecosystem. The internal model has also been reworked to better support future features like asset partitions and
validations.

The ``@asset`` decorator and related changes to the DAG parser enable clearer, asset-centric DAG definitions, allowing
Airflow to more naturally support event-driven and data-aware scheduling patterns.

This renaming impacts modules, classes, functions, configuration keys, and internal models. Key changes include:

- ``Dataset`` → ``Asset``
- ``DatasetEvent`` → ``AssetEvent``
- ``DatasetAlias`` → ``AssetAlias``
- ``airflow.datasets.*`` → ``airflow.sdk.*``
- ``airflow.timetables.simple.DatasetTriggeredTimetable`` → ``airflow.timetables.simple.AssetTriggeredTimetable``
- ``airflow.timetables.datasets.DatasetOrTimeSchedule`` → ``airflow.timetables.assets.AssetOrTimeSchedule``
- ``airflow.listeners.spec.dataset.on_dataset_created`` → ``airflow.listeners.spec.asset.on_asset_created``
- ``airflow.listeners.spec.dataset.on_dataset_changed`` → ``airflow.listeners.spec.asset.on_asset_changed``
- ``core.dataset_manager_class`` → ``core.asset_manager_class``
- ``core.dataset_manager_kwargs`` → ``core.asset_manager_kwargs``

Unified Scheduling Field
""""""""""""""""""""""""

Airflow 3.0 removes the legacy ``schedule_interval`` and ``timetable`` parameters. DAGs must now use the unified
``schedule`` field for all time- and event-based scheduling logic. This simplifies DAG definition and improves
consistency across scheduling paradigms.

Updated Scheduling Defaults
"""""""""""""""""""""""""""

Airflow 3.0 changes the default behavior for new DAGs by setting ``catchup_by_default = False`` in the configuration
file. This means DAGs that do not explicitly set ``catchup=...`` will no longer backfill missed intervals by default.
This change reduces confusion for new users and better reflects the growing use of on-demand and event-driven workflows.

The default DAG schedule has been changed to ``None`` from ``@once``.

Restricted Metadata Database Access
"""""""""""""""""""""""""""""""""""

Task code can no longer directly access the metadata database. Interactions with DAG state, task history, or DAG runs
must be performed via the Airflow REST API or exposed context. This change improves architectural separation and enables
remote execution.

Future Logical Dates No Longer Supported
"""""""""""""""""""""""""""""""""""""""""

Airflow no longer supports triggering DAG runs with a logical date in the future. This change aligns with the logical
execution model and removes ambiguity in backfills and event-driven DAGs. Use ``logical_date=None`` to trigger runs with
the current timestamp.

Context Behavior for Asset and Manually Triggered DAGs
""""""""""""""""""""""""""""""""""""""""""""""""""""""

For DAG runs triggered by an Asset event or through the REST API without specifying a ``logical_date``, Airflow now sets
``logical_date=None`` by default. These DAG runs do not have a data interval, and attempting to access
``data_interval_start``, ``data_interval_end``, or ``logical_date`` from the task context will raise a ``KeyError``.

DAG authors should use ``dag_run.logical_date`` and perform appropriate checks or fallbacks if supporting multiple
trigger types. This change improves consistency with event-driven semantics but may require updates to existing DAGs
that assume these values are always present.

Improved Callback Behavior
""""""""""""""""""""""""""

Airflow 3.0 refines task callback behavior to improve clarity and consistency. In particular, ``on_success_callback`` is
no longer executed when a task is marked as ``SKIPPED``, aligning it more closely with expected semantics.

Updated Default Configuration
"""""""""""""""""""""""""""""

Several default configuration values have been updated in Airflow 3.0 to better reflect modern usage patterns and
simplify onboarding:

- ``catchup_by_default`` is now set to ``False`` by default. DAGs will not automatically backfill unless explicitly configured to do so.
- ``create_cron_data_intervals`` is now set to ``False`` by default. As a result, cron expressions will be interpreted using the ``CronTriggerTimetable`` instead of the legacy ``CronDataIntervalTimetable``.
- ``SimpleAuthManager`` is now the default ``auth_manager``. To continue using Flask AppBuilder-based authentication, install the ``apache-airflow-providers-fab`` provider and explicitly set ``auth_manager = airflow.providers.fab.auth_manager.FabAuthManager``.

These changes represent the most significant evolution of the Airflow platform since the release of 2.0 — setting the
stage for more scalable, event-driven, and language-agnostic orchestration in the years ahead.

Executor & Scheduler Updates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow 3.0 introduces several important improvements and behavior changes in how DAGs and tasks are scheduled,
prioritized, and executed.

Standalone DAG Processor Required
"""""""""""""""""""""""""""""""""

Airflow 3.0 now requires the standalone DAG processor to parse DAGs. This dedicated process improves scheduler
performance, isolation, and observability. It also simplifies architecture by clearly separating DAG parsing from
scheduling logic. This change may affect custom deployments that previously used embedded DAG parsing.

Priority Weight Capped by Pool Slots
"""""""""""""""""""""""""""""""""""""

The ``priority_weight`` value on a task is now capped by the number of available pool slots. This ensures that resource
availability remains the primary constraint in task execution order, preventing high-priority tasks from starving others
when resource contention exists.

Teardown Task Handling During DAG Termination
"""""""""""""""""""""""""""""""""""""""""""""

Teardown tasks will now be executed even when a DAG run is terminated early. This ensures that cleanup logic is
respected, improving reliability for workflows that use teardown tasks to manage ephemeral infrastructure, temporary
files, or downstream notifications.

Improved Scheduler Fault Tolerance
""""""""""""""""""""""""""""""""""

Scheduler components now use ``run_with_db_retries`` to handle transient database issues more gracefully. This enhances
Airflow's fault tolerance in high-volume environments and reduces the likelihood of scheduler restarts due to temporary
database connection problems.

Mapped Task Stats Accuracy
"""""""""""""""""""""""""""

Airflow 3.0 fixes a bug that caused incorrect task statistics to be reported for dynamic task mapping. Stats now
accurately reflect the number of mapped task instances and their statuses, improving observability and debugging for
dynamic workflows.

``SequentialExecutor`` has been removed
"""""""""""""""""""""""""""""""""""""""

``SequentialExecutor`` was primarily used for local testing but is now redundant, as ``LocalExecutor``
supports SQLite with WAL mode and provides better performance with parallel execution.
Users should switch to ``LocalExecutor`` or ``CeleryExecutor`` as alternatives.

DAG Authoring Enhancements
^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow 3.0 includes several changes that improve consistency, clarity, and long-term stability for DAG authors.

New Stable DAG Authoring Interface: ``airflow.sdk``
"""""""""""""""""""""""""""""""""""""""""""""""""""

Airflow 3.0 introduces a new, stable public API for DAG authoring under the ``airflow.sdk`` namespace,
available via the ``apache-airflow-task-sdk`` package.

The goal of this change is to **decouple DAG authoring from Airflow internals** (Scheduler, API Server, etc.),
providing a **forward-compatible, stable interface** for writing and maintaining DAGs across Airflow versions.

DAG authors should now import core constructs from ``airflow.sdk`` rather than internal modules.

**Key Imports from** ``airflow.sdk``:

- Classes:

  - ``Asset``
  - ``BaseNotifier``
  - ``BaseOperator``
  - ``BaseOperatorLink``
  - ``BaseSensorOperator``
  - ``Connection``
  - ``Context``
  - ``DAG``
  - ``EdgeModifier``
  - ``Label``
  - ``ObjectStoragePath``
  - ``Param``
  - ``TaskGroup``
  - ``Variable``

- Decorators and Functions:

  - ``@asset``
  - ``@dag``
  - ``@setup``
  - ``@task``
  - ``@task_group``
  - ``@teardown``
  - ``chain``
  - ``chain_linear``
  - ``cross_downstream``
  - ``get_current_context``
  - ``get_parsing_context``

For an exhaustive list of available classes, decorators, and functions, check ``airflow.sdk.__all__``.

All DAGs should update imports to use ``airflow.sdk`` instead of referencing internal Airflow modules directly.
Legacy import paths (e.g., ``airflow.models.dag.DAG``, ``airflow.decorator.task``) are **deprecated** and
will be **removed** in a future Airflow version. Some additional utilities and helper functions
that DAGs sometimes use from ``airflow.utils.*`` and others will be progressively migrated to the Task SDK in future
minor releases.

These future changes aim to **complete the decoupling** of DAG authoring constructs
from internal Airflow services. DAG authors should expect continued improvements
to ``airflow.sdk`` with no backwards-incompatible changes to existing constructs.

For example, update:

.. code-block:: python

    # Old (Airflow 2.x)
    from airflow.models import DAG
    from airflow.decorators import task

    # New (Airflow 3.x)
    from airflow.sdk import DAG, task

Renamed Parameter: ``fail_stop`` → ``fail_fast``
"""""""""""""""""""""""""""""""""""""""""""""""""

The DAG argument ``fail_stop`` has been renamed to ``fail_fast`` for improved clarity. This parameter controls whether a
DAG run should immediately stop execution when a task fails. DAG authors should update any code referencing
``fail_stop`` to use the new name.

Context Cleanup and Parameter Removal
"""""""""""""""""""""""""""""""""""""

Several legacy context variables have been removed or may no longer be available in certain types of DAG runs,
including:

- ``conf``
- ``execution_date``
- ``dag_run.external_trigger``

In asset-triggered and manually triggered DAG runs with ``logical_date=None``, data interval fields such as
``data_interval_start`` and ``data_interval_end`` may not be present in the task context. DAG authors should use
explicit references such as ``dag_run.logical_date`` and conditionally check for the presence of interval-related fields
where applicable.

Task Context Utilities Moved
""""""""""""""""""""""""""""

Internal task context functions such as ``get_parsing_context`` have been moved to a more appropriate location (e.g.,
``airflow.models.taskcontext``). DAG authors using these utilities directly should update import paths accordingly.

Trigger Rule Restrictions
"""""""""""""""""""""""""

The ``TriggerRule.ALWAYS`` rule can no longer be used with teardown tasks or tasks that are expected to honor upstream
dependency semantics. DAG authors should ensure that teardown logic is defined with the appropriate trigger rules for
consistent task resolution behavior.

Asset Aliases for Reusability
"""""""""""""""""""""""""""""

A new utility function, ``create_asset_aliases()``, allows DAG authors to define reusable aliases for frequently
referenced Assets. This improves modularity and reuse across DAG files and is particularly helpful for teams adopting
asset-centric DAGs.

Operator Links interface changed
""""""""""""""""""""""""""""""""

The Operator Extra links, which can be defined either via plugins or custom operators
now do not execute any user code in the Airflow UI, but instead push the "full"
links to XCom backend and the link is fetched from the XCom backend when viewing
task details, for example from grid view.

Example for users with custom links class:

.. code-block:: python

  @attr.s(auto_attribs=True)
  class CustomBaseIndexOpLink(BaseOperatorLink):
      """Custom Operator Link for Google BigQuery Console."""

      index: int = attr.ib()

      @property
      def name(self) -> str:
          return f"BigQuery Console #{self.index + 1}"

      @property
      def xcom_key(self) -> str:
          return f"bigquery_{self.index + 1}"

      def get_link(self, operator, *, ti_key):
          search_queries = XCom.get_one(
              task_id=ti_key.task_id, dag_id=ti_key.dag_id, run_id=ti_key.run_id, key="search_query"
          )
          return f"https://console.cloud.google.com/bigquery?j={search_query}"

The link has an ``xcom_key`` defined, which is how it will be stored in the XCOM backend, with key as xcom_key and
value as the entire link, this case: ``https://console.cloud.google.com/bigquery?j=search``

Plugins no longer support adding executors, operators & hooks
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Operator (including Sensors), Executors & Hooks can no longer be registered or imported via Airflow's plugin mechanism. These types of classes
are just treated as plain Python classes by Airflow, so there is no need to register them with Airflow. They
can be imported directly from their respective provider packages.

Before:

.. code-block:: python

  from airflow.hooks.my_plugin import MyHook

You should instead import it as:

.. code-block:: python

  from my_plugin import MyHook

Support for ML & AI Use Cases (AIP-83)
"""""""""""""""""""""""""""""""""""""""

Airflow 3.0 expands the types of DAGs that can be expressed by removing the constraint that each DAG run must correspond
to a unique data interval. This change, introduced in AIP-83, enables support for workflows that don't operate on a
fixed schedule — such as model training, hyperparameter tuning, and inference tasks.

These ML- and AI-oriented DAGs often run ad hoc, are triggered by external systems, or need to execute multiple times
with different parameters over the same dataset. By allowing multiple DAG runs with ``logical_date=None``, Airflow now
supports these scenarios natively without requiring workarounds.

Config & Interface Changes
^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow 3.0 introduces several configuration and interface updates that improve consistency, clarify ownership of core
utilities, and remove legacy behaviors that were no longer aligned with modern usage patterns.

Default Value Handling
""""""""""""""""""""""

Airflow no longer silently updates configuration options that retain deprecated default values. Users are now required
to explicitly set any config values that differ from the current defaults. This change improves transparency and
prevents unintentional behavior changes during upgrades.

Refactored Config Defaults
"""""""""""""""""""""""""""

Several configuration defaults have changed in Airflow 3.0 to better reflect modern usage patterns:

- The default value of ``catchup_by_default`` is now ``False``. DAGs will not backfill missed intervals unless explicitly configured to do so.
- The default value of ``create_cron_data_intervals`` is now ``False``. Cron expressions are now interpreted using the ``CronTriggerTimetable`` instead of the legacy ``CronDataIntervalTimetable``. This change simplifies interval logic and aligns with the future direction of Airflow's scheduling system.

Refactored Internal Utilities
"""""""""""""""""""""""""""""

Several core components have been moved to more intuitive or stable locations:

- The ``SecretsMasker`` class has been relocated to ``airflow.sdk.execution_time.secrets_masker``.
- The ``ObjectStoragePath`` utility previously located under ``airflow.io`` is now available via ``airflow.sdk``.

These changes simplify imports and reflect broader efforts to stabilize utility interfaces across the Airflow codebase.

Improved ``inlet_events``, ``outlet_events``, and ``triggering_asset_events``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Asset event mappings in the task context are improved to better support asset use cases, including new features introduced in AIP-74.

Events of an asset or asset alias are now accessed directly by a concrete object to avoid ambiguity. Using a ``str`` to access events is
no longer supported. Use an ``Asset`` or ``AssetAlias`` object, or ``Asset.ref`` to refer to an entity explicitly instead, such as::

    outlet_events[Asset.ref(name="myasset")]  # Get events for asset named "myasset".
    outlet_events[AssetAlias(name="myalias")]  # Get events for asset alias named "myalias".

Alternatively, two helpers ``for_asset`` and ``for_asset_alias`` are added as shortcuts::

    outlet_events.for_asset(name="myasset")  # Get events for asset named "myasset".
    outlet_events.for_asset_alias(name="myalias")  # Get events for asset alias named "myalias".

The internal representation of asset event triggers now also includes an explicit ``uri`` field, simplifying traceability and
aligning with the broader asset-aware execution model introduced in Airflow 3.0. DAG authors interacting directly with
``inlet_events`` may need to update logic that assumes the previous structure.

Behaviour change in ``xcom_pull``
"""""""""""""""""""""""""""""""""

In Airflow 2, the ``xcom_pull()`` method allowed pulling XComs by key without specifying task_ids, despite the fact that the underlying
DB model defines task_id as part of the XCom primary key. This created ambiguity: if two tasks pushed XComs with the same key,
``xcom_pull()`` would pull whichever one happened to be first, leading to unpredictable behavior.

Airflow 3 resolves this inconsistency by requiring ``task_ids`` when pulling by key. This change aligns with the task-scoped nature of
XComs as defined by the schema, ensuring predictable and consistent behavior.

DAG Authors should update their dags to use ``task_ids`` if their dags used ``xcom_pull`` without ``task_ids`` such as::

  kwargs["ti"].xcom_pull(key="key")

Should be updated to::

  kwargs["ti"].xcom_pull(task_ids="task1", key="key")


Removed Configuration Keys
"""""""""""""""""""""""""""

As part of the deprecation cleanup, several legacy configuration options have been removed. These include:

- ``[scheduler] allow_trigger_in_future``
- ``[scheduler] use_job_schedule``
- ``[scheduler] use_local_tz``
- ``[scheduler] processor_poll_interval``
- ``[logging] dag_processor_manager_log_location``
- ``[logging] dag_processor_manager_log_stdout``
- ``[logging] log_processor_filename_template``

All the webserver configurations have also been removed since API server now replaces webserver, so
the configurations like below have no effect:

- ``[webserver] allow_raw_html_descriptions``
- ``[webserver] cookie_samesite``
- ``[webserver] error_logfile``
- ``[webserver] access_logformat``
- ``[webserver] web_server_master_timeout``
- etc

Several configuration options previously located under the ``[webserver]`` section have
been **moved to the new ``[api]`` section**. The following configuration keys have been moved:

- ``[webserver] web_server_host`` → ``[api] host``
- ``[webserver] web_server_port`` → ``[api] port``
- ``[webserver] workers`` → ``[api] workers``
- ``[webserver] web_server_worker_timeout`` → ``[api] worker_timeout``
- ``[webserver] web_server_ssl_cert`` → ``[api] ssl_cert``
- ``[webserver] web_server_ssl_key`` → ``[api] ssl_key``
- ``[webserver] access_logfile`` → ``[api] access_logfile``

The following DAG parsing configuration options were **moved to the new ``[dag_processor]`` section**:

- ``[core] dag_file_processor_timeout`` → ``[dag_processor] dag_file_processor_timeout``
- ``[scheduler] parsing_processes`` → ``[dag_processor] parsing_processes``
- ``[scheduler] file_parsing_sort_mode`` → ``[dag_processor] file_parsing_sort_mode``
- ``[scheduler] max_callbacks_per_loop`` → ``[dag_processor] max_callbacks_per_loop``
- ``[scheduler] min_file_process_interval`` → ``[dag_processor] min_file_process_interval``
- ``[scheduler] stale_dag_threshold`` → ``[dag_processor] stale_dag_threshold``
- ``[scheduler] print_stats_interval`` → ``[dag_processor] print_stats_interval``

Users should review their ``airflow.cfg`` files or use the ``airflow config lint`` command to identify outdated or
removed options.

Upgrade Tooling
""""""""""""""""

Airflow 3.0 includes improved support for upgrade validation. Use the following tools to proactively catch incompatible
configs or deprecated usage patterns:

- ``airflow config lint``: Identifies removed or invalid config keys
- ``ruff check --select AIR30 --preview``: Flags removed interfaces and common migration issues

CLI & API Changes
^^^^^^^^^^^^^^^^^

Airflow 3.0 introduces changes to both the CLI and REST API interfaces to better align with service-oriented deployments
and event-driven workflows.

Split CLI Architecture (AIP-81)
"""""""""""""""""""""""""""""""

The Airflow CLI has been split into two distinct interfaces:

- The core ``airflow`` CLI now handles only local functionality (e.g., ``airflow tasks test``, ``airflow dags list``).
- Remote functionality, including triggering DAGs or managing connections in service-mode environments, is now handled by a separate CLI called ``airflowctl``, distributed via the ``apache-airflow-client`` package.

This change improves security and modularity for deployments that use Airflow in a distributed or API-first context.

REST API v2 replaces v1
"""""""""""""""""""""""

The legacy REST API v1, previously built with Connexion and Marshmallow, has been replaced by a modern FastAPI-based REST API v2.

This new implementation improves performance, aligns more closely with web standards, and provides a consistent developer experience across the API and UI.

Key changes include stricter validation (422 errors instead of 400), the removal of the ``execution_date`` parameter in favor of ``logical_date``, and more consistent query parameter handling.

The v2 API is now the stable, fully supported interface for programmatic access to Airflow, and also powers the new UI - achieving full feature parity between the UI and API.

For details, see the :doc:`Airflow REST API v2 </stable-rest-api-ref>` documentation.

REST API: DAG Trigger Behavior Updated
""""""""""""""""""""""""""""""""""""""

The behavior of the ``POST /dags/{dag_id}/dagRuns`` endpoint has changed. If a ``logical_date`` is not explicitly
provided when triggering a DAG via the REST API, it now defaults to ``None``.

This aligns with event-driven DAGs and manual runs in Airflow 3.0, but may break backward compatibility with scripts or
tools that previously relied on Airflow auto-generating a timestamped ``logical_date``.

Removed CLI Flags and Commands
""""""""""""""""""""""""""""""

Several deprecated CLI arguments and commands that were marked for removal in earlier versions have now been cleaned up
in Airflow 3.0. Run ``airflow --help`` to review the current set of available commands and arguments.

- Deprecated ``--ignore-depends-on-past``  cli option is replaced by ``--depends-on-past ignore``.

- ``--tree`` flag for ``airflow tasks list`` command is removed. The format of the output with that flag can be
  expensive to generate and extremely large, depending on the DAG. ``airflow dag show`` is a better way to
  visualize the relationship of tasks in a DAG.

- Changing ``dag_id`` from flag (``-d``, ``--dag-id``) to a positional argument in the ``dags list-runs`` CLI command.

- The ``airflow db init`` and ``airflow db upgrade`` commands have been removed. Use ``airflow db migrate`` instead
  to initialize or migrate the metadata database. If you would like to create default connections use
  ``airflow connections create-default-connections``.

- ``airflow api-server`` has replaced ``airflow webserver`` cli command.


Provider Refactor & Standardization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow 3.0 completes the migration of several core operators, sensors, and hooks into the new
``apache-airflow-providers-standard`` package. This package now includes commonly used components such as:

- ``PythonOperator``
- ``BashOperator``
- ``EmailOperator``
- ``ShortCircuitOperator``

These operators were previously bundled inside ``airflow-core`` but are now treated as provider-managed components to
improve modularity, testability, and lifecycle independence.

This change enables more consistent versioning across providers and prepares Airflow for a future where all integrations
— including "standard" ones — follow the same interface model.

To maintain compatibility with existing DAGs, the ``apache-airflow-providers-standard`` package is installable on both
Airflow 2.x and 3.x. Users upgrading from Airflow 2.x are encouraged to begin updating import paths and testing provider
installation in advance of the upgrade.

Legacy imports such as ``airflow.operators.python.PythonOperator`` are deprecated and will be removed soon. They should be
replaced with:

.. code-block:: python

    from airflow.providers.standard.operators.python import PythonOperator

The SimpleHttpOperator has been migrated to apache-airflow-providers-http and renamed to HttpOperator

UI & Usability Improvements
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow 3.0 introduces a modernized user experience that complements the new React-based UI architecture (see
Significant Changes). Several areas of the interface have been enhanced to improve visibility, consistency, and
navigability.

New Home Page
"""""""""""""

The Airflow Home page now provides a high-level operational overview of your environment. It includes health checks for
core components (Scheduler, Triggerer, DAG Processor), summary stats for DAG and task instance states, and a real-time
feed of asset-triggered events. This view helps users quickly identify pipeline health, recent activity, and potential
failures.

Unified DAG List View
""""""""""""""""""""""

The DAG List page has been refreshed with a cleaner layout and improved responsiveness. Users can browse DAGs by name,
tags, or owners. While full-text search has not yet been integrated, filters and navigation have been refined for
clarity in large deployments.

Version-Aware Graph and Grid Views
"""""""""""""""""""""""""""""""""""

The Graph and Grid views now display task information in the context of the DAG version that was used at runtime. This
improves traceability for DAGs that evolve over time and provides more accurate debugging of historical runs.

Expanded DAG Graph Visualization
""""""""""""""""""""""""""""""""

The Graph view now supports visualizing the full chain of asset and task dependencies, including assets consumed or
produced across DAG boundaries. This allows users to inspect upstream and downstream lineage in a unified view, making
it easier to trace data flows, debug triggering behavior, and understand conditional dependencies between assets and
tasks.

DAG Code View
"""""""""""""

The "Code" tab now displays the exact DAG source as parsed by the scheduler for the selected DAG version. This allows
users to inspect the precise code that was executed, even for historical runs, and helps debug issues related to
versioned DAG changes.

Improved Task Log Access
"""""""""""""""""""""""""

Task log access has been streamlined across views. Logs are now easier to access from both the Grid and Task Instance
pages, with cleaner formatting and reduced visual noise.

Enhanced Asset and Backfill Views
""""""""""""""""""""""""""""""""""

New UI components support asset-centric DAGs and backfill workflows:

- Asset definitions are now visible from the DAG details page, allowing users to inspect upstream and downstream asset relationships.
- Backfills can be triggered and monitored directly from the UI, including support for scheduler-managed backfills introduced in Airflow 3.0.

These improvements make Airflow more accessible to operators, data engineers, and stakeholders working across both
time-based and event-driven workflows.

Deprecations & Removals
^^^^^^^^^^^^^^^^^^^^^^^^

A number of deprecated features, modules, and interfaces have been removed in Airflow 3.0, completing long-standing
migrations and cleanups.

Users are encouraged to review the following removals to ensure compatibility:

- **SubDag support has been removed** entirely, including the ``SubDagOperator``, related CLI and API interfaces. TaskGroups are now the recommended alternative for nested DAG structures.

- **SLAs have been removed**: The legacy SLA feature, including SLA callbacks and metrics, has been removed. A more flexible replacement mechanism, ``DeadlineAlerts``, is planned for a future version of Airflow. Users who relied on SLA-based notifications should consider implementing custom alerting using task-level success/failure hooks or external monitoring integrations.

- **Pickling support has been removed**: All legacy features related to DAG pickling have been fully removed. This includes the ``PickleDag`` CLI/API, as well as implicit behaviors around ``store_serialized_dags = False``. DAGs must now be serialized using the JSON-based serialization system. Ensure any custom Python objects used in DAGs are JSON-serializable.

- **Context parameter cleanup**: Several previously available context variables have been removed from the task execution context, including ``conf``, ``execution_date``, and ``dag_run.external_trigger``. These values are either no longer applicable or have been renamed (e.g., use ``dag_run.logical_date`` instead of ``execution_date``). DAG authors should ensure that templated fields and Python callables do not reference these deprecated keys.

- **Deprecated core imports** have been fully removed. Any use of ``airflow.operators.*``, ``airflow.hooks.*``, or similar legacy import paths should be updated to import from their respective providers.

- **Configuration cleanup**: Several legacy config options have been removed, including:

  - ``scheduler.allow_trigger_in_future``: DAG runs can no longer be triggered with a future logical date. Use ``logical_date=None`` instead.
  - ``scheduler.use_job_schedule`` and ``scheduler.use_local_tz`` have also been removed. These options were deprecated and no longer had any effect.

- **Deprecated utility methods** such as those in ``airflow.utils.helpers``, ``airflow.utils.process_utils``, and ``airflow.utils.timezone`` have been removed. Equivalent functionality can now be found in the standard Python library or Airflow provider modules.

- **Removal of deprecated CLI flags and behavior**: Several CLI entrypoints and arguments that were marked for removal in earlier versions have been cleaned up.

To assist with the upgrade, tools like ``ruff`` (e.g., rule ``AIR302``) and ``airflow config lint`` can help identify
obsolete imports and configuration keys. These utilities are recommended for locating and resolving common
incompatibilities during migration. Please see :doc:`Upgrade Guide <installation/upgrading_to_airflow3>` for more
information.

Summary of Removed Features
"""""""""""""""""""""""""""

The following table summarizes user-facing features removed in 3.0 and their recommended replacements. Not all of these
are called out individually above.

+-------------------------------------------+----------------------------------------------------------+
| **Feature**                               | **Replacement / Notes**                                  |
+===========================================+==========================================================+
| SubDagOperator / SubDAGs                  | Use TaskGroups                                           |
+-------------------------------------------+----------------------------------------------------------+
| SLA callbacks / metrics                   | Deadline Alerts (planned post-3.0)                       |
+-------------------------------------------+----------------------------------------------------------+
| DAG Pickling                              | Use JSON serialization; pickling is no longer supported  |
+-------------------------------------------+----------------------------------------------------------+
| Xcom Pickling                             | Use custom Xcom backend; pickling is no longer supported |
+-------------------------------------------+----------------------------------------------------------+
| ``execution_date`` context var            | Use ``dag_run.logical_date``                             |
+-------------------------------------------+----------------------------------------------------------+
| ``conf`` and ``dag_run.external_trigger`` | Removed from context; use DAG params or ``dag_run`` APIs |
+-------------------------------------------+----------------------------------------------------------+
| Core ``EmailOperator``                    | Use ``EmailOperator`` from the ``smtp`` provider         |
+-------------------------------------------+----------------------------------------------------------+
| ``none_failed_or_skipped`` rule           | Use ``none_failed_min_one_success``                      |
+-------------------------------------------+----------------------------------------------------------+
| ``dummy`` trigger rule                    | Use ``always``                                           |
+-------------------------------------------+----------------------------------------------------------+
| ``fail_stop`` argument                    | Use ``fail_fast``                                        |
+-------------------------------------------+----------------------------------------------------------+
| ``store_serialized_dags=False``           | DAGs are always serialized; config has no effect         |
+-------------------------------------------+----------------------------------------------------------+
| Deprecated core imports                   | Import from appropriate provider package                 |
+-------------------------------------------+----------------------------------------------------------+
| ``SequentialExecutor`` & ``DebugExecutor``| Use LocalExecutor for testing                            |
+-------------------------------------------+----------------------------------------------------------+
| ``.airflowignore`` regex                  | Uses glob syntax by default                              |
+-------------------------------------------+----------------------------------------------------------+

Migration Tooling & Upgrade Process
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow 3 was designed with migration in mind. Many Airflow 2 DAGs will work without changes, especially if deprecation
warnings were addressed in earlier releases. To support the upgrade, Airflow 3 includes validation tools such as ``ruff``
and ``airflow config update``, as well as a simplified startup model.

For a step-by-step upgrade process, see the :doc:`Upgrade Guide <installation/upgrading_to_airflow3>`.

Minimum Supported Versions
"""""""""""""""""""""""""""

To upgrade to Airflow 3.0, you must be running **Airflow 2.7 or later**.

Airflow 3.0 supports the following Python versions:

- Python 3.9
- Python 3.10
- Python 3.11
- Python 3.12

Earlier versions of Airflow or Python are not supported due to architectural changes and updated dependency requirements.

DAG Compatibility Checks
"""""""""""""""""""""""""

Airflow now includes a Ruff-based linter with custom rules to detect DAG patterns and interfaces that are no longer
compatible with Airflow 3.0. These checks are packaged under the ``AIR30x`` rule series. Example usage:

.. code-block:: bash

    ruff check dags/ --select AIR301  --preview
    ruff check dags/ --select AIR301 --fix  --preview

These checks can automatically fix many common issues such as renamed arguments, removed imports, or legacy context
variable usage.

Configuration Migration
"""""""""""""""""""""""

Airflow 3.0 introduces a new utility to validate and upgrade your Airflow configuration file:

.. code-block:: bash

    airflow config update
    airflow config update --fix

This utility detects removed or deprecated configuration options and, if desired, updates them in-place.

Additional validation is available via:

.. code-block:: bash

    airflow config lint

This command surfaces obsolete configuration keys and helps align your environment with Airflow 3.0 requirements.

Metadata Database Upgrade
"""""""""""""""""""""""""

As with previous major releases, the Airflow 3.0 upgrade includes schema changes to the metadata database. Before
upgrading, it is strongly recommended that you back up your database and optionally run:

.. code-block:: bash

    airflow db clean

to remove old task instance, log, or XCom data. To apply the new schema:

.. code-block:: bash

    airflow db migrate

Startup Behavior Changes
"""""""""""""""""""""""""

Airflow components are now started explicitly. For example:

.. code-block:: bash

    airflow api-server        # Replaces airflow webserver
    airflow dag-processor     # Required in all environments

These changes reflect Airflow's new service-oriented architecture.

Resources
^^^^^^^^^

- :doc:`Upgrade Guide <installation/upgrading_to_airflow3>`
- `Airflow AIPs <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals>`_

Airflow 3.0 represents more than a year of collaboration across hundreds of contributors and dozens of organizations. We
thank everyone who helped shape this release through design discussions, code contributions, testing, documentation, and
community feedback. For full details, migration guidance, and upgrade best practices, refer to the official Upgrade
Guide and join the conversation on the Airflow dev and user mailing lists.

Airflow 2.11.0 (2025-05-20)
---------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

``DeltaTriggerTimetable`` for trigger-based scheduling (#47074)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This change introduces DeltaTriggerTimetable, a new built-in timetable that complements the existing suite of
Airflow timetables by supporting delta-based trigger schedules without relying on data intervals.

Airflow currently has two major types of timetables:
    - Data interval-based (e.g., ``CronDataIntervalTimetable``, ``DeltaDataIntervalTimetable``)
    - Trigger-based (e.g., ``CronTriggerTimetable``)

However, there was no equivalent trigger-based option for delta intervals like ``timedelta(days=1)``.
As a result, even simple schedules like ``schedule=timedelta(days=1)`` were interpreted through a data interval
lens—adding unnecessary complexity for users who don't care about upstream/downstream data dependencies.

This feature is backported to Airflow 2.11.0 to help users begin transitioning before upgrading to Airflow 3.0.

    - In Airflow 2.11, ``schedule=timedelta(...)`` still defaults to ``DeltaDataIntervalTimetable``.
    - A new config option ``[scheduler] create_delta_data_intervals`` (default: ``True``) allows opting in to ``DeltaTriggerTimetable``.
    - In Airflow 3.0, this config defaults to ``False``, meaning ``DeltaTriggerTimetable`` becomes the default for timedelta schedules.

By flipping this config in 2.11, users can preview and adopt the new scheduling behavior in advance — minimizing surprises during upgrade.


Consistent timing metrics across all backends (#39908, #43966)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously, Airflow reported timing metrics in milliseconds for ``StatsD`` but in seconds for other backends
such as ``OpenTelemetry`` and ``Datadog``. This inconsistency made it difficult to interpret or compare
timing metrics across systems.

Airflow 2.11 introduces a new config option:

  - ``[metrics] timer_unit_consistency`` (default: ``False`` in 2.11, ``True`` and dropped in Airflow 3.0).

When enabled, all timing metrics are consistently reported in milliseconds, regardless of the backend.

This setting has become mandatory and always ``True`` in Airflow 3.0 (the config will be removed), so
enabling it in 2.11 allows users to migrate early and avoid surprises during upgrade.

Ease migration to Airflow 3
"""""""""""""""""""""""""""
This release introduces several changes to help users prepare for upgrading to Airflow 3:

  - All models using ``execution_date`` now also include a ``logical_date`` field. Airflow 3 drops ``execution_date`` entirely in favor of ``logical_date`` (#44283)
  - Added ``airflow config lint`` and ``airflow config update`` commands in 2.11 to help audit and migrate configs for Airflow 3.0. (#45736, #50353, #46757)

Python 3.8 support removed
""""""""""""""""""""""""""
Support for Python 3.8 has been removed, as it has reached end-of-life.
Airflow 2.11 requires Python 3.9, 3.10, 3.11, or 3.12.

New Features
""""""""""""

- Introduce ``DeltaTriggerTimetable`` (#47074)
- Backport ``airflow config update`` and ``airflow config lint`` changes to ease migration to Airflow 3 (#45736, #50353)
- Add link to show task in a DAG in DAG Dependencies view (#47721)
- Align timers and timing metrics (ms) across all metrics loggers (#39908, #43966)

Bug Fixes
"""""""""

- Don't resolve path for DAGs folder (#46877)
- Fix ``ti.log_url`` timestamp format from ``"%Y-%m-%dT%H:%M:%S%z"`` to ``"%Y-%m-%dT%H:%M:%S.%f%z"`` (#50306)
- Ensure that the generated ``airflow.cfg`` contains a random ``fernet_key`` and ``secret_key`` (#47755)
- Fixed setting ``rendered_map_index`` via internal api (#49057)
- Store rendered_map_index from ``TaskInstancePydantic`` into ``TaskInstance`` (#48571)
- Allow using ``log_url`` property on ``TaskInstancePydantic`` (Internal API) (#50560)
- Fix Trigger Form with Empty Object Default (#46872)
- Fix ``TypeError`` when deserializing task with ``execution_timeout`` set to ``None`` (#46822)
- Always populate mapped tasks (#46790)
- Ensure ``check_query_exists`` returns a bool (#46707)
- UI: ``/xcom/list`` got exception when applying filter on the ``value`` column (#46053)
- Allow to set note field via the experimental internal api (#47769)

Miscellaneous
"""""""""""""

- Add ``logical_date`` to models using ``execution_date`` (#44283)
- Drop support for Python 3.8 (#49980, #50015)
- Emit warning for deprecated ``BaseOperatorLink.get_link`` signature (#46448)

Doc Only Changes
""""""""""""""""
- Unquote executor ``airflow.cfg`` variable (#48084)
- Update ``XCom`` docs to show examples of pushing multiple ``XComs`` (#46284, #47068)

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
`airflow-mssql-migration repo on GitHub <https://github.com/apache/airflow-mssql-migration>`_.
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

.. spelling::

    nvd
    lineChart
