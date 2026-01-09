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

``apache-airflow-providers-edge3``


Changelog
---------

2.0.1
.....

Misc
~~~~

* ``Bump the edge-ui-package-updates group across 1 directory with 19 updates (#59719)``
* ``Add typescript eslint plugin to edge3 (#59606)``
* ``Make Edge provider SQLA2 compatible (#59414)``
* ``Pnpm upgrade to 10.x and prevent script execution (#59466)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove top-level SDK reference in Core (#59817)``

2.0.0
.....

.. warning::
    This release of the Edge3 provider drops support for Airflow versions below 3.0.0.

    The support for Airflow 2.10-2.11 was experimental and GA for the provider is only for Airflow 3.0+.
    Productive operation was not intended in Airflow 2.x, therefore the support for Airflow 2.x is now dropped
    earlier than the usual release support policy would indicate.


Breaking Changes
~~~~~~~~~~~~~~~~

* ``Drop Airflow 2 Support in Edge Provider (#59143)``

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``
* ``Move the traces and metrics code under a common observability package (#56187)``
* ``Bump minimum prek version to 0.2.0 (#58952)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.6.0
.....

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``Send executor integration info in workload (#57800)``
* ``Prevent duplicate edge workers unless existing worker is offline or unkown (#58586)``
* ``Add multi-select state filter to worker page (#58505)``
* ``Add queue name filtering to Edge Worker tab (#58416)``
* ``Add search functionality to Edge Worker tab (#58331)``

Bug Fixes
~~~~~~~~~

* ``Use 'before_sleep_log' in retries of Edge Worker (#58480)``

Misc
~~~~

* ``Bump the edge-ui-package-updates group across 1 directory with 8 updates (#58780)``
* ``Move out some exceptions to TaskSDK (#54505)``
* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``
* ``Upgrade Edge TS dependencies (#58413)``
* ``Fix lower bound dependency to common-compat provider (#58833)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove SDK reference for NOTSET in Airflow Core (#58258)``
   * ``Prepare release for 2025-11-27 wave of providers (#58697)``

1.5.0
.....

Features
~~~~~~~~

* `` Add push_logs configuration option to Edge executor (#58125)``

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Bump the edge-ui-package-updates group across 1 directory with 19 updates (#58235)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to 6 files in providers (edge3,git) (#58018)``
   * ``Synchronize default versions in all split .pre-commit-config.yaml (#57851)``

1.4.1
.....

.. warning::
  The React Plugin integration in this release is incompatible with Airflow 3.1.0
  It is recommended to use apache-airflow>=3.1.1

Bug Fixes
~~~~~~~~~

* ``Fix Link to Dag in Plugin (#55642)``
* ``Bugfix/support Subpath w/o Execution API Url (#57372)``
* ``Adjust authentication token after UI changes in Airflow 3.1.1 (#57370)``

Misc
~~~~

* ``Bump vite from 7.1.7 to 7.1.11 in plugin integration (#56909)``
* ``Bump happy-dom from 18.0.1 to 20.0.2 in plugin integration (#56686)``
* ``Bump the edge-ui-package-updates group across 1 directory with 23 updates (#57286)``
* ``Fix mypy warnings for SQLA2 migration (#56989)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Extract prek hooks for Edge provider (#57104)``
   * ``Prepare edge3 provider ad-hoc release (October 2025) (#57280)``
   * ``Enable ruff PLW1508 rule (#57653)``
   * ``Fix code formatting via ruff preview (#57641)``
   * ``Prepare edge3 provider ad-hoc release rc2 (October 2025) (#57538)``

1.4.0
.....

Features
~~~~~~~~

* ``Introduce generic Callbacks to support running callbacks on executors (#54796)``
* ``Add revoke_task implementation to EdgeExecutor for task queued timeout support (#56240)``

Bug Fixes
~~~~~~~~~

* ``Bugfix/remove airflow utils deprecations in edge (#56568)``
* ``Fix Edge3 provider navigation with webserver base_url configuration (#56189)``

Misc
~~~~

* ``Migrate edge3 provider to ''common.compat'' (#56998)``
* ``SQLA2: Partially fix type hints in the edge3 provider (#56873)``

Doc-only
~~~~~~~~

* ``Correct 'Dag' to 'DAG' for code snippets in provider docs (#56727)``
* ``Revise details of edge documentation after Airflow 3.1 release (#56166)``
* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Common.Compat: Extract reusable compat utilities and rename to sdk (#56884)``
   * ``Enable PT011 rule to prvoider tests (#56277)``

1.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Regenerate the rest API spec to fix CI (#55986)``
* ``build(open-api): regenerate open api spec (#55931)``

Misc
~~~~

* ``Bump Edge UI packages (#56016)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.3.0
.....


Features
~~~~~~~~

* ``Add Color Scheme to all Operations Buttons in Edge React UI (#55637)``
* ``Add shutdown-all-workers command to Edge CLI (#55626)``
* ``add yellow hover background to enter maintenance icon (#55631)``
* ``Add queue management UI buttons for Edge workers (#55625)``
* ``Add Worker Maintenance Comment Change to React UI (#55547)``
* ``feat: Add delete button for offline edge workers (#55529)``
* ``feat: Add shutdown button for edge workers with confirmation dialog (#55513)``
* ``Add confirmation dialog for exit maintenance action in Edge Worker Page (#55400)``
* ``Add Links to Edge React UI (#55356)``
* ``Feature/edge maintenance plugin beautification (#55348)``
* ``Add worker maintenance mode functionality to Edge3 provider UI (#55301)``

Bug Fixes
~~~~~~~~~

* ``Fix EdgeWorker multiprocessing pickle error on Windows (#55284)``

Misc
~~~~

* ``Pick/vite vitest full (#55623)``
* ``Bump React and React-DOM (#55598)``
* ``Bump axios from 1.11.0 to 1.12.0 in plugin integration (#55550)``
* ``Some small UI polishing for Edge React UI (#55545)``
* ``Adjust Edge color scheme after merge of PR 53981 (#55485)``
* ``Bump vite from 5.4.19 to 5.4.20 in plugin integration (#55449)``

Doc-only
~~~~~~~~

* ``Update docs for new Airflow 3.1 UI Plugins (#55654)``

1.2.0
.....

Features
~~~~~~~~

* ``Provide React UI for Edge (#53563)``
* ``Feature/add auto refresh to edge react UI (#54994)``
* ``Feature/add state badge to edge react UI (#54993)``

Bug Fixes
~~~~~~~~~

* ``Fix setproctitle usage on macos (#53122)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``
* ``Add a note to edgeexecutor doc regarding multiple executors (#54077)``
* ``Fix Airflow 2 reference in README/index of providers (#55240)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove airflow.models.DAG (#54383)``
   * ``Move trigger_rule utils from 'airflow/utils'  to 'airflow.task'and integrate with Execution API spec (#53389)``
   * ``Switch pre-commit to prek (#54258)``

1.1.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix: Prevent duplicate edge_job insertions for deferrable tasks in EdgeExecutor (#53610) (#53927)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove parameter from Edge example (#53997)``

1.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix UnboundLocalError for 'edge_job_command_len' (#52328)``
* ``Extend run detection to dev-mode to load plugin (#53576)``
* ``Add queue and remove queue cli commands for EdgeExecutor (#53505)``
* ``Ensure Edge Plugin for API endpoint is only loaded on API-Server and AF2 Webserver (#52952)``
* ``Fix unreachable code mypy warnings in edge3 provider (#53430)``
* ``Make edge3 provider compatible with mypy 1.16.1 (#53104)``
* ``Fix task configuration defaults for AbstractOperator (#52871)``

Misc
~~~~

* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Deprecate decorators from Core (#53629)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores in edge3 provider where possible (#53248)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove deprecation in Edge for DEFAULT_QUEUE (#52954)``
* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Force the definition of 'execution_api_server_url' based on 'api_url' (#52184)``
* ``Drop support for Python 3.9 (#52072)``
* ``Remove FAB dependency from Edge3 Provider (#51995)``

Doc-only
~~~~~~~~

* ``Clean some leftovers of Python 3.9 removal - All the rest (#52432)``
* ``Update documentation for forcing core execution_api_server_url (#52447)``
* ``Fix spelling in edge provider (#52169)``
* ``Add docs for edge execution_api_server_url (#52082)``
* ``Include docs for Windows (#52004)``
* ``Document EdgeExecutor migration from 'internal_api_secret_key' to 'jwt_secret' (#51905)``
* ``Fix Edge State Model Link (#51860)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Revert "Remove FAB dependency from Edge3 Provider (#51995)"" (#52000)``
   * ``Revert "Remove FAB dependency from Edge3 Provider (#51995)" (#51998)``
   * ``Make dag_version_id in TI non-nullable (#50825)``
   * ``Fix spelling of GitHub brand name (#53735)``
   * ``Replace mock.patch("utcnow") with time_machine in Edge Executor (#53670)``
   * ``Prepare release for July 2025 1st provider wave (#52727)``


1.1.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix Edge Worker Remove when in unknown state (#50717)``

Misc
~~~~

* ``Remove Airflow 2 code path in executors (#51009)``
* ``Refactor Edge Worker CLI for smaller module (#50738)``
* ``Bump some provider dependencies for faster resolution (#51727)``
* ``Edge list worker cli command to list active job metrics (#51720)``
* ``Extend command column in the edge_job table to accomodate more chars (#51716)``

Doc-only
~~~~~~~~

* ``Move example_dags in standard provider to example_dags in sources (#51260)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.1.0
.....

Features
~~~~~~~~

* ``Support For Edge Worker in Daemon Mode (#50425)``
* ``Trigger remote shutdown of edge worker (#50278)``
* ``Extend Edge Worker CLI commands operate on remote edge workers (#49915)``

Bug Fixes
~~~~~~~~~

* ``Edge worker maintenance state is remembered if worker crashes (#50338)``
* ``Fix execution API server URL handling for relative paths (#49782)``
* ``Make default execution server URL be relative to API Base URL (#49747)``
* ``Make Edge3 Intergation Test DAG working in 2.10 (#49474)``
* ``Ensure fab provider is installed when running EdgeExecutor (#49473)``

Misc
~~~~

* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Update some missing config settings in edge executor (#49758)``

Doc-only
~~~~~~~~

* ``Enhance Edge3 Provider docs (#49859)``
* ``Minor doc fix in edge_executor (#49755)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Cleanup Edge3 provider changelog (#49960)``
   * ``Avoid committing history for providers (#49907)``
   * ``Bump min Airflow version in providers to 2.10 (#49843)``

1.0.0
.....

Initial stable version of the provider.

* ``Cleanup redundant hostname lookup and migrate to core hostname function.``
* ``Fix hostname reporting - worker will consistently report defined hostname as task runner.``
* ``Fix JWT token auth in Airflow 3 beta as JWT mechanism changed.``
* ``Edge worker exports not ti.start and ti.finished metrics.``
* ``Edge worker can be set to maintenance via CLI and also return to normal operation.``
* ``Edge worker will not jump to maintenance request from offline maintenance during shut down.``
* ``CLI allows to retrieve status of worker.``
* ``CLI allows to wait until edge worker is completed when stopping.``
* ``User who entered the maintenance mode is moved to the start of the comment.``
* ``User and time information added to maintenance comment.``
* ``Wrap the sql query in text() to make it executable.``
* ``Add maintenance comment field, to make maintenance reason transparent.``
* ``EdgeWorkerVersionException is raised if http 400 is responded on set_state.``
* ``Allow removing an Edge worker that is offline.``
* ``Implement proper CSRF protection on plugin form.``
* ``An Edge worker can remember maintenance mode in case of shut down. It picks up maintenance state at startup.``
* ``Add the option to set edge workers to maintenance mode via UI plugin and API.``
* ``Fix authentication for cases where webserver.base_url is not defined and worker is not using localhost in 2.10.``
* ``Re-add the feature to support pool slots in concurrency calculation for Airflow 3.``
* ``Support Task execution interface (AIP-72) in Airflow 3. Experimental with ongoing development as AIP-72 is also under development.``
* ``Make API retries configurable via ENV. Connection loss is sustained for 5min by default.``
* ``Align retry handling logic and tooling with Task SDK, via retryhttp.``
* ``Replace null value in log file chunk with question mark to fix exception by pushing log into DB.``
* ``Revert removal of Pydantic model support from PR 44552 to restore compatibility with Airflow 2.10.``
* ``Fix to keep edge executor and edge job table in sync. Important in multi scheduler deployments.``
* ``Handle purging of restarting edge jobs.``
* ``Fix check edge worker api call authentication with different base url. Authentication failed when Airflow is not installed in webserver root.``
* ``Make edge executor DB access is multi instance save.``
* ``Remove dependency to Internal API after migration to FastAPI.``
* ``Migrate worker job calls to FastAPI.``
* ``Migrate worker log calls to FastAPI.``
* ``Migrate worker registration and heartbeat to FastAPI.``
* ``Edge worker state is sent as 0 to DB if offline or unknown.``
* ``Edge worker supports concurrency slots feature so that jobs which need more concurrency blocking other jobs being executed on the same worker in parallel.``
* ``Fix race that reporting status fails if the task has been cleaned in parallel.``
* ``Update jobs or edge workers who have been killed to clean up job table.``
* ``Support for FastAPI in Airflow 3 as API backend.``
* ``Fixed reading none UTF-8 signs in log file.``
* ``Fix SIGINT handling of child processes. Ensure graceful shutdown when SIGINT in received (not killing working tasks).``
* ``Fix SIGTERM handling of child processes. Ensure all childs are terminated on SIGTERM.``
* ``Adding some links to host and job overview pages.``
* ``Small beautification for host status in Edge Worker view.``
* ``Remove warning about missing config in edge plugin loading.``
* ``Edge worker triggers graceful shutdown, if worker version and main instance do not match.``
* ``Edge Worker uploads log file in chunks. Chunk size can be defined by push_log_chunk_size value in config.``
* ``Edge Worker exports metrics``
* ``State is set to unknown if worker heartbeat times out.``
* ``Fixed type confusion for PID file paths (#43308)``
* ``Fixed handling of PID files in Edge Worker (#43153)``
* ``Edge Worker can add or remove queues in the queue field in the DB (#43115)``
