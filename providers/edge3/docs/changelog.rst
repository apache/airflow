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


Release Date: ``|PypiReleaseDate|``

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
* ``Bump axios from 1.11.0 to 1.12.0 in /providers/edge3/src/airflow/providers/edge3/plugins/www (#55550)``
* ``Some small UI polishing for Edge React UI (#55545)``
* ``Adjust Edge color scheme after merge of PR 53981 (#55485)``
* ``Bump vite from 5.4.19 to 5.4.20 in /providers/edge3/src/airflow/providers/edge3/plugins/www (#55449)``

Doc-only
~~~~~~~~

* ``Update docs for new Airflow 3.1 UI Plugins (#55654)``

1.2.0
.....

Release Date: ``|PypiReleaseDate|``

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
