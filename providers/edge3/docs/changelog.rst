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
