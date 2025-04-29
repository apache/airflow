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

0.20.3b1
..........

Fix
~~~

* ``Cleanup redundant hostname lookup and migrate to core hostname function.``


0.20.2b1
..........

Fix
~~~

* ``Fix hostname reporting - worker will consistently report defined hostname as task runner.``


0.20.1pre0
..........

Fix
~~~

* ``Fix JWT token auth in Airflow 3 beta as JWT mechanism changed.``


0.20.0pre0
..........

Misc
~~~~

* ``Edge worker exports not ti.start and ti.finished metrics.``


0.19.0pre0
..........

Misc
~~~~

* ``Edge worker can be set to maintenance via CLI and also return to normal operation.``



0.18.1pre0
..........

Fix
~~~

* ``Edge worker will not jump to maintenance request from offline maintenance during shut down.``


0.18.0pre0
..........

Misc
~~~~

* ``CLI allows to retrieve status of worker.``


0.17.0pre0
..........

Misc
~~~~

* ``CLI allows to wait until edge worker is completed when stopping.``


0.16.0pre0
..........

Misc
~~~~

* ``User who entered the maintenance mode is moved to the start of the comment.``


0.15.0pre0
..........

Misc
~~~~

* ``User and time information added to maintenance comment.``


0.14.1pre0
..........

Fix
~~~

* ``Wrap the sql query in text() to make it executable.``


0.14.0pre0
..........

Misc
~~~~

* ``Add maintenance comment field, to make maintenance reason transparent.``


0.13.1pre0
..........

Fix
~~~

* ``EdgeWorkerVersionException is raised if http 400 is responded on set_state.``

0.13.0pre0
..........

Misc
~~~~

* ``Allow removing an Edge worker that is offline.``

Fixes
~~~~~

* ``Implement proper CSRF protection on plugin form.``

0.12.0pre0
..........

Misc
~~~~

* ``An Edge worker can remember maintenance mode in case of shut down. It picks up maintenance state at startup.``

0.11.0pre0
..........

Misc
~~~~

* ``Add the option to set edge workers to maintenance mode via UI plugin and API.``

0.10.2pre0
..........

Misc
~~~~

* ``Fix authentication for cases where webserver.base_url is not defined and worker is not using localhost in 2.10.``

0.10.1pre0
..........

Misc
~~~~

* ``Re-add the feature to support pool slots in concurrency calculation for Airflow 3.``

0.10.0pre0
..........

Feature
~~~~~~~

* ``Support Task execution interface (AIP-72) in Airflow 3. Experimental with ongoing development as AIP-72 is also under development.``

0.9.7pre0
.........

Misc
~~~~

* ``Make API retries configurable via ENV. Connection loss is sustained for 5min by default.``
* ``Align retry handling logic and tooling with Task SDK, via retryhttp.``

0.9.6pre0
.........

Misc
~~~~

* ``Replace null value in log file chunk with question mark to fix exception by pushing log into DB.``

0.9.5pre0
.........

Misc
~~~~

* ``Revert removal of Pydantic model support from PR 44552 to restore compatibility with Airflow 2.10.``

0.9.4pre0
.........

Misc
~~~~

* ``Fix to keep edge executor and edge job table in sync. Important in multi scheduler deployments.``

0.9.3pre0
.........

Misc
~~~~

* ``Handle purging of restarting edge jobs.``

0.9.2pre0
.........

Misc
~~~~

* ``Fix check edge worker api call authentication with different base url. Authentication failed when Airflow is not installed in webserver root.``

0.9.1pre0
.........

Misc
~~~~

* ``Make edge executor DB access is multi instance save.``

0.9.0pre0
.........

Misc
~~~~

* ``Remove dependency to Internal API after migration to FastAPI.``

0.8.2pre0
.........

Misc
~~~~

* ``Migrate worker job calls to FastAPI.``

0.8.1pre0
.........

Misc
~~~~

* ``Migrate worker log calls to FastAPI.``

0.8.0pre0
.........

Misc
~~~~

* ``Migrate worker registration and heartbeat to FastAPI.``

0.7.1pre0
.........

Misc
~~~~

* ``Edge worker state is sent as 0 to DB if offline or unknown.``

0.7.0pre0
.........

Misc
~~~~

* ``Edge worker supports concurrency slots feature so that jobs which need more concurrency blocking other jobs being executed on the same worker in parallel.``

0.6.2pre0
.........

Misc
~~~~

* ``Fix race that reporting status fails if the task has been cleaned in parallel.``

0.6.1pre0
.........

Misc
~~~~

* ``Update jobs or edge workers who have been killed to clean up job table.``

0.6.0pre0
.........

Misc
~~~~

* ``Support for FastAPI in Airflow 3 as API backend.``

0.5.5pre0
.........

Misc
~~~~

* ``Fixed reading none UTF-8 signs in log file.``

0.5.4pre0
.........

Misc
~~~~

* ``Fix SIGINT handling of child processes. Ensure graceful shutdown when SIGINT in received (not killing working tasks).``
* ``Fix SIGTERM handling of child processes. Ensure all childs are terminated on SIGTERM.``

0.5.3pre0
.........

Misc
~~~~

* ``Adding some links to host and job overview pages.``

0.5.2pre0
.........

Misc
~~~~

* ``Small beautification for host status in Edge Worker view.``

0.5.1pre0
.........

Misc
~~~~

* ``Remove warning about missing config in edge plugin loading.``

0.5.0pre0
.........

Misc
~~~~

* ``Edge worker triggers graceful shutdown, if worker version and main instance do not match.``

0.4.0pre0
.........

Misc
~~~~

* ``Edge Worker uploads log file in chunks. Chunk size can be defined by push_log_chunk_size value in config.``

0.3.0pre0
.........

Misc
~~~~

* ``Edge Worker exports metrics``
* ``State is set to unknown if worker heartbeat times out.``

0.2.2re0
.........

Misc
~~~~

* ``Fixed type confusion for PID file paths (#43308)``

0.2.1re0
.........

Misc
~~~~

* ``Fixed handling of PID files in Edge Worker (#43153)``

0.2.0pre0
.........

Misc
~~~~

* ``Edge Worker can add or remove queues in the queue field in the DB (#43115)``

0.1.0pre0
.........


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.1.0
.....

|experimental|

Initial version of the provider.

.. note::
  This provider is currently experimental
