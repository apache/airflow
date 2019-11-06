<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

#### Roadmap items

* UI page answering "Why isn't this task instance running?"
* Attempt removing DagBag caching for the web server
* Distributed scheduler (supervisors)
  * Get the supervisors to run sensors (as opposed to each sensor taking a slot)
  * Improve DagBag differential refresh
  * Pickle all the THINGS! supervisors maintains fresh, versioned pickles in the database as they monitor for change
* Pre-prod running off of master
* Containment / YarnExecutor / Docker?
* Get s3 logs
* Test and migrate to use beeline instead of the Hive CLI
* Run Hive / Hadoop / HDFS tests in Travis-CI

#### UI

* Backfill form
* Better task filtering int duration and landing time charts (operator toggle, task regex, uncheck all button)
* Add templating to adhoc queries

#### Backend

* Add a run_only_latest flag to BaseOperator, runs only most recent task instance where deps are met
* Raise errors when setting dependencies on task in foreign DAGs
* Add an is_test flag to the run context

#### Wishlist

* Pause flag at the task level
* Increase unit test coverage
* Stats logging interface with support for stats and sqlalchemy to collect detailed information from the scheduler and dag processing times

#### Other

* Deprecate TimeSensor
