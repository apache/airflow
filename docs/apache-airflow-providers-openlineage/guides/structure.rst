
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


Structure of OpenLineage Airflow integration
--------------------------------------------

OpenLineage integration implements AirflowPlugin. This allows it to be discovered on Airflow start and
register Airflow Listener.

The listener is then called when certain events happen in Airflow - when DAGs or TaskInstances start, complete or fail.
For DAGs, the listener runs in Airflow Scheduler.
For TaskInstances, the listener runs on Airflow Worker.

When TaskInstance listener method gets called, the ``OpenLineageListener`` constructs metadata like event's unique ``run_id`` and event time.
Then, it tries to find valid Extractor for given operator. The Extractors are a framework
for external extraction of metadata from
