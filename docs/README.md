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

# Documentation configuration

This directory used to contain all the documentation files for the project. The documentation has
been split to separate folders - the documentation is now in the folders in sub-projects that they
are referring to.

If you look for the documentation it is stored as follows:

Documentation in separate distributions:

* `airflow-core/docs` - documentation for Airflow Core
* `providers/**/docs` - documentation for Providers
* `chart/docs` - documentation for Helm Chart
* `task-sdk/docs` - documentation for Task SDK (new format not yet published)
* `airflow-ctl/docs` - documentation for Airflow CLI (future)

Documentation for general overview and summaries not connected with any specific distribution:

* `docker-stack-docs` - documentation for Docker Stack'
* `providers-summary-docs` - documentation for provider summary page
