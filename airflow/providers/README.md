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

# Airflow Providers

Providers are logical abstractions of submodules that can be used to interface with various tools and endpoints from your Airflow DAGs. Each provider is grouped by the relevant top-level service that a user might need to interact with and submodules for specific forms of interaction, including hooks, operators, sensors, and transfers, exist within each provider directory.

## Using Providers

As of Airflow 2.0, the provider packages contained in this subdirectory will be versioned and released independently of the core Airflow codebase. That means that, in order to use the submodules contained within these provider directories, a user will need to install the relevant provider python package into their Airflow environment. The relevant pip commands to install these providers and their submodules are documented in READMEs within each provider subdirectory.

Note that this does not mean that **all** Airflow operators will be abstracted away into python packages- core Airflow hooks and operators that exist in `airflow/operators` and `airflow/hooks` will continue to be included in core Airflow releases and directly accessible within any Airflow environment.
