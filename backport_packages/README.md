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
# Backport package {{ PACKAGE_NAME }}

## Description

This is a backport package for Apache Airflow 1.10.* series. It provides Hooks, Operators, Sensors
and Secrets (in Airflow 1.10.10+) that are developed for Apache Airflow 2.0 in a package that
is compatible with Airflow 1.10.* series.

It provides the classes under airflow.providers.{{ PACKAGE_FOLDER }} package.

This is only one of a number of packages released. The current status and description of all
packages are available in the
[Backport Providers Packages document](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## Installation

You can install this package with pip via 'pip install {{ PACKAGE }}' for the existing airflow 1.10 version.

{{ PACKAGE_DEPENDENCIES }}

{{ PACKAGE_BACKPORT_README }}
