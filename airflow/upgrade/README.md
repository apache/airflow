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

# Apache Airflow Upgrade Check

[![PyPI version](https://badge.fury.io/py/apache-airflow-upgrade-check.svg)](https://badge.fury.io/py/apache-airflow-upgrade-check)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apache-airflow-upgrade-check.svg)](https://pypi.org/project/apache-airflow-upgrade-check/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/apache-airflow-upgrade-check)](https://pypi.org/project/apache-airflow-upgrade-check/)
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheAirflow.svg?style=social&label=Follow)](https://twitter.com/ApacheAirflow)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://s.apache.org/airflow-slack)

This package aims to easy the upgrade journey from [Apache Airflow](https://airflow.apache.org/) 1.10 to 2.0.

While we have put a lot of effort in to making this upgrade as painless as possible, with many changes
providing upgrade path (where the old code continues to work and prints out a deprecation warning) there were
unfortunately some breaking changes where we couldn't provide a compatibility shim.

The recommended upgrade path to get to Airflow 2.0.0 is to first upgrade to the latest release in the 1.10
series (at the time of writing: 1.10.13) and to then run this script.

```bash
pip install apache-airflow-upgrade-check
airflow upgrade_check
```

This will then print out a number of action items that you should follow before upgrading to 2.0.0 or above.

The exit code of the command will be 0 (success) if no problems are reported, or 1 otherwise.

For example:

```
============================================= STATUS =============================================

Legacy UI is deprecated by default......................................................SUCCESS
Users must set a kubernetes.pod_template_file value.....................................FAIL
Changes in import paths of hooks, operators, sensors and others.........................FAIL
Remove airflow.AirflowMacroPlugin class.................................................SUCCESS
Jinja Template Variables cannot be undefined............................................SUCCESS
Fernet is enabled by default............................................................FAIL
Logging configuration has been moved to new section.....................................SUCCESS
Connection.conn_id is not unique........................................................SUCCESS
GCP service account key deprecation.....................................................SUCCESS
Users must delete deprecated configs for KubernetesExecutor.............................FAIL
Changes in import path of remote task handlers..........................................SUCCESS
Chain between DAG and operator not allowed..............................................SUCCESS
SendGrid email uses old airflow.contrib module..........................................SUCCESS
Connection.conn_type is not nullable....................................................SUCCESS
Found 16 problems.

======================================== RECOMMENDATIONS =========================================

Users must set a kubernetes.pod_template_file value
---------------------------------------------------
In Airflow 2.0, KubernetesExecutor Users need to set a pod_template_file as a base
value for all pods launched by the KubernetesExecutor


Problems:

  1.  Please create a pod_template_file by running `airflow generate_pod_template`.
This will generate a pod using your aiflow.cfg settings

...
```

Additionally you can use "upgrade config" to:
- specify rules you would like to ignore
- extend the check using custom rules

For example:
```
airflow upgrade_check --config=/files/upgrade.yaml
```
the configuration file should be a proper yaml file similar to this one:
```yaml
ignored_rules:
  - LegacyUIDeprecated
  - ConnTypeIsNotNullableRule
  - PodTemplateFileRule

custom_rules:
  - path.to.upgrade_module.VeryCustomCheckClass
  - path.to.upgrade_module.VeryCustomCheckClass2
```
