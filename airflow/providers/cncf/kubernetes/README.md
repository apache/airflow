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


# Package apache-airflow-backport-providers-cncf-kubernetes

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
        - [Moved operators](#moved-operators)
    - [Sensors](#sensors)
        - [New sensors](#new-sensors)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `cncf.kubernetes` provider. All classes for this provider package
are in `airflow.providers.cncf.kubernetes` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-cncf-kubernetes`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| cryptography  | &gt;=2.0.0            |
| kubernetes    | &gt;=3.0.0            |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.cncf.kubernetes` package.


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.cncf.kubernetes` package                                                                                              |
|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.spark_kubernetes.SparkKubernetesOperator](https://github.com/apache/airflow/blob/master/airflow/providers/cncf/kubernetes/operators/spark_kubernetes.py) |



### Moved operators

| Airflow 2.0 operators: `airflow.providers.cncf.kubernetes` package                                                                                            | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                |
|:--------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.kubernetes_pod.KubernetesPodOperator](https://github.com/apache/airflow/blob/master/airflow/providers/cncf/kubernetes/operators/kubernetes_pod.py) | [contrib.operators.kubernetes_pod_operator.KubernetesPodOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/kubernetes_pod_operator.py) |




## Sensors


### New sensors

| New Airflow 2.0 sensors: `airflow.providers.cncf.kubernetes` package                                                                                          |
|:--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.spark_kubernetes.SparkKubernetesSensor](https://github.com/apache/airflow/blob/master/airflow/providers/cncf/kubernetes/sensors/spark_kubernetes.py) |




## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.cncf.kubernetes` package                                                                     |
|:---------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.kubernetes.KubernetesHook](https://github.com/apache/airflow/blob/master/airflow/providers/cncf/kubernetes/hooks/kubernetes.py) |







## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                                  |
|:-----------------------------------------------------------------------------------------------|:------------|:-----------------------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                  |
| [f82ad452b](https://github.com/apache/airflow/commit/f82ad452b0f4ebd1428bc9669641a632dc87bb8c) | 2020-05-15  | Fix KubernetesPodOperator pod name length validation (#8829)                             |
| [1ccafc617](https://github.com/apache/airflow/commit/1ccafc617c4cb9622e3460ad7c190f3ee67c3b32) | 2020-04-02  | Add spark_kubernetes system test (#7875)                                                 |
| [cd546b664](https://github.com/apache/airflow/commit/cd546b664fa35a2bf85acd77af578c909a327d92) | 2020-03-23  | Add missing call to Super class in &#39;cncf&#39; &amp; &#39;docker&#39; providers (#7825)                   |
| [6c39a3bf9](https://github.com/apache/airflow/commit/6c39a3bf97414ba2438669894db65c36ccbeb61a) | 2020-03-10  | [AIRFLOW-6542] Add spark-on-k8s operator/hook/sensor (#7163)                             |
| [42eef3821](https://github.com/apache/airflow/commit/42eef38217e709bc7a7f71bf0286e9e61293a43e) | 2020-03-07  | [AIRFLOW-6877] Add cross-provider dependencies as extras (#7506)                         |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)         |
| [0ec277412](https://github.com/apache/airflow/commit/0ec2774120d43fa667a371b384e6006e1d1c7821) | 2020-02-24  | [AIRFLOW-5629] Implement Kubernetes priorityClassName in KubernetesPodOperator (#7395)   |
| [9cbd7de6d](https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b) | 2020-02-18  | [AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412) |
| [967930c0c](https://github.com/apache/airflow/commit/967930c0cb6e2293f2a49e5c9add5aa1917f3527) | 2020-02-11  | [AIRFLOW-5413] Allow K8S worker pod to be configured from JSON/YAML file (#6230)         |
| [96f834389](https://github.com/apache/airflow/commit/96f834389e03884025534fabd862155061f53fd0) | 2020-02-03  | [AIRFLOW-6678] Pull event logs from Kubernetes (#7292)                                   |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                 |
| [cf141506a](https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721) | 2020-02-02  | [AIRFLOW-6708] Set unique logger names (#7330)                                           |
| [373c6aa4a](https://github.com/apache/airflow/commit/373c6aa4a208284b5ff72987e4bd8f4e2ada1a1b) | 2020-01-30  | [AIRFLOW-6682] Move GCP classes to providers package (#7295)                             |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)                       |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)                        |
