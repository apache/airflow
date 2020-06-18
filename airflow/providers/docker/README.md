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


# Package apache-airflow-backport-providers-docker

Release: 2020-06-23

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020-06-23](#release-2020-06-23)

## Backport package

This is a backport providers package for `docker` provider. All classes for this provider package
are in `airflow.providers.docker` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-docker`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| docker        | ~=3.0              |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `docker` provider
are in the `airflow.providers.docker` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.docker` package                                                                                      | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                          |
|:-----------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.docker.DockerOperator](https://github.com/apache/airflow/blob/master/airflow/providers/docker/operators/docker.py)                  | [operators.docker_operator.DockerOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/docker_operator.py)                                  |
| [operators.docker_swarm.DockerSwarmOperator](https://github.com/apache/airflow/blob/master/airflow/providers/docker/operators/docker_swarm.py) | [contrib.operators.docker_swarm_operator.DockerSwarmOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/docker_swarm_operator.py) |







## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.docker` package                                                             | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                     |
|:------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------|
| [hooks.docker.DockerHook](https://github.com/apache/airflow/blob/master/airflow/providers/docker/hooks/docker.py) | [hooks.docker_hook.DockerHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/docker_hook.py) |






## Releases

### Release 2020-06-23

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                                                                                                             |
| [4a74cf1a3](https://github.com/apache/airflow/commit/4a74cf1a34cf20e49383f27e7cdc3ae80b9b0cde) | 2020-06-08  | Fix xcom in DockerOperator when auto_remove is used (#9173)                                                                                                        |
| [b4b84a193](https://github.com/apache/airflow/commit/b4b84a1933d055a2803b80b990482a7257a203ff) | 2020-06-07  | Add kernel capabilities in DockerOperator(#9142)                                                                                                                   |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                                                                                                                      |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                                                                                                        |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                                                                                                       |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                                                                                            |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                                                                                                       |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                                                                                            |
| [511d98e30](https://github.com/apache/airflow/commit/511d98e30ded2bcce9d246b358f806cea45ebcb7) | 2020-05-01  | [AIRFLOW-4363] Fix JSON encoding error (#8287)                                                                                                                     |
| [0a1de1668](https://github.com/apache/airflow/commit/0a1de16682da1d0a3fac668437434a72b3149fda) | 2020-04-27  | Stop DockerSwarmOperator from pulling Docker images (#8533)                                                                                                        |
| [3237c7e31](https://github.com/apache/airflow/commit/3237c7e31d008f73e6ba0ecc1f2331c7c80f0e17) | 2020-04-26  | [AIRFLOW-5850] Capture task logs in DockerSwarmOperator (#6552)                                                                                                    |
| [9626b03d1](https://github.com/apache/airflow/commit/9626b03d19905c6d1bfbd53064f85ffd3c39f0bf) | 2020-03-30  | [AIRFLOW-6574] Adding private_environment to docker operator. (#7671)                                                                                              |
| [733d3d3c3](https://github.com/apache/airflow/commit/733d3d3c32e0305691f82102cfc346e8e85478b0) | 2020-03-25  | [AIRFLOW-4363] Fix JSON encoding error (#7628)                                                                                                                     |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                                                                                                   |
| [cd546b664](https://github.com/apache/airflow/commit/cd546b664fa35a2bf85acd77af578c909a327d92) | 2020-03-23  | Add missing call to Super class in &#39;cncf&#39; &amp; &#39;docker&#39; providers (#7825)                                                                                             |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)                                                                                   |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [dbcd3d878](https://github.com/apache/airflow/commit/dbcd3d8787741fd8203b6d9bdbc5d1da4b10a15b) | 2020-02-18  | [AIRFLOW-6804] Add the basic test for all example DAGs (#7419)                                                                                                     |
| [9cbd7de6d](https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b) | 2020-02-18  | [AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)                                                                           |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                                                                                           |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)                                                                                                 |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)                                                                                                  |
