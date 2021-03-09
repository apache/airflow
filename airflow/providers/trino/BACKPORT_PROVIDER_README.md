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


# Package apache-airflow-backport-providers-trino

Release: 2021.3.3

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Hooks](#hooks)
- [Releases](#releases)
    - [Release 2021.3.3](#release-202133)

## Backport package

This is a backport providers package for `trino` provider. All classes for this provider package
are in `airflow.providers.trino` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-trino`

## PIP requirements

| PIP package | Version required |
|:------------|:-----------------|
| `trino`     | `>=0.305`        |

## Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `trino` provider
are in the `airflow.providers.trino` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


### Hooks

* [airflow.providers.trino.hooks.trino.TrinoHook](airflow/providers/trino/hooks/trino.py)

## Releases

### Release 2021.3.3

TBD
