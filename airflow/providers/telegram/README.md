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


# Package apache-airflow-providers-telegram

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)

## Provider package

This is a provider package for `telegram` provider. All classes for this provider package
are in `airflow.providers.telegram` python package.

## Installation

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-telegram`

## PIP requirements

| PIP package           | Version required   |
|:----------------------|:-------------------|
| python-telegram-bot   | 13.0               |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-providers-telegram[http]
```

| Dependent package                                                                       | Extra   |
|:----------------------------------------------------------------------------------------|:--------|
| [apache-airflow-providers-http](https://pypi.org/project/apache-airflow-providers-http) | http    |
