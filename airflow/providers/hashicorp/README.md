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


# Package apache-airflow-backport-providers-hashicorp

Release: 2020.5.20

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-class-summary)
    - [Secrets](#secrets)
        - [Moved secrets](#moved-secrets)
- [Releases](#releases)
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `hashicorp` provider. All classes for this provider package
are in `airflow.providers.hashicorp` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-hashicorp`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| hvac          | ~=0.10             |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-hashicorp[google]
```

| Dependent package                                                                                                  | Extra   |
|:-------------------------------------------------------------------------------------------------------------------|:--------|
| [apache-airflow-backport-providers-google](https://github.com/apache/airflow/tree/master/airflow/providers/google) | google  |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.hashicorp` package.









## Secrets



### Moved secrets

| Airflow 2.0 protocols: `airflow.providers.hashicorp` package                                                             | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                   |
|:-------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| [secrets.vault.VaultBackend](https://github.com/apache/airflow/blob/master/airflow/providers/hashicorp/secrets/vault.py) | [contrib.secrets.hashicorp_vault.VaultBackend](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/secrets/hashicorp_vault.py) |




## Releases

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                   |
|:-----------------------------------------------------------------------------------------------|:------------|:--------------------------------------------------------------------------|
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                             |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)               |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)              |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                   |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)              |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)   |
| [d8cb0b5dd](https://github.com/apache/airflow/commit/d8cb0b5ddb02d194742e374d9ac90dd8231f6e80) | 2020-05-04  | Support k8s auth method in Vault Secrets provider (#8640)                 |
| [87969a350](https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca) | 2020-04-09  | [AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)          |
| [c1c88abfe](https://github.com/apache/airflow/commit/c1c88abfede7a36c3b1d1b511fbc6c03af46d363) | 2020-03-28  | Get Airflow Variables from Hashicorp Vault (#7944)                        |
| [eb4af4f94](https://github.com/apache/airflow/commit/eb4af4f944c77e67e167bbb6b0a2aaf075a95b50) | 2020-03-28  | Make BaseSecretsBackend.build_path generic (#7948)                        |
| [686d7d50b](https://github.com/apache/airflow/commit/686d7d50bd21622724d6818021355bc6885fd3de) | 2020-03-25  | Standardize SecretBackend class names (#7846)                             |
| [eef87b995](https://github.com/apache/airflow/commit/eef87b9953347a65421f315a07dbef37ded9df66) | 2020-03-23  | [AIRFLOW-7105] Unify Secrets Backend method interfaces (#7830)            |
| [cdf1809fc](https://github.com/apache/airflow/commit/cdf1809fce0e59c8379a799f1738d8d813abbf51) | 2020-03-23  | [AIRFLOW-7104] Add Secret backend for GCP Secrets Manager (#7795)         |
| [a44beaf5b](https://github.com/apache/airflow/commit/a44beaf5bddae2a8de0429af45be5ff78a7d4d4e) | 2020-03-19  | [AIRFLOW-7076] Add support for HashiCorp Vault as Secrets Backend (#7741) |
