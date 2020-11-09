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


# Package apache-airflow-providers-hashicorp

Release: 1.0.0b1

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-classes-summary)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
    - [Secrets](#secrets)
        - [Moved secrets](#moved-secrets)
- [Releases](#releases)
    - [Release 1.0.0b1](#release-100b1)
    - [Release 0.0.2a1](#release-002a1)
    - [Release 0.0.1](#release-001)

## Provider package

This is a provider package for `hashicorp` provider. All classes for this provider package
are in `airflow.providers.hashicorp` python package.



## Installation

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-hashicorp`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| hvac          | ~=0.10             |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-providers-hashicorp[google]
```

| Dependent package                                                                           | Extra   |
|:--------------------------------------------------------------------------------------------|:--------|
| [apache-airflow-providers-google](https://pypi.org/project/apache-airflow-providers-google) | google  |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `hashicorp` provider
are in the `airflow.providers.hashicorp` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.hashicorp` package                                                      |
|:------------------------------------------------------------------------------------------------------------------|
| [hooks.vault.VaultHook](https://github.com/apache/airflow/blob/master/airflow/providers/hashicorp/hooks/vault.py) |



## Secrets



### Moved secrets

| Airflow 2.0 secrets: `airflow.providers.hashicorp` package                                                               | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                   |
|:-------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| [secrets.vault.VaultBackend](https://github.com/apache/airflow/blob/master/airflow/providers/hashicorp/secrets/vault.py) | [contrib.secrets.hashicorp_vault.VaultBackend](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/secrets/hashicorp_vault.py) |



## Releases

### Release 1.0.0b1

| Commit                                                                                         | Committed   | Subject                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------|
| [59eb5de78](https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca) | 2020-11-09  | Update provider READMEs for up-coming 1.0.0beta1 releases (#12206) |
| [b2a28d159](https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32) | 2020-11-09  | Moves provider packages scripts to dev (#12082)                    |
| [4e8f9cc8d](https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68) | 2020-11-03  | Enable Black - Python Auto Formmatter (#9550)                      |
| [dd2442b1e](https://github.com/apache/airflow/commit/dd2442b1e66d4725e7193e0cab0548a4d8c71fbd) | 2020-11-02  | Vault with optional Variables or Connections (#11736)              |
| [5a439e84e](https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0) | 2020-10-26  | Prepare providers release 0.0.2a1 (#11855)                         |


### Release 0.0.2a1

| Commit                                                                                         | Committed   | Subject                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------|
| [872b1566a](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-25  | Generated backport providers readmes/setup for 2020.10.29 (#11826) |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | Add D200 pydocstyle check (#11688)                                 |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487)       |


### Release 0.0.1

| Commit                                                                                         | Committed   | Subject                                                                   |
|:-----------------------------------------------------------------------------------------------|:------------|:--------------------------------------------------------------------------|
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)                |
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                      |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                              |
| [9549274d1](https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9) | 2020-09-09  | Upgrade black to 20.8b1 (#10818)                                          |
| [3867f7662](https://github.com/apache/airflow/commit/3867f7662559761864ec4e7be26b776c64c2f199) | 2020-08-28  | Update Google Cloud branding (#10615)                                     |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                               |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                                   |
| [2f2d8dbfa](https://github.com/apache/airflow/commit/2f2d8dbfafefb4be3dd80f22f31c649c8498f148) | 2020-08-25  | Remove all &#34;noinspection&#34; comments native to IntelliJ (#10525)            |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)                |
| [2f31b3060](https://github.com/apache/airflow/commit/2f31b3060ed8274d5d1b1db7349ce607640b9199) | 2020-07-08  | Get Airflow configs with sensitive data from Secret Backends (#9645)      |
| [44d4ae809](https://github.com/apache/airflow/commit/44d4ae809c1e3784ff95b6a5e95113c3412e56b3) | 2020-07-06  | Upgrade to latest pre-commit checks (#9686)                               |
| [a99aaeb49](https://github.com/apache/airflow/commit/a99aaeb49672e913d5ff79606237f6f3614fc8f5) | 2020-07-03  | Allow setting Hashicorp Vault token from File (#9644)                     |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                            |
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)                |
| [df693e0e3](https://github.com/apache/airflow/commit/df693e0e3138f6601c4776cd529d8cb7bcde2f90) | 2020-06-19  | Add more authentication options for HashiCorp Vault classes (#8974)       |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)               |
| [d47e070a7](https://github.com/apache/airflow/commit/d47e070a79b574cca043ca9c06f91d47eecb3040) | 2020-06-17  | Add HashiCorp Vault Hook (split-out from Vault secret backend) (#9333)    |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                    |
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
