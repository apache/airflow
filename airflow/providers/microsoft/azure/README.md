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


# Package apache-airflow-backport-providers-microsoft-azure

Release: 2020.10.5

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
        - [Moved operators](#moved-operators)
    - [Transfer operators](#transfer-operators)
        - [Moved transfer operators](#moved-transfer-operators)
    - [Sensors](#sensors)
        - [New sensors](#new-sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
        - [Moved hooks](#moved-hooks)
    - [Secrets](#secrets)
        - [New secrets](#new-secrets)
- [Releases](#releases)
    - [Release 2020.10.5](#release-2020105)
    - [Release 2020.6.24](#release-2020624)

## Backport package

This is a backport providers package for `microsoft.azure` provider. All classes for this provider package
are in `airflow.providers.microsoft.azure` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-microsoft-azure`

## PIP requirements

| PIP package                  | Version required   |
|:-----------------------------|:-------------------|
| azure-batch                  | &gt;=8.0.0            |
| azure-cosmos                 | &gt;=3.0.1,&lt;4         |
| azure-datalake-store         | &gt;=0.0.45           |
| azure-identity               | &gt;=1.3.1            |
| azure-keyvault               | &gt;=4.1.0            |
| azure-kusto-data             | &gt;=0.0.43,&lt;0.1      |
| azure-mgmt-containerinstance | &gt;=1.5.0,&lt;2.0       |
| azure-mgmt-datalake-store    | &gt;=0.5.0            |
| azure-mgmt-resource          | &gt;=2.2.0            |
| azure-storage                | &gt;=0.34.0, &lt;0.37.0  |
| azure-storage-blob           | &lt;12.0              |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-microsoft-azure[oracle]
```

| Dependent package                                                                                                  | Extra   |
|:-------------------------------------------------------------------------------------------------------------------|:--------|
| [apache-airflow-backport-providers-oracle](https://github.com/apache/airflow/tree/master/airflow/providers/oracle) | oracle  |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `microsoft.azure` provider
are in the `airflow.providers.microsoft.azure` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.microsoft.azure` package                                                                               |
|:-----------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.adx.AzureDataExplorerQueryOperator](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/operators/adx.py)     |
| [operators.azure_batch.AzureBatchOperator](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/operators/azure_batch.py) |


### Moved operators

| Airflow 2.0 operators: `airflow.providers.microsoft.azure` package                                                                                                                            | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                                                |
|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.adls_list.AzureDataLakeStorageListOperator](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/operators/adls_list.py)                                | [contrib.operators.adls_list_operator.AzureDataLakeStorageListOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/adls_list_operator.py)                                |
| [operators.azure_container_instances.AzureContainerInstancesOperator](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/operators/azure_container_instances.py) | [contrib.operators.azure_container_instances_operator.AzureContainerInstancesOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/azure_container_instances_operator.py) |
| [operators.azure_cosmos.AzureCosmosInsertDocumentOperator](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/operators/azure_cosmos.py)                         | [contrib.operators.azure_cosmos_operator.AzureCosmosInsertDocumentOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/azure_cosmos_operator.py)                         |
| [operators.wasb_delete_blob.WasbDeleteBlobOperator](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/operators/wasb_delete_blob.py)                            | [contrib.operators.wasb_delete_blob_operator.WasbDeleteBlobOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/wasb_delete_blob_operator.py)                            |


## Transfer operators



### Moved transfer operators

| Airflow 2.0 transfers: `airflow.providers.microsoft.azure` package                                                                                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                                              |
|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [transfers.file_to_wasb.FileToWasbOperator](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/transfers/file_to_wasb.py)                                      | [contrib.operators.file_to_wasb.FileToWasbOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/file_to_wasb.py)                                                        |
| [transfers.oracle_to_azure_data_lake.OracleToAzureDataLakeOperator](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/transfers/oracle_to_azure_data_lake.py) | [contrib.operators.oracle_to_azure_data_lake_transfer.OracleToAzureDataLakeOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/oracle_to_azure_data_lake_transfer.py) |


## Sensors


### New sensors

| New Airflow 2.0 sensors: `airflow.providers.microsoft.azure` package                                                                                      |
|:----------------------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.azure_cosmos.AzureCosmosDocumentSensor](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/sensors/azure_cosmos.py) |


### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.microsoft.azure` package                                                                 | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                               |
|:---------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.wasb.WasbBlobSensor](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/sensors/wasb.py)   | [contrib.sensors.wasb_sensor.WasbBlobSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/wasb_sensor.py)   |
| [sensors.wasb.WasbPrefixSensor](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/sensors/wasb.py) | [contrib.sensors.wasb_sensor.WasbPrefixSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/wasb_sensor.py) |


## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.microsoft.azure` package                                                                                  |
|:----------------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.adx.AzureDataExplorerHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/adx.py)                     |
| [hooks.azure_batch.AzureBatchHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_batch.py)            |
| [hooks.azure_data_lake.AzureDataLakeHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_data_lake.py) |
| [hooks.base_azure.AzureBaseHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/base_azure.py)               |


### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.microsoft.azure` package                                                                                                                 | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                         |
|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.azure_container_instance.AzureContainerInstanceHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_container_instance.py) | [contrib.hooks.azure_container_instance_hook.AzureContainerInstanceHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/azure_container_instance_hook.py) |
| [hooks.azure_container_registry.AzureContainerRegistryHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_container_registry.py) | [contrib.hooks.azure_container_registry_hook.AzureContainerRegistryHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/azure_container_registry_hook.py) |
| [hooks.azure_container_volume.AzureContainerVolumeHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_container_volume.py)       | [contrib.hooks.azure_container_volume_hook.AzureContainerVolumeHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/azure_container_volume_hook.py)       |
| [hooks.azure_cosmos.AzureCosmosDBHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_cosmos.py)                                  | [contrib.hooks.azure_cosmos_hook.AzureCosmosDBHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/azure_cosmos_hook.py)                                  |
| [hooks.azure_fileshare.AzureFileShareHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_fileshare.py)                           | [contrib.hooks.azure_fileshare_hook.AzureFileShareHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/azure_fileshare_hook.py)                           |
| [hooks.wasb.WasbHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/wasb.py)                                                           | [contrib.hooks.wasb_hook.WasbHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/wasb_hook.py)                                                           |


## Secrets


### New secrets

| New Airflow 2.0 secrets: `airflow.providers.microsoft.azure` package                                                                                       |
|:-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| [secrets.azure_key_vault.AzureKeyVaultBackend](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/secrets/azure_key_vault.py) |




## Releases

### Release 2020.10.5

| Commit                                                                                         | Committed   | Subject                                                                     |
|:-----------------------------------------------------------------------------------------------|:------------|:----------------------------------------------------------------------------|
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                        |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                                |
| [5093245d6](https://github.com/apache/airflow/commit/5093245d6f77a370fbd2f9e3df35ac6acf46a1c4) | 2020-09-30  | Strict type coverage for Oracle and Yandex provider  (#11198)               |
| [f3e87c503](https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc) | 2020-09-22  | Add D202 pydocstyle check (#11032)                                          |
| [f77a11d5b](https://github.com/apache/airflow/commit/f77a11d5b1e9d76b1d57c8a0d653b3ab28f33894) | 2020-09-13  | Add Secrets backend for Microsoft Azure Key Vault (#10898)                  |
| [9549274d1](https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9) | 2020-09-09  | Upgrade black to 20.8b1 (#10818)                                            |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                                 |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                                     |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)                  |
| [2f552233f](https://github.com/apache/airflow/commit/2f552233f5c99b206c8f4c2088fcc0c05e7e26dc) | 2020-08-21  | Add AzureBaseHook (#9747)                                                   |
| [cdec30125](https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a) | 2020-08-07  | Add correct signature to all operators and sensors (#10205)                 |
| [24c8e4c2d](https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832) | 2020-08-06  | Changes to all the constructors to remove the args argument (#10163)        |
| [aeea71274](https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f) | 2020-08-02  | Remove `args` parameter from provider operator constructors (#10097)        |
| [7d24b088c](https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d) | 2020-07-25  | Stop using start_date in default_args in example_dags (2) (#9985)           |
| [0bf330ba8](https://github.com/apache/airflow/commit/0bf330ba8681c417fd5a10b3ba01c75600dc5f2e) | 2020-07-24  | Add get_blobs_list method to WasbHook (#9950)                               |
| [33f0cd265](https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e) | 2020-07-22  | apply_default keeps the function signature for mypy (#9784)                 |
| [d3c76da95](https://github.com/apache/airflow/commit/d3c76da95250068161580036a86e26ee2790fa07) | 2020-07-12  | Improve type hinting to provider microsoft  (#9774)                         |
| [23f80f34a](https://github.com/apache/airflow/commit/23f80f34adec86da24e4896168c53d213d01a7f6) | 2020-07-08  | Move gcs &amp; wasb task handlers to their respective provider packages (#9714) |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                              |


### Release 2020.6.24

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)                                                                                                         |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)                                                                                                        |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                                                                                                             |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 release of backports (#9026)                                                                                                                      |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                                                                                                        |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                                                                                                       |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                                                                                            |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                                                                                                       |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                                                                                            |
| [87969a350](https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca) | 2020-04-09  | [AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)                                                                                                   |
| [d99833c9b](https://github.com/apache/airflow/commit/d99833c9b5be9eafc0c7851343ee86b6c20aed40) | 2020-04-03  | [AIRFLOW-4529] Add support for Azure Batch Service (#8024)                                                                                                         |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                                                                                                   |
| [a83eb335e](https://github.com/apache/airflow/commit/a83eb335e58c6a15e96c517a1b492bc79c869ce8) | 2020-03-23  | Add call to Super call in microsoft providers (#7821)                                                                                                              |
| [f0e242180](https://github.com/apache/airflow/commit/f0e24218077d4dff8015926d7826477bb0d07f88) | 2020-02-24  | [AIRFLOW-6896] AzureCosmosDBHook: Move DB call out of __init__ (#7520)                                                                                             |
| [4bec1cc48](https://github.com/apache/airflow/commit/4bec1cc489f5d19daf7450c75c3e8057c9709dbd) | 2020-02-24  | [AIRFLOW-6895] AzureFileShareHook: Move DB call out of __init__ (#7519)                                                                                            |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)                                                                                   |
| [086e30724](https://github.com/apache/airflow/commit/086e307245015d97e89af9aa6c677d6fe817264c) | 2020-02-23  | [AIRFLOW-6890] AzureDataLakeHook: Move DB call out of __init__ (#7513)                                                                                             |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [175a16046](https://github.com/apache/airflow/commit/175a1604638016b0a663711cc584496c2fdcd828) | 2020-02-19  | [AIRFLOW-6828] Stop using the zope library (#7448)                                                                                                                 |
| [1e0024301](https://github.com/apache/airflow/commit/1e00243014382d4cb7152ca7c5011b97cbd733b0) | 2020-02-10  | [AIRFLOW-5176] Add Azure Data Explorer (Kusto) operator (#5785)                                                                                                    |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                                                                                           |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)                                                                                                 |
| [057f3ae3a](https://github.com/apache/airflow/commit/057f3ae3a4afedf6d462ecf58b01dd6304d3e135) | 2020-01-29  | [AIRFLOW-6670][depends on AIRFLOW-6669] Move contrib operators to providers package (#7286)                                                                        |
| [290330ba6](https://github.com/apache/airflow/commit/290330ba60653686cc6f009d89a377f09f26f35a) | 2020-01-15  | [AIRFLOW-6552] Move Azure classes to providers.microsoft package (#7158)                                                                                           |
