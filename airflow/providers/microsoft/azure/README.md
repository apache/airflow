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

Release: 2020-06-23

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
        - [Moved operators](#moved-operators)
    - [Transfer operators](#transfers)
        - [Moved transfer operators](#moved-transfers)
    - [Sensors](#sensors)
        - [New sensors](#new-sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020-06-23](#release-2020-06-23)

## Backport package

This is a backport providers package for `microsoft.azure` provider. All classes for this provider package
are in `airflow.providers.microsoft.azure` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-microsoft-azure`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package                  | Version required   |
|:-----------------------------|:-------------------|
| azure-batch                  | &gt;=8.0.0            |
| azure-cosmos                 | &gt;=3.0.1,&lt;4         |
| azure-datalake-store         | &gt;=0.0.45           |
| azure-kusto-data             | &gt;=0.0.43,&lt;0.1      |
| azure-mgmt-containerinstance | &gt;=1.5.0            |
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


### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.microsoft.azure` package                                                                                                                 | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                         |
|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.azure_container_instance.AzureContainerInstanceHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_container_instance.py) | [contrib.hooks.azure_container_instance_hook.AzureContainerInstanceHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/azure_container_instance_hook.py) |
| [hooks.azure_container_registry.AzureContainerRegistryHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_container_registry.py) | [contrib.hooks.azure_container_registry_hook.AzureContainerRegistryHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/azure_container_registry_hook.py) |
| [hooks.azure_container_volume.AzureContainerVolumeHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_container_volume.py)       | [contrib.hooks.azure_container_volume_hook.AzureContainerVolumeHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/azure_container_volume_hook.py)       |
| [hooks.azure_cosmos.AzureCosmosDBHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_cosmos.py)                                  | [contrib.hooks.azure_cosmos_hook.AzureCosmosDBHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/azure_cosmos_hook.py)                                  |
| [hooks.azure_fileshare.AzureFileShareHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_fileshare.py)                           | [contrib.hooks.azure_fileshare_hook.AzureFileShareHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/azure_fileshare_hook.py)                           |
| [hooks.wasb.WasbHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/wasb.py)                                                           | [contrib.hooks.wasb_hook.WasbHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/wasb_hook.py)                                                           |






## Releases

### Release 2020-06-23

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                                                                                                             |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                                                                                                                      |
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
