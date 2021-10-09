 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


Changelog
---------

3.2.0
.....

Features
~~~~~~~~

* ``Rename AzureDataLakeStorage to ADLS (#18493)``
* ``Creating ADF pipeline run operator, sensor + ADF custom conn fields (#17885)``
* ``Rename LocalToAzureDataLakeStorageOperator to LocalFilesystemToADLSOperator (#18168)``
* ``Rename FileToWasbOperator to LocalFilesystemToWasbOperator (#18109)``

Bug Fixes
~~~~~~~~~

* ``Fixed wasb hook attempting to create container when getting a blob client (#18287)``
* ``Removing redundant relabeling of password conn field (#18386)``
* ``Proper handling of Account URL custom conn field in AzureBatchHook (#18456)``
* ``Proper handling of custom conn field values in the AzureDataExplorerHook (#18203)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updating miscellaneous provider DAGs to use TaskFlow API where applicable (#18278)``

Main
....

Changes in operators names and import paths are listed in the following table
This is a backward compatible change. Deprecated operators will be removed in the next major release.

+------------------------------------+--------------------+---------------------------------------------------------+--------------------------------------------------+
| Deprecated operator name           | New operator name  | Deprecated path                                         | New path                                         |
+------------------------------------+--------------------+---------------------------------------------------------+--------------------------------------------------+
| AzureDataLakeStorageListOperator   | ADLSListOperator   | airflow.providers.microsoft.azure.operators.adls_list   | airflow.providers.microsoft.azure.operators.adls |
+------------------------------------+--------------------+---------------------------------------------------------+--------------------------------------------------+
| AzureDataLakeStorageDeleteOperator | ADLSDeleteOperator | airflow.providers.microsoft.azure.operators.adls_delete | airflow.providers.microsoft.azure.operators.adls |
+------------------------------------+--------------------+---------------------------------------------------------+--------------------------------------------------+

3.1.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``
* ``Adds secrets backend/logging/auth information to provider yaml (#17625)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

3.1.0
.....

Features
~~~~~~~~

* ``Add support for managed identity in WASB hook (#16628)``
* ``Reduce log messages for happy path (#16626)``

Bug Fixes
~~~~~~~~~

* ``Fix multiple issues in Microsoft AzureContainerInstancesOperator (#15634)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Removes pylint from our toolchain (#16682)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
   * ``Remove/refactor default_args pattern for Microsoft example DAGs (#16873)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

* ``Fixes AzureFileShare connection extras (#16388)``

``Azure Container Volume`` and ``Azure File Share`` have now dedicated connection types with editable
UI fields. You should not use ``Wasb`` connection type any more for those connections. Names of
connection ids for those hooks/operators were changed to reflect that.

Features
~~~~~~~~

* ``add oracle  connection link (#15632)``
* ``Add delimiter argument to WasbHook delete_file method (#15637)``

Bug Fixes
~~~~~~~~~

* ``Fix colon spacing in ``AzureDataExplorerHook`` docstring (#15841)``
* ``fix wasb remote logging when blob already exists (#16280)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
   * ``Rename example bucket names to use INVALID BUCKET NAME by default (#15651)``
   * ``Docs: Replace 'airflow' to 'apache-airflow' to install extra (#15628)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Removes unnecessary AzureContainerInstance connection type (#15514)``

This change removes ``azure_container_instance_default`` connection type and replaces it with the
``azure_default``. The problem was that AzureContainerInstance was not needed as it was exactly the
same as the plain "azure" connection, however it's presence caused duplication in the field names
used in the UI editor for connections and unnecessary warnings generated. This version uses
plain Azure Hook and connection also for Azure Container Instance. If you already have
``azure_container_instance_default`` connection created in your DB, it will continue to work, but
the first time you edit it with the UI you will have to change it's type to ``azure_default``.

Features
~~~~~~~~

* ``Add dynamic connection fields to Azure Connection (#15159)``

Bug fixes
~~~~~~~~~

* ``Fix 'logging.exception' redundancy (#14823)``


1.3.0
.....

Features
~~~~~~~~

* ``A bunch of template_fields_renderers additions (#15130)``

Bug fixes
~~~~~~~~~

* ``Fix attributes for AzureDataFactory hook (#14704)``

1.2.0
.....

Features
~~~~~~~~

* ``Add Azure Data Factory hook (#11015)``

Bug fixes
~~~~~~~~~

* ``BugFix: Fix remote log in azure storage blob displays in one line (#14313)``
* ``Fix AzureDataFactoryHook failing to instantiate its connection (#14565)``

1.1.0
.....

Updated documentation and readme files.

Features
~~~~~~~~

* ``Upgrade azure blob to v12 (#12188)``
* ``Fix Azure Data Explorer Operator (#13520)``
* ``add AzureDatalakeStorageDeleteOperator (#13206)``

1.0.0
.....

Initial version of the provider.
