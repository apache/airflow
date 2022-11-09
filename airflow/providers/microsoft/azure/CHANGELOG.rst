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


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

Changelog
---------

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.3+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers
* In AzureFileShareHook, if both ``extra__azure_fileshare__foo`` and ``foo`` existed in connection extra
  dict, the prefixed version would be used; now, the non-prefixed version will be preferred.
* ``Remove deprecated classes (#27417)``

4.3.0
.....

Features
~~~~~~~~

* ``Add DataFlow operations to Azure DataFactory hook (#26345)``
* ``Add network_profile param in AzureContainerInstancesOperator (#26117)``
* ``Add Azure synapse operator (#26038)``
* ``Auto tail file logs in Web UI (#26169)``
* ``Implement Azure Service Bus Topic Create, Delete Operators (#25436)``

Bug Fixes
~~~~~~~~~

* ``Fix AzureBatchOperator false negative task status (#25844)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

4.2.0
.....

Features
~~~~~~~~

* ``Add 'test_connection' method to AzureContainerInstanceHook (#25362)``
* ``Add test_connection to Azure Batch hook (#25235)``
* ``Bump typing-extensions and mypy for ParamSpec (#25088)``
* ``Implement Azure Service Bus (Update and Receive) Subscription Operator (#25029)``
* ``Set default wasb Azure http logging level to warning; fixes #16224 (#18896)``

4.1.0
.....

Features
~~~~~~~~

* ``Add 'test_connection' method to AzureCosmosDBHook (#25018)``
* ``Add test_connection method to AzureFileShareHook (#24843)``
* ``Add test_connection method to Azure WasbHook (#24771)``
* ``Implement Azure service bus subscription Operators (#24625)``
* ``Implement Azure Service Bus Queue Operators (#24038)``

Bug Fixes
~~~~~~~~~

* ``Update providers to use functools compat for ''cached_property'' (#24582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

Features
~~~~~~~~

* ``Pass connection extra parameters to wasb BlobServiceClient (#24154)``


Misc
~~~~

* ``Apply per-run log templates to log handlers (#24153)``
* ``Migrate Microsoft example DAGs to new design #22452 - azure (#24141)``
* ``Add typing to Azure Cosmos Client Hook (#23941)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Clean up f-strings in logging calls (#23597)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

3.9.0
.....

Features
~~~~~~~~

* ``wasb hook: user defaultAzureCredentials instead of managedIdentity (#23394)``

Misc
~~~~

* ``Replace usage of 'DummyOperator' with 'EmptyOperator' (#22974)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump pre-commit hook versions (#22887)``
   * ``Fix new MyPy errors in main (#22884)``
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``

3.8.0
.....

Features
~~~~~~~~

* ``Update secrets backends to use get_conn_value instead of get_conn_uri (#22348)``

Misc
~~~~

* ``Docs: Fix example usage for 'AzureCosmosDocumentSensor' (#22735)``


3.7.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

3.7.1
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * `` Add map_index to XCom model and interface (#22112)``
   * ``Protect against accidental misuse of XCom.get_value() (#22244)``

3.7.0
.....

Features
~~~~~~~~

* ``Add 'test_connection' method to 'AzureDataFactoryHook' (#21924)``
* ``Add pre-commit check for docstring param types (#21398)``
* ``Make container creation configurable when uploading files via WasbHook (#20510)``

Misc
~~~~

* ``Support for Python 3.10``
* ``(AzureCosmosDBHook) Update to latest Cosmos API (#21514)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Change BaseOperatorLink interface to take a ti_key, not a datetime (#21798)``

3.6.0
.....

Features
~~~~~~~~

* ``Add optional features in providers. (#21074)``

Misc
~~~~

* ``Refactor operator links to not create ad hoc TaskInstances (#21285)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Remove all "fake" stub files (#20936)``
   * ``Explain stub files are introduced for Mypy errors in examples (#20827)``
   * ``Add documentation for January 2021 providers release (#21257)``

3.5.0
.....

Features
~~~~~~~~

* ``Azure: New sftp to wasb operator (#18877)``
* ``Removes InputRequired validation with azure extra (#20084)``
* ``Add operator link to monitor Azure Data Factory pipeline runs (#20207)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixing MyPy issues inside providers/microsoft (#20409)``
   * ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
   * ``Fix mypy errors in Microsoft Azure provider (#19923)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Use isort on pyi files (#20556)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Fix mypy errors in Google Cloud provider (#20611)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Update documentation for provider December 2021 release (#20523)``

3.4.0
.....

Features
~~~~~~~~

* ``Remove unnecessary connection form customizations in Azure (#19595)``
* ``Update Azure modules to comply with AIP-21 (#19431)``
* ``Remove 'host' from hidden fields in 'WasbHook' (#19475)``
* ``use DefaultAzureCredential if login not provided for Data Factory (#19079)``

Bug Fixes
~~~~~~~~~

* ``Fix argument error in AzureContainerInstancesOperator (#19668)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Ensure ''catchup=False'' is used in example dags (#19396)``

3.3.0
.....

Features
~~~~~~~~

* ``update azure cosmos to latest version (#18695)``
* ``Added sas_token var to BlobServiceClient return. Updated tests (#19234)``
* ``Add pre-commit hook for common misspelling check in files (#18964)``

Bug Fixes
~~~~~~~~~

* ``Fix changelog for Azure Provider (#18736)``

Other
~~~~~

* ``Expanding docs on client auth for AzureKeyVaultBackend (#18659)``
* ``Static start_date and default arg cleanup for Microsoft providers example DAGs (#19062)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``More f-strings (#18855)``
   * ``Revert 'update azure cosmos version (#18663)' (#18694)``
   * ``update azure cosmos version (#18663)``

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
