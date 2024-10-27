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

``apache-airflow-providers-microsoft-azure``


Changelog
---------

Breaking changes
~~~~~~~~~~~~~~~~
.. warning::
   * We changed the message callback for Azure Service Bus messages to take two parameters, the message and the context, rather than just the message. This allows pushing message information into XComs. To upgrade from the previous version, which only took the message, please update your callback to take the context as a second parameter.


10.5.1
......

Bug Fixes
~~~~~~~~~

* ``(bugfix): Paginated results in MSGraphAsyncOperator (#42414)``

Misc
~~~~

* ``Workaround pin azure kusto data (#42576)``
* ``Removed conditional check for task context logging in airflow version 2.8.0 and above (#42764)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

10.5.0
......

Features
~~~~~~~~

* ``Allow custom api versions in MSGraphAsyncOperator (#41331)``
* `` Add callback to process Azure Service Bus message contents (#41601)``

Misc
~~~~

* ``remove deprecated soft_fail from providers (#41710)``
* ``Remove deprecated log handler argument filename_template (#41552)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

10.4.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Microsoft Power BI operator to refresh the dataset (#40356)``
* ``Export Azure Container Instance log messages to XCOM (#41142)``

Bug Fixes
~~~~~~~~~

* ``Fix mypy checks for new azure libraries (#41386)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

10.3.0
......

Features
~~~~~~~~

* ``Added priority to Azure Container Instances (#40616)``

Misc
~~~~

* ``Bump minimum version for azure containerinstance. (#40767)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

10.2.0
......

Features
~~~~~~~~

* ``Add S3ToAzureBlobStorageOperator (#40511)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

10.1.2
......

Bug Fixes
~~~~~~~~~

* ``Switch AzureDataLakeStorageV2Hook to use DefaultAzureCredential for managed identity/workload auth (#38497)``
* ``BUGFIX: Make sure XComs work correctly in MSGraphAsyncOperator with paged results and dynamic task mapping (#40301)``

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``

.. Review and move the new changes to one of the sections above:
   * ``Revert "refactor: Make sure xcoms work correctly in multi-threaded environmen…" (#40300)``
   * ``refactor: Make sure xcoms work correctly in multi-threaded environment by taking the map_index into account (#40297)``

10.1.1
......

Misc
~~~~

* ``Remove unused backward compatibility _read function in WasbTaskHandler (#39827)``
* ``Update example AzureContainerInstancesOperator (#39466)``

10.1.0
......

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``add dns_config and diagnostics parameters to AzureContainerInstancesOperator (#39156)``
* ``Add stacklevel into the 'AzureSynapsePipelineHook' deprecation warnings (#39192)``
* ``Adding MSGraphOperator in Microsoft Azure provider (#38111)``
* ``Make handling of connection by fs/adls.py closer to that of WasbHook and add unit tests. (#38747)``
* ``Implement run-method on KiotaRequestAdapterHook and move logic away from triggerer to hook (#39237)``
* ``Implemented MSGraphSensor as a deferrable sensor (#39304)``

Bug Fixes
~~~~~~~~~

* ``Fix: Only quote the keys of the query_parameters in MSGraphOperator (#39207)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``
* ``Reapply templates for all providers (#39554)``
* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Review and move the new changes to one of the sections above:
   * ``Prepare docs 1st wave May 2024 (#39328)``

10.0.0
......

.. warning::
   * We bumped the minimum version of azure-cosmos to 4.6.0, and providing a partition key is now required to create, get or delete a container and to get a document.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
   * ``azure_synapse_pipeline`` connection type has been changed to ``azure_synapse``.
   * The usage of ``default_conn_name=azure_synapse_connection`` is deprecated and will be removed in future. The new default connection name for ``AzureSynapsePipelineHook`` is: ``default_conn_name=azure_synapse_default``.

* ``Feature/refactor azure synapse pipeline class (#38723)``

Features
~~~~~~~~

* ``Add 'ADLSCreateObjectOperator' (#37821)``

Bug Fixes
~~~~~~~~~

* ``fix(microsoft/azure): add return statement to yield within a while loop in triggers (#38393)``
* ``fix cosmos hook static checks by making providing partition_key mandatory (#38199)``

Misc
~~~~

* ``refactor: Refactored __new__ magic method of BaseOperatorMeta to avoid bad mixing classic and decorated operators (#37937)``
* ``update to latest service bus (#38384)``
* ``Limit azure-cosmos (#38175)``

.. Review and move the new changes to one of the sections above:
   * ``fix: try002 for provider microsoft azure (#38805)``
   * ``Bump ruff to 0.3.3 (#38240)``

9.0.1
.....

Bug Fixes
~~~~~~~~~

* ``fix: Pass proxies config when using ClientSecretCredential in AzureDataLakeStorageV2Hook (#37103)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add comment about versions updated by release manager (#37488)``
   * ``D401 Support in Microsoft providers (#37327)``

9.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
   In this version of the provider, ``include`` and ``delimiter`` params have been removed from
   ``WasbPrefixSensorTrigger``. These params will now need to passed through ``check_options`` param

* ``Fix WasbPrefixSensor arg inconsistency between sync and async mode (#36806)``
* ``add WasbPrefixSensorTrigger params breaking change to azure provider changelog (#36940)``

Bug Fixes
~~~~~~~~~

* ``Fix failed tasks are not detected in 'AzureBatchHook' (#36785)``
* ``Fix assignment of template field in '__init__' in 'container_instances.py' (#36529)``

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

.. Review and move the new changes to one of the sections above:
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Fix stacklevel in warnings.warn into the providers (#36831)``
   * ``Standardize airflow build process and switch to Hatchling build backend (#36537)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Add docs for RC2 wave of providers for 2nd round of Jan 2024 (#37019)``

8.5.1
.....

Misc
~~~~

* ``Remove unused '_parse_version' function (#36450)``
* ``Clean WASB task handler code after bumping min Airflow version to 2.6.0 (#36421)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

8.5.0
.....

Features
~~~~~~~~

* ``Allow storage options to be passed (#35820)``

Bug Fixes
~~~~~~~~~

* ``azurefilesharehook fix with connection type azure (#36309)``
* ``Follow BaseHook connection fields method signature in child classes (#36086)``

Misc
~~~~

* ``Add code snippet formatting in docstrings via Ruff (#36262)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

8.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Fix reraise outside of try block in 'AzureSynapsePipelineRunLink.get_fields_from_url' (#36009)``
* ``Do not catch too broad exception in 'WasbHook.delete_container' (#36034)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add feature to build "chicken-egg" packages from sources (#35890)``

8.3.0
.....

Features
~~~~~~~~

* ``Add Azure Synapse Pipeline connection-type in the UI (#35709)``
* ``Add task context logging feature to allow forwarding messages to task logs (#32646)``
* ``Add operator to invoke Azure-Synapse pipeline (#35091)``
* ``Extend task context logging support for remote logging using WASB (Azure Blob Storage) (#32972)``

Misc
~~~~

* ``Check attr on parent not self re TaskContextLogger set_context (#35780)``
* ``Remove backcompat with Airflow 2.3/2.4 in providers (#35727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Use reproducible builds for provider packages (#35693)``

8.2.0
.....

Features
~~~~~~~~

* ``add managed identity support to AsyncDefaultAzureCredential (#35394)``
* ``feat(provider/azure): add managed identity support to container_registry hook (#35320)``
* ``feat(provider/azure): add managed identity support to wasb hook (#35326)``
* ``feat(provider/azure): add managed identity support to asb hook (#35324)``
* ``feat(provider/azure): add managed identity support to cosmos hook (#35323)``
* ``feat(provider/azure): add managed identity support to container_volume hook (#35321)``
* ``feat(provider/azure): add managed identity support to container_instance hook (#35319)``
* ``feat(provider/azure): add managed identity support to adx hook (#35325)``
* ``feat(provider/azure): add managed identity support to batch hook (#35327)``
* ``feat(provider/azure): add managed identity support to data_factory hook (#35328)``
* ``feat(provider/azure): add managed identity support to synapse hook (#35329)``
* ``feat(provider/azure): add managed identity support to fileshare hook (#35330)``

Bug Fixes
~~~~~~~~~

* ``Fix AzureContainerInstanceOperator remove_on_error (#35212)``
* ``fix(providers/microsoft): setting use_async=True for get_async_default_azure_credential (#35432)``


Misc
~~~~

* ``Remove empty TYPE_CHECKING block into the Azure provider (#35477)``
* ``Refactor azure managed identity (#35367)``
* ``Reuse get_default_azure_credential method from Azure utils method (#35318)``
* `` make DefaultAzureCredential configurable in AzureKeyVaultBackend (#35052)``
* ``Make DefaultAzureCredential in AzureBaseHook configuration (#35051)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch from Black to Ruff formatter (#35287)``

8.1.0
.....

Features
~~~~~~~~

* ``AIP-58: Add Airflow ObjectStore (AFS) (#34729)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Upgrade pre-commits (#35033)``

8.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
   In this version of the provider, we have removed network_profile param from AzureContainerInstancesOperator and
   AzureDataFactoryHook methods and AzureDataFactoryRunPipelineOperator arguments resource_group_name and factory_name
   is now required instead of kwargs

* resource_group_name and factory_name is now required argument in AzureDataFactoryHook method get_factory, update_factory,
  create_factory, delete_factory, get_linked_service, delete_linked_service, get_dataset, delete_dataset, get_dataflow,
  update_dataflow, create_dataflow, delete_dataflow, get_pipeline, delete_pipeline, run_pipeline, get_pipeline_run,
  get_trigger, get_pipeline_run_status, cancel_pipeline_run, create_trigger, delete_trigger, start_trigger,
  stop_trigger, get_adf_pipeline_run_status, cancel_pipeline_run
* resource_group_name and factory_name is now required in AzureDataFactoryRunPipelineOperator
* Remove class ``PipelineRunInfo`` from ``airflow.providers.microsoft.azure.hooks.data_factory``
* Remove ``network_profile`` param from ``AzureContainerInstancesOperator``
* Remove deprecated ``extra__azure__tenantId`` from azure_container_instance connection extras
* Remove deprecated ``extra__azure__subscriptionId`` from azure_container_instance connection extras


* ``Bump azure-mgmt-containerinstance (#34738)``
* ``Upgrade azure-mgmt-datafactory in microsift azure provider (#34040)``

Features
~~~~~~~~

* ``Add subnet_ids param in AzureContainerInstancesOperator (#34850)``
* ``allow providing credentials through keyword argument in AzureKeyVaultBackend (#34706)``

Bug Fixes
~~~~~~~~~

* ``Name params while invoking ClientSecretCredential (#34732)``
* ``fix(providers/microsoft-azure): respect soft_fail argument when exception is raised (#34494)``
* ``Error handling for when Azure container log cannot be read in properly. (#34627)``
* ``Fix hardcoded container name in remote logging option for Azure Blob Storage (#32779)``

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Consolidate hook management in AzureBatchOperator (#34437)``
* ``Consolidate hook management in AzureDataExplorerQueryOperator (#34436)``

.. Review and move the new changes to one of the sections above:
   * ``Refactor: consolidate import time in providers (#34402)``
   * ``Refactor usage of str() in providers (#34320)``
   * ``Refactor: reduce some conditions in providers (#34440)``

7.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  In this version of the provider, we have changed AzureFileShareHook to use azure-storage-file-share library instead
  of azure-storage-file this change has impact on existing hook method see below for details, removed deprecated
  extra__azure_fileshare__ prefix from connection extras param and removed protocol param from connection extras

* get_conn from AzureFileShareHook return None instead FileService
* Remove protocol param from Azure fileshare connection extras
* Remove deprecated extra__azure_fileshare__ prefix from Azure fileshare connection extras, list_files
* Remove share_name, directory_name param from AzureFileShareHook method check_for_directory,
  list_directories_and_files, create_directory in favor of AzureFileShareHook share_name and directory_path param
* AzureFileShareHook method create_share and delete_share accept kwargs from ShareServiceClient.create_share
  and ShareServiceClient.delete_share
* Remove share_name, directory_name, file_name param from AzureFileShareHook method get_file, get_file_to_stream
  and load_file in favor of AzureFileShareHook share_name and file_path
* Remove AzureFileShareHook.check_for_file method
* Remove AzureFileShareHook.load_string, AzureFileShareHook.load_stream in favor of AzureFileShareHook.load_data

.. note::
  ``LocalToAzureDataLakeStorageOperator`` class has been removed in favor of ``LocalFilesystemToADLSOperator``
  ``AzureDataFactoryPipelineRunStatusAsyncSensor`` class has been removed in favor of ``AzureDataFactoryPipelineRunStatusSensor``

* ``Update Azure fileshare hook to use azure-storage-file-share instead of azure-storage-file (#33904)``
* ``Remove 'AzureDataFactoryPipelineRunStatusAsyncSensor' class (#34036)``
* ``Remove 'LocalToAzureDataLakeStorageOperator' class (#34035)``

Features
~~~~~~~~

* ``feat(providers/microsoft): add AzureContainerInstancesOperator.volume as template field (#34070)``
* ``Add DefaultAzureCredential support to AzureContainerRegistryHook (#33825)``
* ``feat(providers/microsoft): add DefaultAzureCredential support to AzureContainerVolumeHook (#33822)``

Misc
~~~~

* ``Refactor regex in providers (#33898)``
* ``Improve docs on AzureBatchHook DefaultAzureCredential support (#34098)``
* ``Remove  azure-storage-common from microsoft azure providers (#34038)``
* ``Remove useless string join from providers (#33968)``
* ``Refactor unneeded  jumps in providers (#33833)``


6.3.0
.....

Features
~~~~~~~~

* ``Add AzureBatchOperator example (#33716)``
* ``feat(providers/microsoft): add DefaultAzureCredential support to AzureContainerInstanceHook (#33467)``
* ``Add DefaultAzureCredential auth for ADX service (#33627)``
* ``feat(providers/microsoft): add DefaultAzureCredential to data_lake (#33433)``
* ``Allow passing fully_qualified_namespace and credential to initialize Azure Service Bus Client (#33493)``
* ``Add DefaultAzureCredential support to cosmos (#33436)``
* ``Add DefaultAzureCredential support to AzureBatchHook (#33469)``

Bug Fixes
~~~~~~~~~

* ``Fix updating account url for WasbHook (#33457)``
* ``Fix Azure Batch Hook instantation (#33731)``
* ``Truncate Wasb storage account name if it's more than 24 characters (#33851)``
* ``Remove duplicated message commit in Azure MessageHook (#33776)``
* ``fix(providers/azure): remove json.dumps when querying AzureCosmosDBHook (#33653)``

Misc
~~~~

* ``Refactor: Remove useless str() calls (#33629)``
* ``Bump azure-kusto-data>=4.1.0 (#33598)``
* ``Simplify conditions on len() in providers/microsoft (#33566)``
* ``Set logging level to WARNING (#33314)``
* ``Simplify 'X for X in Y' to 'Y' where applicable (#33453)``
* ``Bump azure-mgmt-containerinstance>=7.0.0,<9.0.0 (#33696)``
* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
* ``Use a single  statement with multiple contexts instead of nested  statements in providers (#33768)``
* ``remove unnecessary and rewrite it using list in providers (#33763)``
* ``Optimise Airflow DB backend usage in Azure Provider (#33750)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix typos (double words and it's/its) (#33623)``
   * ``Further improvements for provider verification (#33670)``
   * ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
   * ``Move Azure examples into system tests (#33727)``

6.2.4
.....

Misc
~~~~~

* ``Clean microsoft azure provider by deleting the custom prefix from conn extra fields (#30558)``

6.2.3
.....

Misc
~~~~

* ``Refactor account_url use in WasbHook (#32980)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete azure cosmos DB sensor example_dag (#32906)``
   * ``Add issue link for TODO wrt Azure integration pinned dependencies (#33064)``

6.2.2
.....

Misc
~~~~

* ``Add Redis task handler (#31855)``
* ``Add deprecation info to the providers modules and classes docstring (#32536)``

6.2.1
.....

.. note::
  Note: this version contains a fix to ``get_blobs_list_async`` method in ``WasbHook`` where it returned
  a list of blob names, but advertised (via type hints) that it returns a list of ``BlobProperties`` objects.
  This was a bug in the implementation and it was fixed in this release. However, if you were relying on the
  previous behaviour, you might need to retrieve ``name`` property from the array elements returned by
  this method.

Bug Fixes
~~~~~~~~~

* ``Fix breaking change when Active Directory ID is used as host in WASB (#32560)``
* ``Fix get_blobs_list_async method to return BlobProperties (#32545)``

Misc
~~~~

* ``Moves 'AzureBlobStorageToGCSOperator' from Azure to Google provider (#32306)``

.. Review and move the new changes to one of the sections above:
   * ``D205 Support - Providers: Stragglers and new additions (#32447)``

6.2.0
.....

Features
~~~~~~~~

* ``Adds connection test for ADLS Gen2  (#32126)``
* ``Add option to pass extra configs to ClientSecretCredential  (#31783)``
* ``Added 'AzureBlobStorageToS3Operator' transfer operator (#32270)``

Bug Fixes
~~~~~~~~~

* ``Cancel pipeline if unexpected exception caught (#32238)``
* ``Fix where account url is build if not provided using login (account name) (#32082)``
* ``refresh connection if an exception is caught in "AzureDataFactory" (#32323)``

Misc
~~~~

* ``Doc changes: Added Transfers section in Azure provider docs (#32241)``
* ``Adds Sensor section in the Azure providers docs  (#32299)``
* ``Add default_deferrable config (#31712)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improve provider documentation and README structure (#32125)``
   * ``invalid args fix (#32326)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: GRPC to Oracle (inclusive) (#32357)``

6.1.2
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Replace unicodecsv with standard csv library (#31693)``
* ``Removed unused variables in AzureBlobStorageToGCSOperator (#31765)``
* ``Remove Python 3.7 support (#30963)``
* ``Add docstring and signature for _read_remote_logs (#31623)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Replace spelling directive with spelling:word-list (#31752)``
   * ``Add D400 pydocstyle check - Microsoft provider only (#31425)``
   * ``Add discoverability for triggers in provider.yaml (#31576)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``
   * ``Microsoft provider docstring improvements (#31708)``

6.1.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix deferrable mode execution in WasbPrefixSensor (#31411)``

Misc
~~~~

* ``Optimize deferred mode execution for wasb sensors (#31009)``

6.1.0
.....
.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add deferrable mode to 'WasbPrefixSensor' (#30252)``

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``
* ``Optimize deferrable execution mode 'AzureDataFactoryPipelineRunStatusSensor' (#30983)``
* ``Optimize deferred execution for AzureDataFactoryRunPipelineOperator (#31214)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move TaskInstanceKey to a separate file (#31033)``
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Upgrade ruff to 0.0.262 (#30809)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

6.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  In this version of the provider, deprecated GCS hook's param ``delegate_to`` is removed from ``AzureBlobStorageToGCSOperator``.
  Impersonation can be achieved instead by utilizing the ``impersonation_chain`` param.

* ``remove delegate_to from GCP operators and hooks (#30748)``

Misc
~~~~

* ``Merge WasbBlobAsyncSensor to WasbBlobSensor (#30488)``

5.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix AzureDataFactoryPipelineRunLink get_link method (#30514)``
* ``Load subscription_id from extra__azure__subscriptionId (#30556)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add mechanism to suspend providers (#30422)``
   * ``Prepare docs for ad hoc release of Providers (#30545)``

5.3.0
.....

Features
~~~~~~~~

* ``Add deferrable 'AzureDataFactoryRunPipelineOperator' (#30147)``
* ``Add deferrable 'AzureDataFactoryPipelineRunStatusSensor' (#29801)``
* ``Support deleting the local log files when using remote logging (#29772)``

Bug Fixes
~~~~~~~~~

* ``Fix ADF job failure during deferral (#30248)``
* ``Fix AzureDataLakeStorageV2Hook 'account_url' with Active Directory authentication (#29980) (#29981)``

Misc
~~~~

* ``merge AzureDataFactoryPipelineRunStatusAsyncSensor to AzureDataFactoryPipelineRunStatusSensor (#30250)``
* ``Expose missing params in AzureSynapseHook API docs (#30099)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``organize azure provider.yaml (#30155)``

5.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Handle deleting more than 256 blobs using 'WasbHook.delete_file()' (#29565)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Restore trigger logging (#29482)``
   * ``Revert "Enable individual trigger logging (#27758)" (#29472)``

5.2.0
.....

Features
~~~~~~~~

* ``Enable individual trigger logging (#27758)``

Bug Fixes
~~~~~~~~~

* ``Fix params rendering in AzureSynapseHook Python API docs (#29041)``

Misc
~~~~

* ``Deprecate 'delegate_to' param in GCP operators and update docs (#29088)``

5.1.0
.....

Features
~~~~~~~~

* ``Add hook for Azure Data Lake Storage Gen2 (#28262)``

Bug Fixes
~~~~~~~~~

* ``Hide 'extra' field in WASB connection form (#28914)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch to ruff for faster static checks (#28893)``

5.0.2
.....

Misc
~~~~

* ``Re-enable azure service bus on ARM as it now builds cleanly (#28442)``

5.0.1
.....


Bug Fixes
~~~~~~~~~

* ``Make arguments 'offset' and 'length' not required (#28234)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):


5.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

* In AzureFileShareHook, if both ``extra__azure_fileshare__foo`` and ``foo`` existed in connection extra
  dict, the prefixed version would be used; now, the non-prefixed version will be preferred.
* ``Remove deprecated classes (#27417)``
* In Azure Batch ``vm_size`` and ``vm_node_agent_sku_id`` parameters are required.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``Add azure, google, authentication library limits to eaager upgrade (#27535)``
* ``Allow and prefer non-prefixed extra fields for remaining azure (#27220)``
* ``Allow and prefer non-prefixed extra fields for AzureFileShareHook (#27041)``
* ``Allow and prefer non-prefixed extra fields for AzureDataExplorerHook (#27219)``
* ``Allow and prefer non-prefixed extra fields for AzureDataFactoryHook (#27047)``
* ``Update WasbHook to reflect preference for unprefixed extra (#27024)``
* ``Look for 'extra__' instead of 'extra_' in 'get_field' (#27489)``

Bug Fixes
~~~~~~~~~

* ``Fix Azure Batch errors revealed by added typing to azure batch lib (#27601)``
* ``Fix separator getting added to variables_prefix when empty (#26749)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
  * ``Upgrade dependencies in order to avoid backtracking (#27531)``
  * ``Suppress any Exception in wasb task handler (#27495)``
  * ``Update old style typing (#26872)``
  * ``Enable string normalization in python formatting - providers (#27205)``
  * ``Update azure-storage-blob version (#25426)``


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

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

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
same as the plain "azure" connection, however its presence caused duplication in the field names
used in the UI editor for connections and unnecessary warnings generated. This version uses
plain Azure Hook and connection also for Azure Container Instance. If you already have
``azure_container_instance_default`` connection created in your DB, it will continue to work, but
the first time you edit it with the UI you will have to change its type to ``azure_default``.

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
