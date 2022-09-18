
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


Package apache-airflow-providers-microsoft-azure
------------------------------------------------------

`Microsoft Azure <https://azure.microsoft.com/>`__


This is detailed commit list of changes for versions provider package: ``microsoft.azure``.
For high-level changelog, see :doc:`package information including changelog <index>`.



4.2.0
.....

Latest change: 2022-08-09

=================================================================================================  ===========  ===================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================
`d5f40d739f <https://github.com/apache/airflow/commit/d5f40d739fc583c50ae3b3f4b4bde29e61c8d81b>`_  2022-08-09   ``Set default wasb Azure http logging level to warning; fixes #16224 (#18896)``
`8bb0c4fd32 <https://github.com/apache/airflow/commit/8bb0c4fd32b21bf2900e33ec29b1dc7d772589c9>`_  2022-07-28   ``Add 'test_connection' method to AzureContainerInstanceHook (#25362)``
`eab0167f1b <https://github.com/apache/airflow/commit/eab0167f1beb81de8e613685da79ef9a04eef5b3>`_  2022-07-22   ``Add test_connection to Azure Batch hook (#25235)``
`e32e9c5880 <https://github.com/apache/airflow/commit/e32e9c58802fe9363cc87ea283a59218df7cec3a>`_  2022-07-18   ``Bump typing-extensions and mypy for ParamSpec (#25088)``
`292440d54f <https://github.com/apache/airflow/commit/292440d54f4db84aaf0c5a98cf5fcf34303f2fa8>`_  2022-07-14   ``Implement Azure Service Bus (Update and Receive) Subscription Operator (#25029)``
=================================================================================================  ===========  ===================================================================================

4.1.0
.....

Latest change: 2022-07-13

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`bfd506cbfc <https://github.com/apache/airflow/commit/bfd506cbfcf4561c2df87e5240d27787793813ce>`_  2022-07-13   ``Add 'test_connection' method to AzureCosmosDBHook (#25018)``
`aa8bf2cf85 <https://github.com/apache/airflow/commit/aa8bf2cf85d6a9df40de267672936f20fbac970d>`_  2022-07-12   ``Implement Azure service bus subscription Operators (#24625)``
`b27fc0367c <https://github.com/apache/airflow/commit/b27fc0367cd1125f4d312497ba5337115476315e>`_  2022-07-06   ``Add test_connection method to AzureFileShareHook (#24843)``
`f18c609d12 <https://github.com/apache/airflow/commit/f18c609d127f54fbbf4dae6b290c6cdcfc7f98d0>`_  2022-07-01   ``Add test_connection method to Azure WasbHook (#24771)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
`09f38ad3f6 <https://github.com/apache/airflow/commit/09f38ad3f6872bae5059a1de226362eb358c4a7a>`_  2022-06-23   ``Implement Azure Service Bus Queue Operators (#24038)``
`9c59831ee7 <https://github.com/apache/airflow/commit/9c59831ee78f14de96421c74986933c494407afa>`_  2022-06-21   ``Update providers to use functools compat for ''cached_property'' (#24582)``
=================================================================================================  ===========  =============================================================================

4.0.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`c23826915d <https://github.com/apache/airflow/commit/c23826915dcdca4f22b52b74633336cb2f4a1eca>`_  2022-06-07   ``Apply per-run log templates to log handlers (#24153)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`389e858d93 <https://github.com/apache/airflow/commit/389e858d934a7813c7f15ab4e46df33c5720e415>`_  2022-06-03   ``Pass connection extra parameters to wasb BlobServiceClient (#24154)``
`6e83885c95 <https://github.com/apache/airflow/commit/6e83885c954f781c5c64fcb6e7a0f5a9b113e717>`_  2022-06-03   ``Migrate Microsoft example DAGs to new design #22452 - azure (#24141)``
`3393647aa6 <https://github.com/apache/airflow/commit/3393647aa63cbfdd2e6b90b7a5c9971732a54fc2>`_  2022-05-26   ``Add typing to Azure Cosmos Client Hook (#23941)``
`ec6761a5c0 <https://github.com/apache/airflow/commit/ec6761a5c0d031221d53ce213c0e42813606c55d>`_  2022-05-23   ``Clean up f-strings in logging calls (#23597)``
=================================================================================================  ===========  ==================================================================================

3.9.0
.....

Latest change: 2022-05-12

=================================================================================================  ===========  ===============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================================
`75c60923e0 <https://github.com/apache/airflow/commit/75c60923e01375ffc5f71c4f2f7968f489e2ca2f>`_  2022-05-12   ``Prepare provider documentation 2022.05.11 (#23631)``
`8f181c1034 <https://github.com/apache/airflow/commit/8f181c10344bd319ac5f6aeb102ee3c06e1f1637>`_  2022-05-08   ``wasb hook: user defaultAzureCredentials instead of managedIdentity (#23394)``
`2d109401b3 <https://github.com/apache/airflow/commit/2d109401b3566aef613501691d18cf7e4c776cd2>`_  2022-05-04   ``Bump pre-commit hook versions (#22887)``
`8b6b0848a3 <https://github.com/apache/airflow/commit/8b6b0848a3cacf9999477d6af4d2a87463f03026>`_  2022-04-23   ``Use new Breese for building, pulling and verifying the images. (#23104)``
`49e336ae03 <https://github.com/apache/airflow/commit/49e336ae0302b386a2f47269a6d13988382d975f>`_  2022-04-13   ``Replace usage of 'DummyOperator' with 'EmptyOperator' (#22974)``
`6933022e94 <https://github.com/apache/airflow/commit/6933022e94acf139b2dea9a589bb8b25c62a5d20>`_  2022-04-10   ``Fix new MyPy errors in main (#22884)``
=================================================================================================  ===========  ===============================================================================

3.8.0
.....

Latest change: 2022-04-07

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`56ab82ed7a <https://github.com/apache/airflow/commit/56ab82ed7a5c179d024722ccc697b740b2b93b6a>`_  2022-04-07   ``Prepare mid-April provider documentation. (#22819)``
`d3976d9b20 <https://github.com/apache/airflow/commit/d3976d9b20163550dbfe8cf9b326260516fd9bb8>`_  2022-04-04   ``Docs: Fix example usage for 'AzureCosmosDocumentSensor' (#22735)``
`7ab45d41d6 <https://github.com/apache/airflow/commit/7ab45d41d6c4de322dc8afe8a74b712d0bae4ee7>`_  2022-03-24   ``Update secrets backends to use get_conn_value instead of get_conn_uri (#22348)``
=================================================================================================  ===========  ==================================================================================

3.7.2
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
=================================================================================================  ===========  ==============================================================

3.7.1
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
`c1ab8e2d7b <https://github.com/apache/airflow/commit/c1ab8e2d7b68a31408e750129592e16432474512>`_  2022-03-14   ``Protect against accidental misuse of XCom.get_value() (#22244)``
`d08284ed25 <https://github.com/apache/airflow/commit/d08284ed251b7c5712190181623b500a38cd640d>`_  2022-03-11   `` Add map_index to XCom model and interface (#22112)``
=================================================================================================  ===========  ====================================================================

3.7.0
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ===================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`ba79adb631 <https://github.com/apache/airflow/commit/ba79adb6318d783807dead86cf209f5f7d6f0452>`_  2022-03-02   ``Make container creation configurable when uploading files via WasbHook (#20510)``
`f42559a773 <https://github.com/apache/airflow/commit/f42559a773ed51c96ce27bad1d87c4d49bb40d4b>`_  2022-03-02   ``Add 'test_connection' method to 'AzureDataFactoryHook' (#21924)``
`08575ddd8a <https://github.com/apache/airflow/commit/08575ddd8a72f96a3439f73e973ee9958188eb83>`_  2022-03-01   ``Change BaseOperatorLink interface to take a ti_key, not a datetime (#21798)``
`3c4524b4ec <https://github.com/apache/airflow/commit/3c4524b4ec2b42a8af0a8c7b9d8f1d065b2bfc83>`_  2022-02-23   ``(AzureCosmosDBHook) Update to latest Cosmos API (#21514)``
`0a3ff43d41 <https://github.com/apache/airflow/commit/0a3ff43d41d33d05fb3996e61785919effa9a2fa>`_  2022-02-08   ``Add pre-commit check for docstring param types (#21398)``
=================================================================================================  ===========  ===================================================================================

3.6.0
.....

Latest change: 2022-02-08

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`ddb5246bd1 <https://github.com/apache/airflow/commit/ddb5246bd1576e2ce6abf8c80c3328d7d71a75ce>`_  2022-02-03   ``Refactor operator links to not create ad hoc TaskInstances (#21285)``
`cb73053211 <https://github.com/apache/airflow/commit/cb73053211367e2c2dd76d5279cdc7dc7b190124>`_  2022-01-27   ``Add optional features in providers. (#21074)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`730db3fb77 <https://github.com/apache/airflow/commit/730db3fb774f60127ab1c865e19031f1f9c193f7>`_  2022-01-18   ``Remove all "fake" stub files (#20936)``
`f8fd0f7b4c <https://github.com/apache/airflow/commit/f8fd0f7b4ca6cb52307be4323028bf4e409566e7>`_  2022-01-13   ``Explain stub files are introduced for Mypy errors in examples (#20827)``
=================================================================================================  ===========  ==========================================================================

3.5.0
.....

Latest change: 2021-12-31

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`a22d5bd076 <https://github.com/apache/airflow/commit/a22d5bd07696d9cafe10a3e246ea9f3a381585ee>`_  2021-12-31   ``Fix mypy errors in Google Cloud provider (#20611)``
`83f8e178ba <https://github.com/apache/airflow/commit/83f8e178ba7a3d4ca012c831a5bfc2cade9e812d>`_  2021-12-31   ``Even more typing in operators (template_fields/ext) (#20608)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`3299064958 <https://github.com/apache/airflow/commit/3299064958e5fbcfc8e91e905ababb18d7339421>`_  2021-12-29   ``Use isort on pyi files (#20556)``
`e63e23c582 <https://github.com/apache/airflow/commit/e63e23c582cd757ea6593bdb4dfde66d76a8c9f1>`_  2021-12-23   ``Fixing MyPy issues inside providers/microsoft (#20409)``
`341bf5ab1f <https://github.com/apache/airflow/commit/341bf5ab1f528a98fa2c7325113cfe425843cff1>`_  2021-12-22   ``Azure: New sftp to wasb operator (#18877)``
`05e4cd1c6a <https://github.com/apache/airflow/commit/05e4cd1c6a93ba96f9adbaf7973e7729697ca934>`_  2021-12-18   ``Add operator link to monitor Azure Data Factory pipeline runs (#20207)``
`2fb5e1d0ec <https://github.com/apache/airflow/commit/2fb5e1d0ec306839a3ff21d0bddbde1d022ee8c7>`_  2021-12-15   ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
`42f133c5f6 <https://github.com/apache/airflow/commit/42f133c5f63011399eb46ee6f046c401103cf546>`_  2021-12-06   ``Removes InputRequired validation with azure extra (#20084)``
`374574b8d0 <https://github.com/apache/airflow/commit/374574b8d0ef795855f8d2bb212ba6d653e62727>`_  2021-12-06   ``Fix mypy errors in Microsoft Azure provider (#19923)``
=================================================================================================  ===========  ==========================================================================

3.4.0
.....

Latest change: 2021-11-30

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`e25446a8b1 <https://github.com/apache/airflow/commit/e25446a8b19197e55989174f210e1c94ae5ff65f>`_  2021-11-18   ``Fix argument error in AzureContainerInstancesOperator (#19668)``
`11e73d2db1 <https://github.com/apache/airflow/commit/11e73d2db192e8abb551a728ca5c2d5dcf69d5d8>`_  2021-11-16   ``Remove unnecessary connection form customizations in Azure (#19595)``
`4212c49324 <https://github.com/apache/airflow/commit/4212c4932433a50bda09f3e771a02f5ded4553a7>`_  2021-11-14   ``Update Azure modules to comply with AIP-21 (#19431)``
`0f516458be <https://github.com/apache/airflow/commit/0f516458be079fd3d55204718978711acf06d3e6>`_  2021-11-08   ``Remove 'host' from hidden fields in 'WasbHook' (#19475)``
`ca679c014c <https://github.com/apache/airflow/commit/ca679c014cad86976c1b2e248b099d9dc9fc99eb>`_  2021-11-07   ``use DefaultAzureCredential if login not provided for Data Factory (#19079)``
`490a382ed6 <https://github.com/apache/airflow/commit/490a382ed6ce088bee650751b6409c510e19845a>`_  2021-11-04   ``Ensure ''catchup=False'' is used in example dags (#19396)``
=================================================================================================  ===========  ==============================================================================

3.3.0
.....

Latest change: 2021-10-29

=================================================================================================  ===========  ===========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================================
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`61d0093054 <https://github.com/apache/airflow/commit/61d009305478e76e53aaf43ce07a181ebbd259d3>`_  2021-10-27   ``Added sas_token var to BlobServiceClient return. Updated tests (#19234)``
`ceb2b53a10 <https://github.com/apache/airflow/commit/ceb2b53a109b8fdd617f725a72c6fdb9c119550b>`_  2021-10-20   ``Static start_date and default arg cleanup for Microsoft providers example DAGs (#19062)``
`86a2a19ad2 <https://github.com/apache/airflow/commit/86a2a19ad2bdc87a9ad14bb7fde9313b2d7489bb>`_  2021-10-17   ``More f-strings (#18855)``
`1571f80546 <https://github.com/apache/airflow/commit/1571f80546853688778c2a3ec5194e5c8be0edbd>`_  2021-10-14   ``Add pre-commit hook for common misspelling check in files (#18964)``
`1b75f9181f <https://github.com/apache/airflow/commit/1b75f9181f80062a2c25d2fdd627d4f4d2735811>`_  2021-10-05   ``Fix changelog for Azure Provider (#18736)``
`181ac36db3 <https://github.com/apache/airflow/commit/181ac36db3749050a60fc1f08ceace005c5cb58b>`_  2021-10-05   ``update azure cosmos to latest version (#18695)``
`6d504b43ea <https://github.com/apache/airflow/commit/6d504b43ea8d6c80be831c7830f4893727689404>`_  2021-10-04   ``Expanding docs on client auth for AzureKeyVaultBackend (#18659)``
`c8485a83bc <https://github.com/apache/airflow/commit/c8485a83bc58ad76fd112c8a53ee0c9c8e8f6663>`_  2021-10-03   ``Revert "update azure cosmos version (#18663)" (#18694)``
`10421c6931 <https://github.com/apache/airflow/commit/10421c693199eeea2c1ea54844319080fd6f7153>`_  2021-10-01   ``update azure cosmos version (#18663)``
=================================================================================================  ===========  ===========================================================================================

3.2.0
.....

Latest change: 2021-09-30

=================================================================================================  ===========  ========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================================
`840ea3efb9 <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`a458fcc573 <https://github.com/apache/airflow/commit/a458fcc573845ff65244a2dafd204ed70129f3e8>`_  2021-09-27   ``Updating miscellaneous provider DAGs to use TaskFlow API where applicable (#18278)``
`46484466c4 <https://github.com/apache/airflow/commit/46484466c43bd0a9c8b25f11f24d3d36a0b2d956>`_  2021-09-25   ``Removing redundant relabeling of password conn field (#18386)``
`97d6892318 <https://github.com/apache/airflow/commit/97d6892318ce2866f09f2c21247ed3b1b9975695>`_  2021-09-25   ``Rename AzureDataLakeStorage to ADLS (#18493)``
`1d2924c94e <https://github.com/apache/airflow/commit/1d2924c94e38ade7cd21af429c9f451c14eba183>`_  2021-09-24   ``Proper handling of Account URL custom conn field in AzureBatchHook (#18456)``
`11e34535e8 <https://github.com/apache/airflow/commit/11e34535e8cda2f22b26eb3f951a952e3acfe333>`_  2021-09-19   ``Creating ADF pipeline run operator, sensor + ADF custom conn fields (#17885)``
`410e6d7967 <https://github.com/apache/airflow/commit/410e6d7967c6db0a968f26eb903d072e356f1348>`_  2021-09-18   ``Initial commit (#18203)``
`2dac083ae2 <https://github.com/apache/airflow/commit/2dac083ae241b96241deda20db7725e2fcf3a93e>`_  2021-09-16   ``Fixed wasb hook attempting to create container when getting a blob client (#18287)``
`d119ae8f3f <https://github.com/apache/airflow/commit/d119ae8f3fec587f12ee90f4a698186ebe54458e>`_  2021-09-12   ``Rename LocalToAzureDataLakeStorageOperator to LocalFilesystemToADLSOperator (#18168)``
`28de326d61 <https://github.com/apache/airflow/commit/28de326d6192bcb4871d5c2ea85857b022aaabd5>`_  2021-09-09   ``Rename FileToWasbOperator to LocalFilesystemToWasbOperator (#18109)``
=================================================================================================  ===========  ========================================================================================

3.1.1
.....

Latest change: 2021-08-30

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`0a68588479 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
`29aab6434f <https://github.com/apache/airflow/commit/29aab6434ffe0fb8c83b6fd6c9e44310966d496a>`_  2021-08-17   ``Adds secrets backend/logging/auth information to provider yaml (#17625)``
=================================================================================================  ===========  ============================================================================

3.1.0
.....

Latest change: 2021-07-26

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`87f408b1e7 <https://github.com/apache/airflow/commit/87f408b1e78968580c760acb275ae5bb042161db>`_  2021-07-26   ``Prepares docs for Rc2 release of July providers (#17116)``
`48ca9374bf <https://github.com/apache/airflow/commit/48ca9374bfe4a0784b5eb9ec74c1e3262a833677>`_  2021-07-26   ``Remove/refactor default_args pattern for Microsoft example DAGs (#16873)``
`d02ded65ea <https://github.com/apache/airflow/commit/d02ded65eaa7d2281e249b3fa028605d1b4c52fb>`_  2021-07-15   ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
`b916b75079 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`866a601b76 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
`caf0a8499f <https://github.com/apache/airflow/commit/caf0a8499f6099c943b0dd5054a9480b2e046bf1>`_  2021-06-25   ``Add support for managed identity in WASB hook (#16628)``
`ffb1fcacff <https://github.com/apache/airflow/commit/ffb1fcacff21c31d7cacfbd843a84208fca38d1e>`_  2021-06-24   ``Fix multiple issues in Microsoft AzureContainerInstancesOperator (#15634)``
`a2a58d27ef <https://github.com/apache/airflow/commit/a2a58d27efaee515141d5e7cee373020b84acc2f>`_  2021-06-24   ``Reduce log messages for happy path (#16626)``
=================================================================================================  ===========  =============================================================================

3.0.0
.....

Latest change: 2021-06-18

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`bbc627a3da <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`cbf8001d76 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`1fba5402bb <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`0c80a7d411 <https://github.com/apache/airflow/commit/0c80a7d41100bf8d18b661c8286d6056e6d5d2f1>`_  2021-06-11   ``Fixes AzureFileShare connection extras (#16388)``
`29b7f795d6 <https://github.com/apache/airflow/commit/29b7f795d6fb9fb8cab14158905c1b141044236d>`_  2021-06-07   ``fix wasb remote logging when blob already exists (#16280)``
`9c94b72d44 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`476d0f6e3d <https://github.com/apache/airflow/commit/476d0f6e3d2059f56532cda36cdc51aa86bafb37>`_  2021-05-22   ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
`c844ff742e <https://github.com/apache/airflow/commit/c844ff742e786973273c56348a09d073a4928878>`_  2021-05-18   ``Fix colon spacing in ''AzureDataExplorerHook'' docstring (#15841)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
`3b4fdd0a7a <https://github.com/apache/airflow/commit/3b4fdd0a7a176bfb2e9a17d4627b1d4ed40f1c86>`_  2021-05-06   ``add oracle  connection link (#15632)``
`b1bd59440b <https://github.com/apache/airflow/commit/b1bd59440baa839eccdb2770145d0713ade4f82a>`_  2021-05-04   ``Add delimiter argument to WasbHook delete_file method (#15637)``
`0f97a3970d <https://github.com/apache/airflow/commit/0f97a3970d2c652beedbf2fbaa33e2b2bfd69bce>`_  2021-05-04   ``Rename example bucket names to use INVALID BUCKET NAME by default (#15651)``
`db557a8c4a <https://github.com/apache/airflow/commit/db557a8c4a3e1f0d67b2534010e5092be4f4a9fd>`_  2021-05-01   ``Docs: Replace 'airflow' to 'apache-airflow' to install extra (#15628)``
=================================================================================================  ===========  ==============================================================================

2.0.0
.....

Latest change: 2021-05-01

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`657384615f <https://github.com/apache/airflow/commit/657384615fafc060f9e2ed925017306705770355>`_  2021-04-27   ``Fix 'logging.exception' redundancy (#14823)``
`d65e492a3e <https://github.com/apache/airflow/commit/d65e492a3ee43b198c5082b40cab011b15595d12>`_  2021-04-25   ``Removes unnecessary AzureContainerInstance connection type (#15514)``
`cb1344b63d <https://github.com/apache/airflow/commit/cb1344b63d6650de537320460b7b0547efd2353c>`_  2021-04-16   ``Update azure connection documentation (#15352)``
`1a85ba9e93 <https://github.com/apache/airflow/commit/1a85ba9e93d44601a322546e31814bd9ef11c125>`_  2021-04-13   ``Add dynamic connection fields to Azure Connection (#15159)``
=================================================================================================  ===========  =======================================================================

1.3.0
.....

Latest change: 2021-04-06

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`042be2e4e0 <https://github.com/apache/airflow/commit/042be2e4e06b988f5ba2dc146f53774dabc8b76b>`_  2021-04-06   ``Updated documentation for provider packages before April release (#15236)``
`9b76b94c94 <https://github.com/apache/airflow/commit/9b76b94c940d472290861930a1d5860b43b3b2b2>`_  2021-04-02   ``A bunch of template_fields_renderers additions (#15130)``
`a7ca1b3b0b <https://github.com/apache/airflow/commit/a7ca1b3b0bdf0b7677e53be1b11e833714dfbbb4>`_  2021-03-26   ``Fix Sphinx Issues with Docstrings (#14968)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
`4372d45615 <https://github.com/apache/airflow/commit/4372d456154a6922e0c0547a487af3cdadb43b4a>`_  2021-03-12   ``Fix attributes for AzureDataFactory hook (#14704)``
=================================================================================================  ===========  =============================================================================

1.2.0
.....

Latest change: 2021-03-08

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`b753c7fa60 <https://github.com/apache/airflow/commit/b753c7fa60e8d92bbaab68b557a1fbbdc1ec5dd0>`_  2021-03-08   ``Prepare ad-hoc release of the four previously excluded providers (#14655)``
`e7bb17aeb8 <https://github.com/apache/airflow/commit/e7bb17aeb83b2218620c5320241b0c9f902d74ff>`_  2021-03-06   ``Use built-in 'cached_property' on Python 3.8 where possible (#14606)``
`630aeff72c <https://github.com/apache/airflow/commit/630aeff72c7903ae8d4608f3530057bb6255e10b>`_  2021-03-02   ``Fix AzureDataFactoryHook failing to instantiate its connection (#14565)``
`589d6dec92 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`11d03d2f63 <https://github.com/apache/airflow/commit/11d03d2f63d88a284d6aaded5f9ab6642a60561b>`_  2021-02-26   ``Add Azure Data Factory hook (#11015)``
`5bfa0f123b <https://github.com/apache/airflow/commit/5bfa0f123b39babe1ef66c139e59e452240a6bd7>`_  2021-02-25   ``BugFix: Fix remote log in azure storage blob displays in one line (#14313)``
`ca35bd7f7f <https://github.com/apache/airflow/commit/ca35bd7f7f6bc2fb4f2afd7762114ce262c61941>`_  2021-02-21   ``By default PIP will install all packages in .local folder (#14125)``
`10343ec29f <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
=================================================================================================  ===========  ==============================================================================

1.1.0
.....

Latest change: 2021-02-04

=================================================================================================  ===========  =============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================
`88bdcfa0df <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98d <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`94b1531230 <https://github.com/apache/airflow/commit/94b1531230231c57610d720e59563ccd98e7ecb2>`_  2021-01-23   ``Upgrade azure blob to v12 (#12188)``
`a9ac2b040b <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`3fd5ef3555 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`b2cb6ee5ba <https://github.com/apache/airflow/commit/b2cb6ee5ba895983e4e9d9327ff62a9262b765a2>`_  2021-01-07   ``Fix Azure Data Explorer Operator (#13520)``
`295d66f914 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`a1e9195076 <https://github.com/apache/airflow/commit/a1e91950766d12022a89bd667cc1ef1a4dec387c>`_  2020-12-26   ``add system test for azure local to adls operator (#13190)``
`5185d81ff9 <https://github.com/apache/airflow/commit/5185d81ff99523fe363bd5024cef9660c94214ff>`_  2020-12-24   ``add AzureDatalakeStorageDeleteOperator (#13206)``
`6cf76d7ac0 <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
`5090fb0c89 <https://github.com/apache/airflow/commit/5090fb0c8967d2d8719c6f4a468f2151395b5444>`_  2020-12-15   ``Add script to generate integrations.json (#13073)``
=================================================================================================  ===========  =============================================================

1.0.0
.....

Latest change: 2020-12-09

=================================================================================================  ===========  ======================================================================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================================================================================================
`32971a1a2d <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa085 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f24780 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`bd90136aaf <https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2>`_  2020-11-30   ``Move operator guides to provider documentation packages (#12681)``
`2037303eef <https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143>`_  2020-11-29   ``Adds support for Connection/Hook discovery from providers (#12466)``
`543d88b3a1 <https://github.com/apache/airflow/commit/543d88b3a1ec7f0a41af390273868d9aed4edb7b>`_  2020-11-28   ``Add example dag and system tests for azure wasb and fileshare (#12673)``
`6b3c6add9e <https://github.com/apache/airflow/commit/6b3c6add9ea245b43ee367491bf9193d59bd248c>`_  2020-11-27   ``Update setup.py to get non-conflicting set of dependencies (#12636)``
`c34ef853c8 <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`0080354502 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`7ca0b6f121 <https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c>`_  2020-11-18   ``Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)``
`ae7cb4a1e2 <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333cf <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f590 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`dd2095f4a8 <https://github.com/apache/airflow/commit/dd2095f4a8b07c9b1a4c279a3578cd1e23b71a1b>`_  2020-11-10   ``Simplify string expressions & Use f-string (#12216)``
`85a18e13d9 <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78c <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d1590 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`3ff7e0743a <https://github.com/apache/airflow/commit/3ff7e0743a1156efe1d6aaf7b8f82136d0bba08f>`_  2020-11-08   ``azure key vault optional lookup (#12174)``
`41bf172c1d <https://github.com/apache/airflow/commit/41bf172c1dc75099f4f9d8b3f3350b4b1f523ef9>`_  2020-11-04   ``Simplify string expressions (#12093)``
`4e8f9cc8d0 <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b00 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`6ce855af11 <https://github.com/apache/airflow/commit/6ce855af118daeaa4c249669079ab9d9aad23945>`_  2020-10-24   ``Fix spelling (#11821)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`f8ff217e2f <https://github.com/apache/airflow/commit/f8ff217e2f2152bbb9fc701ff4c0b6eb447ad65c>`_  2020-10-18   ``Fix incorrect typing and move config args out of extra connection config to operator args (#11635)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`686e0ee7df <https://github.com/apache/airflow/commit/686e0ee7dfb26224e2f91c9af6ef41d59e2f2e96>`_  2020-10-11   ``Fix incorrect typing, remove hardcoded argument values and improve code in AzureContainerInstancesOperator (#11408)``
`d2754ef769 <https://github.com/apache/airflow/commit/d2754ef76958f8df4dcb6974e2cd2c1edb17935e>`_  2020-10-09   ``Strict type check for Microsoft  (#11359)``
`832a7850f1 <https://github.com/apache/airflow/commit/832a7850f12a3a54767d59f1967a9541e0e33293>`_  2020-10-08   ``Add Azure Blob Storage to GCS transfer operator (#11321)``
`5d007fd2ff <https://github.com/apache/airflow/commit/5d007fd2ff7365229c3d85bc2bbb506ead00247e>`_  2020-10-08   ``Strict type check for azure hooks (#11342)``
`b0fcf67559 <https://github.com/apache/airflow/commit/b0fcf675595494b306800e1a516548dc0dc671f8>`_  2020-10-07   ``Add AzureFileShareToGCSOperator (#10991)``
`c51016b0b8 <https://github.com/apache/airflow/commit/c51016b0b8e894f8d94c2de408c5fc9b472aba3b>`_  2020-10-05   ``Add LocalToAzureDataLakeStorageOperator (#10814)``
`fd682fd70a <https://github.com/apache/airflow/commit/fd682fd70a97a1f937786a1a136f0fa929c8fb80>`_  2020-10-05   ``fix job deletion (#11272)``
`4210618789 <https://github.com/apache/airflow/commit/4210618789215dfe9cb2ab350f6477d3c6ce365e>`_  2020-10-03   ``Ensure target_dedicated_nodes or enable_auto_scale is set in AzureBatchOperator (#11251)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`5093245d6f <https://github.com/apache/airflow/commit/5093245d6f77a370fbd2f9e3df35ac6acf46a1c4>`_  2020-09-30   ``Strict type coverage for Oracle and Yandex provider  (#11198)``
`f3e87c5030 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`f77a11d5b1 <https://github.com/apache/airflow/commit/f77a11d5b1e9d76b1d57c8a0d653b3ab28f33894>`_  2020-09-13   ``Add Secrets backend for Microsoft Azure Key Vault (#10898)``
`9549274d11 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`3696c34c28 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`ee7ca128a1 <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`2f552233f5 <https://github.com/apache/airflow/commit/2f552233f5c99b206c8f4c2088fcc0c05e7e26dc>`_  2020-08-21   ``Add AzureBaseHook (#9747)``
`cdec301254 <https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a>`_  2020-08-07   ``Add correct signature to all operators and sensors (#10205)``
`24c8e4c2d6 <https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832>`_  2020-08-06   ``Changes to all the constructors to remove the args argument (#10163)``
`aeea71274d <https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f>`_  2020-08-02   ``Remove 'args' parameter from provider operator constructors (#10097)``
`7d24b088cd <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`0bf330ba86 <https://github.com/apache/airflow/commit/0bf330ba8681c417fd5a10b3ba01c75600dc5f2e>`_  2020-07-24   ``Add get_blobs_list method to WasbHook (#9950)``
`33f0cd2657 <https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e>`_  2020-07-22   ``apply_default keeps the function signature for mypy (#9784)``
`d3c76da952 <https://github.com/apache/airflow/commit/d3c76da95250068161580036a86e26ee2790fa07>`_  2020-07-12   ``Improve type hinting to provider microsoft  (#9774)``
`23f80f34ad <https://github.com/apache/airflow/commit/23f80f34adec86da24e4896168c53d213d01a7f6>`_  2020-07-08   ``Move gcs & wasb task handlers to their respective provider packages (#9714)``
`d0e7db4024 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a0800 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`c7e5bce57f <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`f6bd817a3a <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`0b0e4f7a4c <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d0 <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`375d1ca229 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8ae <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e3 <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4cb <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`87969a350d <https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca>`_  2020-04-09   ``[AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)``
`d99833c9b5 <https://github.com/apache/airflow/commit/d99833c9b5be9eafc0c7851343ee86b6c20aed40>`_  2020-04-03   ``[AIRFLOW-4529] Add support for Azure Batch Service (#8024)``
`4bde99f132 <https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a>`_  2020-03-23   ``Make airflow/providers pylint compatible (#7802)``
`a83eb335e5 <https://github.com/apache/airflow/commit/a83eb335e58c6a15e96c517a1b492bc79c869ce8>`_  2020-03-23   ``Add call to Super call in microsoft providers (#7821)``
`f0e2421807 <https://github.com/apache/airflow/commit/f0e24218077d4dff8015926d7826477bb0d07f88>`_  2020-02-24   ``[AIRFLOW-6896] AzureCosmosDBHook: Move DB call out of __init__ (#7520)``
`4bec1cc489 <https://github.com/apache/airflow/commit/4bec1cc489f5d19daf7450c75c3e8057c9709dbd>`_  2020-02-24   ``[AIRFLOW-6895] AzureFileShareHook: Move DB call out of __init__ (#7519)``
`3320e432a1 <https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc>`_  2020-02-24   ``[AIRFLOW-6817] Lazy-load 'airflow.DAG' to keep user-facing API untouched (#7517)``
`086e307245 <https://github.com/apache/airflow/commit/086e307245015d97e89af9aa6c677d6fe817264c>`_  2020-02-23   ``[AIRFLOW-6890] AzureDataLakeHook: Move DB call out of __init__ (#7513)``
`4d03e33c11 <https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc>`_  2020-02-22   ``[AIRFLOW-6817] remove imports from 'airflow/__init__.py', replaced implicit imports with explicit imports, added entry to 'UPDATING.MD' - squashed/rebased (#7456)``
`175a160463 <https://github.com/apache/airflow/commit/175a1604638016b0a663711cc584496c2fdcd828>`_  2020-02-19   ``[AIRFLOW-6828] Stop using the zope library (#7448)``
`1e00243014 <https://github.com/apache/airflow/commit/1e00243014382d4cb7152ca7c5011b97cbd733b0>`_  2020-02-10   ``[AIRFLOW-5176] Add Azure Data Explorer (Kusto) operator (#5785)``
`97a429f9d0 <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`83c037873f <https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2>`_  2020-01-30   ``[AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)``
`057f3ae3a4 <https://github.com/apache/airflow/commit/057f3ae3a4afedf6d462ecf58b01dd6304d3e135>`_  2020-01-29   ``[AIRFLOW-6670][depends on AIRFLOW-6669] Move contrib operators to providers package (#7286)``
`290330ba60 <https://github.com/apache/airflow/commit/290330ba60653686cc6f009d89a377f09f26f35a>`_  2020-01-15   ``[AIRFLOW-6552] Move Azure classes to providers.microsoft package (#7158)``
=================================================================================================  ===========  ======================================================================================================================================================================
