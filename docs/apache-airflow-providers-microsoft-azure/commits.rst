
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



3.1.0
.....

Latest change: 2021-07-26

================================================================================================  ===========  =============================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =============================================================================
`48ca9374b <https://github.com/apache/airflow/commit/48ca9374bfe4a0784b5eb9ec74c1e3262a833677>`_  2021-07-26   ``Remove/refactor default_args pattern for Microsoft example DAGs (#16873)``
`d02ded65e <https://github.com/apache/airflow/commit/d02ded65eaa7d2281e249b3fa028605d1b4c52fb>`_  2021-07-15   ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
`b916b7507 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`866a601b7 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
`caf0a8499 <https://github.com/apache/airflow/commit/caf0a8499f6099c943b0dd5054a9480b2e046bf1>`_  2021-06-25   ``Add support for managed identity in WASB hook (#16628)``
`ffb1fcacf <https://github.com/apache/airflow/commit/ffb1fcacff21c31d7cacfbd843a84208fca38d1e>`_  2021-06-24   ``Fix multiple issues in Microsoft AzureContainerInstancesOperator (#15634)``
`a2a58d27e <https://github.com/apache/airflow/commit/a2a58d27efaee515141d5e7cee373020b84acc2f>`_  2021-06-24   ``Reduce log messages for happy path (#16626)``
================================================================================================  ===========  =============================================================================

3.0.0
.....

Latest change: 2021-06-18

================================================================================================  ===========  ==============================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ==============================================================================
`bbc627a3d <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`cbf8001d7 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`1fba5402b <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`0c80a7d41 <https://github.com/apache/airflow/commit/0c80a7d41100bf8d18b661c8286d6056e6d5d2f1>`_  2021-06-11   ``Fixes AzureFileShare connection extras (#16388)``
`29b7f795d <https://github.com/apache/airflow/commit/29b7f795d6fb9fb8cab14158905c1b141044236d>`_  2021-06-07   ``fix wasb remote logging when blob already exists (#16280)``
`9c94b72d4 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`476d0f6e3 <https://github.com/apache/airflow/commit/476d0f6e3d2059f56532cda36cdc51aa86bafb37>`_  2021-05-22   ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
`c844ff742 <https://github.com/apache/airflow/commit/c844ff742e786973273c56348a09d073a4928878>`_  2021-05-18   ``Fix colon spacing in ''AzureDataExplorerHook'' docstring (#15841)``
`37681bca0 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
`3b4fdd0a7 <https://github.com/apache/airflow/commit/3b4fdd0a7a176bfb2e9a17d4627b1d4ed40f1c86>`_  2021-05-06   ``add oracle  connection link (#15632)``
`b1bd59440 <https://github.com/apache/airflow/commit/b1bd59440baa839eccdb2770145d0713ade4f82a>`_  2021-05-04   ``Add delimiter argument to WasbHook delete_file method (#15637)``
`0f97a3970 <https://github.com/apache/airflow/commit/0f97a3970d2c652beedbf2fbaa33e2b2bfd69bce>`_  2021-05-04   ``Rename example bucket names to use INVALID BUCKET NAME by default (#15651)``
`db557a8c4 <https://github.com/apache/airflow/commit/db557a8c4a3e1f0d67b2534010e5092be4f4a9fd>`_  2021-05-01   ``Docs: Replace 'airflow' to 'apache-airflow' to install extra (#15628)``
================================================================================================  ===========  ==============================================================================

2.0.0
.....

Latest change: 2021-05-01

================================================================================================  ===========  =======================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =======================================================================
`807ad32ce <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`657384615 <https://github.com/apache/airflow/commit/657384615fafc060f9e2ed925017306705770355>`_  2021-04-27   ``Fix 'logging.exception' redundancy (#14823)``
`d65e492a3 <https://github.com/apache/airflow/commit/d65e492a3ee43b198c5082b40cab011b15595d12>`_  2021-04-25   ``Removes unnecessary AzureContainerInstance connection type (#15514)``
`cb1344b63 <https://github.com/apache/airflow/commit/cb1344b63d6650de537320460b7b0547efd2353c>`_  2021-04-16   ``Update azure connection documentation (#15352)``
`1a85ba9e9 <https://github.com/apache/airflow/commit/1a85ba9e93d44601a322546e31814bd9ef11c125>`_  2021-04-13   ``Add dynamic connection fields to Azure Connection (#15159)``
================================================================================================  ===========  =======================================================================

1.3.0
.....

Latest change: 2021-04-06

================================================================================================  ===========  =============================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =============================================================================
`042be2e4e <https://github.com/apache/airflow/commit/042be2e4e06b988f5ba2dc146f53774dabc8b76b>`_  2021-04-06   ``Updated documentation for provider packages before April release (#15236)``
`9b76b94c9 <https://github.com/apache/airflow/commit/9b76b94c940d472290861930a1d5860b43b3b2b2>`_  2021-04-02   ``A bunch of template_fields_renderers additions (#15130)``
`a7ca1b3b0 <https://github.com/apache/airflow/commit/a7ca1b3b0bdf0b7677e53be1b11e833714dfbbb4>`_  2021-03-26   ``Fix Sphinx Issues with Docstrings (#14968)``
`68e4c4dcb <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
`4372d4561 <https://github.com/apache/airflow/commit/4372d456154a6922e0c0547a487af3cdadb43b4a>`_  2021-03-12   ``Fix attributes for AzureDataFactory hook (#14704)``
================================================================================================  ===========  =============================================================================

1.2.0
.....

Latest change: 2021-03-08

================================================================================================  ===========  ==============================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ==============================================================================
`b753c7fa6 <https://github.com/apache/airflow/commit/b753c7fa60e8d92bbaab68b557a1fbbdc1ec5dd0>`_  2021-03-08   ``Prepare ad-hoc release of the four previously excluded providers (#14655)``
`e7bb17aeb <https://github.com/apache/airflow/commit/e7bb17aeb83b2218620c5320241b0c9f902d74ff>`_  2021-03-06   ``Use built-in 'cached_property' on Python 3.8 where possible (#14606)``
`630aeff72 <https://github.com/apache/airflow/commit/630aeff72c7903ae8d4608f3530057bb6255e10b>`_  2021-03-02   ``Fix AzureDataFactoryHook failing to instantiate its connection (#14565)``
`589d6dec9 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`11d03d2f6 <https://github.com/apache/airflow/commit/11d03d2f63d88a284d6aaded5f9ab6642a60561b>`_  2021-02-26   ``Add Azure Data Factory hook (#11015)``
`5bfa0f123 <https://github.com/apache/airflow/commit/5bfa0f123b39babe1ef66c139e59e452240a6bd7>`_  2021-02-25   ``BugFix: Fix remote log in azure storage blob displays in one line (#14313)``
`ca35bd7f7 <https://github.com/apache/airflow/commit/ca35bd7f7f6bc2fb4f2afd7762114ce262c61941>`_  2021-02-21   ``By default PIP will install all packages in .local folder (#14125)``
`10343ec29 <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
================================================================================================  ===========  ==============================================================================

1.1.0
.....

Latest change: 2021-02-04

================================================================================================  ===========  =============================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =============================================================
`88bdcfa0d <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98 <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`94b153123 <https://github.com/apache/airflow/commit/94b1531230231c57610d720e59563ccd98e7ecb2>`_  2021-01-23   ``Upgrade azure blob to v12 (#12188)``
`a9ac2b040 <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`3fd5ef355 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`b2cb6ee5b <https://github.com/apache/airflow/commit/b2cb6ee5ba895983e4e9d9327ff62a9262b765a2>`_  2021-01-07   ``Fix Azure Data Explorer Operator (#13520)``
`295d66f91 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`a1e919507 <https://github.com/apache/airflow/commit/a1e91950766d12022a89bd667cc1ef1a4dec387c>`_  2020-12-26   ``add system test for azure local to adls operator (#13190)``
`5185d81ff <https://github.com/apache/airflow/commit/5185d81ff99523fe363bd5024cef9660c94214ff>`_  2020-12-24   ``add AzureDatalakeStorageDeleteOperator (#13206)``
`6cf76d7ac <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
`5090fb0c8 <https://github.com/apache/airflow/commit/5090fb0c8967d2d8719c6f4a468f2151395b5444>`_  2020-12-15   ``Add script to generate integrations.json (#13073)``
================================================================================================  ===========  =============================================================

1.0.0
.....

Latest change: 2020-12-09

================================================================================================  ===========  ======================================================================================================================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ======================================================================================================================================================================
`32971a1a2 <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa08 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f2478 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`bd90136aa <https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2>`_  2020-11-30   ``Move operator guides to provider documentation packages (#12681)``
`2037303ee <https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143>`_  2020-11-29   ``Adds support for Connection/Hook discovery from providers (#12466)``
`543d88b3a <https://github.com/apache/airflow/commit/543d88b3a1ec7f0a41af390273868d9aed4edb7b>`_  2020-11-28   ``Add example dag and system tests for azure wasb and fileshare (#12673)``
`6b3c6add9 <https://github.com/apache/airflow/commit/6b3c6add9ea245b43ee367491bf9193d59bd248c>`_  2020-11-27   ``Update setup.py to get non-conflicting set of dependencies (#12636)``
`c34ef853c <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`008035450 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`7ca0b6f12 <https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c>`_  2020-11-18   ``Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)``
`ae7cb4a1e <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333c <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f59 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`dd2095f4a <https://github.com/apache/airflow/commit/dd2095f4a8b07c9b1a4c279a3578cd1e23b71a1b>`_  2020-11-10   ``Simplify string expressions & Use f-string (#12216)``
`85a18e13d <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78 <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d159 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`3ff7e0743 <https://github.com/apache/airflow/commit/3ff7e0743a1156efe1d6aaf7b8f82136d0bba08f>`_  2020-11-08   ``azure key vault optional lookup (#12174)``
`41bf172c1 <https://github.com/apache/airflow/commit/41bf172c1dc75099f4f9d8b3f3350b4b1f523ef9>`_  2020-11-04   ``Simplify string expressions (#12093)``
`4e8f9cc8d <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b0 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84e <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`6ce855af1 <https://github.com/apache/airflow/commit/6ce855af118daeaa4c249669079ab9d9aad23945>`_  2020-10-24   ``Fix spelling (#11821)``
`349b0811c <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`f8ff217e2 <https://github.com/apache/airflow/commit/f8ff217e2f2152bbb9fc701ff4c0b6eb447ad65c>`_  2020-10-18   ``Fix incorrect typing and move config args out of extra connection config to operator args (#11635)``
`16e712971 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`686e0ee7d <https://github.com/apache/airflow/commit/686e0ee7dfb26224e2f91c9af6ef41d59e2f2e96>`_  2020-10-11   ``Fix incorrect typing, remove hardcoded argument values and improve code in AzureContainerInstancesOperator (#11408)``
`d2754ef76 <https://github.com/apache/airflow/commit/d2754ef76958f8df4dcb6974e2cd2c1edb17935e>`_  2020-10-09   ``Strict type check for Microsoft  (#11359)``
`832a7850f <https://github.com/apache/airflow/commit/832a7850f12a3a54767d59f1967a9541e0e33293>`_  2020-10-08   ``Add Azure Blob Storage to GCS transfer operator (#11321)``
`5d007fd2f <https://github.com/apache/airflow/commit/5d007fd2ff7365229c3d85bc2bbb506ead00247e>`_  2020-10-08   ``Strict type check for azure hooks (#11342)``
`b0fcf6755 <https://github.com/apache/airflow/commit/b0fcf675595494b306800e1a516548dc0dc671f8>`_  2020-10-07   ``Add AzureFileShareToGCSOperator (#10991)``
`c51016b0b <https://github.com/apache/airflow/commit/c51016b0b8e894f8d94c2de408c5fc9b472aba3b>`_  2020-10-05   ``Add LocalToAzureDataLakeStorageOperator (#10814)``
`fd682fd70 <https://github.com/apache/airflow/commit/fd682fd70a97a1f937786a1a136f0fa929c8fb80>`_  2020-10-05   ``fix job deletion (#11272)``
`421061878 <https://github.com/apache/airflow/commit/4210618789215dfe9cb2ab350f6477d3c6ce365e>`_  2020-10-03   ``Ensure target_dedicated_nodes or enable_auto_scale is set in AzureBatchOperator (#11251)``
`0a0e1af80 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4 <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c38 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`5093245d6 <https://github.com/apache/airflow/commit/5093245d6f77a370fbd2f9e3df35ac6acf46a1c4>`_  2020-09-30   ``Strict type coverage for Oracle and Yandex provider  (#11198)``
`f3e87c503 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`f77a11d5b <https://github.com/apache/airflow/commit/f77a11d5b1e9d76b1d57c8a0d653b3ab28f33894>`_  2020-09-13   ``Add Secrets backend for Microsoft Azure Key Vault (#10898)``
`9549274d1 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`fdd9b6f65 <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`3696c34c2 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`ee7ca128a <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`2f552233f <https://github.com/apache/airflow/commit/2f552233f5c99b206c8f4c2088fcc0c05e7e26dc>`_  2020-08-21   ``Add AzureBaseHook (#9747)``
`cdec30125 <https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a>`_  2020-08-07   ``Add correct signature to all operators and sensors (#10205)``
`24c8e4c2d <https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832>`_  2020-08-06   ``Changes to all the constructors to remove the args argument (#10163)``
`aeea71274 <https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f>`_  2020-08-02   ``Remove 'args' parameter from provider operator constructors (#10097)``
`7d24b088c <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`0bf330ba8 <https://github.com/apache/airflow/commit/0bf330ba8681c417fd5a10b3ba01c75600dc5f2e>`_  2020-07-24   ``Add get_blobs_list method to WasbHook (#9950)``
`33f0cd265 <https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e>`_  2020-07-22   ``apply_default keeps the function signature for mypy (#9784)``
`d3c76da95 <https://github.com/apache/airflow/commit/d3c76da95250068161580036a86e26ee2790fa07>`_  2020-07-12   ``Improve type hinting to provider microsoft  (#9774)``
`23f80f34a <https://github.com/apache/airflow/commit/23f80f34adec86da24e4896168c53d213d01a7f6>`_  2020-07-08   ``Move gcs & wasb task handlers to their respective provider packages (#9714)``
`d0e7db402 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a080 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`c7e5bce57 <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`f6bd817a3 <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`0b0e4f7a4 <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`375d1ca22 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8a <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4c <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`87969a350 <https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca>`_  2020-04-09   ``[AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)``
`d99833c9b <https://github.com/apache/airflow/commit/d99833c9b5be9eafc0c7851343ee86b6c20aed40>`_  2020-04-03   ``[AIRFLOW-4529] Add support for Azure Batch Service (#8024)``
`4bde99f13 <https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a>`_  2020-03-23   ``Make airflow/providers pylint compatible (#7802)``
`a83eb335e <https://github.com/apache/airflow/commit/a83eb335e58c6a15e96c517a1b492bc79c869ce8>`_  2020-03-23   ``Add call to Super call in microsoft providers (#7821)``
`f0e242180 <https://github.com/apache/airflow/commit/f0e24218077d4dff8015926d7826477bb0d07f88>`_  2020-02-24   ``[AIRFLOW-6896] AzureCosmosDBHook: Move DB call out of __init__ (#7520)``
`4bec1cc48 <https://github.com/apache/airflow/commit/4bec1cc489f5d19daf7450c75c3e8057c9709dbd>`_  2020-02-24   ``[AIRFLOW-6895] AzureFileShareHook: Move DB call out of __init__ (#7519)``
`3320e432a <https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc>`_  2020-02-24   ``[AIRFLOW-6817] Lazy-load 'airflow.DAG' to keep user-facing API untouched (#7517)``
`086e30724 <https://github.com/apache/airflow/commit/086e307245015d97e89af9aa6c677d6fe817264c>`_  2020-02-23   ``[AIRFLOW-6890] AzureDataLakeHook: Move DB call out of __init__ (#7513)``
`4d03e33c1 <https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc>`_  2020-02-22   ``[AIRFLOW-6817] remove imports from 'airflow/__init__.py', replaced implicit imports with explicit imports, added entry to 'UPDATING.MD' - squashed/rebased (#7456)``
`175a16046 <https://github.com/apache/airflow/commit/175a1604638016b0a663711cc584496c2fdcd828>`_  2020-02-19   ``[AIRFLOW-6828] Stop using the zope library (#7448)``
`1e0024301 <https://github.com/apache/airflow/commit/1e00243014382d4cb7152ca7c5011b97cbd733b0>`_  2020-02-10   ``[AIRFLOW-5176] Add Azure Data Explorer (Kusto) operator (#5785)``
`97a429f9d <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`83c037873 <https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2>`_  2020-01-30   ``[AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)``
`057f3ae3a <https://github.com/apache/airflow/commit/057f3ae3a4afedf6d462ecf58b01dd6304d3e135>`_  2020-01-29   ``[AIRFLOW-6670][depends on AIRFLOW-6669] Move contrib operators to providers package (#7286)``
`290330ba6 <https://github.com/apache/airflow/commit/290330ba60653686cc6f009d89a377f09f26f35a>`_  2020-01-15   ``[AIRFLOW-6552] Move Azure classes to providers.microsoft package (#7158)``
================================================================================================  ===========  ======================================================================================================================================================================
