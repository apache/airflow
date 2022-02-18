
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


Package apache-airflow-providers-elasticsearch
------------------------------------------------------

`Elasticsearch <https://www.elastic.co/elasticsearch>`__


This is detailed commit list of changes for versions provider package: ``elasticsearch``.
For high-level changelog, see :doc:`package information including changelog <index>`.



2.2.0
.....

Latest change: 2022-02-12

================================================================================================  ===========  ============================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ============================================================================
`44bd211b1 <https://github.com/apache/airflow/commit/44bd211b19dcb75eeb53ced5bea2cf0c80654b1a>`_  2022-02-12   ``Use compat data interval shim in log handlers (#21289)``
`0a3ff43d4 <https://github.com/apache/airflow/commit/0a3ff43d41d33d05fb3996e61785919effa9a2fa>`_  2022-02-08   ``Add pre-commit check for docstring param types (#21398)``
`d94fa3783 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6184facba <https://github.com/apache/airflow/commit/6184facbaa6c654d15d493b9de918e7ee74b3cce>`_  2022-02-07   ``Emit "logs not found" message when ES logs appear to be missing (#21261)``
`d8c4449a9 <https://github.com/apache/airflow/commit/d8c4449a91b9b93691c03e1af45bdedc5e23fd5e>`_  2022-02-06   ``Clarify ElasticsearchTaskHandler docstring (#21255)``
`6c3a67d4f <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`602abe839 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`f77417eb0 <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`6e51608f2 <https://github.com/apache/airflow/commit/6e51608f28f4c769c019624ea0caaa0c6e671f80>`_  2021-12-16   ``Fix mypy for providers: elasticsearch, oracle, yandex (#20344)``
`853576d90 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`fe682ec3d <https://github.com/apache/airflow/commit/fe682ec3d376f0983410d64beb4f3529fb7b0f99>`_  2021-11-24   ``Fix duplicate changelog entries (#19759)``
================================================================================================  ===========  ============================================================================

2.1.0
.....

Latest change: 2021-10-29

================================================================================================  ===========  ===========================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ===========================================================================
`d9567eb10 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`840ea3efb <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`de9b02f79 <https://github.com/apache/airflow/commit/de9b02f797931efbd081996b4f81ba14ca76a17d>`_  2021-09-28   ``Updating the Elasticsearch example DAG to use the TaskFlow API (#18565)``
`060345c0d <https://github.com/apache/airflow/commit/060345c0d982765e39da5fa8b2e2c6a01e89e394>`_  2021-09-21   ``Add docs for AIP 39: Timetables (#17552)``
`a0a05ffed <https://github.com/apache/airflow/commit/a0a05ffeddab54199e43b76016703c7ccaed3cd1>`_  2021-09-04   ``Adds example showing the ES_hook (#17944)``
================================================================================================  ===========  ===========================================================================

2.0.3
.....

Latest change: 2021-08-30

================================================================================================  ===========  ============================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ============================================================================
`0a6858847 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`be75dcd39 <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
`29aab6434 <https://github.com/apache/airflow/commit/29aab6434ffe0fb8c83b6fd6c9e44310966d496a>`_  2021-08-17   ``Adds secrets backend/logging/auth information to provider yaml (#17625)``
`944dc32c2 <https://github.com/apache/airflow/commit/944dc32c2b4a758564259133a08f2ea8d28dcb6c>`_  2021-08-12   ``Fix Invalid log order in ElasticsearchTaskHandler (#17551)``
================================================================================================  ===========  ============================================================================

2.0.2
.....

Latest change: 2021-06-28

================================================================================================  ===========  ===========================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ===========================================================
`866a601b7 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
`6f445e69c <https://github.com/apache/airflow/commit/6f445e69c3c9d95bff18f327aeeab126cc36a6e1>`_  2021-06-26   ``Update release documentation for elasticsearch (#16662)``
================================================================================================  ===========  ===========================================================

2.0.1
.....

Latest change: 2021-06-18

================================================================================================  ===========  =====================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =====================================================================
`bbc627a3d <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`3cf67be38 <https://github.com/apache/airflow/commit/3cf67be3875fa0b91408ed0433779970e4f6acf5>`_  2021-06-16   ``Support non-https elasticsearch external links (#16489)``
`247ba3187 <https://github.com/apache/airflow/commit/247ba31872aa5a8a9e92f781a6beba75945ece1b>`_  2021-06-16   ``Fix Elasticsearch external log link with ''json_format'' (#16467)``
`5e12b3de3 <https://github.com/apache/airflow/commit/5e12b3de31dd1cf3d6e5088edbf497f91dcae4d8>`_  2021-06-16   ``Remove support jinja templated log_id in elasticsearch (#16465)``
`cbf8001d7 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`e31e515b2 <https://github.com/apache/airflow/commit/e31e515b28a745b7428b42f1559ab456305fb3a0>`_  2021-06-15   ``Fix external elasticsearch logs link (#16357)``
`1fba5402b <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`e8d3de828 <https://github.com/apache/airflow/commit/e8d3de828f834ea9527c8d3d0d434675c0b3ee41>`_  2021-06-15   ``Add ElasticSearch Connection Doc (#16436)``
`5cd0bf733 <https://github.com/apache/airflow/commit/5cd0bf733b839951c075c54e808a595ac923c4e8>`_  2021-06-11   ``Support remote logging in elasticsearch with filebeat 7 (#14625)``
`9c94b72d4 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`3bdcd1a7d <https://github.com/apache/airflow/commit/3bdcd1a7d46ca06115a93b97f394486e0acaf52d>`_  2021-06-05   ``Docs: Fix url for ''Elasticsearch'' (#16275)``
`476d0f6e3 <https://github.com/apache/airflow/commit/476d0f6e3d2059f56532cda36cdc51aa86bafb37>`_  2021-05-22   ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
================================================================================================  ===========  =====================================================================

1.0.4
.....

Latest change: 2021-05-01

================================================================================================  ===========  =============================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =============================================================================
`807ad32ce <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`657384615 <https://github.com/apache/airflow/commit/657384615fafc060f9e2ed925017306705770355>`_  2021-04-27   ``Fix 'logging.exception' redundancy (#14823)``
`71c673e42 <https://github.com/apache/airflow/commit/71c673e427a89cae2a9f3174c32c5c85556d6342>`_  2021-04-22   ``Update Docstrings of Modules with Missing Params (#15391)``
`5da831910 <https://github.com/apache/airflow/commit/5da831910c358ecbd7a5c33ee31fe0d909508bea>`_  2021-04-10   ``Fix exception caused by missing keys in the ElasticSearch Record (#15163)``
`68e4c4dcb <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
`eb884cd38 <https://github.com/apache/airflow/commit/eb884cd380f2f010b027bf6657f4da2e2e3309aa>`_  2021-03-14   ``Add elasticsearch to the fixes of backport providers (#14763)``
================================================================================================  ===========  =============================================================================

1.0.3
.....

Latest change: 2021-03-13

================================================================================================  ===========  ===================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ===================================================================
`662cb8c6a <https://github.com/apache/airflow/commit/662cb8c6ac8becb26ff405f8b21acfccdd8de2ae>`_  2021-03-13   ``Prepare for releasing Elasticsearch Provider 1.0.3 (#14748)``
`923bde2b9 <https://github.com/apache/airflow/commit/923bde2b917099135adfe470a5453f663131fd5f>`_  2021-03-09   ``Elasticsearch Provider: Fix logs downloading for tasks (#14686)``
================================================================================================  ===========  ===================================================================

1.0.2
.....

Latest change: 2021-02-27

================================================================================================  ===========  =======================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =======================================================================
`589d6dec9 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`10343ec29 <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
================================================================================================  ===========  =======================================================================

1.0.1
.....

Latest change: 2021-02-04

================================================================================================  ===========  =====================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =====================================================================
`88bdcfa0d <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98 <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`3fd5ef355 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`295d66f91 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`b6bf25306 <https://github.com/apache/airflow/commit/b6bf25306243e78bf12528f9a080ea100a575641>`_  2020-12-25   ``Respect LogFormat when using ES logging with Json Format (#13310)``
`6cf76d7ac <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
================================================================================================  ===========  =====================================================================

1.0.0
.....

Latest change: 2020-12-09

================================================================================================  ===========  =========================================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =========================================================================================
`32971a1a2 <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa08 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f2478 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`2037303ee <https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143>`_  2020-11-29   ``Adds support for Connection/Hook discovery from providers (#12466)``
`c34ef853c <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`008035450 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`ae7cb4a1e <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333c <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f59 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`85a18e13d <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78 <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`61feb6ec4 <https://github.com/apache/airflow/commit/61feb6ec453f8dda1a0e1fe3ebcc0f1e3224b634>`_  2020-11-09   ``Provider's readmes generated for elasticsearch and google packages (#12194)``
`b2a28d159 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`4e8f9cc8d <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b0 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84e <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`349b0811c <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e712971 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`0a0e1af80 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4 <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c38 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`f3e87c503 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`ac943c9e1 <https://github.com/apache/airflow/commit/ac943c9e18f75259d531dbda8c51e650f57faa4c>`_  2020-09-08   ``[AIRFLOW-3964][AIP-17] Consolidate and de-dup sensor tasks using Smart Sensor (#5499)``
`70f05ac67 <https://github.com/apache/airflow/commit/70f05ac6775152d856d212f845e9561282232844>`_  2020-09-01   ``Add 'log_id' field to log lines on ES handler (#10411)``
`fdd9b6f65 <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`d76026545 <https://github.com/apache/airflow/commit/d7602654526fdd2876466371404784bd17cfe0d2>`_  2020-08-25   ``PyDocStyle: No whitespaces allowed surrounding docstring text (#10533)``
`3696c34c2 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`ee7ca128a <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`d5d119bab <https://github.com/apache/airflow/commit/d5d119babc97bbe3f3f690ad4a93e3b73bd3b172>`_  2020-07-21   ``Increase typing coverage for Elasticsearch (#9911)``
`a79e2d4c4 <https://github.com/apache/airflow/commit/a79e2d4c4aa105f3fac5ae6a28e29af9cd572407>`_  2020-07-06   ``Move provider's log task handlers to the provider package (#9604)``
`e13a14c87 <https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84>`_  2020-06-21   ``Enable & Fix Whitespace related PyDocStyle Checks (#9458)``
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
`65dd28eb7 <https://github.com/apache/airflow/commit/65dd28eb77d996ec8306c67d5ce1ccee2c14cc9d>`_  2020-02-18   ``[AIRFLOW-1202] Create Elasticsearch Hook (#7358)``
================================================================================================  ===========  =========================================================================================
