
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


Package apache-airflow-providers-apache-kafka
------------------------------------------------------

`Apache Kafka  <https://kafka.apache.org/>`__


This is detailed commit list of changes for versions provider package: ``apache.kafka``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.2.0
.....

Latest change: 2023-10-05

=================================================================================================  ===========  ========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================
`0c8e30e43b <https://github.com/apache/airflow/commit/0c8e30e43b70e9d033e1686b327eb00aab82479c>`_  2023-10-05   ``Bump min airflow version of providers (#34728)``
`659d94f0ae <https://github.com/apache/airflow/commit/659d94f0ae89f47a7d4b95d6c19ab7f87bd3a60f>`_  2023-09-21   ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``
`c077d19060 <https://github.com/apache/airflow/commit/c077d190609f931387c1fcd7b8cc34f12e2372b9>`_  2023-08-26   ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
`a54c2424df <https://github.com/apache/airflow/commit/a54c2424df51bf1acec420f4792a237dabcfa12b>`_  2023-08-23   ``Fix typos (double words and it's/its) (#33623)``
`c645d8e40c <https://github.com/apache/airflow/commit/c645d8e40c167ea1f6c332cdc3ea0ca5a9363205>`_  2023-08-12   ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``
`c9d0fcd967 <https://github.com/apache/airflow/commit/c9d0fcd967cf21ea8373662c3686a5c8468eaae0>`_  2023-08-12   ``Fixes the Kafka provider's max message limit error (#32926) (#33321)``
=================================================================================================  ===========  ========================================================================

1.1.2
.....

Latest change: 2023-07-06

=================================================================================================  ===========  ===============================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================================================
`225e3041d2 <https://github.com/apache/airflow/commit/225e3041d269698d0456e09586924c1898d09434>`_  2023-07-06   ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
`3878fe6fab <https://github.com/apache/airflow/commit/3878fe6fab3ccc1461932b456c48996f2763139f>`_  2023-07-05   ``Remove spurious headers for provider changelogs (#32373)``
`cb4927a018 <https://github.com/apache/airflow/commit/cb4927a01887e2413c45d8d9cb63e74aa994ee74>`_  2023-07-05   ``Prepare docs for July 2023 wave of Providers (#32298)``
`1b599c9fbf <https://github.com/apache/airflow/commit/1b599c9fbfb6151a41a588edaa786745f50eec38>`_  2023-06-30   ``Break AwaitMessageTrigger execution when finding a message with the desired format (#31803)``
`8c37b74a20 <https://github.com/apache/airflow/commit/8c37b74a208a808d905c1b86d081d69d7a1aa900>`_  2023-06-28   ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
`09d4718d3a <https://github.com/apache/airflow/commit/09d4718d3a46aecf3355d14d3d23022002f4a818>`_  2023-06-27   ``Improve provider documentation and README structure (#32125)``
=================================================================================================  ===========  ===============================================================================================

1.1.1
.....

Latest change: 2023-06-20

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`79bcc2e668 <https://github.com/apache/airflow/commit/79bcc2e668e648098aad6eaa87fe8823c76bc69a>`_  2023-06-20   ``Prepare RC1 docs for June 2023 wave of Providers (#32001)``
`547f5846bf <https://github.com/apache/airflow/commit/547f5846bfdf9bd973d40be2dd63484329f95dd4>`_  2023-06-20   ``Add note about dropping Python 3.7 for kafka and impala (#32017)``
`dc5bf3fd02 <https://github.com/apache/airflow/commit/dc5bf3fd02c589578209cb0dd5b7d005b1516ae9>`_  2023-06-02   ``Add discoverability for triggers in provider.yaml (#31576)``
`a473facf6c <https://github.com/apache/airflow/commit/a473facf6c0b36f7d051ecc2d1aa94ba6957468d>`_  2023-06-01   ``Add D400 pydocstyle check - Apache providers only (#31424)``
`9fa75aaf7a <https://github.com/apache/airflow/commit/9fa75aaf7a391ebf0e6b6949445c060f6de2ceb9>`_  2023-05-29   ``Remove Python 3.7 support (#30963)``
=================================================================================================  ===========  ====================================================================

1.1.0
.....

Latest change: 2023-05-19

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`45548b9451 <https://github.com/apache/airflow/commit/45548b9451fba4e48c6f0c0ba6050482c2ea2956>`_  2023-05-19   ``Prepare RC2 docs for May 2023 wave of Providers (#31416)``
`abea189022 <https://github.com/apache/airflow/commit/abea18902257c0250fedb764edda462f9e5abc84>`_  2023-05-18   ``Use '__version__' in providers not 'version' (#31393)``
`f5aed58d9f <https://github.com/apache/airflow/commit/f5aed58d9fb2137fa5f0e3ce75b6709bf8393a94>`_  2023-05-18   ``Fixing circular import error in providers caused by airflow version check (#31379)``
`d9ff55cf6d <https://github.com/apache/airflow/commit/d9ff55cf6d95bb342fed7a87613db7b9e7c8dd0f>`_  2023-05-16   ``Prepare docs for May 2023 wave of Providers (#31252)``
`eef5bc7f16 <https://github.com/apache/airflow/commit/eef5bc7f166dc357fea0cc592d39714b1a5e3c14>`_  2023-05-03   ``Add full automation for min Airflow version for providers (#30994)``
`a7eb32a5b2 <https://github.com/apache/airflow/commit/a7eb32a5b222e236454d3e474eec478ded7c368d>`_  2023-04-30   ``Bump minimum Airflow version in providers (#30917)``
`9409446097 <https://github.com/apache/airflow/commit/940944609751e2584b191aa776b6221aa78703d3>`_  2023-04-24   ``Add cli cmd to list the provider trigger info (#30822)``
`c585ad51c5 <https://github.com/apache/airflow/commit/c585ad51c522c6e9f3bbbf7ae6e0132e25a3a378>`_  2023-04-22   ``Upgrade ruff to 0.0.262 (#30809)``
=================================================================================================  ===========  ======================================================================================

1.0.0
.....

Latest change: 2023-04-21

=================================================================================================  ===========  ==========================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================
`522661b6ad <https://github.com/apache/airflow/commit/522661b6ad4479e3c8243b2d2c8a793d1af82c17>`_  2023-04-21   ``Add provider for Apache Kafka (#30175)``
=================================================================================================  ===========  ==========================================
