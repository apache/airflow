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

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!

Package apache-airflow-providers-openlineage
------------------------------------------------------

`OpenLineage <https://openlineage.io/>`__


This is detailed commit list of changes for versions provider package: ``openlineage``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.2.1
.....

Latest change: 2023-11-06

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`59b32dc0a0 <https://github.com/apache/airflow/commit/59b32dc0a0bcdffd124b82d92428f334646cd8cd>`_  2023-11-06   ``Fix bad regexp in mypy-providers specification in pre-commits (#35465)``
`6858ea46eb <https://github.com/apache/airflow/commit/6858ea46eb5282034b0695720d797dcb7ef91100>`_  2023-11-04   ``Make schema filter uppercase in 'create_filter_clauses' (#35428)``
`63cc915cd3 <https://github.com/apache/airflow/commit/63cc915cd38a5034df6bf9c618e12f8690eeade0>`_  2023-10-31   ``Switch from Black to Ruff formatter (#35287)``
=================================================================================================  ===========  ==========================================================================

1.2.0
.....

Latest change: 2023-10-28

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`d1c58d86de <https://github.com/apache/airflow/commit/d1c58d86de1267d9268a1efe0a0c102633c051a1>`_  2023-10-28   ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
`3592ff4046 <https://github.com/apache/airflow/commit/3592ff40465032fa041600be740ee6bc25e7c242>`_  2023-10-28   ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
`0940d09859 <https://github.com/apache/airflow/commit/0940d098590139c8ab5940813f628530c86944b6>`_  2023-10-25   ``Send column lineage from SQL operators. (#34843)``
`dd7ba3cae1 <https://github.com/apache/airflow/commit/dd7ba3cae139cb10d71c5ebc25fc496c67ee784e>`_  2023-10-19   ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
=================================================================================================  ===========  ==================================================================

1.1.1
.....

Latest change: 2023-10-13

=================================================================================================  ===========  ===============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================
`e9987d5059 <https://github.com/apache/airflow/commit/e9987d50598f70d84cbb2a5d964e21020e81c080>`_  2023-10-13   ``Prepare docs 1st wave of Providers in October 2023 (#34916)``
`73dd877961 <https://github.com/apache/airflow/commit/73dd877961cfaca0d29f127b0d868308d174bcd1>`_  2023-10-11   ``Adjust log levels in OpenLineage provider (#34801)``
=================================================================================================  ===========  ===============================================================

1.1.0
.....

Latest change: 2023-09-08

=================================================================================================  ===========  ===================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================================
`21990ed894 <https://github.com/apache/airflow/commit/21990ed8943ee4dc6e060ee2f11648490c714a3b>`_  2023-09-08   ``Prepare docs for 09 2023 - 1st wave of Providers (#34201)``
`5eea4e632c <https://github.com/apache/airflow/commit/5eea4e632c8ae50812e07b1d844ea4f52e0d6fe1>`_  2023-09-07   ``Add OpenLineage support for DBT Cloud. (#33959)``
`e403c74524 <https://github.com/apache/airflow/commit/e403c74524a980030ba120c3602de0c3dc867d86>`_  2023-09-06   ``Fix import in 'get_custom_facets'. (#34122)``
`875387afa5 <https://github.com/apache/airflow/commit/875387afa53c207364fa20b515d154100b5d0a8d>`_  2023-09-01   ``Refactor unneeded  jumps in providers (#33833)``
`b4d4f55b47 <https://github.com/apache/airflow/commit/b4d4f55b479d07c13ab25bb2e80cb053378b56d7>`_  2023-08-31   ``Refactor: Replace lambdas with comprehensions in providers (#33771)``
`0d49d1fed9 <https://github.com/apache/airflow/commit/0d49d1fed970c324698efb3419d5a403de0a37eb>`_  2023-08-29   ``Allow to disable openlineage at operator level (#33685)``
`9d8c77e447 <https://github.com/apache/airflow/commit/9d8c77e447f5515b9a6aa85fa72511a86a128c28>`_  2023-08-27   ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
=================================================================================================  ===========  ===================================================================================================

1.0.2
.....

Latest change: 2023-08-26

=================================================================================================  ===========  ===================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================
`c077d19060 <https://github.com/apache/airflow/commit/c077d190609f931387c1fcd7b8cc34f12e2372b9>`_  2023-08-26   ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
`626d3daa9b <https://github.com/apache/airflow/commit/626d3daa9b5348fec6dfb4d29edcff97bba20298>`_  2023-08-24   ``Add OpenLineage support for Trino. (#32910)``
`1cdd82391e <https://github.com/apache/airflow/commit/1cdd82391e0f7a24ab7f0badbe8f44a54f51d757>`_  2023-08-21   ``Simplify conditions on len() in other providers (#33569)``
`abef61ff3d <https://github.com/apache/airflow/commit/abef61ff3d6b9ae8dcb7f9dbbea78a9648a0c50b>`_  2023-08-20   ``Replace repr() with proper formatting (#33520)``
`6d3b71c333 <https://github.com/apache/airflow/commit/6d3b71c33390c8063502acfe0fc2cd936db74814>`_  2023-08-19   ``openlineage: don't run task instance listener in executor (#33366)``
`8e738cd0ad <https://github.com/apache/airflow/commit/8e738cd0ad0e7dce644f66bb749a7b46770badee>`_  2023-08-15   ``openlineage: do not try to redact Proxy objects from deprecated config (#33393)``
`23d5076635 <https://github.com/apache/airflow/commit/23d507663541ab49f02d7863d42f9baf458cc48f>`_  2023-08-13   ``openlineage: defensively check for provided datetimes in listener (#33343)``
=================================================================================================  ===========  ===================================================================================

1.0.1
.....

Latest change: 2023-08-05

=================================================================================================  ===========  ===================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================================
`60677b0ba3 <https://github.com/apache/airflow/commit/60677b0ba3c9e81595ec2aa3d4be2737e5b32054>`_  2023-08-05   ``Prepare docs for Aug 2023 1st wave of Providers (#33128)``
`bdc10a5ff6 <https://github.com/apache/airflow/commit/bdc10a5ff6fea0fd968345fd4a9b732be49b9761>`_  2023-08-04   ``Move openlineage configuration to provider (#33124)``
`11ff650e1b <https://github.com/apache/airflow/commit/11ff650e1b122aadebcea462adfae5492a76ed94>`_  2023-08-04   ``openlineage: disable running listener if not configured (#33120)``
`e10aa6ae6a <https://github.com/apache/airflow/commit/e10aa6ae6ad07830cbf5ec59d977654c52012c22>`_  2023-08-04   ``openlineage, bigquery: add openlineage method support for BigQueryExecuteQueryOperator (#31293)``
`2a39914cbd <https://github.com/apache/airflow/commit/2a39914cbd091fb7b19de80197afcaf82c8ec240>`_  2023-08-01   ``Don't use database as fallback when no schema parsed. (#32959)``
=================================================================================================  ===========  ===================================================================================================

1.0.0
.....

Latest change: 2023-07-29

=================================================================================================  ===========  ===============================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================================================================================
`d06b7af69a <https://github.com/apache/airflow/commit/d06b7af69a65c50321ba2a9904551f3b8affc7f1>`_  2023-07-29   ``Prepare docs for July 2023 3rd wave of Providers (#32875)``
`0924389a87 <https://github.com/apache/airflow/commit/0924389a877c5461733ef8a048e860b951d81a56>`_  2023-07-28   ``Fix MIN_AIRFLOW_VERSION_EXCEPTIONS for openlineage (#32909)``
`5c8223c335 <https://github.com/apache/airflow/commit/5c8223c33598f06820aa215f2cd07760ccbb063e>`_  2023-07-28   ``Bump common-sql version in  provider (#32907)``
`8a0f410010 <https://github.com/apache/airflow/commit/8a0f410010cc39ce8d31ee7b64a352fbd2ad19ef>`_  2023-07-28   ``Update openlineage provider to min version of airflow 2.7.0 (#32882)``
`b73366799d <https://github.com/apache/airflow/commit/b73366799d98195a5ccc49a2008932186c4763b5>`_  2023-07-27   ``openlineage, gcs: add openlineage methods for GcsToGcsOperator (#31350)``
`9194144dab <https://github.com/apache/airflow/commit/9194144dab01d1898877215379e1c019fe6f10cd>`_  2023-07-27   ``Replace Ruff setting known-third-party with namespace-packages (#32873)``
`3878fe6fab <https://github.com/apache/airflow/commit/3878fe6fab3ccc1461932b456c48996f2763139f>`_  2023-07-05   ``Remove spurious headers for provider changelogs (#32373)``
`ee4a838d49 <https://github.com/apache/airflow/commit/ee4a838d49461b3b053a9cbe660dbff06a17fff5>`_  2023-07-05   ``Pass SQLAlchemy engine to construct information schema query. (#32371)``
`1240dcc167 <https://github.com/apache/airflow/commit/1240dcc167c4b47331db81deff61fc688df118c2>`_  2023-07-05   ``D205 Support - Providers: GRPC to Oracle (inclusive) (#32357)``
`65fad4affc <https://github.com/apache/airflow/commit/65fad4affc24b33c4499ad0fbcdfff535fbae3bf>`_  2023-07-04   ``Change default schema behaviour in SQLParser. (#32347)``
`f2e2125b07 <https://github.com/apache/airflow/commit/f2e2125b070794b6a66fb3e2840ca14d07054cf2>`_  2023-06-29   ``openlineage, common.sql:  provide OL SQL parser as internal OpenLineage provider API (#31398)``
`09d4718d3a <https://github.com/apache/airflow/commit/09d4718d3a46aecf3355d14d3d23022002f4a818>`_  2023-06-27   ``Improve provider documentation and README structure (#32125)``
`1d564adc1c <https://github.com/apache/airflow/commit/1d564adc1c5dc31d0c9717d608250b60f9742acb>`_  2023-06-20   ``set contributor note in CHANGELOG.rst openlienage (#32018)``
`ebd7b0eb53 <https://github.com/apache/airflow/commit/ebd7b0eb5353428e0345d67a98298292f1804897>`_  2023-06-13   ``openlineage: fix typing errors produced by bumping version, bump minimum version to 0.28, remove outdated warnings (#31874)``
`6f8cd65bde <https://github.com/apache/airflow/commit/6f8cd65bde8d2ecb26a35398fdd8373b66904b30>`_  2023-06-06   ``Limit openlineage-integration-common until breaking change is fixed (#31739)``
`9276310a43 <https://github.com/apache/airflow/commit/9276310a43d17a9e9e38c2cb83686a15656896b2>`_  2023-06-05   ``Improve docstrings in providers (#31681)``
`a59076eaee <https://github.com/apache/airflow/commit/a59076eaeed03dd46e749ad58160193b4ef3660c>`_  2023-06-02   ``Add D400 pydocstyle check - Providers (#31427)``
`abea189022 <https://github.com/apache/airflow/commit/abea18902257c0250fedb764edda462f9e5abc84>`_  2023-05-18   ``Use '__version__' in providers not 'version' (#31393)``
`f5aed58d9f <https://github.com/apache/airflow/commit/f5aed58d9fb2137fa5f0e3ce75b6709bf8393a94>`_  2023-05-18   ``Fixing circular import error in providers caused by airflow version check (#31379)``
`6b21e4b88c <https://github.com/apache/airflow/commit/6b21e4b88c3d18eb1ba176e6ac53da90a4523880>`_  2023-05-13   ``Bring back detection of implicit single-line string concatenation (#31270)``
`981afe2a4f <https://github.com/apache/airflow/commit/981afe2a4f998335e657c3897ffc7f8df269f680>`_  2023-05-12   ``openlineage: add extractors for python and bash operators (#30713)``
`51603efbf7 <https://github.com/apache/airflow/commit/51603efbf7e9c8b7bc7d4b4c9e7e6514dab66bfd>`_  2023-05-04   ``Allow configuring OpenLineage client from Airflow config. (#30735)``
`eef5bc7f16 <https://github.com/apache/airflow/commit/eef5bc7f166dc357fea0cc592d39714b1a5e3c14>`_  2023-05-03   ``Add full automation for min Airflow version for providers (#30994)``
`2f570c2bf7 <https://github.com/apache/airflow/commit/2f570c2bf7794e100e6960ba3abe0d6998c1e497>`_  2023-04-20   ``Fix when OpenLineage plugins has listener disabled. (#30708)``
`cbde23e6bc <https://github.com/apache/airflow/commit/cbde23e6bcdd2235f8becb0abf858a7ffcf6e91c>`_  2023-04-17   ``Upgrade to MyPy 1.2.0 (#30687)``
`6a6455ad1c <https://github.com/apache/airflow/commit/6a6455ad1c2d76eaf9c60814c2b0a0141ad29da0>`_  2023-04-17   ``Correctly pass a type to attrs.has() (#30677)``
`8d81963c01 <https://github.com/apache/airflow/commit/8d81963c014398a7ab14505fd8e27e432f1aaf5c>`_  2023-04-16   ``Workaround type-incompatibility with new attrs in openlineage (#30674)``
`55963de61e <https://github.com/apache/airflow/commit/55963de61edbbaa5f54d70f94e3f4682e824743f>`_  2023-04-14   ``First commit of OpenLineage provider. (#29940)``
=================================================================================================  ===========  ===============================================================================================================================
