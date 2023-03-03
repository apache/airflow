
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


Package apache-airflow-providers-dbt-cloud
------------------------------------------------------

`dbt Cloud <https://www.getdbt.com/product/what-is-dbt/>`__


This is detailed commit list of changes for versions provider package: ``dbt.cloud``.
For high-level changelog, see :doc:`package information including changelog <index>`.



3.1.0
.....

Latest change: 2023-02-23

=================================================================================================  ===========  ============================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================
`e6d3176082 <https://github.com/apache/airflow/commit/e6d317608251d2725627ac2da0e60d5c5b206c1e>`_  2023-02-23   ``Add 'DbtCloudJobRunAsyncSensor' (#29695)``
=================================================================================================  ===========  ============================================

3.0.0
.....

Latest change: 2023-02-08

=================================================================================================  ===========  ================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================
`ce6ae2457e <https://github.com/apache/airflow/commit/ce6ae2457ef3d9f44f0086b58026909170bbf22a>`_  2023-02-08   ``Prepare docs for Feb 2023 wave of Providers (#29379)``
`91c0ce7666 <https://github.com/apache/airflow/commit/91c0ce7666f131176cb6368058dc1f259275b894>`_  2023-02-02   ``Drop Connection.schema use in DbtCloudHook (#29166)``
`f805b4154a <https://github.com/apache/airflow/commit/f805b4154a8155823d7763beb9b6da76889ebd62>`_  2023-01-23   ``Allow downloading of dbt Cloud artifacts to non-existent paths (#29048)``
`55049c50d5 <https://github.com/apache/airflow/commit/55049c50d52323e242c2387f285f0591ea38cde7>`_  2023-01-23   ``Add deferrable mode to 'DbtCloudRunJobOperator' (#29014)``
`4f91931b35 <https://github.com/apache/airflow/commit/4f91931b359f76ae38272c727bfe21a18a470f2b>`_  2023-01-17   ``Provide more context for 'trigger_reason' in DbtCloudRunJobOperator (#28994)``
=================================================================================================  ===========  ================================================================================

2.3.1
.....

Latest change: 2023-01-14

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`911b708ffd <https://github.com/apache/airflow/commit/911b708ffddd4e7cb6aaeac84048291891eb0f1f>`_  2023-01-14   ``Prepare docs for Jan 2023 mid-month wave of Providers (#28929)``
`141338b24e <https://github.com/apache/airflow/commit/141338b24efeddb9460b53b8501654b50bc6b86e>`_  2023-01-12   ``Use entire tenant domain name in dbt Cloud connection (#28890)``
=================================================================================================  ===========  ==================================================================

2.3.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  =========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
=================================================================================================  ===========  =========================================================================

2.2.0
.....

Latest change: 2022-09-28

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`55d8bc0bba <https://github.com/apache/airflow/commit/55d8bc0bbabe0f152b3dd3ae1511327af175f19d>`_  2022-09-26   ``Add 'DbtCloudListJobsOperator' (#26475)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
=================================================================================================  ===========  ====================================================================================

2.1.0
.....

Latest change: 2022-08-10

=================================================================================================  ===========  =================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`c8af0592c0 <https://github.com/apache/airflow/commit/c8af0592c08017ee48f69f608ad4a6529ee14292>`_  2022-07-26   ``Improve taskflow type hints with ParamSpec (#25173)``
=================================================================================================  ===========  =================================================================

2.0.1
.....

Latest change: 2022-07-13

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
`9c59831ee7 <https://github.com/apache/airflow/commit/9c59831ee78f14de96421c74986933c494407afa>`_  2022-06-21   ``Update providers to use functools compat for ''cached_property'' (#24582)``
=================================================================================================  ===========  =============================================================================

2.0.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`7498fba826 <https://github.com/apache/airflow/commit/7498fba826ec477b02a40a2e23e1c685f148e20f>`_  2022-06-06   ``Enable dbt Cloud provider to interact with single tenant instances (#24264)``
`5e6997ed45 <https://github.com/apache/airflow/commit/5e6997ed45be0972bf5ea7dc06e4e1cef73b735a>`_  2022-06-06   ``Update dbt.py (#24218)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`8f2213fcd0 <https://github.com/apache/airflow/commit/8f2213fcd0b8f775792bf2fd4931607992649046>`_  2022-06-05   ``AIP-47 - Migrate dbt DAGs to new design #22472 (#24202)``
`75c60923e0 <https://github.com/apache/airflow/commit/75c60923e01375ffc5f71c4f2f7968f489e2ca2f>`_  2022-05-12   ``Prepare provider documentation 2022.05.11 (#23631)``
`8b6b0848a3 <https://github.com/apache/airflow/commit/8b6b0848a3cacf9999477d6af4d2a87463f03026>`_  2022-04-23   ``Use new Breese for building, pulling and verifying the images. (#23104)``
`f935c9f163 <https://github.com/apache/airflow/commit/f935c9f163bbc2de9034ddf4c0a0cc960a031661>`_  2022-04-23   ``Fix typo in dbt Cloud provider description (#23179)``
`49e336ae03 <https://github.com/apache/airflow/commit/49e336ae0302b386a2f47269a6d13988382d975f>`_  2022-04-13   ``Replace usage of 'DummyOperator' with 'EmptyOperator' (#22974)``
`6933022e94 <https://github.com/apache/airflow/commit/6933022e94acf139b2dea9a589bb8b25c62a5d20>`_  2022-04-10   ``Fix new MyPy errors in main (#22884)``
=================================================================================================  ===========  ==================================================================================

1.0.2
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
=================================================================================================  ===========  ==============================================================

1.0.1
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
`c1ab8e2d7b <https://github.com/apache/airflow/commit/c1ab8e2d7b68a31408e750129592e16432474512>`_  2022-03-14   ``Protect against accidental misuse of XCom.get_value() (#22244)``
`d08284ed25 <https://github.com/apache/airflow/commit/d08284ed251b7c5712190181623b500a38cd640d>`_  2022-03-11   `` Add map_index to XCom model and interface (#22112)``
`f8c01317ef <https://github.com/apache/airflow/commit/f8c01317ef35110217b0054d472d9a276d2924b0>`_  2022-03-10   ``Pass explicit overrides in 'DbtCloudJobRunOperator' to 'DbtCloudHook' (#22136)``
`4388808e0e <https://github.com/apache/airflow/commit/4388808e0e81beb78d48e125c7f51a1283cf1084>`_  2022-03-10   ``Add more template fields to 'DbtCloudJobRunOperator' (#22126)``
`08575ddd8a <https://github.com/apache/airflow/commit/08575ddd8a72f96a3439f73e973ee9958188eb83>`_  2022-03-01   ``Change BaseOperatorLink interface to take a ti_key, not a datetime (#21798)``
`e782b37a3f <https://github.com/apache/airflow/commit/e782b37a3fdf58e60cdefea33b5b865deb69b1d7>`_  2022-02-27   ``Add dbt Cloud provider (#20998)``
=================================================================================================  ===========  ==================================================================================
