
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


Package apache-airflow-providers-smtp
------------------------------------------------------

`Simple Mail Transfer Protocol (SMTP) <https://tools.ietf.org/html/rfc5321>`__


This is detailed commit list of changes for versions provider package: ``smtp``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.1.0
.....

Latest change: 2023-05-18

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`abea189022 <https://github.com/apache/airflow/commit/abea18902257c0250fedb764edda462f9e5abc84>`_  2023-05-18   ``Use '__version__' in providers not 'version' (#31393)``
`f5aed58d9f <https://github.com/apache/airflow/commit/f5aed58d9fb2137fa5f0e3ce75b6709bf8393a94>`_  2023-05-18   ``Fixing circular import error in providers caused by airflow version check (#31379)``
`d9ff55cf6d <https://github.com/apache/airflow/commit/d9ff55cf6d95bb342fed7a87613db7b9e7c8dd0f>`_  2023-05-16   ``Prepare docs for May 2023 wave of Providers (#31252)``
`eef5bc7f16 <https://github.com/apache/airflow/commit/eef5bc7f166dc357fea0cc592d39714b1a5e3c14>`_  2023-05-03   ``Add full automation for min Airflow version for providers (#30994)``
`a7eb32a5b2 <https://github.com/apache/airflow/commit/a7eb32a5b222e236454d3e474eec478ded7c368d>`_  2023-04-30   ``Bump minimum Airflow version in providers (#30917)``
=================================================================================================  ===========  ======================================================================================

1.0.1
.....

Latest change: 2023-04-09

=================================================================================================  ===========  =======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================================
`874ea9588e <https://github.com/apache/airflow/commit/874ea9588e3ce7869759440302e53bb6a730a11e>`_  2023-04-09   ``Prepare docs for ad hoc release of Providers (#30545)``
`806b0279ac <https://github.com/apache/airflow/commit/806b0279acd5897e2ad6b816764bb25b4bcdf5b0>`_  2023-04-09   ``Accept None for 'EmailOperator.from_email' to load it from smtp connection (#30533)``
`a15e734785 <https://github.com/apache/airflow/commit/a15e73478521707487e1a6d6f7ef7f213b282023>`_  2023-04-07   ``'EmailOperator': fix wrong assignment of 'from_email' (#30524)``
`d23a3bbed8 <https://github.com/apache/airflow/commit/d23a3bbed89ae04369983f21455bf85ccc1ae1cb>`_  2023-04-04   ``Add mechanism to suspend providers (#30422)``
=================================================================================================  ===========  =======================================================================================

1.0.0
.....

Latest change: 2023-03-14

=================================================================================================  ===========  ===================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================
`3fac5c3540 <https://github.com/apache/airflow/commit/3fac5c35409ccfde771ce08ea8daeaac056b2c10>`_  2023-03-14   ``Creating SMTP provider (#29968)``
=================================================================================================  ===========  ===================================
