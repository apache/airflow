
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


Package apache-airflow-providers-alibaba
------------------------------------------------------

Alibaba Cloud integration (including `Alibaba Cloud <https://www.alibabacloud.com//>`__).


This is detailed commit list of changes for versions provider package: ``alibaba``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.0.1
.....

Latest change: 2022-02-08

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`73c0d241d8 <https://github.com/apache/airflow/commit/73c0d241d804507abc651a365f93d60c543349d5>`_  2022-01-21   ``Remove a few stray ':type's in docs (#21014)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`730db3fb77 <https://github.com/apache/airflow/commit/730db3fb774f60127ab1c865e19031f1f9c193f7>`_  2022-01-18   ``Remove all "fake" stub files (#20936)``
`f8fd0f7b4c <https://github.com/apache/airflow/commit/f8fd0f7b4ca6cb52307be4323028bf4e409566e7>`_  2022-01-13   ``Explain stub files are introduced for Mypy errors in examples (#20827)``
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`3299064958 <https://github.com/apache/airflow/commit/3299064958e5fbcfc8e91e905ababb18d7339421>`_  2021-12-29   ``Use isort on pyi files (#20556)``
`b5d520cf73 <https://github.com/apache/airflow/commit/b5d520cf73100df714c71ac9898a97bc0df29a31>`_  2021-12-29   ``Reinstate 'region' to 'default_args' for Alibaba example DAGs (#20423)``
`73ab0edce5 <https://github.com/apache/airflow/commit/73ab0edce58d996e2854d478f054b25c4bb627c4>`_  2021-12-18   ``Fix MyPy Errors for Alibaba provider. (#20393)``
`2fb5e1d0ec <https://github.com/apache/airflow/commit/2fb5e1d0ec306839a3ff21d0bddbde1d022ee8c7>`_  2021-12-15   ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`f5ad26dcdd <https://github.com/apache/airflow/commit/f5ad26dcdd7bcb724992528dce71056965b94d26>`_  2021-10-21   ``Fixup string concatenations (#19099)``
`840ea3efb9 <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`ef037e7021 <https://github.com/apache/airflow/commit/ef037e702182e4370cb00c853c4fb0e246a0479c>`_  2021-09-29   ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
`0fe0b06bb0 <https://github.com/apache/airflow/commit/0fe0b06bb0bf468f46195946f938f9e6e3d46216>`_  2021-09-03   ``Adding missing init file in example_dags directory (#18019)``
=================================================================================================  ===========  ======================================================================================

1.0.0
.....

Latest change: 2021-08-23

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
`75ca6543da <https://github.com/apache/airflow/commit/75ca6543da3754f0dfb53d059588ad66a2f8235a>`_  2021-08-07   ``[AIRFLOW-17200] Add Alibaba Cloud OSS support (#17201)``
=================================================================================================  ===========  ============================================================================
