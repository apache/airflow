
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


Package apache-airflow-providers-apache-beam
------------------------------------------------------

`Apache Beam <https://beam.apache.org/>`__.


This is detailed commit list of changes for versions provider package: ``apache.beam``.
For high-level changelog, see :doc:`package information including changelog <index>`.



3.1.0
.....

Latest change: 2021-10-17

================================================================================================  ===========  ===================================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ===================================================================================
`a418fd96f <https://github.com/apache/airflow/commit/a418fd96f70eac1d4d7dc91553f41d5153beda93>`_  2021-10-17   ``Use google cloud credentials when executing beam command in subprocess (#18992)``
================================================================================================  ===========  ===================================================================================

3.0.1
.....

Latest change: 2021-08-30

================================================================================================  ===========  ===================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ===================================================================
`0a6858847 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`87f408b1e <https://github.com/apache/airflow/commit/87f408b1e78968580c760acb275ae5bb042161db>`_  2021-07-26   ``Prepares docs for Rc2 release of July providers (#17116)``
`d02ded65e <https://github.com/apache/airflow/commit/d02ded65eaa7d2281e249b3fa028605d1b4c52fb>`_  2021-07-15   ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
`b916b7507 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`866a601b7 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
================================================================================================  ===========  ===================================================================

3.0.0
.....

Latest change: 2021-06-18

================================================================================================  ===========  ==============================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ==============================================================================
`bbc627a3d <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`cbf8001d7 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`1fba5402b <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`9c94b72d4 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`1e647029e <https://github.com/apache/airflow/commit/1e647029e469c1bb17e9ad051d0184f3357644c3>`_  2021-06-01   ``Rename the main branch of the Airflow repo to be 'main' (#16149)``
`904709d34 <https://github.com/apache/airflow/commit/904709d34fbe0b6062d72932b72954afe13ec148>`_  2021-05-27   ``Check synctatic correctness for code-snippets (#16005)``
`37681bca0 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
`0f97a3970 <https://github.com/apache/airflow/commit/0f97a3970d2c652beedbf2fbaa33e2b2bfd69bce>`_  2021-05-04   ``Rename example bucket names to use INVALID BUCKET NAME by default (#15651)``
================================================================================================  ===========  ==============================================================================

2.0.0
.....

Latest change: 2021-04-29

================================================================================================  ===========  =======================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =======================================================================
`814e471d1 <https://github.com/apache/airflow/commit/814e471d137aad68bd64a21d20736e7b88403f97>`_  2021-04-29   ``Update pre-commit checks (#15583)``
`40a2476a5 <https://github.com/apache/airflow/commit/40a2476a5db14ee26b5108d72635da116eab720b>`_  2021-04-28   ``Adds interactivity when generating provider documentation. (#15518)``
`4b031d39e <https://github.com/apache/airflow/commit/4b031d39e12110f337151cda6693e2541bf71c2c>`_  2021-04-27   ``Make Airflow code Pylint 2.8 compatible (#15534)``
`e229f3541 <https://github.com/apache/airflow/commit/e229f3541dd764db54785625875a7c5e94225736>`_  2021-04-27   ``Use Pip 21.* to install airflow officially (#15513)``
`68e4c4dcb <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
================================================================================================  ===========  =======================================================================

1.0.1
.....

Latest change: 2021-03-08

================================================================================================  ===========  ======================================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ======================================================================================
`b753c7fa6 <https://github.com/apache/airflow/commit/b753c7fa60e8d92bbaab68b557a1fbbdc1ec5dd0>`_  2021-03-08   ``Prepare ad-hoc release of the four previously excluded providers (#14655)``
`4e5763060 <https://github.com/apache/airflow/commit/4e5763060683456405ab6173cdee1f2facc231e5>`_  2021-03-03   ``Remove WARNINGs from BeamHook (#14554)``
`589d6dec9 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`8a731f536 <https://github.com/apache/airflow/commit/8a731f536cc946cc62c20921187354b828df931e>`_  2021-02-05   ``Improve Apache Beam operators - refactor operator - common Dataflow logic (#14094)``
`10343ec29 <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
================================================================================================  ===========  ======================================================================================

1.0.0
.....

Latest change: 2021-02-04

================================================================================================  ===========  ===========================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ===========================================================================
`d45739f7c <https://github.com/apache/airflow/commit/d45739f7ce0de183329d67fff88a9da3943a9280>`_  2021-02-04   ``Fixes to release process after releasing 2nd wave of providers (#14059)``
`1872d8719 <https://github.com/apache/airflow/commit/1872d8719d24f94aeb1dcba9694837070b9884ca>`_  2021-02-03   ``Add Apache Beam operators (#12814)``
================================================================================================  ===========  ===========================================================================
