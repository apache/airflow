
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


Package apache-airflow-providers-microsoft-psrp
------------------------------------------------------

This package provides remote execution capabilities via the
`PowerShell Remoting Protocol (PSRP)
<https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-psrp/>`__.


This is detailed commit list of changes for versions provider package: ``microsoft.psrp``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.1.0
.....

Latest change: 2022-02-05

================================================================================================  ===========  =========================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =========================================================================
`6c3a67d4f <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`b8564daf5 <https://github.com/apache/airflow/commit/b8564daf50e049bdb27971104973b8981b7ea121>`_  2022-02-01   ``PSRP improvements (#19806)``
`602abe839 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`f77417eb0 <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`d56e7b56b <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`e63e23c58 <https://github.com/apache/airflow/commit/e63e23c582cd757ea6593bdb4dfde66d76a8c9f1>`_  2021-12-23   ``Fixing MyPy issues inside providers/microsoft (#20409)``
================================================================================================  ===========  =========================================================================

1.0.1
.....

Latest change: 2021-09-03

================================================================================================  ===========  ================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ================================================================
`fd5d65751 <https://github.com/apache/airflow/commit/fd5d65751ca026d2b5f0ec1e4d9ce1b1e09e5b22>`_  2021-09-03   ``Update release notes for 3 extra providers released (#18018)``
`9c644194e <https://github.com/apache/airflow/commit/9c644194ed6bc6a3a065b72ea7cf89c02d1c5275>`_  2021-09-03   ``Fix unexpected bug in exiting hook context manager (#18014)``
================================================================================================  ===========  ================================================================

1.0.0
.....

Latest change: 2021-08-27

================================================================================================  ===========  ============================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ============================================================
`41632e03b <https://github.com/apache/airflow/commit/41632e03b8caf71de308414c48e9cb211a083761>`_  2021-08-27   ``Fix provider.yaml errors due to exit(0) in test (#17858)``
`69d2ed65c <https://github.com/apache/airflow/commit/69d2ed65cb7c9384d309ae5e499d5798c2c3ac96>`_  2021-08-08   ``Add Microsoft PSRP provider (#17361)``
================================================================================================  ===========  ============================================================
