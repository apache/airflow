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

Accepting New Community Providers
==================================

.. contents:: Table of Contents
   :depth: 3
   :local:

The Airflow community welcomes new provider contributions. All new providers enter through the
**Incubation** stage (unless specifically accelerated by the PMC) to ensure alignment with ASF
guidelines and community principles.

For details on the incubation stage and graduation criteria, see
`Provider Governance <PROVIDER_GOVERNANCE.rst#incubation-stage>`_.

Prerequisites for proposing a new provider
------------------------------------------

1. **Working codebase**: A functional implementation demonstrating the integration
2. **Stewardship commitment**: At least two individuals willing to serve as stewards
3. **Committer sponsorship**: At least one existing Airflow Committer willing to sponsor the stewards
4. **Quality standards**: Code, tests, and documentation meeting the Contributing Guide standards

Approval process
-----------------

Accepting new community providers requires a ``[DISCUSSION]`` followed by ``[VOTE]`` thread at the
Airflow `devlist <https://airflow.apache.org/community/#mailing-list>`_.

For integrations with well-established open-source software (Apache Software Foundation, Linux
Foundation, or similar organizations with established governance), a ``[LAZY CONSENSUS]`` process
may be sufficient, provided the PR includes comprehensive test coverage and documentation.

The ``[DISCUSSION]`` thread should include:

* Description of the integration and its value to the Airflow ecosystem
* Identification of the proposed stewards and their sponsoring committer(s)
* Commitment to meet incubation health metrics within 6 months
* Plan for participating in quarterly governance updates

The ``[VOTE]`` follows the usual Apache Software Foundation voting rules concerning
`Votes on Code Modification <https://www.apache.org/foundation/voting.html#votes-on-code-modification>`_

Alternative: 3rd-party managed providers
-----------------------------------------

For service providers or systems integrators with dedicated teams to manage their provider and who wish to not participate
in the Airflow community, we encourage considering 3rd-party management of providers. The
`Ecosystem page <https://airflow.apache.org/ecosystem/#third-party-airflow-plugins-and-providers>`_
provides visibility for 3rd-party providers, and this approach allows service providers or systems integrators to:

* Synchronize releases with their service updates
* Maintain direct control over the integration
* Support older Airflow versions if needed

There is no difference in technical capabilities between community and 3rd-party providers.
See `3rd-party providers <THIRD_PARTY_PROVIDERS.rst>`_ for more details.

Examples
--------

* Vespa.ai - `[PROPOSAL] New Provider: Vespa.ai - AIP-95 <https://lists.apache.org/thread/6myz043vx7v57zk95qo3wd0qcwt9r9th>`_, `Missing Vote <missing>`_
* Informatica - `[PROPOSAL] New Provider: Informatica - AIP-95 <https://lists.apache.org/thread/wsfgh23jm6hkrly4lx1m21ftllqshpgo>`_, `Missing Vote <missing>`_
* Stripe - `[DISCUSS] Interest in adding a Stripe provider to Airflow <https://lists.apache.org/thread/r9fr6571w3ssx07bnd5lgc9m88r1xy3c>`_, `Missing Vote <missing>`_

Historical examples (Before AIP-95)
-----------------------------------

* Huawei Cloud provider - `Discussion <https://lists.apache.org/thread/f5tk9c734wlyv616vyy8r34ymth3dqbc>`_
* Cloudera provider - `Discussion <https://lists.apache.org/thread/2z0lvgj466ksxxrbvofx41qvn03jrwwb>`_, `Vote <https://lists.apache.org/thread/8b1jvld3npgzz2z0o3gv14lvtornbdrm>`_
