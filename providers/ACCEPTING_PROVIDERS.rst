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

.. note::

   There are two options for providers:

   1. A provider registered under Apache Airflow, subject to the process described below.
   2. A 3rd-party managed provider published on the
      `Ecosystem page <https://airflow.apache.org/ecosystem/#third-party-airflow-plugins-and-providers>`_.

Prerequisites for proposing a new provider
------------------------------------------

1. **Working codebase**: A functional implementation demonstrating the integration
2. **Stewardship commitment**: At least two individuals willing to serve as stewards
3. **Committer sponsorship**: At least one existing Airflow Committer willing to sponsor the stewards
4. **Quality standards**: Code, tests, and documentation meeting the Contributing Guide standards
5. **System tests**: If the provider integrates with a live external service, the proposal should
   describe how the integration will be continuously validated. The community encourages stewards
   to run system tests and publish results on a publicly accessible dashboard. See the
   `System Test Dashboards <https://airflow.apache.org/ecosystem/#airflow-provider-system-test-dashboards>`_
   for examples of how existing providers handle this.

Approval process
-----------------

Accepting new community providers requires a ``[DISCUSS]`` thread on the Airflow
`devlist <https://airflow.apache.org/community/#mailing-list>`_. No formal vote is required.

The discussion thread should include:

* Description of the integration and its value to the Airflow ecosystem
* Identification of the proposed stewards and their sponsoring committer(s)
* Commitment to meet incubation health metrics within 6 months
* Plan for participating in quarterly governance updates
* Sponsoring committer(s) if you already have alignment with one or asking for a volunteer

If no objections are raised within 7 days, the proposal is considered accepted and the
contributor may proceed with opening a PR.

Discussion thread template
~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the following template when opening your ``[DISCUSS]`` thread on the devlist:

.. code-block:: text

   Subject: [DISCUSS] New Community Provider: <name/service>

   Hi Airflow community,

   I'd like to propose adding a new community provider for <Service/Tool Name>.

   Integration overview
   --------------------
   <Brief description of what the service/tool does and why it is valuable to Airflow users.>

   System tests
   ------------
   <If the provider integrates with a live external service, describe how system tests will be
   run and validated, and whether results will be published to a public dashboard. If no live
   service is involved, state that system tests are not required. See the existing dashboards
   for examples: https://airflow.apache.org/ecosystem/#airflow-provider-system-test-dashboards>

   Proposed stewards
   -----------------
   - <Name, GitHub handle, email>
   - <Name, GitHub handle, email>

   Sponsoring committer(s)
   -----------------------
   - <Name, GitHub handle>

   Working implementation
   ----------------------
   <Link to the draft PR or repository demonstrating a working implementation.>

   Incubation commitment
   ---------------------
   We commit to:
   - Maintaining the provider and responding to issues within a reasonable time
   - Meeting the incubation health metrics within 6 months
   - Participating in quarterly governance updates

   Best regards,
   <Your name>

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
See the `Ecosystem page <https://airflow.apache.org/ecosystem/#third-party-airflow-plugins-and-providers>`_
for a registry of available 3rd-party providers.

Examples
--------

WIP: To be added later

Examples (pre policy simplification)
-------------------------------------

* Akeyless - `[PROPOSAL] New community provider: apache-airflow-providers-akeyless <https://lists.apache.org/thread/ztf1mqbkoc39qz6o419lynttk4fhoz0t>`_, `[VOTE] New Community Provider: apache-airflow-providers-akeyless <https://lists.apache.org/thread/jqvbo8x82g87okn0fqh5o2bxxx84g9qj>`_, `[RESULT][VOTE] New Community Provider: apache-airflow-providers-akeyless <https://lists.apache.org/thread/87zn154n2vppg2vsxlbhscnfl68vb4ls>`_
* Vespa.ai - `[DISCUSS] New Provider: Vespa.ai - AIP-95 <https://lists.apache.org/thread/6myz043vx7v57zk95qo3wd0qcwt9r9th>`_
* Informatica - `[DISCUSS] New Provider: Informatica - AIP-95 <https://lists.apache.org/thread/wsfgh23jm6hkrly4lx1m21ftllqshpgo>`_
* Stripe - `[DISCUSS] Interest in adding a Stripe provider to Airflow <https://lists.apache.org/thread/r9fr6571w3ssx07bnd5lgc9m88r1xy3c>`_

Historical examples (pre-AIP-95)
--------------------------------

* Huawei Cloud provider - `Discussion <https://lists.apache.org/thread/f5tk9c734wlyv616vyy8r34ymth3dqbc>`_
* Cloudera provider - `Discussion <https://lists.apache.org/thread/2z0lvgj466ksxxrbvofx41qvn03jrwwb>`_, `Vote <https://lists.apache.org/thread/8b1jvld3npgzz2z0o3gv14lvtornbdrm>`_
