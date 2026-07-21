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

OCI Generative AI Hosted Applications
=====================================

:class:`~airflow.providers.oracle.hooks.generative_ai.OciGenerativeAIHook` uses the official
`OCI Python SDK <https://docs.oracle.com/en-us/iaas/tools/python/latest/>`__ to manage
`Hosted Applications and deployments
<https://docs.oracle.com/en-us/iaas/Content/generative-ai/agents.htm#deployments>`__.
Install ``apache-airflow-providers-oracle[oci]`` and configure an
:ref:`OCI connection <howto/connection:oci>` before using the hook.
The hook exposes the native :class:`oci.generative_ai.GenerativeAiClient` through ``conn`` and
``get_conn()``. Operators can therefore call OCI SDK methods directly without an Airflow wrapper
for every API operation.

Oracle exposes separate application APIs for `two inbound authentication variants
<https://docs.oracle.com/en-us/iaas/Content/generative-ai/create-application.htm#authentication>`__:

* Identity domain bearer tokens use the OCI SDK ``HostedApplication`` resource and client methods
  ending in ``hosted_application`` or ``hosted_applications``.
* OCI IAM request signing uses ``HostedApplicationIam`` and client methods ending in
  ``hosted_application_iam`` or ``hosted_applications_iam``.

This distinction configures how clients invoke the deployed application. It does not change how
the Airflow hook authenticates to the OCI management API; both variants use the configured
:ref:`OCI connection <howto/connection:oci>`.

Management endpoints
--------------------

The OCI SDK derives the management endpoint as
``https://generativeai.<region>.oci.oraclecloud.com`` and adds the ``20231130`` API base path.
The hook exposes these operations without changing OCI retry, pagination, or concurrency-control
arguments.

==========================================================  ===================================================
OCI SDK client method                                       REST operation
==========================================================  ===================================================
``create_hosted_application``                               ``POST /20231130/hostedApplications``
``get_hosted_application``                                  ``GET /20231130/hostedApplications/{id}``
``list_hosted_applications``                                ``GET /20231130/hostedApplications``
``update_hosted_application``                               ``PUT /20231130/hostedApplications/{id}``
``delete_hosted_application``                               ``DELETE /20231130/hostedApplications/{id}``
``create_hosted_application_iam``                           ``POST /20231130/hostedApplicationsIam``
``get_hosted_application_iam``                              ``GET /20231130/hostedApplicationsIam/{id}``
``list_hosted_applications_iam``                            ``GET /20231130/hostedApplicationsIam``
``update_hosted_application_iam``                           ``PUT /20231130/hostedApplicationsIam/{id}``
``delete_hosted_application_iam``                           ``DELETE /20231130/hostedApplicationsIam/{id}``
``create_hosted_deployment``                                ``POST /20231130/hostedDeployments``
``get_hosted_deployment``                                   ``GET /20231130/hostedDeployments/{id}``
``list_hosted_deployments``                                 ``GET /20231130/hostedDeployments``
``update_hosted_deployment``                                ``PUT /20231130/hostedDeployments/{id}``
``delete_hosted_deployment``                                ``DELETE /20231130/hostedDeployments/{id}``
``get_work_request``                                        ``GET /20231130/workRequests/{id}``
``list_work_request_errors``                                ``GET /20231130/workRequests/{id}/errors``
``list_work_request_logs``                                  ``GET /20231130/workRequests/{id}/logs``
``list_work_requests``                                      ``GET /20231130/workRequests``
==========================================================  ===================================================

All client methods return the native :class:`oci.response.Response`. This preserves response data and
headers such as ``etag``, ``opc-request-id``, and ``opc-work-request-id``. Create, update, and
delete operations can be asynchronous; use ``opc-work-request-id`` with
``hook.conn.get_work_request`` to observe their status.

Use ``hook.get_compartment_id()`` to resolve an explicit compartment or the connection default
before calling list methods. To filter deployments for a Hosted Application, pass its OCID as the
OCI SDK ``application_id`` keyword argument.

Creating an identity domain Hosted Application
-----------------------------------------------

Identity domain applications require an ``InboundAuthConfig`` containing the identity domain URL
and OAuth settings:

.. code-block:: python

    from oci.generative_ai.models import (
        CreateHostedApplicationDetails,
        IdcsAuthConfig,
        InboundAuthConfig,
    )

    from airflow.providers.oracle.hooks.generative_ai import OciGenerativeAIHook

    hook = OciGenerativeAIHook(oci_conn_id="oci_default")
    response = hook.conn.create_hosted_application(
        CreateHostedApplicationDetails(
            display_name="airflow-agent-oauth",
            compartment_id="ocid1.compartment.oc1..example",
            inbound_auth_config=InboundAuthConfig(
                inbound_auth_config_type="IDCS_AUTH_CONFIG",
                idcs_config=IdcsAuthConfig(
                    domain_url="https://idcs-example.identity.oraclecloud.com",
                    scope="agent.invoke",
                    audience="https://agent.example.com",
                ),
            ),
        )
    )
    work_request_id = response.headers.get("opc-work-request-id")

Creating an OCI IAM Hosted Application
--------------------------------------

OCI IAM applications do not require an OAuth or identity domain configuration:

.. code-block:: python

    from oci.generative_ai.models import CreateHostedApplicationIamDetails

    from airflow.providers.oracle.hooks.generative_ai import OciGenerativeAIHook

    hook = OciGenerativeAIHook(oci_conn_id="oci_default")
    response = hook.conn.create_hosted_application_iam(
        CreateHostedApplicationIamDetails(
            display_name="airflow-agent",
            compartment_id="ocid1.compartment.oc1..example",
            description="Hosted application managed by Airflow",
        )
    )
    work_request_id = response.headers.get("opc-work-request-id")

Agent invocation
----------------

This hook covers the Generative AI management API only. Invoking an active Hosted Application uses
the Generative AI inference endpoint and a custom application path; it is intentionally outside this
management hook's contract.
