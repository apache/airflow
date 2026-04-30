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

JWT Token Authentication
========================

This document describes how JWT (JSON Web Token) authentication works in Apache Airflow
for both the public REST API (Core API) and the internal Execution API used by workers.

.. contents::
   :local:
   :depth: 2

Overview
--------

Airflow uses JWT tokens as the primary authentication mechanism for its APIs. There are two
distinct JWT authentication flows:

1. **REST API (Core API)** — used by UI users, CLI tools, and external clients to interact
   with the Airflow public API.
2. **Execution API** — used internally by workers, the Dag File Processor, and the Triggerer
   to communicate task state and retrieve runtime data (connections, variables, XComs).

Both flows share the same underlying JWT infrastructure (``JWTGenerator`` and ``JWTValidator``
classes in ``airflow.api_fastapi.auth.tokens``) but differ in audience, token lifetime, subject
claims, and scope semantics.


Signing and Cryptography
------------------------

Airflow supports two mutually exclusive signing modes:

**Symmetric (shared secret)**
   Uses a pre-shared secret key (``[api_auth] jwt_secret``) with the **HS512** algorithm.
   All components that generate or validate tokens must share the same secret. If no secret
   is configured, Airflow auto-generates a random 16-byte key at startup — but this key is
   ephemeral and different across processes, which will cause authentication failures in
   multi-component deployments. Deployment Managers must explicitly configure this value.

**Asymmetric (public/private key pair)**
   Uses a PEM-encoded private key (``[api_auth] jwt_private_key_path``) for signing and
   the corresponding public key for validation. Supported algorithms: **RS256** (``RSA``) and
   **EdDSA** (``Ed25519``). The algorithm is auto-detected from the key type when
   ``[api_auth] jwt_algorithm`` is set to ``GUESS`` (the default).

   Validation can use either:

   - A JWKS (JSON Web Key Set) endpoint configured via ``[api_auth] trusted_jwks_url``
     (local file or remote HTTP/HTTPS URL, polled periodically for updates).
   - The public key derived from the configured private key (automatic fallback when
     ``trusted_jwks_url`` is not set).

REST API Authentication Flow
-----------------------------

Token acquisition
^^^^^^^^^^^^^^^^^

1. A client sends a ``POST`` request to ``/auth/token`` with credentials (e.g., username
   and password in JSON body).
2. The auth manager validates the credentials and creates a user object.
3. The auth manager serializes the user into JWT claims and calls ``JWTGenerator.generate()``.
4. The generated token is returned in the response as ``access_token``.

For UI-based authentication, the token is stored in a secure, HTTP-only cookie (``_token``)
with ``SameSite=Lax``.

The CLI uses a separate endpoint (``/auth/token/cli``) with a different (shorter) expiration
time.

Token structure (REST API)
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Claim
     - Description
   * - ``jti``
     - Unique token identifier (UUID4 hex). Used for token revocation.
   * - ``iss``
     - Issuer (from ``[api_auth] jwt_issuer``).
   * - ``aud``
     - Audience (from ``[api_auth] jwt_audience``).
   * - ``sub``
     - User identifier (serialized by the auth manager).
   * - ``iat``
     - Issued-at timestamp (Unix epoch seconds).
   * - ``nbf``
     - Not-before timestamp (same as ``iat``).
   * - ``exp``
     - Expiration timestamp (``iat + jwt_expiration_time``).

Token validation (REST API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^

On each API request, the token is extracted in this order of precedence:

1. ``Authorization: Bearer <token>`` header.
2. OAuth2 query parameter.
3. ``_token`` cookie.

The ``JWTValidator`` verifies the signature, expiry (``exp``), not-before (``nbf``),
issued-at (``iat``), audience, and issuer claims. A configurable leeway
(``[api_auth] jwt_leeway``, default 10 seconds) accounts for clock skew.

Token revocation (REST API only)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Token revocation applies only to REST API and UI tokens — it is **not** used for Execution API
tokens issued to workers.

Revoked tokens are tracked in the ``revoked_token`` database table by their ``jti`` claim.
On logout or explicit revocation, the token's ``jti`` and ``exp`` are inserted into this
table. Expired entries are automatically cleaned up at a cadence of ``2× jwt_expiration_time``.

Token refresh (REST API)
^^^^^^^^^^^^^^^^^^^^^^^^

The ``JWTRefreshMiddleware`` runs on UI requests. When the middleware detects that the
current token's ``_token`` cookie is approaching expiry, it calls
``auth_manager.refresh_user()`` to generate a new token and sets it as the updated cookie.

Default timings (REST API)
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Setting
     - Default
   * - ``[api_auth] jwt_expiration_time``
     - 86400 seconds (24 hours)
   * - ``[api_auth] jwt_cli_expiration_time``
     - 3600 seconds (1 hour)
   * - ``[api_auth] jwt_leeway``
     - 10 seconds


Execution API Authentication Flow
----------------------------------

The Execution API is an  API used for use by Airflow itself (not third party callers)
to report and set task state transitions, send heartbeats, and to retrieve connections,
variables, and XComs at task runtime, trigger execution and Dag parsing.

Token generation (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. The **Scheduler** generates a JWT for each task instance before
   dispatching it (via the executor) to a worker. The executor's
   ``jwt_generator`` property creates a ``JWTGenerator`` configured with the ``[execution_api]`` settings.
2. The token's ``sub`` (subject) claim is set to the **task instance UUID**.
3. The token is embedded in the workload JSON payload (``BaseWorkloadSchema.token`` field)
   that is sent to the worker process.

Token structure (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Claim
     - Description
   * - ``jti``
     - Unique token identifier (UUID4 hex).
   * - ``iss``
     - Issuer (from ``[api_auth] jwt_issuer``).
   * - ``aud``
     - Audience (from ``[execution_api] jwt_audience``, default: ``urn:airflow.apache.org:task``).
   * - ``sub``
     - Task instance UUID — the identity of the workload.
   * - ``scope``
     - Token scope: ``"execution"`` or ``"workload"``.
   * - ``iat``
     - Issued-at timestamp.
   * - ``nbf``
     - Not-before timestamp.
   * - ``exp``
     - Expiration timestamp (``iat + [execution_api] jwt_expiration_time``).

Token scopes (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Execution API defines two token scopes with different lifetimes:

**workload**
   A token embedded in the workload JSON payload when the Scheduler
   dispatches a task. The longer lifetime
   allows tasks to remain valid while waiting in executor queues before execution
   begins. When a worker calls the ``/run`` endpoint with a ``workload`` token, the
   server issues a fresh ``execution``-scoped token in the ``Refreshed-API-Token``
   response header. Lifetime equals ``[scheduler] task_queued_timeout`` (default
   600 seconds) — the same timeout the scheduler uses to reap queue-starved tasks —
   so tuning ``task_queued_timeout`` also widens the window a task can wait in a
   backed-up queue before its workload token expires.

**execution**
   A short-lived token (default 10 minutes) accepted by all Execution API endpoints.
   This is the standard scope for worker communication during task execution. Issued
   by the server when the worker transitions to running via the ``/run`` endpoint.
   The ``JWTReissueMiddleware`` refreshes ``execution`` tokens transparently,
   so the worker maintains access for the duration of the task.

Tokens without a ``scope`` claim default to ``"execution"`` for backwards compatibility.

Token delivery to workers
^^^^^^^^^^^^^^^^^^^^^^^^^

The token flows through the execution stack as follows:

1. **Scheduler** generates a ``workload``-scoped token (lifetime equals
   ``[scheduler] task_queued_timeout``, default 600 seconds) and embeds it in the workload
   JSON payload that it passes to **Executor**.
2. The workload JSON is passed to the worker process (via the executor-specific mechanism:
   Celery message, Kubernetes Pod spec, local subprocess arguments, etc.).
3. The worker's ``execute_workload()`` function reads the workload JSON and extracts the token.
4. The ``supervise_task()`` function receives the token and creates an ``httpx.Client`` instance
   with ``BearerAuth(token)`` for all Execution API HTTP requests.
5. The worker calls the ``/run`` endpoint with the ``workload``-scoped token to mark the task
   as running. The server responds with a fresh ``execution``-scoped token in the
   ``Refreshed-API-Token`` header.
6. The client's ``_update_auth()`` hook detects the header and transparently updates
   the ``BearerAuth`` instance to use the new ``execution`` token for all subsequent requests.

Token validation (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``JWTBearer`` security dependency validates the token once per request:

1. Extracts the token from the ``Authorization: Bearer`` header.
2. Performs cryptographic signature validation via ``JWTValidator``.
3. Verifies standard claims (``exp``, ``iat``, ``aud`` — ``nbf`` and ``iss`` if configured).
4. Defaults the ``scope`` claim to ``"execution"`` if absent.
5. Creates a ``TIToken`` object with the task instance ID and claims.
6. Caches the validated token on the ASGI request scope for the duration of the request.

Route-level enforcement is handled by ``require_auth``:

- Checks the token's ``scope`` against the route's ``allowed_token_types`` (precomputed
  by ``ExecutionAPIRoute`` from ``token:*`` Security scopes at route registration time).
- Enforces ``ti:self`` scope — verifies that the token's ``sub`` claim matches the
  ``{task_instance_id}`` path parameter, preventing a worker from accessing another task's
  endpoints.

Token refresh (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``JWTReissueMiddleware`` automatically refreshes valid tokens that are approaching
expiry. The token must be valid at the start of the request for refresh to occur:

1. After each response, the middleware checks the token's remaining validity.
2. If less than **20%** of the total validity remains (minimum 30 seconds), the server
   generates a new token preserving all original claims (including ``scope`` and ``sub``).
3. The refreshed token is returned in the ``Refreshed-API-Token`` response header.
4. The client's ``_update_auth()`` hook detects this header and transparently updates
   the ``BearerAuth`` instance for subsequent requests.

The middleware only refreshes ``execution``-scoped tokens. ``workload``-scoped tokens are
sized to span the queued-timeout window and are explicitly skipped by the middleware —
they are designed to survive executor queue wait times without needing refresh. This
ensures long-running tasks do not lose API access without requiring the worker to
re-authenticate.

No token revocation (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Execution API tokens are not subject to revocation. ``execution``-scoped tokens are short-lived
(default 10 minutes) and automatically refreshed by the ``JWTReissueMiddleware``.
``workload``-scoped tokens (tracking ``[scheduler] task_queued_timeout``) are not refreshed —
they expire naturally after their validity period. Revocation is not part of the Execution API
security model.



Default timings (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Setting
     - Default
   * - ``[execution_api] jwt_expiration_time``
     - 600 seconds (10 minutes)
   * - Workload token lifetime (derived)
     - ``[scheduler] task_queued_timeout`` (default 600 seconds)
   * - ``[execution_api] jwt_audience``
     - ``urn:airflow.apache.org:task``
   * - Token refresh threshold
     - 20% of validity remaining (minimum 30 seconds)


Dag File Processor and Triggerer
---------------------------------

The **Dag File Processor** and **Triggerer** are internal Airflow components that also
interact with the Execution API, but they do so via an **in-process** transport
(``InProcessExecutionAPI``) rather than over the network. This in-process API:

- Runs the Execution API application directly within the same process, using an ASGI/WSGI
  bridge.
- **Potentially bypasses JWT authentication** — the JWT bearer dependency is overridden to
  always return a synthetic ``TIToken`` with the ``"execution"`` scope, effectively bypassing
  token validation.
- Also potentially bypasses per-resource access controls (connection, variable, and XCom access
  checks are overridden to always allow).

Airflow implements software guards that prevent accidental direct database access from Dag
author code in these components. However, because the child processes that parse Dag files and
execute trigger code run as the **same Unix user** as their parent processes, these guards do
not protect against intentional access. A deliberately malicious Dag author can potentially
retrieve the parent process's database credentials (via ``/proc/<PID>/environ``, configuration
files, or secrets manager access) and gain full read/write access to the metadata database and
all Execution API operations — without needing a valid JWT token.

This is in contrast to workers/task execution, where the isolation is implemented ad deployment
level - where sensitive configuration of database credentials is not available to Airflow
processes because they are not set in their deployment configuration at all, and communicate
exclusively through the Execution API.

In the default deployment, a **single Dag File Processor instance** parses Dag files for all
teams and a **single Triggerer instance** handles all triggers across all teams. This means
that Dag author code from different teams executes within the same process, with potentially
shared access to the in-process Execution API and the metadata database.

For multi-team deployments that require isolation, Deployment Managers must run **separate
Dag File Processor and Triggerer instances per team** as a deployment-level measure — Airflow
does not provide built-in support for per-team DFP or Triggerer instances. Even with separate
instances, each retains the same Unix user as the parent process. To prevent credential
retrieval, Deployment Managers must implement Unix user-level isolation (running child
processes as a different, low-privilege user) or network-level restrictions.

See :doc:`/security/security_model` for the full security implications, deployment hardening
guidance, and the planned strategic and tactical improvements.


Workload Isolation and Current Limitations
------------------------------------------

For a detailed discussion of workload isolation protections, current limitations, and planned
improvements, see :ref:`workload-isolation`.


Configuration Reference
------------------------

All JWT-related configuration parameters:

.. list-table::
   :header-rows: 1
   :widths: 40 15 45

   * - Parameter
     - Default
     - Description
   * - ``[api_auth] jwt_secret``
     - Auto-generated if missing
     - Symmetric secret key for signing tokens. Must be the same across all components. Mutually exclusive with ``jwt_private_key_path``.
   * - ``[api_auth] jwt_private_key_path``
     - None
     - Path to PEM-encoded private key (``RSA`` or ``Ed25519``). Mutually exclusive with ``jwt_secret``.
   * - ``[api_auth] jwt_algorithm``
     - ``GUESS``
     - Signing algorithm. Auto-detected from key type: ``HS512`` for symmetric, ``RS256`` for ``RSA``, ``EdDSA`` for ``Ed25519``.
   * - ``[api_auth] jwt_kid``
     - Auto (``RFC 7638`` thumbprint)
     - Key ID placed in token header. Ignored for symmetric keys.
   * - ``[api_auth] jwt_issuer``
     - None
     - Issuer claim (``iss``). Recommended to be unique per deployment.
   * - ``[api_auth] jwt_audience``
     - None
     - Audience claim (``aud``) for REST API tokens.
   * - ``[api_auth] jwt_expiration_time``
     - 86400 (24h)
     - REST API token lifetime in seconds.
   * - ``[api_auth] jwt_cli_expiration_time``
     - 3600 (1h)
     - CLI token lifetime in seconds.
   * - ``[api_auth] jwt_leeway``
     - 10
     - Clock skew tolerance in seconds for token validation.
   * - ``[api_auth] trusted_jwks_url``
     - None
     - JWKS endpoint URL or local file path for token validation. Mutually exclusive with ``jwt_secret``.
   * - ``[execution_api] jwt_expiration_time``
     - 600 (10 min)
     - Execution API ``execution``-scoped token lifetime in seconds.
   * - ``[scheduler] task_queued_timeout``
     - 600.0 (10 min)
     - Queue-starvation timeout. Also sets the ``workload``-scoped token lifetime to the same value.
   * - ``[execution_api] jwt_audience``
     - ``urn:airflow.apache.org:task``
     - Audience claim for Execution API tokens.

.. important::

   Time synchronization across all Airflow components is critical. Use NTP (e.g., ``ntpd`` or
   ``chrony``) to keep clocks in sync. Clock skew beyond the configured ``jwt_leeway`` will cause
   authentication failures.
