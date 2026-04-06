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
   the corresponding public key for validation. Supported algorithms: **RS256** (RSA) and
   **EdDSA** (Ed25519). The algorithm is auto-detected from the key type when
   ``[api_auth] jwt_algorithm`` is set to ``GUESS`` (the default).

   Validation can use either:

   - A JWKS (JSON Web Key Set) endpoint configured via ``[api_auth] trusted_jwks_url``
     (local file or remote HTTP/HTTPS URL, polled periodically for updates).
   - The public key derived from the configured private key (automatic fallback when
     ``trusted_jwks_url`` is not set).

The asymmetric mode is recommended for production deployments where you want workers
and the API server to operate with different credentials (workers only need the private key for
token generation; the API server only needs the JWKS for validation).


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
     - Issuer (from ``[api_auth] jwt_issuer``). Optional but recommended.
   * - ``aud``
     - Audience (from ``[api_auth] jwt_audience``). Optional but recommended.
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

Execution API tokens are not subject to revocation. They are short-lived (default 10 minutes)
and automatically refreshed by the ``JWTReissueMiddleware``, so revocation is not part of the
Execution API security model. Once an Execution API token is issued to a worker, it remains
valid until it expires.

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

The Execution API is an internal API used by workers to report task state transitions,
heartbeats, and to retrieve connections, variables, and XComs at task runtime.

Token generation (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. The **Scheduler** (via the executor) generates a JWT for each task instance before
   dispatching it to a worker. The executor's ``jwt_generator`` property creates a
   ``JWTGenerator`` configured with the ``[execution_api]`` settings.
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
     - Issuer (from ``[api_auth] jwt_issuer``). Optional.
   * - ``aud``
     - Audience (from ``[execution_api] jwt_audience``, default: ``urn:airflow.apache.org:task``).
   * - ``sub``
     - Task instance UUID — the identity of the workload.
   * - ``scope``
     - Token scope: ``"execution"`` (default) or ``"workload"`` (restricted).
   * - ``iat``
     - Issued-at timestamp.
   * - ``nbf``
     - Not-before timestamp.
   * - ``exp``
     - Expiration timestamp (``iat + [execution_api] jwt_expiration_time``).

Token scopes (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Execution API defines two token scopes:

**execution** (default)
   Accepted by all Execution API endpoints. This is the standard scope for worker
   communication.

**workload**
   A restricted scope accepted only on endpoints that explicitly opt in via
   ``Security(require_auth, scopes=["token:workload"])``. Used for endpoints that
   manage task state transitions.

Tokens without a ``scope`` claim default to ``"execution"`` for backwards compatibility.

Token delivery to workers
^^^^^^^^^^^^^^^^^^^^^^^^^

The token flows through the execution stack as follows:

1. **Executor** generates the token and embeds it in the workload JSON payload.
2. The workload JSON is passed to the worker process (via the executor-specific mechanism:
   Celery message, Kubernetes Pod spec, local subprocess arguments, etc.).
3. The worker's ``execute_workload()`` function reads the workload JSON and extracts the token.
4. The ``supervise()`` function receives the token and creates an ``httpx.Client`` instance
   with ``BearerAuth(token)`` for all Execution API HTTP requests.
5. The token is included in the ``Authorization: Bearer <token>`` header of every request.

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

The ``JWTReissueMiddleware`` automatically refreshes tokens that are approaching expiry:

1. After each response, the middleware checks the token's remaining validity.
2. If less than **20%** of the total validity remains (minimum 30 seconds), the server
   generates a new token preserving all original claims (including ``scope`` and ``sub``).
3. The refreshed token is returned in the ``Refreshed-API-Token`` response header.
4. The client's ``_update_auth()`` hook detects this header and transparently updates
   the ``BearerAuth`` instance for subsequent requests.

This mechanism ensures long-running tasks do not lose API access due to token expiry,
without requiring the worker to re-authenticate.

Default timings (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Setting
     - Default
   * - ``[execution_api] jwt_expiration_time``
     - 600 seconds (10 minutes)
   * - ``[execution_api] jwt_audience``
     - ``urn:airflow.apache.org:task``
   * - Token refresh threshold
     - 20% of validity remaining (minimum 30 seconds, i.e., at ~120 seconds before expiry
       with the default 600-second token lifetime)


Dag File Processor and Triggerer
---------------------------------

The **Dag File Processor** and **Triggerer** are internal Airflow components that also
interact with the Execution API, but they do so via an **in-process** transport
(``InProcessExecutionAPI``) rather than over the network. This in-process API:

- Runs the Execution API application directly within the same process, using an ASGI/WSGI
  bridge.
- **Bypasses JWT authentication entirely** — the JWT bearer dependency is overridden to
  always return a synthetic ``TIToken`` with the ``"execution"`` scope.
- Also bypasses per-resource access controls (connection, variable, and XCom access checks
  are overridden to always allow).

This design means that code running in the Dag File Processor or Triggerer has **unrestricted
access** to all Execution API operations without needing a valid JWT token. Since the Dag File
Processor parses user-submitted Dag files and the Triggerer executes user-submitted trigger
code, Dag authors whose code runs in these components effectively have the same level of
access as the internal API itself.

In the default deployment, a **single Dag File Processor instance** parses Dag files for all
teams and a **single Triggerer instance** handles all triggers across all teams. This means
that Dag author code from different teams executes within the same process, with shared access
to the in-process Execution API and the metadata database.

For multi-team deployments that require isolation, Deployment Managers must run **separate
Dag File Processor and Triggerer instances per team** as a deployment-level measure — Airflow
does not provide built-in support for per-team DFP or Triggerer instances. However, even with
separate instances, these components still have direct access to the metadata database
(the Dag File Processor needs it to store serialized Dags, and the Triggerer needs it to
manage trigger state). A Dag author whose code runs in these components can potentially
access the database directly, including reading or modifying data belonging to other teams,
or obtaining the JWT signing key if it is available in the process environment.

See :doc:`/security/security_model` for the full security implications and deployment
hardening guidance.


Workload Isolation and Current Limitations
------------------------------------------

The current JWT authentication model operates under the following assumptions and limitations:

**Worker process memory protection (Linux)**
   On Linux, the supervisor process calls ``prctl(PR_SET_DUMPABLE, 0)`` at the start of
   ``supervise()`` before forking the task process. This flag is inherited by the forked
   child. Marking processes as non-dumpable prevents same-UID sibling processes from reading
   ``/proc/<pid>/mem``, ``/proc/<pid>/environ``, or ``/proc/<pid>/maps``, and blocks
   ``ptrace(PTRACE_ATTACH)``. This is critical because each supervisor holds a distinct JWT
   token in memory — without this protection, a malicious task process running as the same
   Unix user could steal tokens from sibling supervisor processes.

   This protection is one of the reasons that passing sensitive configuration via environment
   variables is safer than via configuration files: environment variables are only readable
   by the process itself (and root), whereas configuration files on disk are readable by any
   process with filesystem access running as the same user.

   .. note::

      This protection is Linux-specific. On non-Linux platforms, the
      ``_make_process_nondumpable()`` call is a no-op. Deployment Managers running Airflow
      on non-Linux platforms should implement alternative isolation measures.

**No cross-workload isolation**
   All worker workloads authenticate to the same Execution API with tokens that share the
   same signing key, audience, and issuer. While the ``ti:self`` scope enforcement prevents
   a worker from accessing *another task instance's* specific endpoints (e.g., heartbeat,
   state transitions), the token grants access to shared resources such as connections,
   variables, and XComs that are not scoped to individual tasks.

**No team-level isolation in Execution API (experimental multi-team feature)**
   The experimental multi-team feature (``[core] multi_team``) provides UI-level and REST
   API-level RBAC isolation between teams, but **does not yet guarantee task-level isolation**.
   At the Execution API level, there is no enforcement of team-based access boundaries.
   A task from one team can access the same connections, variables, and XComs as a task from
   another team. All workloads share the same JWT signing keys and audience regardless of team
   assignment.

   In deployments where additional hardening measures are not implemented at the deployment
   level, a task from one team can potentially access resources belonging to another team
   (see :doc:`/security/security_model`). A deep understanding of configuration and deployment
   security is required by Deployment Managers to configure it in a way that can guarantee
   separation between teams. Task-level team isolation will be improved in future versions
   of Airflow.

**Dag File Processor and Triggerer bypass**
   As described above, the default deployment runs a single Dag File Processor and a single
   Triggerer for all teams. Both bypass JWT authentication entirely via in-process transport.
   For multi-team isolation, Deployment Managers must run separate instances per team, but
   even then, each instance retains direct database access. A Dag author whose code runs
   in these components can potentially access the database directly — including data belonging
   to other teams or the JWT signing key configuration — unless the Deployment Manager
   restricts the database credentials and configuration available to each instance.

**Planned improvements**
   Future versions of Airflow will address these limitations with:

   - Finer-grained token scopes tied to specific resources (connections, variables) and teams.
   - Enforcement of team-based isolation in the Execution API.
   - Built-in support for per-team Dag File Processor and Triggerer instances.
   - Improved sandboxing of user-submitted code in the Dag File Processor and Triggerer.
   - Full task-level isolation for the multi-team feature.


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
     - Auto-generated
     - Symmetric secret key for signing tokens. Must be the same across all components. Mutually exclusive with ``jwt_private_key_path``.
   * - ``[api_auth] jwt_private_key_path``
     - None
     - Path to PEM-encoded private key (RSA or Ed25519). Mutually exclusive with ``jwt_secret``.
   * - ``[api_auth] jwt_algorithm``
     - ``GUESS``
     - Signing algorithm. Auto-detected from key type: HS512 for symmetric, RS256 for RSA, EdDSA for Ed25519.
   * - ``[api_auth] jwt_kid``
     - Auto (RFC 7638 thumbprint)
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
     - Execution API token lifetime in seconds.
   * - ``[execution_api] jwt_audience``
     - ``urn:airflow.apache.org:task``
     - Audience claim for Execution API tokens.

.. important::

   Time synchronization across all Airflow components is critical. Use NTP (e.g., ``ntpd`` or
   ``chrony``) to keep clocks in sync. Clock skew beyond the configured ``jwt_leeway`` will cause
   authentication failures.
