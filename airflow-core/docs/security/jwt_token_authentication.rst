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

.. mermaid::

    flowchart LR
        subgraph Clients
            UI[UI / browser]
            CLI[CLI]
            EXT[External REST clients]
        end
        subgraph Internal["Internal Airflow components"]
            WORKER[Worker / Task]
            DFP[Dag File Processor]
            TRG[Triggerer]
        end
        APISVR[API Server]
        EXECAPI[Execution API]
        UI -->|JWT cookie / Bearer| APISVR
        CLI -->|Bearer| APISVR
        EXT -->|Bearer| APISVR
        WORKER -->|Bearer<br/>workload &rarr; execution| EXECAPI
        DFP -. in-process<br/>JWT bypassed .-> EXECAPI
        TRG -. in-process<br/>JWT bypassed .-> EXECAPI

        classDef internal fill:#bbdefb,stroke:#1565c0,stroke-width:2px,color:#000
        class WORKER,DFP,TRG internal


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

.. mermaid::

    flowchart TB
        subgraph Sym["Symmetric (HS512)"]
            direction LR
            S1[Scheduler / API Server]
            S2[Shared secret<br/>jwt_secret]
            S3[Token validator]
            S1 -->|sign| S2 -->|same secret<br/>also validates| S3
        end
        subgraph Asym["Asymmetric (RS256 / EdDSA)"]
            direction LR
            A1[Scheduler / API Server]
            A2[Private key<br/>jwt_private_key_path]
            A3[Public key /<br/>JWKS endpoint]
            A4[Token validator]
            A1 -->|sign| A2
            A2 -. derives or<br/>publishes .-> A3
            A3 -->|verify only| A4
        end

        classDef secret fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#000
        classDef pub fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#000
        class S2 secret
        class A2 secret
        class A3 pub

In asymmetric mode, validators (workers, downstream services) need only the public key — the
private signing key can be tightly scoped to the issuing components (API Server, Scheduler).
In symmetric mode, any component that can validate tokens can also forge them, because there
is only one key. See :ref:`jwt-authentication-and-workload-isolation` for the deployment
implications.

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

The ``/auth/logout`` endpoint always invokes ``auth_manager.revoke_token()`` before any
redirect or cookie deletion. This includes deployments where the configured auth manager
(for example ``FabAuthManager`` or ``KeycloakAuthManager``) redirects the user to an external
logout URL — the JWT is invalidated in Airflow's ``revoked_token`` table regardless of what
the external Identity Provider does with its own session. The ``revoke_token`` call is
unconditional; auth managers that do not implement server-side revocation can keep the
default no-op implementation.

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
     - UUID identifying the task instance, callback, processor session, or parsing attempt, depending on the scope.
   * - ``scope``
     - Token scope: ``"execution"``, ``"workload"``, ``"callback"``, ``"dag_processor_session"``,
       ``"dag_processor"``, or ``"dag_parse"``.
   * - ``iat``
     - Issued-at timestamp.
   * - ``nbf``
     - Not-before timestamp.
   * - ``exp``
     - Expiration timestamp, determined by the scope's lifetime and any parent credential's expiry.

Token scopes (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Execution API uses the following token scopes. Each endpoint accepts only its declared scopes:

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
   A short-lived token (default 10 minutes) accepted by endpoints that allow task or callback execution.
   This is the standard scope for worker communication during task execution. Issued
   by the server when the worker transitions to running via the ``/run`` endpoint.
   The ``JWTReissueMiddleware`` refreshes ``execution`` tokens transparently,
   so the worker maintains access for the duration of the task.

**callback**
   A single-use token exchanged at ``/callbacks/{callback_id}/run`` for an ``execution``
   token when a queued callback starts.

**dag_processor_session**
   A provisioned credential accepted only by ``POST /jobs`` for Job registration and renewal.

**dag_processor**
   A Job credential for heartbeats, completion, Connection and Variable reads for granted bundles,
   and parsing-token exchange.

**dag_parse**
   A credential for one parsing attempt, granting parse-time access within its signed bundle.

See `Dag processor HTTP client credentials`_ for provisioning, lifetime limits, and authorization details.

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
4. The ``supervise_task()`` function receives the token and creates an ``httpx2.Client`` instance
   with ``BearerAuth(token)`` for all Execution API HTTP requests.
5. The worker calls the ``/run`` endpoint with the ``workload``-scoped token to mark the task
   as running. The server responds with a fresh ``execution``-scoped token in the
   ``Refreshed-API-Token`` header.
6. The client's ``_update_auth()`` hook detects the header and transparently updates
   the ``BearerAuth`` instance to use the new ``execution`` token for all subsequent requests.

.. mermaid::

    sequenceDiagram
        autonumber
        participant SCH as Scheduler
        participant EXE as Executor<br/>(Celery / K8s / Local)
        participant WRK as Worker
        participant API as Execution API

        Note over SCH: Task ready to dispatch
        SCH->>SCH: generate workload token<br/>scope=workload<br/>exp = task_queued_timeout
        SCH->>EXE: workload JSON<br/>(includes token)
        Note over EXE: Task waits in queue<br/>(can be minutes)
        EXE->>WRK: dispatch (workload JSON)
        WRK->>API: PATCH /run<br/>Bearer: workload token
        Note over API: validates workload scope<br/>checks TI in QUEUED/RESTARTING<br/>409 if not
        API-->>WRK: 200 OK<br/>Refreshed-API-Token: execution token<br/>(scope=execution, ~10 min)
        WRK->>WRK: BearerAuth swaps to<br/>execution token
        loop For all subsequent calls (heartbeats, XComs, ...)
            WRK->>API: Bearer: execution token
            alt token expiring (less than 20% left)
                API-->>WRK: 200 OK<br/>Refreshed-API-Token: new execution token
                WRK->>WRK: BearerAuth swaps again
            end
        end

Even if a workload token is intercepted in transit, it can only call ``/run``. That endpoint
rejects re-runs (``409 Conflict`` unless the task instance is in ``QUEUED`` or ``RESTARTING``),
so the attack surface for the longer-lived token is bounded to "start a task that is already
queued". Every other endpoint rejects workload tokens.

Token validation (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``JWTBearer`` security dependency validates the token once per request:

1. Extracts the token from the ``Authorization: Bearer`` header.
2. Performs cryptographic signature validation via ``JWTValidator``.
3. Verifies standard claims (``exp``, ``iat``, ``aud`` — ``nbf`` and ``iss`` if configured).
4. Defaults the ``scope`` claim to ``"execution"`` if absent.
5. **Validates the identity and its scope-specific claims against typed Pydantic schemas.**
   ``ExecutionToken`` (in ``airflow.api_fastapi.execution_api.datamodels.token``) parses ``sub``
   as a UUID. Its claims form a discriminated union selected by ``scope``: ``TIClaims`` for
   ``"execution"``, ``"workload"``, and ``"callback"``; ``DagProcessorSessionClaims`` for
   provisioning; ``DagProcessorClaims`` for a registered Job; and ``DagParseClaims`` for a
   parsing attempt. Each processor claim type requires expiry and its scope-specific grants
   and identity fields. Unknown scopes, invalid subjects, or missing required claims return
   ``403 Forbidden`` even when cryptographic validation succeeds. All claim types allow extra
   fields so auth managers can attach deployment-specific claims.
6. Constructs the corresponding token object: ``TIToken``, ``DagProcessorSessionToken``,
   ``DagProcessorToken``, or ``DagParseToken``.
7. Caches the validated token on the ASGI request scope for the duration of the request.

Route-level enforcement is handled by ``require_auth``:

- Checks the token's ``scope`` against the route's ``allowed_token_types`` (precomputed
  by ``ExecutionAPIRoute`` from ``token:*`` Security scopes at route registration time).
- Enforces ``ti:self`` scope — verifies that the token's ``sub`` claim matches the
  ``{task_instance_id}`` path parameter, preventing a worker from accessing another task's
  endpoints.

The task-only, processor-session, and processor-Job dependencies additionally reject tokens of
the wrong principal type. Endpoints shared by tasks and parsing attempts use the general
``CurrentExecutionToken`` dependency with their declared route scopes.

.. mermaid::

    flowchart TD
        REQ([Incoming request<br/>Authorization: Bearer ...])
        REQ --> CACHE{Cached on<br/>request.scope?}
        CACHE -->|yes| RET([Return cached token])
        CACHE -->|no| SIG[JWTValidator:<br/>verify signature]
        SIG -->|fail| F1([403 Forbidden])
        SIG -->|ok| STD[Verify exp / iat / nbf<br/>aud / iss]
        STD -->|fail| F1
        STD -->|ok| SCOPE[Default scope to<br/>'execution' if absent]
        SCOPE --> SCHEMA[ExecutionToken:<br/>UUID identity and<br/>scope-discriminated claims]
        SCHEMA -->|ValidationError| F1
        SCHEMA -->|ok| TYP{require_auth:<br/>scope in<br/>route.allowed_token_types?}
        TYP -->|no| F1
        TYP -->|yes| SELF{ti:self scope<br/>declared?}
        SELF -->|no| PRINCIPAL{Typed dependency:<br/>principal type matches?}
        SELF -->|yes| MATCH{token.sub ==<br/>task_instance_id?}
        MATCH -->|no| F1
        MATCH -->|yes| PRINCIPAL
        PRINCIPAL -->|no| F1
        PRINCIPAL -->|yes or general dependency| OK([Return validated token])

        classDef fail fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#000
        classDef pass fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#000
        class F1 fail
        class OK,RET pass

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

Task-token revocation (Execution API)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Task Execution API tokens are not subject to revocation. ``execution``-scoped tokens are short-lived
(default 10 minutes) and automatically refreshed by the ``JWTReissueMiddleware``.
``workload``-scoped tokens (tracking ``[scheduler] task_queued_timeout``) are not refreshed —
they expire naturally after their validity period. Processor and parsing credentials additionally
require an open, owning Job, as described under "Dag processor HTTP client credentials" below.



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


Dag processor HTTP client credentials
-------------------------------------

The core ``DagProcessorAPIClient`` lets processor integrations authenticate to the Execution
API without holding its signing key. The standard ``airflow dag-processor`` command does
not yet use this client. Provisioning a token file alone does not switch that command to
HTTP or remove its need for database access.

In-memory sessions from workload identity
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Processor integrations can use ``WorkloadIdentitySessionTokenProvider`` to obtain and renew
``dag_processor_session`` credentials through ``POST /execution/dag-processor/session-token``.
The provider retains these credentials only in memory. It requires a renewable, signed workload
identity from the deployment platform; Airflow does not create that identity or require the
processor to hold an Airflow signing key.

Configure the API server's ``[execution_api] dag_processor_workload_identity`` option with JSON::

    {
      "issuer": "https://identity.example/",
      "jwks_url": "https://identity.example/openid/v1/jwks",
      "audience": "airflow-dag-processor-bootstrap",
      "algorithm": "RS256",
      "instance_id_claim": ["kubernetes.io", "pod", "uid"],
      "grants": {
        "system:serviceaccount:airflow:processor": ["dags-folder"]
      }
    }

Use your identity provider's exact issuer, HTTPS JWKS URL, and signing algorithm (``RS256``,
``ES256``, or ``EdDSA``). Use a dedicated audience, distinct from ``[execution_api] jwt_audience``.
The API server verifies the signature, issuer, audience, issued-at time, expiry, and any not-before
claim, using ``[api_auth] jwt_leeway`` for clock skew. Only exact subjects listed in ``grants`` can
obtain a session. If a grant names a Dag bundle that the API server does not have configured, the
API server logs an error at startup and refuses that subject's exchanges with HTTP 403. This option
is empty by default, disabling the exchange.
Apply the same trust configuration to all API-server replicas and restart them after changes.

The option trusts a single issuer. Dag processors whose identities come from different issuers, such
as separate Kubernetes clusters, cannot all obtain sessions from the same API server.

The JWKS endpoint must accept unauthenticated HTTPS GET requests from the Airflow API server.
The fetcher sends no authentication credentials and uses HTTPX's default certificate trust.
``SSL_CERT_FILE`` or ``SSL_CERT_DIR``, when set, replace the default CA bundle rather than adding
to it, and they apply to every HTTPX client in the API-server process. A bundle that holds only a
private CA cuts those clients off from public roots, so include the roots they still need. There is
no per-issuer CA or authentication option.
Authenticated JWKS endpoints, including Kubernetes API-server discovery protected by service-account
authentication, are not supported by this implementation. For those clusters, serve the public JWKS
through a suitable HTTPS endpoint before enabling the exchange. See Kubernetes'
`service account issuer discovery documentation <https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/#service-account-issuer-discovery>`_.
The processor-to-Airflow TLS settings described below do not configure this outbound JWKS connection.

``instance_id_claim`` is a path through signed claims to a nonempty string identifying one
processor replica. A shared service-account subject alone is insufficient: replicas must have
different instance identities. Run one processor per identity. The session ID stays stable across
identity-token renewal and API-server restarts. Changes to the issuer, audience, subject, instance
claim, instance value, or bundle grants produce a different session. Request bodies and caller
hostnames cannot select the session or its grants.

Changing grants while API-server replicas serve different configurations can repeatedly retire
processor registrations, because those replicas issue different session IDs for the same identity.
Stop the affected processors, finish deploying identical trust configuration to every API-server
replica, and then restart the processors.

For Kubernetes, the example uses the Pod UID in a
`bound service-account token <https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/#bound-service-account-token-volume-mechanism>`_.
Project a token with the dedicated audience and mount the projection directory read-only in the
processor container. Configure the projection's ``expirationSeconds`` and allow Kubernetes to
rotate it. See `serviceAccountToken projected volumes
<https://kubernetes.io/docs/concepts/storage/projected-volumes/#serviceaccounttoken>`_.

A container restart within the same Pod keeps the Pod UID and therefore the session ID.
A replacement process cannot register while the previous Job is still alive. The client raises
``DagProcessorJobAlreadyRunning``; the integration must retry registration with backoff until the
previous Job completes or its heartbeat becomes stale under ``[dag_processor] health_check_threshold``.
The client does not wait automatically. This prevents two processes sharing an identity from owning
active Jobs at the same time.

The processor integration supplies a callable that reads the current platform identity on each
exchange. Any exception the callable raises is re-raised as ``WorkloadIdentityUnavailableError``, an
``OSError``, so a failed optional renewal leaves the valid Job credential in use. For example:

.. code-block:: python

    from pathlib import Path

    from airflow.dag_processing.api_client import DagProcessorAPIClient
    from airflow.dag_processing.credentials import WorkloadIdentitySessionTokenProvider

    identity_path = Path("/var/run/airflow-identity/token")
    provider = WorkloadIdentitySessionTokenProvider(
        base_url="https://airflow.example/execution/",
        identity_token=identity_path.read_text,
    )
    processor = DagProcessorAPIClient(
        base_url="https://airflow.example/execution/",
        token_provider=provider,
        hostname="processor-1",
    )

Use this client for the integration's Job lifecycle and close it during shutdown; closing it also
closes the provider and clears its session cache. The file in this example contains the platform's
projected identity, not an Airflow session credential. The provider never writes the exchanged
session JWT to disk and does not fall back to a session-token file. ``token_provider`` and
``token_file`` are mutually exclusive. HTTPS is required for the exchange, and redirects are rejected.
The exchange uses the same API TLS configuration and request timeout as the Task SDK client,
including private certificate authorities and client certificates. Registration retries transient
exchange failures using the configured Execution API retry policy, waiting at least the
``Retry-After`` interval of an HTTP 503 before the next attempt. Optional early renewal makes one
bounded attempt so it does not delay requests using a valid Job credential.

Session expiry is capped by both the external identity's remaining lifetime and
``[execution_api] jwt_expiration_time``. Clock-skew leeway never extends this cap, and an identity
with less than one second remaining cannot obtain a session. Renewal rereads the platform identity
and retains the same session and Job. During an identity-provider or API outage, the client can continue using
an already-issued Job credential until it expires; it cannot renew without a valid identity.
Removing a grant stops future exchanges once the API servers restart with the new configuration. Credentials
already issued remain subject to their expiry and Job ownership checks.

Verification uses the issuer's JWKS and does not perform online token introspection. In particular,
it does not check whether a Kubernetes Pod still exists. A previously issued identity may remain
usable until expiry. The JWKS cache refreshes hourly and also refreshes when an identity names an
unknown signing key. Fetches, including failed attempts, are limited to one per ten seconds per
API-server process. Publish new keys before signing with them and retain old keys while their tokens
remain valid.

If a required signing key cannot be fetched, the exchange returns HTTP 503 with ``Retry-After``, and
registration waits at least that long before retrying. A cached matching key remains usable during
an outage. Invalid identities and keys absent from a successfully fetched key set return HTTP 401.

File-based provisioning
^^^^^^^^^^^^^^^^^^^^^^^

On a trusted host with the API signing key, provision a session token for the bundles
the client may access::

    airflow dag-processor-token --token-file /run/airflow/processor.jwt \
        --bundle-name dags-folder --rotate

The command writes the token atomically. A new file is readable only by its owner, and replacing
an existing file keeps its mode and group, so grant the processor read access once. ACLs are not
carried over. Keep the provisioner running and mount its directory rather than a single file so
token replacement stays visible. Each processor process needs its own session. Restarting the
provisioner with the same token file and bundles keeps that session, so running processors keep
their Jobs.

Credential permissions and renewal
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The three credentials have different issuers and permissions:

- ``dag_processor_session`` is issued by the identity exchange or trusted file provisioning. It can only register a
  Job or renew that Job's credential through ``POST /jobs``.
- ``dag_processor`` is returned by Job registration. It can heartbeat and complete that
  Job, read Connections and Variables for a granted bundle, and exchange a parsing
  credential through ``POST /jobs/{job_id}/parse-token``. It cannot outlive the session
  token used to obtain it.
- ``dag_parse`` is returned by the parsing-token exchange for one attempt, bundle, and
  relative file location. It permits parse-time requests, including callback-context reads,
  but cannot manage Jobs or exchange tokens. It cannot outlive its parent Job credential.

Registration retries keep the same registration UUID. A new process uses a new UUID;
it cannot replace a session's Job while that Job is alive. Ending or replacing the Job
invalidates its runtime credentials.

The client selects the bundle with ``use_bundle`` and a parsing attempt with ``use_parse``.
A request header cannot override the parsing token's signed bundle.
Connection and Variable access uses the bundle's current team. A registered Job's teams come from
``[dag_processor] dag_bundle_config_list`` on the API server, so it needs the same bundle-to-team
configuration as the processor. A ``dag_parse`` token can also write and delete Variables, as a
parsing subprocess can without this client. ``Variable.set`` upserts on the key alone, so in
multi-team mode a write can overwrite another team's Variable with the same key, as it can with task
tokens.

The client attempts renewal at 80% of token lifetime. Transient early-renewal failures leave
the existing token usable until expiry. A retired registration signals that its owner must
restart. Completion retains its original identity and outcome across retries.

File and attempt identity are carried in the token but are not currently used for access
checks or request attribution. Archives retain one file identity under the current processor
model. These credentials do not isolate hostile code from other credentials accessible on
the processor host; the process and deployment limitations above still apply.


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
   * - ``[execution_api] dag_processor_workload_identity``
     - Empty (disabled)
     - JSON trust settings and subject-to-bundle grants for workload identity session provisioning.

.. important::

   Time synchronization across all Airflow components is critical. Use NTP (e.g., ``ntpd`` or
   ``chrony``) to keep clocks in sync. Clock skew beyond the configured ``jwt_leeway`` will cause
   authentication failures.
