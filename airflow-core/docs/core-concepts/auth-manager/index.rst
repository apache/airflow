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

Auth manager
============

Auth (for authentication/authorization) manager is the component in Airflow to handle user authentication and user authorization. They have a common
API and are "pluggable", meaning you can swap auth managers based on your installation needs.

.. image:: ../../img/diagram_auth_manager_airflow_architecture.png

Airflow can only have one auth manager configured at a time; this is set by the ``auth_manager`` option in the
``[core]`` section of :doc:`the configuration file </howto/set-config>`.

.. note::
    For more information on Airflow's configuration, see :doc:`/howto/set-config`.

If you want to check which auth manager is currently set, you can use the
``airflow config get-value core auth_manager`` command:

.. code-block:: bash

    $ airflow config get-value core auth_manager
    airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

.. toctree::
    :hidden:

    simple/index

Available auth managers to use
------------------------------

Here is the list of auth managers available today that you can use in your Airflow environment.

Provided by Airflow:

* :doc:`simple/index`

Provided by providers. The list of supported auth managers is available in :doc:`apache-airflow-providers:core-extensions/auth-managers`.

Why pluggable auth managers?
----------------------------

Airflow is used by a lot of different users with a lot of different configurations. Some Airflow environment might be
used by only one user and some might be used by thousand of users. An Airflow environment with only one (or very few)
users does not need the same user management as an environment used by thousand of them.

This is why the whole user management (user authentication and user authorization) is packaged in one component
called auth manager. So that it is easy to plug-and-play an auth manager that suits your specific needs.

By default, Airflow comes with the :doc:`simple/index`.

.. note::
    Switching to a different auth manager is a heavy operation and should be considered as such. It will
    impact users of the environment. The sign-in and sign-off experience will very likely change and disturb them if
    they are not advised. Plus, all current users and permissions will have to be copied over from the previous auth
    manager to the next.

Writing your own auth manager
-----------------------------

All Airflow auth managers implement a common interface so that they are pluggable and any auth manager has access
to all abilities and integrations within Airflow. This interface is used across Airflow to perform all user
authentication and user authorization related operation.

The public interface is :class:`~airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager`.
You can look through the code for the most detailed and up to date interface, but some important highlights are
outlined below.

.. note::
    For more information about Airflow's public interface see :doc:`/public-airflow-interface`.

Some reasons you may want to write a custom auth manager include:

* An auth manager does not exist which fits your specific use case, such as a specific tool or service for user management.
* You'd like to use an auth manager that leverages an identity provider from your preferred cloud provider.
* You have a private user management tool that is only available to you or your organization.

User representation
^^^^^^^^^^^^^^^^^^^

:class:`~airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager` defines an authentication manager,
parameterized by a user class T representing the authenticated user type.
Auth manager implementations (subclasses of :class:`~airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager`)
should specify the associated concrete user type. Each auth manager has its own user type definition.
Concrete user types should be subclass of :class:`~airflow.api_fastapi.auth.managers.models.base_user.BaseUser`.

Authentication related methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* ``get_url_login``: Return the URL the user is redirected to for signing in.
* ``get_url_logout``: Return the URL the user is redirected to when logging out. This is an optional method,
  this redirection is usually needed to invalidate resources when logging out, such as a session.
* ``serialize_user``: Serialize a user instance to a dict. This dict is the actual content of the JWT token.
  It should contain all the information needed to identify the user and make an authorization request.
* ``deserialize_user``: Create a user instance from a dict. The dict is the payload of the JWT token.
  This is the same dict returned by ``serialize_user``.

Authorization related methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most of authorization methods in :class:`~airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager` look the same.
Let's go over the different parameters used by most of these methods.

* ``method``: Use HTTP method naming to determine the type of action being done on a specific resource.

  * ``GET``: Can the user read the resource?
  * ``POST``: Can the user create a resource?
  * ``PUT``: Can the user modify the resource?
  * ``DELETE``: Can the user delete the resource?

* ``details``: Optional details about the resource being accessed.
* ``user``: The user trying to access the resource.


These authorization methods are:

* ``is_authorized_configuration``: Return whether the user is authorized to access Airflow configuration. Some details about the configuration can be provided (e.g. the config section).
* ``is_authorized_connection``: Return whether the user is authorized to access Airflow connections. Some details about the connection can be provided (e.g. the connection ID).
* ``is_authorized_dag``: Return whether the user is authorized to access a Dag. Some details about the Dag can be provided (e.g. the Dag ID).
  Also, ``is_authorized_dag`` is called for any entity related to Dags (e.g. task instances, Dag runs, ...). This information is passed in ``access_entity``.
  Example: ``auth_manager.is_authorized_dag(method="GET", access_entity=DagAccessEntity.Run, details=DagDetails(id="dag-1"))`` asks
  whether the user has permission to read the Dag runs of the Dag "dag-1".
* ``is_authorized_asset``: Return whether the user is authorized to access Airflow assets. Some details about the asset can be provided (e.g. the asset ID).
* ``is_authorized_asset_alias``: Return whether the user is authorized to access Airflow asset aliases. Some details about the asset alias can be provided (e.g. the asset alias ID).
* ``is_authorized_pool``: Return whether the user is authorized to access Airflow pools. Some details about the pool can be provided (e.g. the pool name).
* ``is_authorized_variable``: Return whether the user is authorized to access Airflow variables. Some details about the variable can be provided (e.g. the variable key).
* ``is_authorized_view``: Return whether the user is authorized to access a specific view in Airflow. The view is specified through ``access_view`` (e.g. ``AccessView.CLUSTER_ACTIVITY``). An optional ``team_name`` scopes the check to a team -- see :ref:`team-scoped-view-authorization` below.
* ``is_authorized_custom_view``: Return whether the user is authorized to access a specific view not defined in Airflow. This view can be provided by the auth manager itself or a plugin defined by the user.
* ``filter_authorized_menu_items``: Given the list of menu items in the UI, return the list of menu items the user has access to.
* ``is_authorized_hitl_task``: Return whether the user is authorized to approve or reject a Human-in-the-loop (HITL) task.
  This is an optional method: the default implementation returns whether the user's ID is in ``assigned_users``, the IDs of the users assigned to the task.
  Airflow only calls this method for tasks that have assigned users. When a task has none, Airflow skips this method and any user allowed to
  update the task's HITL detail (``is_authorized_dag`` with ``access_entity=DagAccessEntity.HITL_DETAIL``) can respond.

It should be noted that the ``method`` parameter listed above may only have relevance for a specific subset of the auth manager's authorization methods.
For example, the ``configuration`` resource is by definition read-only, so only the ``GET`` parameter is relevant in the context of ``is_authorized_configuration``.

.. _team-scoped-view-authorization:

Team-scoped view authorization
''''''''''''''''''''''''''''''

.. versionadded:: 3.4.0
   ``is_authorized_view`` accepts an optional ``team_name`` argument.

In a multi-team deployment, access to a read-only view can be restricted to the users of a
specific team. ``is_authorized_view`` accepts an optional ``team_name`` for that purpose. An
auth manager that implements multi-team isolation honors it and only authorizes users who
belong to ``team_name``; an auth manager without multi-team support accepts the argument and
ignores it, which authorizes the view across all teams (the same behaviour it had before the
argument existed).

Core never calls ``is_authorized_view`` with ``team_name`` directly. It goes through
``BaseAuthManager.authorize_view``, which first checks whether the auth manager's
``is_authorized_view`` accepts ``team_name``. This keeps auth managers that predate the
argument working: a custom or out-of-tree auth manager whose ``is_authorized_view`` still has
the old ``(access_view, user)`` signature is called *without* ``team_name`` -- it does not
raise -- and a ``RemovedInAirflow4Warning`` is emitted, warning that some views that should be
restricted to a single team are instead authorized across all teams until the auth manager is
upgraded.

To make an auth manager team-aware, add ``team_name`` to the override (managers without
multi-team support may accept and ignore it):

.. code-block:: python

    def is_authorized_view(
        self, *, access_view: AccessView, user: MyUser, team_name: str | None = None
    ) -> bool: ...

The fallback for the older signature is removed in Airflow 4, which requires ``team_name``.

JWT token management by auth managers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The auth manager is responsible for creating the JWT token needed to interact with Airflow public API.
To achieve this, the auth manager **must** provide an endpoint to create this JWT token. This endpoint is usually
available at ``POST /auth/token``.
Please double check the auth manager documentation to find the accurate token generation endpoint.

The auth manager is also responsible for passing the JWT token to the Airflow UI. The protocol to exchange the JWT
token between the auth manager and Airflow UI uses a cookie named ``_token``. The auth manager must attach this
cookie to the final response that redirects the authenticated user to the UI. The browser then sends the
``httponly`` cookie with subsequent requests; the UI does not manage the token.

.. note::
  Ensure that the cookie parameter ``httponly`` is set to ``True``. The UI does not manage the token.

Redirect-based UI login flows
'''''''''''''''''''''''''''''

OAuth, OIDC, SAML, and similar login protocols leave Airflow while the identity provider authenticates the user.
For these flows, complete authentication and set the Airflow JWT cookie before returning to the UI:

#. When an unauthenticated UI request receives a ``401``, the UI navigates to ``/api/v2/auth/login`` and sends its
   original destination in ``next``.
#. Airflow validates ``next`` and forwards it to the auth manager's mounted login endpoint.
#. The login endpoint validates the return URL again and preserves it across the external redirects in
   integrity-protected state, together with a unique nonce for this login attempt.
#. The callback verifies and consumes the CSRF/state value, completes the provider exchange, verifies the provider
   credentials, constructs the Airflow user, and calls ``get_auth_manager().generate_jwt(user)``.
#. Only after those operations succeed does the callback create the final ``RedirectResponse`` and attach the
   ``_token`` cookie to that same response.
#. The callback returns a ``303`` redirect to the validated return URL, or to the configured ``[api] base_url`` when
   there was no original destination.

The following compact example shows the two auth-manager handlers. The ``validate_airflow_return_url``,
``store_login_nonce``, ``sign_login_state``, ``verify_and_consume_login_state``,
``build_provider_authorization_url``, and ``exchange_code_and_build_user`` helpers are placeholders that the auth
manager must implement. Airflow does not provide provider token exchange or state signing.

.. code-block:: python

    import secrets
    from urllib.parse import urlsplit, urlunsplit

    from fastapi import APIRouter, HTTPException, Request, status
    from fastapi.responses import RedirectResponse

    from airflow.api_fastapi.app import (
        AUTH_MANAGER_FASTAPI_APP_PREFIX,
        get_auth_manager,
        get_cookie_path,
    )
    from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
    from airflow.configuration import conf

    router = APIRouter()
    airflow_base_url = conf.get("api", "base_url", fallback="/")
    base_url_parts = urlsplit(airflow_base_url)
    callback_url = urlunsplit(
        (
            base_url_parts.scheme,
            base_url_parts.netloc,
            f"{AUTH_MANAGER_FASTAPI_APP_PREFIX.rstrip('/')}/callback",
            "",
            "",
        )
    )


    @router.get("/login")
    def login(next: str | None = None) -> RedirectResponse:
        validated_return_url = validate_airflow_return_url(next, base_url=airflow_base_url)
        nonce = secrets.token_urlsafe(32)
        store_login_nonce(nonce)
        state = sign_login_state({"nonce": nonce, "return_url": validated_return_url})
        provider_url = build_provider_authorization_url(redirect_uri=callback_url, state=state)
        return RedirectResponse(url=provider_url, status_code=303)


    @router.get("/callback")
    def callback(
        request: Request,
        state: str | None = None,
        code: str | None = None,
        error: str | None = None,
    ) -> RedirectResponse:
        if state is None:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Missing login state")

        # This verifies the signature and expiry, matches the stored nonce, and consumes it to prevent replay.
        login_state = verify_and_consume_login_state(state)
        if error is not None or code is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")

        validated_return_url = validate_airflow_return_url(login_state["return_url"], base_url=airflow_base_url)
        user = exchange_code_and_build_user(code=code, redirect_uri=callback_url)
        token = get_auth_manager().generate_jwt(user)

        secure = request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))
        response = RedirectResponse(url=validated_return_url, status_code=303)
        response.set_cookie(
            COOKIE_NAME_JWT_TOKEN,
            token,
            path=get_cookie_path(),
            secure=secure,
            httponly=True,
            samesite="lax",
        )
        return response

Treat the return URL as opaque state so that its query string is neither lost nor double-encoded. Never redirect to a
raw ``next`` value returned by the identity provider: protect its integrity and restrict both the initial and restored
values to the configured Airflow origin and base path. Use a unique, expiring nonce for each attempt rather than one
global mutable return URL, so concurrent logins in separate tabs cannot overwrite each other. Reject missing,
expired, replayed, or mismatched state before generating the Airflow JWT.

On provider denial, token-exchange failure, user-construction failure, or JWT-generation failure, return an error
without setting ``_token`` or redirecting to the UI. Do not log provider credentials, authorization codes, or Airflow
JWTs. The UI must not be an intermediate callback page and must not be revisited before the cookie is attached.

Use ``get_cookie_path()`` consistently, especially when Airflow is served under a URL prefix; setting another
``_token`` cookie at ``/`` can make competing cookies cause redirect loops. Derive ``secure`` consistently with the
example above, and configure reverse-proxy forwarding so that the API server detects HTTPS correctly. If an explicit
cookie lifetime is used, it must not exceed the configured JWT validity; do not copy an arbitrary ``max_age`` from an
example.

The `FAB OAuth fix <https://github.com/apache/airflow/pull/61287>`_ marks a Flask session as modified so that FAB's
session is persisted before its redirect. A FastAPI custom auth manager should use the final-callback JWT-cookie
pattern above instead of Flask session APIs. Logout and token refresh are separate flows; refresh behavior is
documented below.

Refreshing JWT Token
''''''''''''''''''''
Refreshing token is optional feature and its availability depends on the specific implementation of the auth manager.
The auth manager is responsible for refreshing the JWT token when it expires.
The Airflow API uses middleware that intercepts every request and checks the validity of the JWT token.
Token communication is handled through ``httponly`` cookies to improve security.
When the token expires, the `JWTRefreshMiddleware <https://github.com/apache/airflow/blob/3.1.5/airflow-core/src/airflow/api_fastapi/auth/middlewares/refresh_token.py>`_ middleware calls the auth manager's ``refresh_user`` method to obtain a new token.


To support token refresh operations, the auth manager must implement the ``refresh_user`` method.
This method receives an expired token and must return a new valid token.
User information is extracted from the expired token and used to generate a fresh token.

An example implementation of ``refresh_user`` could be:
`KeycloakAuthManager::refresh_user <https://github.com/apache/airflow/blob/3.1.5/providers/keycloak/src/airflow/providers/keycloak/auth_manager/keycloak_auth_manager.py#L113-L121>`_
User information is derived from the ``BaseUser`` instance.
It is important that the user object contains all the fields required to refresh the token. An example user class could be:
`KeycloakAuthManagerUser(BaseUser) <https://github.com/apache/airflow/blob/3.1.5/providers/keycloak/src/airflow/providers/keycloak/auth_manager/user.pys>`_.

Optional methods recommended to override for optimization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following methods aren't required to override to have a functional Airflow auth manager. However, it is recommended to override these to make your auth manager faster (and potentially less costly):

* ``batch_is_authorized_connection``: Batch version of ``is_authorized_connection``. If not overridden, it calls ``is_authorized_connection`` for every single item.
* ``batch_is_authorized_dag``: Batch version of ``is_authorized_dag``. If not overridden, it calls ``is_authorized_dag`` for every single item.
* ``batch_is_authorized_pool``: Batch version of ``is_authorized_pool``. If not overridden, it calls ``is_authorized_pool`` for every single item.
* ``batch_is_authorized_variable``: Batch version of ``is_authorized_variable``. If not overridden, it calls ``is_authorized_variable`` for every single item.
* ``filter_authorized_assets``: Given a list of assets (each carrying its id, name and uri), return the ids of the assets the user has access to.  If not overridden, it calls ``is_authorized_asset`` for every single asset passed as parameter.
* ``filter_authorized_connections``: Given a list of connection IDs (``conn_id``), return the list of connection IDs the user has access to.  If not overridden, it calls ``is_authorized_connection`` for every single connection passed as parameter.
* ``filter_authorized_dag_ids``: Given a list of Dag IDs, return the list of Dag IDs the user has access to.  If not overridden, it calls ``is_authorized_dag`` for every single Dag passes as parameter.
* ``filter_authorized_pools``: Given a list of pool names, return the list of pool names the user has access to.  If not overridden, it calls ``is_authorized_pool`` for every single pool passed as parameter.
* ``filter_authorized_variables``: Given a list of variable keys, return the list of variable keys the user has access to.  If not overridden, it calls ``is_authorized_variable`` for every single variable passed as parameter.

CLI
^^^

.. important::
  Starting in Airflow ``3.2.0``, provider-level CLI commands are available to manage core extensions such as auth managers and executors. Implementing provider-level CLI commands can reduce CLI startup time by avoiding heavy imports when they are not required.
  See :doc:`provider-level CLI <apache-airflow-providers:core-extensions/cli-commands>` for implementation guidance.

Auth managers may vend CLI commands which will be included in the ``airflow`` command line tool by implementing the ``get_cli_commands`` method. The commands can be used to setup required resources. Commands are only vended for the currently configured auth manager. A pseudo-code example of implementing CLI command vending from an auth manager can be seen below:

.. code-block:: python

    @staticmethod
    def get_cli_commands() -> list[CLICommand]:
        sub_commands = [
            ActionCommand(
                name="command_name",
                help="Description of what this specific command does",
                func=lazy_load_command("path.to.python.function.for.command"),
                args=(),
            ),
        ]

        return [
            GroupCommand(
                name="my_cool_auth_manager",
                help="Description of what this group of commands do",
                subcommands=sub_commands,
            ),
        ]

.. note::
    Currently there are no strict rules in place for the Airflow command namespace. It is up to developers to use names for their CLI commands that are sufficiently unique so as to not cause conflicts with other Airflow components.

.. note::
    When creating a new auth manager, or updating any existing auth manager, be sure to not import or execute any expensive operations/code at the module level. Auth manager classes are imported in several places and if they are slow to import this will negatively impact the performance of your Airflow environment, especially for CLI commands.

Extending API server application
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Auth managers have the option to extend the Airflow API server. Doing so, allow, for instance, to vend additional public API endpoints.
To extend the API server application, you need to implement the ``get_fastapi_app`` method.
Such additional endpoints can be used to manage resources such as users, groups, roles (if any) handled by your auth manager.
Endpoints defined by ``get_fastapi_app`` are mounted in ``/auth``.

Other optional methods
^^^^^^^^^^^^^^^^^^^^^^

* ``init``: This method is executed when Airflow is initializing.
  Override this method if you need to make any action (e.g. create resources, API call) that the auth manager needs.
* ``get_extra_menu_items``: Provide additional links to be added to the menu in the UI.
* ``get_db_manager``: If your auth manager requires one or several database managers (see :class:`~airflow.utils.db_manager.BaseDBManager`),
  their class paths need to be returned as part of this method. By doing so, Airflow automatically loads the
  respective database managers.


Additional Caveats
^^^^^^^^^^^^^^^^^^

* Your auth manager should not reference anything from the ``airflow.security.permissions`` module, as that module is in the process of being deprecated.
  Instead, your code should use the definitions in ``airflow.api_fastapi.auth.managers.models.resource_details``. For more details on the ``airflow.security.permissions`` deprecation, see :doc:`/security/deprecated_permissions`
* The ``access_control`` attribute of a Dag instance is only compatible with the FAB auth manager. Custom auth manager implementations should leverage ``get_authorized_dag_ids`` for Dag instance attribute-based access controls in more customizable ways (e.g. authorization based on Dag tags, Dag bundles, etc.).
* You may find it useful to define a private, generalized ``_is_authorized`` method which acts as the standardized authorization mechanism, and which each
  public ``is_authorized_*`` method calls with the appropriate parameters.
  For concrete examples of this, refer to the ``SimpleAuthManager._is_authorized_method``. Further, it may be useful to optionally use the ``airflow.api_fastapi.auth.managers.base_auth_manager.ExtendedResourceMethod`` reference within your private method.

Dag and Dag Sub-Component Authorization
---------------------------------------

Given the hierarchical structure of Dags and their composite resources, the auth manager's ``is_authorized_dag`` method should also handle the authorization logic for Dag runs, tasks, and task instances.
The ``access_entity`` parameter passed to ``is_authorized_dag`` indicates which (if any) Dag sub-component the user is attempting to access. This leads to a few important points:

* If the ``access_entity`` parameter is ``None``, then the user is attempting to interact directly with the Dag, not any of its sub-components.
* When the ``access_entity`` parameter is not ``None``, it means the user is attempting to access some sub-component of the Dag. This is noteworthy, as in some cases the ``method`` parameter may be valid
  for the Dag's sub-entity, but not a valid action directly on the Dag itself. For example, the ``POST`` method is valid for Dag runs, but **not** for Dags.
* One potential way to model the example request mentioned above -- where the ``method`` only has meaning for the Dag sub-component -- is to authorize the user if **both** statements are true:

  * The user has ``PUT`` ("edit") permissions for the given Dag.
  * The user has ``POST`` ("create") permissions for Dag runs.

Next Steps
----------

Once you have created a new auth manager class implementing the :class:`~airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager` interface, you can configure Airflow to use it by setting the ``core.auth_manager`` configuration value to the module path of your auth manager:

.. code-block:: ini

    [core]
    auth_manager = my_company.auth_managers.MyCustomAuthManager

.. note::
    For more information on Airflow's configuration, see :doc:`/howto/set-config` and for more information on managing Python modules in Airflow see :doc:`/administration-and-deployment/modules_management`.
