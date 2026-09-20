# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
import logging
import os
import re
import warnings
from hashlib import md5
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from airflow.api_fastapi.auth.tokens import get_signing_key
from airflow.exceptions import AirflowConfigException, AirflowException

log = logging.getLogger(__name__)

_AIRFLOW_PATH = Path(__file__).parents[3]

# Language codes ("en", "zh-CN") and namespaces ("common") of the UI translation files. Anchored
# so a path parameter cannot smuggle in path separators or ``..`` and escape the locales directory.
_I18N_SEGMENT = re.compile(r"^[A-Za-z0-9_-]+$")


def init_ui_translation_views(app: FastAPI, *, dev_mode: bool, dist_directory: Path) -> None:
    """
    Register the routes that serve plugin-contributed UI translations.

    Translations are merged with the bundled files and serialized once here at startup (not per
    request); only the files a plugin changes get a route, so every other locale file keeps being
    served from the static mount. A bundled-language override gets a route per overridden namespace;
    a brand-new language gets one route for all its namespaces (empty object for those it omits, so
    i18next falls back to English). These stay off the authenticated ``ui_router`` because
    translations load before login, and are registered ahead of the static mount to take precedence.
    """
    from airflow import plugins_manager

    plugin_translations = plugins_manager.get_ui_translations()

    @app.get("/static/i18n/languages.json", include_in_schema=False)
    def ui_translation_languages():
        """List the plugin-contributed languages so the UI can offer them for selection."""
        # No version cache-buster, so revalidate rather than risk hiding a newly added language.
        return JSONResponse({"languages": sorted(plugin_translations)}, headers={"Cache-Control": "no-cache"})

    if not plugin_translations:
        return

    locales_directory = (
        _AIRFLOW_PATH / "airflow/ui/public/i18n/locales" if dev_mode else dist_directory / "i18n/locales"
    )

    # English is the reference for which keys exist; warn (never fail) about plugin keys missing
    # from it, since translations are not versioned in lockstep with Airflow.
    try:
        plugins_manager.warn_about_unknown_translation_keys(plugin_translations, locales_directory / "en")
    except Exception:
        log.exception("Failed to check plugin UI translations against the English reference")

    bundled_languages: set[str] = set()
    if locales_directory.is_dir():
        bundled_languages = {entry.name for entry in locales_directory.iterdir() if entry.is_dir()}

    def merged_body(language: str, namespace: str, keys: dict) -> bytes:
        base: dict = {}
        base_file = locales_directory / language / f"{namespace}.json"
        if base_file.is_file():
            try:
                base = json.loads(base_file.read_text("utf-8"))
            except (OSError, ValueError):
                log.warning("Could not read bundled translation %s/%s.json", language, namespace)
        return json.dumps(plugins_manager.merge_translations(base, keys)).encode()

    def json_with_etag(request: Request, body: bytes) -> Response:
        # ETag so the browser revalidates with a conditional GET (304), like the static mount does.
        etag = f'"{md5(body, usedforsecurity=False).hexdigest()}"'
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers={"ETag": etag})
        return Response(content=body, media_type="application/json", headers={"ETag": etag})

    def serve_body(body: bytes):
        def route(request: Request) -> Response:
            return json_with_etag(request, body)

        return route

    def serve_language(language_bodies: dict[str, bytes]):
        def route(request: Request, namespace: str) -> Response:
            if not _I18N_SEGMENT.match(namespace):
                return JSONResponse(status_code=404, content={"error": "Not found"})
            return json_with_etag(request, language_bodies.get(namespace, b"{}"))

        return route

    for language, namespaces in plugin_translations.items():
        if not _I18N_SEGMENT.match(language):
            log.warning("Skipping plugin UI translations for invalid language code %r", language)
            continue

        bodies = {
            namespace: merged_body(language, namespace, keys)
            for namespace, keys in namespaces.items()
            if _I18N_SEGMENT.match(namespace)
        }

        if language in bundled_languages:
            for namespace, body in bodies.items():
                app.add_api_route(
                    f"/static/i18n/locales/{language}/{namespace}.json",
                    serve_body(body),
                    include_in_schema=False,
                )
        else:
            app.add_api_route(
                f"/static/i18n/locales/{language}/{{namespace}}.json",
                serve_language(bodies),
                include_in_schema=False,
            )


def init_views(app: FastAPI) -> None:
    """Init views by registering the different routers."""
    from airflow.api_fastapi.core_api.routes.public import public_router
    from airflow.api_fastapi.core_api.routes.ui import ui_router

    app.include_router(ui_router)
    app.include_router(public_router)

    dev_mode = os.environ.get("DEV_MODE", str(False)) == "true"

    directory = _AIRFLOW_PATH / ("airflow/ui/dev" if dev_mode else "airflow/ui/dist")

    # During python tests or when the backend is run without having the frontend build
    # those directories might not exist. App should not fail initializing in those scenarios.
    Path(directory).mkdir(exist_ok=True)

    templates = Jinja2Templates(directory=directory)

    # Ahead of the static mounts below so plugin-overridden locales take precedence.
    init_ui_translation_views(app, dev_mode=dev_mode, dist_directory=directory)

    if dev_mode:
        app.mount(
            "/static/i18n/locales",
            StaticFiles(directory=_AIRFLOW_PATH / "airflow/ui/public/i18n/locales"),
            name="dev_i18n_static",
        )

    app.mount(
        "/static",
        StaticFiles(
            directory=directory,
            html=True,
        ),
        name="webapp_static_folder",
    )

    @app.get("/health", include_in_schema=False)
    def old_health():
        # If someone has the `/health` endpoint from Airflow 2 set up, we want this to be a 404, not serve the
        # default index.html for the SPA.
        #
        # This is a 404, not a redirect, as setups need correcting to account for this, and a redirect might
        # hide the issue
        return JSONResponse(
            status_code=404,
            content={"error": "Moved in Airflow 3. Please change config to check `/api/v2/monitor/health`"},
        )

    @app.get("/api/v1/{_:path}", include_in_schema=False)
    def old_api(_):
        return JSONResponse(
            status_code=404,
            content={
                "error": "/api/v1 has been removed in Airflow 3, please use its upgraded version /api/v2 instead."
            },
        )

    @app.get("/api/{_:path}", include_in_schema=False)
    def api_not_found(_):
        """Catch all route to handle invalid API endpoints."""
        return JSONResponse(status_code=404, content={"error": "API route not found"})

    @app.get("/{rest_of_path:path}", response_class=HTMLResponse, include_in_schema=False)
    def webapp(request: Request, rest_of_path: str):
        return templates.TemplateResponse(
            request,
            "/index.html",
            {"backend_server_base_url": request.base_url.path},
            media_type="text/html",
        )


def init_flask_plugins(app: FastAPI) -> None:
    """Integrate Flask plugins (plugins from Airflow 2)."""
    from airflow import plugins_manager

    blueprints, appbuilder_views, appbuilder_menu_links = plugins_manager.get_flask_plugins()

    # If no Airflow 2.x plugin is in the environment, no need to go further
    if not blueprints and not appbuilder_views and not appbuilder_menu_links:
        return

    from fastapi.middleware.wsgi import WSGIMiddleware

    try:
        from airflow.providers.fab.www.app import create_app
    except ImportError:
        raise AirflowException(
            "Some Airflow 2 plugins have been detected in your environment. "
            "To run them with Airflow 3, you must install the FAB provider in your Airflow environment."
        )

    warnings.warn(
        "You have a plugin that is using a FAB view or Flask Blueprint, which was used for the Airflow 2 UI,"
        "and is now deprecated. Please update your plugin to be compatible with the Airflow 3 UI.",
        DeprecationWarning,
        stacklevel=2,
    )

    flask_app = create_app(enable_plugins=True)
    app.mount("/pluginsv2", WSGIMiddleware(flask_app))


def init_config(app: FastAPI) -> None:
    from airflow.configuration import conf

    allow_origins = conf.getlist("api", "access_control_allow_origins")
    allow_methods = conf.getlist("api", "access_control_allow_methods")
    allow_headers = conf.getlist("api", "access_control_allow_headers")
    allow_origin_regex = conf.get("api", "access_control_allow_origin_regex", fallback="") or None

    if "*" in allow_origins:
        # The CORS spec forbids combining `Access-Control-Allow-Origin: *` with
        # `Access-Control-Allow-Credentials: true`, and browsers reject any response that does so
        # (see https://fetch.spec.whatwg.org/#cors-protocol-and-credentials). Airflow's API needs
        # credentialed requests for cookie / Authorization-header auth, so a wildcard origin is
        # never a valid configuration. Fail loudly at startup instead of silently shipping a
        # response shape that no browser will accept.
        raise AirflowConfigException(
            "`[api] access_control_allow_origins` must not contain `*`: the wildcard origin is "
            "incompatible with the credentialed CORS Airflow's API requires, and browsers will "
            "reject every cross-origin response. List the exact origins that need access "
            "(e.g. `https://airflow.mycompany.com`) instead."
        )

    if allow_origins or allow_methods or allow_headers or allow_origin_regex:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allow_origins,
            allow_origin_regex=allow_origin_regex,
            allow_credentials=True,
            allow_methods=allow_methods,
            allow_headers=allow_headers,
        )

    app.state.secret_key = get_signing_key("api", "secret_key")


def init_middlewares(app: FastAPI) -> None:
    from airflow.api_fastapi.app import get_auth_manager
    from airflow.api_fastapi.auth.middlewares.refresh_token import JWTRefreshMiddleware

    app.add_middleware(JWTRefreshMiddleware)

    for middleware_cls, middleware_kwargs in get_auth_manager().get_fastapi_middlewares():
        app.add_middleware(middleware_cls, **middleware_kwargs)

    # GZipMiddleware must be inside HttpAccessLogMiddleware so that access logs capture
    # the full end-to-end duration including compression time. HttpAccessLogMiddleware is
    # installed by ``init_access_logging`` in ``create_app``, which runs after this
    # function — do not reorder those calls.
    # See https://github.com/apache/airflow/issues/60165
    app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=5)
