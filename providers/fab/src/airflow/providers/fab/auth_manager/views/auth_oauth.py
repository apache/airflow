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

"""Custom OAuth authentication view to fix session timing race condition."""

from __future__ import annotations

import logging

from flask import request, session
from flask_appbuilder import expose
from flask_appbuilder.security.views import AuthOAuthView

from airflow.providers.common.compat.sdk import conf

log = logging.getLogger(__name__)


class CustomAuthOAuthView(AuthOAuthView):
    """
    Custom OAuth authentication view with proxy-aware URL scheme and session commit.

    Fixes issue #70023, where the OAuth redirect URI used ``http://`` behind a reverse
    proxy, by honouring ``X-Forwarded-Proto`` to build the correct external scheme.

    Fixes issue #57981, where UI requests failed with 401 during the OAuth flow because
    the Flask session was not yet committed when the redirect response was sent.
    """

    @expose("/login/")
    @expose("/login/<provider>")
    def login(self, provider=None):
        """
        OAuth login handler that corrects the URL scheme behind a reverse proxy.

        When the proxy sets ``X-Forwarded-Proto``, honour it here so the generated
        redirect URI matches the external scheme regardless of ProxyFix config.

        Args:
            provider: OAuth provider name (e.g., 'azure', 'google', 'github')

        Returns:
            Flask response object (redirect to the OAuth provider or home page)
        """
        forwarded_proto = request.headers.get("X-Forwarded-Proto")
        if forwarded_proto:
            request.environ["wsgi.url_scheme"] = forwarded_proto

        return super().login(provider)

    @expose("/oauth-authorized/<provider>")
    def oauth_authorized(self, provider):
        """
        OAuth callback handler that explicitly commits session before redirect.

        This method overrides the parent's oauth_authorized to ensure that the
        Flask session (containing OAuth tokens and user authentication) is fully
        committed to the session backend (cookie or database) before redirecting
        the user to the UI.

        This prevents the race condition where the UI makes API requests before
        the session is available, causing temporary 401 errors during login.

        Args:
            provider: OAuth provider name (e.g., 'azure', 'google', 'github')

        Returns:
            Flask response object (redirect to original URL or home page)
        """
        log.debug("OAuth callback received for provider: %s", provider)

        # Call parent's OAuth callback handling
        # This processes the OAuth response, authenticates the user, and sets session data
        response = super().oauth_authorized(provider)

        # Explicitly mark the Flask session as modified to ensure it's persisted
        # before the redirect response is sent to the client
        session.modified = True
        session_backend = conf.get("fab", "SESSION_BACKEND", fallback="securecookie")
        log.debug("Marked session as modified for %s backend", session_backend)

        log.debug("OAuth callback handling completed for provider: %s", provider)

        return response
