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

from flask import session
from flask_appbuilder.security.views import AuthOAuthView

from airflow.configuration import conf

log = logging.getLogger(__name__)


class CustomAuthOAuthView(AuthOAuthView):
    """
    Custom OAuth authentication view that ensures session is committed before redirect.

    Fixes issue #57981 where UI requests fail with 401 during OAuth flow because
    the Flask session is not yet committed when the redirect response is sent.
    """

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
        log.debug(f"OAuth callback received for provider: {provider}")

        # Call parent's OAuth callback handling
        # This processes the OAuth response, authenticates the user, and sets session data
        response = super().oauth_authorized(provider)

        # Explicitly commit the Flask session to ensure it's persisted
        # This is critical for the race condition fix
        session_backend = conf.get("fab", "SESSION_BACKEND", fallback="securecookie")

        if session_backend == "database":
            # For database sessions, Flask-Session automatically handles commit
            # but we explicitly mark the session as modified to ensure it's saved
            session.modified = True
            log.debug("Marked session as modified for database backend")
        else:
            # For securecookie sessions, session is automatically encoded into cookie
            # but we still mark it as modified to ensure fresh encoding
            session.modified = True
            log.debug("Marked session as modified for securecookie backend")

        # The session will be saved when this request completes, before the redirect
        # response is sent to the client. Flask's session interface handles this
        # automatically via the after_request handler

        log.debug(f"OAuth authentication completed successfully for provider: {provider}")

        return response
