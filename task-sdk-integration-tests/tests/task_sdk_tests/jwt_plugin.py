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

"""JWT Token Plugin for Task SDK Integration Tests."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any

import jwt


class JWTTokenGenerator:
    """Generator for JWT tokens used in Task SDK API authentication."""

    def __init__(self):
        """Initialize JWT configuration from environment variables."""
        self.secret = os.getenv("AIRFLOW__API_AUTH__JWT_SECRET", "test-secret-key-for-testing")
        self.issuer = os.getenv("AIRFLOW__API_AUTH__JWT_ISSUER", "airflow-test")
        self.audience = os.getenv("AIRFLOW__API_AUTH__JWT_AUDIENCE", "urn:airflow.apache.org:task")
        self.algorithm = os.getenv("AIRFLOW__API_AUTH__JWT_ALGORITHM", "HS512")
        self.kid = os.getenv("AIRFLOW__API_AUTH__JWT_KID", "test-key-id")

    def generate_token(
        self,
        task_instance_id: str,
        expires_in_seconds: int = 3600,
        extra_claims: dict[str, Any] | None = None,
        extra_headers: dict[str, Any] | None = None,
    ) -> str:
        """
        Generate a JWT token for task instance authentication.

        Args:
            task_instance_id: The task instance ID to use as the 'sub' claim
            expires_in_seconds: Token expiration time in seconds (default: 1 hour)
            extra_claims: Additional claims to include in the token
            extra_headers: Additional headers to include in the token

        Returns:
            JWT token as a string
        """
        now = int(datetime.now(timezone.utc).timestamp())

        claims = {
            "jti": uuid.uuid4().hex,
            "iss": self.issuer,
            "aud": self.audience,
            "nbf": now,
            "exp": now + expires_in_seconds,
            "iat": now,
            "sub": task_instance_id,
        }

        # Remove audience if not set
        if not claims.get("aud"):
            del claims["aud"]

        # Add extra claims if provided
        if extra_claims:
            claims.update(extra_claims)

        # Base JWT headers
        headers = {
            "alg": self.algorithm,
            "kid": self.kid,
        }

        # Add extra headers if provided
        if extra_headers:
            headers.update(extra_headers)

        # Generate and return the token
        token = jwt.encode(claims, self.secret, algorithm=self.algorithm, headers=headers)
        return token


def generate_jwt_token(task_instance_id: str, expires_in_seconds: int = 3600) -> str:
    """
    Convenience function to generate a JWT token.

    Args:
        task_instance_id: The task instance ID to use as the 'sub' claim
        expires_in_seconds: Token expiration time in seconds (default: 1 hour)

    Returns:
        JWT token as a string
    """
    generator = JWTTokenGenerator()
    return generator.generate_token(task_instance_id, expires_in_seconds)
