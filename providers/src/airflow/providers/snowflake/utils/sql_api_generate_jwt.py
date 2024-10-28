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

import base64
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

# This class relies on the PyJWT module (https://pypi.org/project/PyJWT/).
import jwt
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

logger = logging.getLogger(__name__)


ISSUER = "iss"
EXPIRE_TIME = "exp"
ISSUE_TIME = "iat"
SUBJECT = "sub"

# If you generated an encrypted private key, implement this method to return
# the passphrase for decrypting your private key. As an example, this function
# prompts the user for the passphrase.


class JWTGenerator:
    """
    Creates and signs a JWT with the specified private key file, username, and account identifier.

    The JWTGenerator keeps the generated token and only regenerates the token if a specified period
    of time has passed.

    Creates an object that generates JWTs for the specified user, account identifier, and private key

    :param account: Your Snowflake account identifier.
        See https://docs.snowflake.com/en/user-guide/admin-account-identifier.html. Note that if you are using
        the account locator, exclude any region information from the account locator.
    :param user: The Snowflake username.
    :param private_key: Private key from the file path for signing the JWTs.
    :param lifetime: The number of minutes (as a timedelta) during which the key will be valid.
    :param renewal_delay: The number of minutes (as a timedelta) from now after which the JWT
        generator should renew the JWT.
    """

    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59 minute lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes
    ALGORITHM = "RS256"  # Tokens will be generated using RSA with SHA256

    def __init__(
        self,
        account: str,
        user: str,
        private_key: Any,
        lifetime: timedelta = LIFETIME,
        renewal_delay: timedelta = RENEWAL_DELTA,
    ):
        logger.info(
            """Creating JWTGenerator with arguments
            account : %s, user : %s, lifetime : %s, renewal_delay : %s""",
            account,
            user,
            lifetime,
            renewal_delay,
        )

        # Construct the fully qualified name of the user in uppercase.
        self.account = self.prepare_account_name_for_jwt(account)
        self.user = user.upper()
        self.qualified_username = self.account + "." + self.user

        self.lifetime = lifetime
        self.renewal_delay = renewal_delay
        self.private_key = private_key
        self.renew_time = datetime.now(timezone.utc)
        self.token: str | None = None

    def prepare_account_name_for_jwt(self, raw_account: str) -> str:
        """
        Prepare the account identifier for use in the JWT.

        For the JWT, the account identifier must not include the subdomain or any region or cloud provider
        information.

        :param raw_account: The specified account identifier.
        """
        account = raw_account
        if ".global" not in account:
            # Handle the general case.
            account = account.partition(".")[0]
        else:
            # Handle the replication case.
            account = account.partition("-")[0]
        # Use uppercase for the account identifier.
        return account.upper()

    def get_token(self) -> str | None:
        """
        Generate a new JWT.

        If a JWT has been already been generated earlier, return the previously
        generated token unless the specified renewal time has passed.
        """
        now = datetime.now(timezone.utc)  # Fetch the current time

        # If the token has expired or doesn't exist, regenerate the token.
        if self.token is None or self.renew_time <= now:
            logger.info(
                "Generating a new token because the present time (%s) is later than the renewal time (%s)",
                now,
                self.renew_time,
            )
            # Calculate the next time we need to renew the token.
            self.renew_time = now + self.renewal_delay

            # Prepare the fields for the payload.
            # Generate the public key fingerprint for the issuer in the payload.
            public_key_fp = self.calculate_public_key_fingerprint(self.private_key)

            # Create our payload
            payload = {
                # Set the issuer to the fully qualified username concatenated with the public key fingerprint.
                ISSUER: self.qualified_username + "." + public_key_fp,
                # Set the subject to the fully qualified username.
                SUBJECT: self.qualified_username,
                # Set the issue time to now.
                ISSUE_TIME: now,
                # Set the expiration time, based on the lifetime specified for this object.
                EXPIRE_TIME: now + self.lifetime,
            }

            # Regenerate the actual token
            token = jwt.encode(
                payload, key=self.private_key, algorithm=JWTGenerator.ALGORITHM
            )

            if isinstance(token, bytes):
                token = token.decode("utf-8")
            self.token = token

        return self.token

    def calculate_public_key_fingerprint(self, private_key: Any) -> str:
        """
        Given a private key in PEM format, return the public key fingerprint.

        :param private_key: private key
        """
        # Get the raw bytes of public key.
        public_key_raw = private_key.public_key().public_bytes(
            Encoding.DER, PublicFormat.SubjectPublicKeyInfo
        )

        # Get the sha256 hash of the raw bytes.
        sha256hash = hashlib.sha256()
        sha256hash.update(public_key_raw)

        # Base64-encode the value and prepend the prefix 'SHA256:'.
        public_key_fp = "SHA256:" + base64.b64encode(sha256hash.digest()).decode("utf-8")

        return public_key_fp
