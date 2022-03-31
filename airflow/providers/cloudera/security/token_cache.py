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

"""Handles token caching and its various caching mechanisms"""
import base64
import os
from abc import ABC, abstractmethod
from functools import wraps
from json import JSONDecodeError, dumps, loads
from pathlib import Path
from typing import Callable, Optional, Type

from cryptography.fernet import Fernet, InvalidToken

from airflow.providers.cloudera.security import SecurityError, TokenResponse
from airflow.utils.log.logging_mixin import LoggingMixin, logging  # type: ignore

LOG = logging.getLogger(__name__)


class CacheError(SecurityError):
    """Exception used when there is an issue while interacting with token cache"""


class FetchAuthTokenError(SecurityError):
    """Exception used when there is an issue while fetching the token from the Cloudera APIs"""


class GetAuthTokenError(SecurityError):
    """Exception used when there is an issue while getting the token"""


class TokenCacheStrategy(ABC, LoggingMixin):
    """Base class from which token caching strategies must be created.
    A Fernet based encryptor is available for encrypting cache content if necessary
    """

    def __init__(self, token_response_class: Type[TokenResponse], encryption_key: Optional[str]) -> None:
        self.token_response_class = token_response_class
        if encryption_key:
            fernet_encrytion_key = self.get_fernet_encryption_key(encryption_key)
            self.encryptor = Fernet(fernet_encrytion_key)

    @classmethod
    def get_fernet_encryption_key(cls, encryption_key: str) -> bytes:
        """
        Get valid encryption key from candidate encryption key
        if it does not fit Fernet's module requirements (32 characters and base64 encoding)
        """
        if not encryption_key:
            raise ValueError("Encryption key cannot be None or empty")
        if len(encryption_key) < 32:
            raise ValueError("Encryption key is too short. It must be at least 32 characters.")
        if len(encryption_key) > 32:
            final_encryption_key = encryption_key[:32]
            LOG.debug("Encryption key is too long. Truncating to right size")
        else:
            final_encryption_key = encryption_key
            LOG.debug("Encryption key has right size.")

        # Truncate in case of accented letters.
        # TODO: If need to be more generic, we should handle that the input encryption key
        #       can be in other encoding than utf-8. Currently not an issue.
        final_encryption_key_bytes = final_encryption_key.encode("utf-8")[:32]
        base64key = base64.urlsafe_b64encode(final_encryption_key_bytes)

        return base64key

    @abstractmethod
    def get_cached_auth_token(self, cache_key: str) -> TokenResponse:
        """Gets token from the cache

        Args:
            cache_key: cache key to retrieve

        Returns:
            token associated to the cache key

        Raises:
            CacheError if it cannot be obtained from the cache
        """
        raise NotImplementedError

    @abstractmethod
    def cache_auth_token(self, cache_key: str, token: TokenResponse) -> None:
        """Caches the token and associates it to the given cache key

        Args:
            cache_key: cache key used to store the token
            token: the token to cache

        Raises:
            CacheError if the token cannot be cached
        """
        raise NotImplementedError

    @abstractmethod
    def clear_cached_auth_token(self, cache_key: str) -> None:
        """Deletes token associated to the cache key from the cache

        Args:
            cache_key: cache key to clear

        Raises:
            CacheError if the cache entry cannot be deleted
        """
        raise NotImplementedError


class EncryptedFileTokenCacheStrategy(TokenCacheStrategy):
    """
    File based caching mechanism. A file is created for each cache entry.
    Content of the cache is encrypted
    """

    CACHE_SUB_DIR = "token_cache"

    def __init__(
        self,
        token_response_class: Type[TokenResponse],
        encryption_key: str,
        cache_dir: Optional[str] = ".",
    ) -> None:
        super().__init__(token_response_class, encryption_key=encryption_key)
        try:
            if cache_dir and cache_dir.strip():
                self.cache_dir = Path(cache_dir.strip())
                if not os.path.isdir(self.cache_dir):
                    raise ValueError(f"Cache dir {self.cache_dir} is not a directory.")
                self.cache_dir = self.cache_dir / Path(self.CACHE_SUB_DIR)
                LOG.debug("Creating directory %s", self.cache_dir)
                try:
                    self.cache_dir.mkdir(mode=0o770, exist_ok=True)
                    LOG.debug("Directory created successfully")
                except Exception as err:
                    LOG.error("Failed to create %s", self.cache_dir)
                    raise CacheError(
                        err,
                        msg=f"Cache Directory {self.cache_dir} could not be created",
                    ) from err
            else:
                LOG.error("No value defined for cache_dir")
                raise ValueError("Cache dir is empty")
        except ValueError as err:
            LOG.error("Failed to initialize the caching mechanism")
            raise CacheError(err, msg="Cache Directory and Cache Keys must be specified") from err
        self.cache_encoding = "utf-8"

    def get_cache_path(self, cache_key: str) -> Path:
        """Cache path for associated cache key

        Args:
            cache_key: the cache key

        Returns:
            Absolute cache path
        """
        if cache_key:
            return (self.cache_dir / Path(cache_key)).absolute()
        raise ValueError("Cache key must not be empty")

    def get_cached_auth_token(self, cache_key: str) -> TokenResponse:
        # Read raw content
        try:
            with open(self.get_cache_path(cache_key)) as cache_file:
                content = cache_file.read().splitlines()
        except Exception as err:
            raise CacheError(
                err, f"Cache file {cache_key} does not exist or issues while reading it"
            ) from err

        # Decrypt content
        try:
            content_dict = loads(self.encryptor.decrypt(content[0].encode(self.cache_encoding)))
            token = self.token_response_class(**content_dict)  # type: ignore
            return token
        except InvalidToken as err:
            raise CacheError(
                err,
                "Issue while decrypting cache content. Please check if the file is corrupted.",
            ) from err
        except IndexError as err:
            raise CacheError(err, f"Issues while reading cache {cache_key}") from err
        except (TypeError, JSONDecodeError) as err:
            raise CacheError(err, "Malformed cache token. Please check if the file is corrupted.") from err

    def cache_auth_token(self, cache_key: str, token: TokenResponse) -> None:
        try:
            with open(self.get_cache_path(cache_key), "w") as cache_file:
                serialized_unencrypted_token_bytes = dumps(token.__dict__).encode(self.cache_encoding)
                serialized_encrypted_token_bytes = self.encryptor.encrypt(serialized_unencrypted_token_bytes)
                serialized_encrypted_token = serialized_encrypted_token_bytes.decode(self.cache_encoding)
                cache_file.write(serialized_encrypted_token)
        except Exception as err:
            raise CacheError(err, f"Issues while writing cache to {cache_key}") from err

    def clear_cached_auth_token(self, cache_key: str) -> None:
        try:
            os.unlink(self.get_cache_path(cache_key))
        except FileNotFoundError:
            LOG.info("Cache file does not exist, nothing to clear.")
        except Exception as err:
            raise CacheError(err, f"Issues while clearing cache to {cache_key}") from err


class CacheableTokenAuth:
    """Base class for authentications which needs to use caching."""

    def __init__(self, token_cache_strategy: TokenCacheStrategy) -> None:
        self.token_cache_strategy = token_cache_strategy

    def get_cache_key(self) -> str:
        """Cache key which will be used to store the token

        Returns:
            String representation of the cache key
        """


class Cache:
    """Decorator for leveraging token caching on a function which fetches a token"""

    def __init__(self, token_response_type) -> None:
        self.token_response_type = token_response_type

    def __call__(self, fetch_func: Callable[..., TokenResponse]):
        """Gets token from either the cache or the target system.

        If the token from the cache expired it requires a new one from the target system.

        Returns:
            A valid token

        Raises:
            GetAuthTokenError if there was an issue while getting the token.
        """

        @wraps(fetch_func)
        def wrapper(
            token_auth: CacheableTokenAuth, *args, **kwargs
        ) -> self.token_response_type:  # type: ignore
            # Attempt to retrieve a cached access token
            if isinstance(token_auth.token_cache_strategy, TokenCacheStrategy):
                try:
                    token = token_auth.token_cache_strategy.get_cached_auth_token(token_auth.get_cache_key())
                    if token.is_valid():

                        LOG.info(
                            "%s: %s",
                            "Using cached token from cache key",
                            token_auth.get_cache_key(),
                        )

                        return token

                    LOG.info("Acquiring new token: cached token has expired.")
                except CacheError as err:
                    if isinstance(err.raised_from, FileNotFoundError):
                        LOG.info("Acquiring new token: No cache found")
                    else:
                        LOG.warning(
                            ("Acquiring new token: Issue while reading the cached token." " Reason %s"),
                            repr(err),
                        )

            try:
                token = fetch_func(token_auth, *args, **kwargs)
            except FetchAuthTokenError as err:
                LOG.error("Could not obtain authentication token. Reason: %s", repr(err))
                raise GetAuthTokenError(err) from err

            if isinstance(token_auth.token_cache_strategy, TokenCacheStrategy):
                # Cache the token
                try:
                    token_auth.token_cache_strategy.cache_auth_token(token_auth.get_cache_key(), token)
                except CacheError as err:
                    LOG.warning(
                        "%s: %s",
                        "Failed to cache authentication token. Reason: ",
                        repr(err),
                    )
            return token

        return wrapper
