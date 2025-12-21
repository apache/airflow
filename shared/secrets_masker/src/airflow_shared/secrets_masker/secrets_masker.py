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
"""Mask sensitive information from logs."""

from __future__ import annotations

import collections.abc
import contextlib
import functools
import inspect
import logging
import re
import sys
from collections.abc import Generator, Iterable, Iterator
from enum import Enum
from functools import cache, cached_property
from re import Pattern
from typing import TYPE_CHECKING, Any, Protocol, TextIO, TypeAlias, TypeVar, overload

# We have to import this here, as it is used in the type annotations at runtime even if it seems it is
# not used in the code. This is because Pydantic uses type at runtime to validate the types of the fields.
from pydantic import JsonValue  # noqa: TC002

if TYPE_CHECKING:
    from typing import TypeGuard

    class _V1EnvVarLike(Protocol):
        def to_dict(self) -> dict[str, Any]: ...


V1EnvVar = TypeVar("V1EnvVar")
Redactable: TypeAlias = str | V1EnvVar | dict[Any, Any] | tuple[Any, ...] | list[Any]
Redacted: TypeAlias = Redactable | str

log = logging.getLogger(__name__)

DEFAULT_SENSITIVE_FIELDS = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "authorization",
        "passphrase",
        "passwd",
        "password",
        "private_key",
        "proxies",
        "secret",
        "token",
        "keyfile_dict",
        "service_account",
    }
)
"""Names of fields (Connection extra, Variable key name etc.) that are deemed sensitive"""

SECRETS_TO_SKIP_MASKING = {"airflow"}
"""Common terms that should be excluded from masking in both production and tests"""


def should_hide_value_for_key(name):
    """
    Return if the value for this given name should be hidden.

    Name might be a Variable name, or key in conn.extra_dejson, for example.
    """
    return _secrets_masker().should_hide_value_for_key(name)


def mask_secret(secret: JsonValue, name: str | None = None) -> None:
    """
    Mask a secret from appearing in the logs.

    If ``name`` is provided, then it will only be masked if the name matches one of the configured "sensitive"
    names.

    If ``secret`` is a dict or a iterable (excluding str) then it will be recursively walked and keys with
    sensitive names will be hidden.

    If the secret value is too short (by default 5 characters or fewer, configurable via the
    :ref:`[logging] min_length_masked_secret <config:logging__min_length_masked_secret>` setting) it will not
    be masked
    """
    if not secret:
        return

    _secrets_masker().add_mask(secret, name)


def redact(
    value: Redactable, name: str | None = None, max_depth: int | None = None, replacement: str = "***"
) -> Redacted:
    """Redact any secrets found in ``value`` with the given replacement."""
    return _secrets_masker().redact(value, name, max_depth, replacement=replacement)


@overload
def merge(new_value: str, old_value: str, name: str | None = None, max_depth: int | None = None) -> str: ...


@overload
def merge(new_value: dict, old_value: dict, name: str | None = None, max_depth: int | None = None) -> str: ...


def merge(
    new_value: Redacted, old_value: Redactable, name: str | None = None, max_depth: int | None = None
) -> Redacted:
    """
    Merge a redacted value with its original unredacted counterpart.

    Takes a user-modified redacted value and merges it with the original unredacted value.
    For sensitive fields that still contain "***" (unchanged), the original value is restored.
    For fields that have been updated by the user, the new value is preserved.
    """
    return _secrets_masker().merge(new_value, old_value, name, max_depth)


@cache
def _secrets_masker() -> SecretsMasker:
    """
    Get or create the module-level secrets masker instance.

    This function implements a module level singleton pattern within this specific
    module. Note that different import paths (e.g., airflow._shared vs
    airflow.sdk._shared) will have separate global variables and thus separate
    masker instances.
    """
    return SecretsMasker()


def reset_secrets_masker() -> None:
    """
    Reset the secrets masker to clear existing patterns and replacer.

    This utility ensures that an execution environment starts with a fresh masker,
    preventing any carry over of patterns or replacer from previous execution or parent processes.

    New processor types should invoke this method when setting up their own masking to avoid
    inheriting masking rules from existing execution environments.
    """
    _secrets_masker().reset_masker()


def _is_v1_env_var(v: Any) -> TypeGuard[_V1EnvVarLike]:
    """Check if object is V1EnvVar, avoiding unnecessary imports."""
    # Quick check: if k8s not imported, can't be a V1EnvVar instance
    if "kubernetes.client" not in sys.modules:
        return False

    # K8s is loaded, safe to get/cache the type
    v1_type = _get_v1_env_var_type_cached()
    return isinstance(v, v1_type)


@cache
def _get_v1_env_var_type_cached() -> type:
    """Get V1EnvVar type (cached, only called when k8s is already loaded)."""
    try:
        from kubernetes.client import V1EnvVar

        return V1EnvVar
    except ImportError:
        # Shouldn't happen since we check sys.modules first
        return type("V1EnvVar", (), {})


class SecretsMasker(logging.Filter):
    """Redact secrets from logs."""

    replacer: Pattern | None = None
    patterns: set[str]

    ALREADY_FILTERED_FLAG = "__SecretsMasker_filtered"
    MAX_RECURSION_DEPTH = 5
    _has_warned_short_secret = False
    mask_secrets_in_logs = False

    min_length_to_mask = 5
    secret_mask_adapter = None

    def __init__(self):
        super().__init__()
        self.patterns = set()
        self.sensitive_variables_fields = []

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if cls._redact is not SecretsMasker._redact:
            sig = inspect.signature(cls._redact)
            # Compat for older versions of the OpenLineage plugin which subclasses this -- call the method
            # without the replacement character
            for param in sig.parameters.values():
                if param.name == "replacement" or param.kind == param.VAR_KEYWORD:
                    break
            else:
                # Block only runs if no break above.

                f = cls._redact

                @functools.wraps(f)
                def _redact(*args, replacement: str = "***", **kwargs):
                    return f(*args, **kwargs)

                cls._redact = _redact
                ...

    @classmethod
    def enable_log_masking(cls) -> None:
        """Enable secret masking in logs."""
        cls.mask_secrets_in_logs = True

    @classmethod
    def disable_log_masking(cls) -> None:
        """Disable secret masking in logs."""
        cls.mask_secrets_in_logs = False

    @classmethod
    def is_log_masking_enabled(cls) -> bool:
        """Check if secret masking in logs is enabled."""
        return cls.mask_secrets_in_logs

    @cached_property
    def _record_attrs_to_ignore(self) -> Iterable[str]:
        # Doing log.info(..., extra={'foo': 2}) sets extra properties on
        # record, i.e. record.foo. And we need to filter those too. Fun
        #
        # Create a record, and look at what attributes are on it, and ignore
        # all the default ones!

        record = logging.getLogRecordFactory()(
            # name, level, pathname, lineno, msg, args, exc_info, func=None, sinfo=None,
            "x",
            logging.INFO,
            __file__,
            1,
            "",
            (),
            exc_info=None,
            func="funcname",
        )
        return frozenset(record.__dict__).difference({"msg", "args"})

    def _redact_exception_with_context(self, exception):
        # Exception class may not be modifiable (e.g. declared by an
        # extension module such as JDBC).
        with contextlib.suppress(AttributeError):
            exception.args = (self.redact(v) for v in exception.args)
        if exception.__context__:
            self._redact_exception_with_context(exception.__context__)
        if exception.__cause__ and exception.__cause__ is not exception.__context__:
            self._redact_exception_with_context(exception.__cause__)

    def filter(self, record) -> bool:
        if not self.is_log_masking_enabled():
            return True

        if self.ALREADY_FILTERED_FLAG in record.__dict__:
            # Filters are attached to multiple handlers and logs, keep a
            # "private" flag that stops us needing to process it more than once
            return True

        if self.replacer:
            for k, v in record.__dict__.items():
                if k not in self._record_attrs_to_ignore:
                    record.__dict__[k] = self.redact(v)
            if record.exc_info and record.exc_info[1] is not None:
                exc = record.exc_info[1]
                self._redact_exception_with_context(exc)
        record.__dict__[self.ALREADY_FILTERED_FLAG] = True

        return True

    # Default on `max_depth` is to support versions of the OpenLineage plugin (not the provider) which called
    # this function directly. New versions of that provider, and this class itself call it with a value
    def _redact_all(
        self,
        item: Redactable,
        depth: int,
        max_depth: int = MAX_RECURSION_DEPTH,
        *,
        replacement: str = "***",
    ) -> Redacted:
        if depth > max_depth or isinstance(item, str):
            return replacement
        if isinstance(item, dict):
            return {
                dict_key: self._redact_all(subval, depth + 1, max_depth, replacement=replacement)
                for dict_key, subval in item.items()
            }
        if isinstance(item, (tuple, set)):
            # Turn set in to tuple!
            return tuple(
                self._redact_all(subval, depth + 1, max_depth, replacement=replacement) for subval in item
            )
        if isinstance(item, list):
            return list(
                self._redact_all(subval, depth + 1, max_depth, replacement=replacement) for subval in item
            )
        return item

    def _redact(
        self, item: Redactable, name: str | None, depth: int, max_depth: int, replacement: str = "***"
    ) -> Redacted:
        # Avoid spending too much effort on redacting on deeply nested
        # structures. This also avoid infinite recursion if a structure has
        # reference to self.
        if depth > max_depth:
            return item
        try:
            if name and self.should_hide_value_for_key(name):
                return self._redact_all(item, depth, max_depth, replacement=replacement)
            if isinstance(item, dict):
                to_return = {
                    dict_key: self._redact(
                        subval, name=dict_key, depth=(depth + 1), max_depth=max_depth, replacement=replacement
                    )
                    for dict_key, subval in item.items()
                }
                return to_return
            if isinstance(item, Enum):
                return self._redact(
                    item=item.value, name=name, depth=depth, max_depth=max_depth, replacement=replacement
                )
            if _is_v1_env_var(item):
                tmp = item.to_dict()
                if self.should_hide_value_for_key(tmp.get("name", "")) and "value" in tmp:
                    tmp["value"] = replacement
                else:
                    return self._redact(
                        item=tmp, name=name, depth=depth, max_depth=max_depth, replacement=replacement
                    )
                return tmp
            if isinstance(item, str):
                if self.replacer:
                    # We can't replace specific values, but the key-based redacting
                    # can still happen, so we can't short-circuit, we need to walk
                    # the structure.
                    return self.replacer.sub(replacement, str(item))
                return item
            if isinstance(item, (tuple, set)):
                # Turn set in to tuple!
                return tuple(
                    self._redact(
                        subval, name=None, depth=(depth + 1), max_depth=max_depth, replacement=replacement
                    )
                    for subval in item
                )
            if isinstance(item, list):
                return [
                    self._redact(
                        subval, name=None, depth=(depth + 1), max_depth=max_depth, replacement=replacement
                    )
                    for subval in item
                ]
            return item
        # I think this should never happen, but it does not hurt to leave it just in case
        # Well. It happened (see https://github.com/apache/airflow/issues/19816#issuecomment-983311373)
        # but it caused infinite recursion, to avoid this we mark the log as already filtered.
        except Exception as exc:
            log.warning(
                "Unable to redact value of type %s, please report this via "
                "<https://github.com/apache/airflow/issues>. Error was: %s: %s",
                type(item),
                type(exc).__name__,
                exc,
                extra={self.ALREADY_FILTERED_FLAG: True},
            )
            # Rather than expose sensitive info, lets play it safe
            return "<redaction-failed>"

    def _merge(
        self,
        new_item: Redacted,
        old_item: Redactable,
        *,
        name: str | None,
        depth: int,
        max_depth: int,
        force_sensitive: bool = False,
        replacement: str,
    ) -> Redacted:
        """Merge a redacted item with its original unredacted counterpart."""
        if depth > max_depth:
            if isinstance(new_item, str) and new_item == "***":
                return old_item
            return new_item

        try:
            # Determine if we should treat this as sensitive
            is_sensitive = force_sensitive or (name is not None and self.should_hide_value_for_key(name))

            if isinstance(new_item, dict) and isinstance(old_item, dict):
                merged = {}
                for key in new_item.keys():
                    if key in old_item:
                        # For dicts, pass the key as name unless we're in sensitive mode
                        child_name = None if is_sensitive else key
                        merged[key] = self._merge(
                            new_item[key],
                            old_item[key],
                            name=child_name,
                            depth=depth + 1,
                            max_depth=max_depth,
                            force_sensitive=is_sensitive,
                            replacement=replacement,
                        )
                    else:
                        merged[key] = new_item[key]
                return merged

            if isinstance(new_item, (list, tuple)) and type(old_item) is type(new_item):
                merged_list = []
                for i in range(len(new_item)):
                    if i < len(old_item):
                        # In sensitive mode, check if individual item is redacted
                        if is_sensitive and isinstance(new_item[i], str) and new_item[i] == "***":
                            merged_list.append(old_item[i])
                        else:
                            merged_list.append(
                                self._merge(
                                    new_item[i],
                                    old_item[i],
                                    name=None,
                                    depth=depth + 1,
                                    max_depth=max_depth,
                                    force_sensitive=is_sensitive,
                                    replacement=replacement,
                                )
                            )
                    else:
                        merged_list.append(new_item[i])

                if isinstance(new_item, list):
                    return list(merged_list)
                return tuple(merged_list)

            if isinstance(new_item, set) and isinstance(old_item, set):
                # Sets are unordered, we cannot restore original items.
                return new_item

            if _is_v1_env_var(new_item) and _is_v1_env_var(old_item):
                # TODO: Handle Kubernetes V1EnvVar objects if needed
                return new_item

            if is_sensitive and isinstance(new_item, str) and new_item == "***":
                return old_item
            return new_item

        except (TypeError, AttributeError, ValueError):
            return new_item

    def redact(
        self,
        item: Redactable,
        name: str | None = None,
        max_depth: int | None = None,
        replacement: str = "***",
    ) -> Redacted:
        """
        Redact an any secrets found in ``item``, if it is a string.

        If ``name`` is given, and it's a "sensitive" name (see
        :func:`should_hide_value_for_key`) then all string values in the item
        is redacted.
        """
        return self._redact(
            item, name, depth=0, max_depth=max_depth or self.MAX_RECURSION_DEPTH, replacement=replacement
        )

    def merge(
        self,
        new_item: Redacted,
        old_item: Redactable,
        name: str | None = None,
        max_depth: int | None = None,
        replacement: str = "***",
    ) -> Redacted:
        """
        Merge a redacted item with its original unredacted counterpart.

        Takes a user-modified redacted item and merges it with the original unredacted item.
        For sensitive fields that still contain "***" (or whatever the ``replacement`` is specified as), the
        original value is restored. For fields that have been updated, the new value is preserved.
        """
        return self._merge(
            new_item,
            old_item,
            name=name,
            depth=0,
            max_depth=max_depth or self.MAX_RECURSION_DEPTH,
            force_sensitive=False,
            replacement=replacement,
        )

    def _adaptations(self, secret: str) -> Generator[str, None, None]:
        """Yield the secret along with any adaptations to the secret that should be masked."""
        yield secret

        if self.secret_mask_adapter:
            # This can return an iterable of secrets to mask OR a single secret as a string
            secret_or_secrets = self.secret_mask_adapter(secret)
            if not isinstance(secret_or_secrets, str):
                # if its not a string, it must be an iterable
                yield from secret_or_secrets
            else:
                yield secret_or_secrets

    def should_hide_value_for_key(self, name):
        """
        Return if the value for this given name should be hidden.

        Name might be a Variable name, or key in conn.extra_dejson, for example.
        """
        from airflow import settings

        if isinstance(name, str) and settings.HIDE_SENSITIVE_VAR_CONN_FIELDS:
            name = name.strip().lower()
            return any(s in name for s in self.sensitive_variables_fields)
        return False

    def add_mask(self, secret: JsonValue, name: str | None = None):
        """Add a new secret to be masked to this filter instance."""
        if isinstance(secret, dict):
            for k, v in secret.items():
                self.add_mask(v, k)
        elif isinstance(secret, str):
            if not secret:
                return

            if secret.lower() in SECRETS_TO_SKIP_MASKING:
                return

            min_length = self.min_length_to_mask
            if len(secret) < min_length:
                if not SecretsMasker._has_warned_short_secret:
                    log.warning(
                        "Skipping masking for a secret as it's too short (<%d chars)",
                        min_length,
                        extra={self.ALREADY_FILTERED_FLAG: True},
                    )
                    SecretsMasker._has_warned_short_secret = True
                return

            new_mask = False
            for s in self._adaptations(secret):
                if s:
                    if len(s) < min_length:
                        continue

                    if s.lower() in SECRETS_TO_SKIP_MASKING:
                        continue

                    pattern = re.escape(s)
                    if pattern not in self.patterns and (not name or self.should_hide_value_for_key(name)):
                        self.patterns.add(pattern)
                        new_mask = True
            if new_mask:
                self.replacer = re.compile("|".join(self.patterns))

        elif isinstance(secret, collections.abc.Iterable):
            for v in secret:
                self.add_mask(v, name)

    def reset_masker(self):
        """Reset the patterns and the replacer in the masker instance."""
        self.patterns = set()
        self.replacer = None


class RedactedIO(TextIO):
    """
    IO class that redacts values going into stdout.

    Expected usage::

        with contextlib.redirect_stdout(RedactedIO()):
            ...  # Writes to stdout will be redacted.
    """

    def __init__(self):
        self.target = sys.stdout

    def __enter__(self) -> TextIO:
        return self.target.__enter__()

    def __exit__(self, t, v, b) -> None:
        return self.target.__exit__(t, v, b)

    def __iter__(self) -> Iterator[str]:
        return iter(self.target)

    def __next__(self) -> str:
        return next(self.target)

    def close(self) -> None:
        return self.target.close()

    def fileno(self) -> int:
        return self.target.fileno()

    def flush(self) -> None:
        return self.target.flush()

    def isatty(self) -> bool:
        return self.target.isatty()

    def read(self, n: int = -1) -> str:
        return self.target.read(n)

    def readable(self) -> bool:
        return self.target.readable()

    def readline(self, n: int = -1) -> str:
        return self.target.readline(n)

    def readlines(self, n: int = -1) -> list[str]:
        return self.target.readlines(n)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self.target.seek(offset, whence)

    def seekable(self) -> bool:
        return self.target.seekable()

    def tell(self) -> int:
        return self.target.tell()

    def truncate(self, s: int | None = None) -> int:
        return self.target.truncate(s)

    def writable(self) -> bool:
        return self.target.writable()

    def write(self, s: str) -> int:
        s = str(redact(s))
        return self.target.write(s)

    def writelines(self, lines) -> None:
        self.target.writelines(lines)
