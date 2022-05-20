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
"""Mask sensitive information from logs"""
import collections
import logging
import re
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from airflow import settings
from airflow.compat.functools import cache, cached_property

if TYPE_CHECKING:
    from airflow.typing_compat import RePatternType

    RedactableItem = Union[str, Dict[Any, Any], Tuple[Any, ...], List[Any]]


log = logging.getLogger(__name__)


DEFAULT_SENSITIVE_FIELDS = frozenset(
    {
        'access_token',
        'api_key',
        'apikey',
        'authorization',
        'passphrase',
        'passwd',
        'password',
        'private_key',
        'secret',
        'token',
        'keyfile_dict',
        'service_account',
    }
)
"""Names of fields (Connection extra, Variable key name etc.) that are deemed sensitive"""

SECRETS_TO_SKIP_MASKING_FOR_TESTS = {'airflow'}


@cache
def get_sensitive_variables_fields():
    """Get comma-separated sensitive Variable Fields from airflow.cfg."""
    from airflow.configuration import conf

    sensitive_fields = DEFAULT_SENSITIVE_FIELDS.copy()
    sensitive_variable_fields = conf.get('core', 'sensitive_var_conn_names')
    if sensitive_variable_fields:
        sensitive_fields |= frozenset({field.strip() for field in sensitive_variable_fields.split(',')})
    return sensitive_fields


def should_hide_value_for_key(name):
    """Should the value for this given name (Variable name, or key in conn.extra_dejson) be hidden"""
    from airflow import settings

    if isinstance(name, str) and settings.HIDE_SENSITIVE_VAR_CONN_FIELDS:
        name = name.strip().lower()
        return any(s in name for s in get_sensitive_variables_fields())
    return False


def mask_secret(secret: Union[str, dict, Iterable], name: Optional[str] = None) -> None:
    """
    Mask a secret from appearing in the task logs.

    If ``name`` is provided, then it will only be masked if the name matches
    one of the configured "sensitive" names.

    If ``secret`` is a dict or a iterable (excluding str) then it will be
    recursively walked and keys with sensitive names will be hidden.
    """
    # Filtering all log messages is not a free process, so we only do it when
    # running tasks
    if not secret:
        return

    _secrets_masker().add_mask(secret, name)


def redact(value: "RedactableItem", name: Optional[str] = None) -> "RedactableItem":
    """Redact any secrets found in ``value``."""
    return _secrets_masker().redact(value, name)


@cache
def _secrets_masker() -> "SecretsMasker":

    for flt in logging.getLogger('airflow.task').filters:
        if isinstance(flt, SecretsMasker):
            return flt
    raise RuntimeError(
        "Logging Configuration Error! No SecretsMasker found! If you have custom logging, please make "
        "sure you configure it taking airflow configuration as a base as explained at "
        "https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/logging-tasks.html"
        "#advanced-configuration"
    )


class SecretsMasker(logging.Filter):
    """Redact secrets from logs"""

    replacer: Optional["RePatternType"] = None
    patterns: Set[str]

    ALREADY_FILTERED_FLAG = "__SecretsMasker_filtered"
    MAX_RECURSION_DEPTH = 5

    def __init__(self):
        super().__init__()
        self.patterns = set()

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
            tuple(),
            exc_info=None,
            func="funcname",
        )
        return frozenset(record.__dict__).difference({'msg', 'args'})

    def _redact_exception_with_context(self, exception):
        # Exception class may not be modifiable (e.g. declared by an
        # extension module such as JDBC).
        try:
            exception.args = (self.redact(v) for v in exception.args)
        except AttributeError:
            pass
        if exception.__context__:
            self._redact_exception_with_context(exception.__context__)
        if exception.__cause__ and exception.__cause__ is not exception.__context__:
            self._redact_exception_with_context(exception.__cause__)

    def filter(self, record) -> bool:
        if settings.MASK_SECRETS_IN_LOGS is not True:
            return True

        if self.ALREADY_FILTERED_FLAG in record.__dict__:
            # Filters are attached to multiple handlers and logs, keep a
            # "private" flag that stops us needing to process it more than once
            return True

        if self.replacer:
            for k, v in record.__dict__.items():
                if k in self._record_attrs_to_ignore:
                    continue
                record.__dict__[k] = self.redact(v)
            if record.exc_info and record.exc_info[1] is not None:
                exc = record.exc_info[1]
                self._redact_exception_with_context(exc)
        record.__dict__[self.ALREADY_FILTERED_FLAG] = True

        return True

    def _redact_all(self, item: "RedactableItem", depth: int) -> "RedactableItem":
        if depth > self.MAX_RECURSION_DEPTH or isinstance(item, str):
            return '***'
        if isinstance(item, dict):
            return {dict_key: self._redact_all(subval, depth + 1) for dict_key, subval in item.items()}
        elif isinstance(item, (tuple, set)):
            # Turn set in to tuple!
            return tuple(self._redact_all(subval, depth + 1) for subval in item)
        elif isinstance(item, list):
            return list(self._redact_all(subval, depth + 1) for subval in item)
        else:
            return item

    def _redact(self, item: "RedactableItem", name: Optional[str], depth: int) -> "RedactableItem":
        # Avoid spending too much effort on redacting on deeply nested
        # structures. This also avoid infinite recursion if a structure has
        # reference to self.
        if depth > self.MAX_RECURSION_DEPTH:
            return item
        try:
            if name and should_hide_value_for_key(name):
                return self._redact_all(item, depth)
            if isinstance(item, dict):
                return {
                    dict_key: self._redact(subval, name=dict_key, depth=(depth + 1))
                    for dict_key, subval in item.items()
                }
            elif isinstance(item, str):
                if self.replacer:
                    # We can't replace specific values, but the key-based redacting
                    # can still happen, so we can't short-circuit, we need to walk
                    # the structure.
                    return self.replacer.sub('***', item)
                return item
            elif isinstance(item, (tuple, set)):
                # Turn set in to tuple!
                return tuple(self._redact(subval, name=None, depth=(depth + 1)) for subval in item)
            elif isinstance(item, list):
                return [self._redact(subval, name=None, depth=(depth + 1)) for subval in item]
            else:
                return item
        # I think this should never happen, but it does not hurt to leave it just in case
        # Well. It happened (see https://github.com/apache/airflow/issues/19816#issuecomment-983311373)
        # but it caused infinite recursion, so we need to cast it to str first.
        except Exception as e:
            log.warning(
                "Unable to redact %s, please report this via <https://github.com/apache/airflow/issues>. "
                "Error was: %s: %s",
                repr(item),
                type(e).__name__,
                str(e),
            )
            return item

    def redact(self, item: "RedactableItem", name: Optional[str] = None) -> "RedactableItem":
        """Redact an any secrets found in ``item``, if it is a string.

        If ``name`` is given, and it's a "sensitive" name (see
        :func:`should_hide_value_for_key`) then all string values in the item
        is redacted.
        """
        return self._redact(item, name, depth=0)

    def add_mask(self, secret: Union[str, dict, Iterable], name: Optional[str] = None):
        """Add a new secret to be masked to this filter instance."""
        from airflow.configuration import conf

        test_mode: bool = conf.getboolean('core', 'unit_test_mode')
        if isinstance(secret, dict):
            for k, v in secret.items():
                self.add_mask(v, k)
        elif isinstance(secret, str):
            if not secret or (test_mode and secret in SECRETS_TO_SKIP_MASKING_FOR_TESTS):
                return
            pattern = re.escape(secret)
            if pattern not in self.patterns and (not name or should_hide_value_for_key(name)):
                self.patterns.add(pattern)
                self.replacer = re.compile('|'.join(self.patterns))
        elif isinstance(secret, collections.abc.Iterable):
            for v in secret:
                self.add_mask(v, name)
