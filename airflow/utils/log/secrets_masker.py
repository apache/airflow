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
import logging
import re
from typing import TYPE_CHECKING, Any, Iterable, List, Optional

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property


if TYPE_CHECKING:
    from airflow.typing_compat import RePatternType


class SecretsMasker(logging.Filter):
    """Redact secrets from logs"""

    replacer: Optional["RePatternType"] = None
    patterns: List[str]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.patterns = []

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
        return frozenset(record.__dict__.keys()) - frozenset(('msg', 'args'))

    def filter(self, record) -> bool:
        if self.replacer:
            for k, v in record.__dict__.items():
                if k in self._record_attrs_to_ignore:
                    continue
                if isinstance(v, dict):
                    v = {dict_key: self.redact(subval) for dict_key, subval in v.items()}
                elif isinstance(v, tuple):
                    v = tuple(self.redact(subval) for subval in v)
                else:
                    v = self.redact(v)
                record.__dict__[k] = v
            if record.exc_info:
                exc = record.exc_info[1]
                # I'm not sure if this is a good idea!
                exc.args = (self.redact(v) for v in exc.args)

        return True

    def redact(self, item: Any) -> Any:
        """Redact an any secrets found in ``item``, if it is a string"""
        if isinstance(item, str):
            return self.replacer.sub('***', item)
        return item

    def add_mask(self, secret: str):
        """Add a new secret to be masked to this filter instance."""
        self.patterns.append(re.escape(secret))

        self.replacer = re.compile('|'.join(self.patterns))
