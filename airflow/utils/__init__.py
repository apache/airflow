# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import

import warnings

from .decorators import apply_defaults as _apply_defaults


def apply_defaults(func):
    warnings.warn_explicit(
        """
        You are importing apply_defaults from airflow.utils which
        will be deprecated in a future version.
        Please use :

        from airflow.utils.decorators import apply_defaults
        """,
        category=PendingDeprecationWarning,
        filename=func.__code__.co_filename,
        lineno=func.__code__.co_firstlineno + 1
    )
    return _apply_defaults(func)
