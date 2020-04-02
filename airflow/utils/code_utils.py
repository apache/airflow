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

import functools
import inspect
from typing import Any, Optional


def get_python_source(x: Any) -> Optional[str]:
    """
    Helper function to get Python source (or not), preventing exceptions
    """
    if isinstance(x, str):
        return x

    if x is None:
        return None

    source_code = None

    if isinstance(x, functools.partial):
        source_code = inspect.getsource(x.func)

    if source_code is None:
        try:
            source_code = inspect.getsource(x)
        except TypeError:
            pass

    if source_code is None:
        try:
            source_code = inspect.getsource(x.__call__)
        except (TypeError, AttributeError):
            pass

    if source_code is None:
        source_code = 'No source code available for {}'.format(type(x))
    return source_code
