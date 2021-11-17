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

import inspect
from collections import namedtuple

InspectSignatureResult = namedtuple('InspectSignatureResult', ['bound_arguments', 'has_kwargs'])


def inspect_function_arguments(function) -> InspectSignatureResult:
    """
    Returns the list of variables names of a function and if it
    accepts keyword arguments.
    :param function: The function to inspect
    :type function: Callable
    :rtype: InspectSignatureResult
    """
    parameters = inspect.signature(function).parameters
    bound_arguments = [
        name for name, p in parameters.items() if p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
    ]
    has_kwargs = any(p.kind == p.VAR_KEYWORD for p in parameters.values())
    return InspectSignatureResult(list(bound_arguments), has_kwargs)
