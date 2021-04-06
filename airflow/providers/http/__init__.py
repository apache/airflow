#
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
#
from typing import Callable, Dict, List, Tuple, Union


def determine_kwargs(func: Callable, args: Union[Tuple, List], kwargs: Dict) -> Dict:
    """
    Inspect the signature of a given callable to determine which arguments in kwargs need
    to be passed to the callable.
    :param func: The callable that you want to invoke
    :param args: The positional arguments that needs to be passed to the callable, so we
        know how many to skip.
    :param kwargs: The keyword arguments that need to be filtered before passing to the callable.
    :return: A dictionary which contains the keyword arguments that are compatible with the callable.
    """
    import inspect
    import itertools

    signature = inspect.signature(func)
    has_kwargs = any(p.kind == p.VAR_KEYWORD for p in signature.parameters.values())

    for name in itertools.islice(signature.parameters.keys(), len(args)):
        # Check if args conflict with names in kwargs
        if name in kwargs:
            raise ValueError(f"The key {name} in args is part of kwargs and therefore reserved.")

    if has_kwargs:
        # If the callable has a **kwargs argument, it's ready to accept all the kwargs.
        return kwargs

    # If the callable has no **kwargs argument, it only wants the arguments it requested.
    return {key: kwargs[key] for key in signature.parameters if key in kwargs}


def make_kwargs_callable(func: Callable) -> Callable:
    """
    Make a new callable that can accept any number of positional or keyword arguments
    but only forwards those required by the given callable func.
    """
    import functools

    @functools.wraps(func)
    def kwargs_func(*args, **kwargs):
        kwargs = determine_kwargs(func, args, kwargs)
        return func(*args, **kwargs)

    return kwargs_func
