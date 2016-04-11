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
import os

# inspect.signature is only available in Python 3. funcsigs.signature is
# a backport.
try:
    import inspect
    signature = inspect.signature
except AttributeError:
    import funcsigs
    signature = funcsigs.signature

from copy import copy
from functools import wraps

from airflow.exceptions import AirflowException


def apply_defaults(func):
    """
    Function decorator that Looks for an argument named "default_args", and
    fills the unspecified arguments from it.

    Since python2.* isn't clear about which arguments are missing when
    calling a function, and that this can be quite confusing with multi-level
    inheritance and argument defaults, this decorator also alerts with
    specific information about the missing arguments.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 1:
            raise AirflowException(
                "Use keyword arguments when initializing operators")
        dag_args = {}
        dag_params = {}
        if 'dag' in kwargs and kwargs['dag']:
            dag = kwargs['dag']
            dag_args = copy(dag.default_args) or {}
            dag_params = copy(dag.params) or {}

        params = {}
        if 'params' in kwargs:
            params = kwargs['params']
        dag_params.update(params)

        default_args = {}
        if 'default_args' in kwargs:
            default_args = kwargs['default_args']
            if 'params' in default_args:
                dag_params.update(default_args['params'])
                del default_args['params']

        dag_args.update(default_args)
        default_args = dag_args

        sig = signature(func)
        non_optional_args = [
            name for (name, param) in sig.parameters.items()
            if param.default == param.empty and
            param.name != 'self' and
            param.kind not in (param.VAR_POSITIONAL, param.VAR_KEYWORD)]
        for arg in sig.parameters:
            if arg in default_args and arg not in kwargs:
                kwargs[arg] = default_args[arg]
        missing_args = list(set(non_optional_args) - set(kwargs))
        if missing_args:
            msg = "Argument {0} is required".format(missing_args)
            raise AirflowException(msg)

        kwargs['params'] = dag_params

        result = func(*args, **kwargs)
        return result
    return wrapper

if 'BUILDING_AIRFLOW_DOCS' in os.environ:
    # Monkey patch hook to get good function headers while building docs
    apply_defaults = lambda x: x
