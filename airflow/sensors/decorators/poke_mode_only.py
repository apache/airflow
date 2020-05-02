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
import inspect
import os
from functools import wraps

from airflow.sensors.base_sensor_operator import BaseSensorOperator


def poke_mode_only(cls):
    """
    Class Decorator for child classes of BaseSensorOperator to indicate
    that instances of this class are only safe to use poke mode.

    Will decorate all methods in the class to assert they did not change
    the mode from 'poke'.

    :param cls: BaseSensor class to enforce methods only use 'poke' mode.
    :type cls: type
    """
    def decorate(cls):
        if not issubclass(cls, BaseSensorOperator):
            raise ValueError("poke_mode_only decorator should only be "
                             + "applied to subclasses of BaseSensorOperator,"
                             + f" got:{cls}.")
        for method in inspect.getmembers(cls, inspect.isfunction):
            setattr(cls, method[0], _poke_mode_only_func_decorator(method))
        return cls

    return decorate(cls)


def _poke_mode_only_func_decorator(method):
    """
    Decorator that raises an error if method changed mode to anything but
    'poke'.function
    :param method: BaseSensor class to enforce methods only use 'poke' mode.
    :type method: function
    """
    func = method[1]  # function object from inspect.get_attr

    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        # Poor mans way of getting instnace w/o bound methods in python3
        # checking if fist parameter is self (aka non-static method).
        if isinstance(args[0], BaseSensorOperator):
            instance = args[0]
            try:
                if instance.mode != 'poke':
                    raise ValueError(
                        f"cannot set mode to 'poke'.")
            except AttributeError:  # this happens on _set_context
                pass

        return result

    return wrapper


if 'BUILDING_AIRFLOW_DOCS' in os.environ:
    # flake8: noqa: F811
    # Monkey patch hook to get good function headers while building docs
    apply_defaults = lambda x: x
