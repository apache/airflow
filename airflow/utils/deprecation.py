# -*- coding: utf-8 -*-
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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import functools
import warnings


def deprecated_args(renamed=None, dropped=None):
    """Decorator for the deprecation of renamed/removed keyword arguments.

    Wraps functions with changed keyword arguments (either renamed to new
    arguments or dropped entirely). Functions calls with deprecated arguments
    raise appropriate warnings. For removed arguments, any given values are
    ignored (outside of the warning). For renamed arguments, values are
    transparently proxied to their new argument names.

    :param dict[str, str] renamed: Dict mapping old arguments to their
        argument names in the new function/method.
    :param list[str] dropped: List of arguments that have been removed
        in the new function/method.
    """

    renamed = renamed or {}
    dropped = dropped or set()

    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            new_kwargs = {}

            for key in kwargs:
                if key in dropped:
                    warnings.warn(
                        "Argument {!r} is no longer supported and will be"
                        "removed in a future version of Airflow.".format(key),
                        category=DeprecationWarning,
                    )
                elif key in renamed:
                    warnings.warn(
                        "Argument {!r} has been renamed to {!r}. The old name "
                        "will no longer be supported in a future version of "
                        "Airflow.".format(key, renamed[key]),
                        category=DeprecationWarning,
                    )

                    new_kwargs[renamed[key]] = kwargs[key]
                else:
                    new_kwargs[key] = kwargs[key]

            return function(*args, **new_kwargs)

        return wrapper

    return decorator


def deprecated(new_name=None):
    """This is a decorator which can be used to mark functions as deprecated.

       The decorator ensures a warning is emitted whenever the function is
       called to warn the user of its deprecated status. The parameter new_name
       can be used to indicate a replacing function, if the deprecated function
       has been renamed or replaced.

    :param str new_name: Optional name of a replacing function, if applicable.
    """

    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            if new_name:
                message = (
                    "{} has been deprecated and will be replaced by "
                    "{} in a future version of Airflow.".format(
                        function.__name__, new_name
                    )
                )
            else:
                message = (
                    "{} has been deprecated and will be removed "
                    "in a future version of Airflow.".format(function.__name__)
                )
            warnings.warn(message, category=DeprecationWarning)
            return function(*args, **kwargs)

        return wrapper

    return decorator


class RenamedClass(object):
    """Helper class used for deprecating old classes that have new names.

    For example, we can use this class to rename the (old) class
    `HDFSHook` to it's new class name `HdfsHook` as follows:

        class HdfsHook(object):
            ...

        HDFSHook = RenamedClass('HDFSHook', new_class=HdfsHook)

    so that old code can still use the deprecated form:

        hook = HDFSHook(...)

    which will raise an appropriate warning when called.

    :param str old_name: Name of the old class.
    :param class new_class: The replacing class.
    :param str old_module: Name of the module containing the old class.
    """

    def __init__(self, old_name, new_class, old_module=None):
        self._old_name = old_name
        self._old_module = old_module
        self._new_class = new_class

    def _warn(self):
        old_name = self._old_name

        if self._old_module:
            old_name = self._old_module + '.' + old_name

        message = ("Class {!r} has been renamed to {!r}. Support for the old "
                   "class name will be removed in future versions of Airflow."
                   .format(old_name, _full_class_name(self._new_class)))
        warnings.warn(message, category=DeprecationWarning)

    def __call__(self, *args, **kwargs):
        self._warn()
        return self._new_class(*args, **kwargs)

    def __getattr__(self, attr):
        self._warn()
        return getattr(self._new_class, attr)


def _full_class_name(cls):
    return cls.__module__ + "." + cls.__name__
