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
from __future__ import unicode_literals

import errno
import os
import shutil
from contextlib import contextmanager
from tempfile import mkdtemp

from past.builtins import basestring


@contextmanager
def TemporaryDirectory(suffix='', prefix=None, dir=None):
    name = mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    try:
        yield name
    finally:
        try:
            shutil.rmtree(name)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e


def mkdirs(path, mode):
    """
    Creates the directory specified by path, creating intermediate directories
    as necessary. If directory already exists, this is a no-op.

    :param path: The directory to create
    :type path: str
    :param mode: The mode to give to the directory e.g. 0o755
    :type mode: int
    :return: A list of directories that were created
    :rtype: list[str]
    """
    if not path or os.path.exists(path):
        return []
    (head, _) = os.path.split(path)
    res = mkdirs(head, mode)
    os.mkdir(path)
    os.chmod(path, mode)
    res += [path]
    return res


def each(func, path, file_extensions, filters=None):
    """
    for each file in the path run func and pass absolute, and relative paths as
    well as filename, as its three params

    :return: array of return values
    :param func: the function to run
    :type func: function
    :param path: the directory of files to loop through
    :type path: str
    :param file_extensions: a set of file extensions. eg: (".yaml")
    :type file_extensions: collections.iterable
    :param filters: a list of filename to run the func for. (Don't run for other files)
    :type filters: array or set
    """
    if isinstance(file_extensions, basestring):
        file_extensions = [file_extensions]

    rets = {}
    for root, _, files in os.walk(path):
        for filename in sorted(files):
            if filters and filename not in filters:
                continue
            if not any([filename.endswith(ext) for ext in file_extensions]):
                continue
            abs_path = os.path.join(root, filename)
            rel_path = abs_path[len(path) + 1:]
            rets[abs_path] = func(abs_path, rel_path, filename)
            # early terminate if one of the calls returns False
            if rets[abs_path] is False:
                return rets
    return rets
