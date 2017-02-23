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
from tempfile import mkdtemp

from contextlib import contextmanager


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
