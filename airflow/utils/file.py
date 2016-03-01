from __future__ import absolute_import
from __future__ import unicode_literals

import errno
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
