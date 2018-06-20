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
#

from builtins import super
import errno
import importlib
import posixpath

from airflow.hooks.base_hook import BaseHook

from . import _fnmatch as fnmatch

_FS_BASE_MODULE = '.'.join(__name__.split('.')[:-1])


class FsHook(BaseHook):
    """Base FsHook defining the FsHook interface and providing some basic
       functionality built on this interface.
    """

    _conn_classes = {
        'ftp': _FS_BASE_MODULE + '.ftp.FtpHook',
        'hdfs': _FS_BASE_MODULE + '.hdfs3.Hdfs3Hook',
        'local': _FS_BASE_MODULE + '.local.LocalFsHook',
        's3': _FS_BASE_MODULE + '.s3.S3FsHook',
        'sftp': _FS_BASE_MODULE + '.sftp.SftpHook'
    }

    sep = posixpath.sep

    def __init__(self, conn_id=None):
        super().__init__(source=None)
        self._conn_id = conn_id

    @classmethod
    def for_connection(cls, conn_id=None):
        """Return appropriate hook for the given connection."""

        if conn_id is None or conn_id == 'local':
            conn_type = 'local'
        else:
            conn_type = cls.get_connection(conn_id).conn_type

        try:
            class_ = cls._conn_classes[conn_type]
        except KeyError:
            raise ValueError('Conn type {!r} is not supported'
                             .format(conn_type))

        if isinstance(class_, str):
            # conn_class is a string identifier, import
            # class from the indicated module.
            split = class_.split('.')
            module_name = '.'.join(split[:-1])
            class_name = split[-1]

            module = importlib.import_module(module_name)
            class_ = getattr(module, class_name)

        return class_(conn_id=conn_id)

    @classmethod
    def register_hook(cls, conn_type, class_):
        """Register FsHook subclass for the given connection type.

        Registered FsHook subclasses are used by `for_connection` when
        instantiating the appropriate hook for a given connection, based
        on its connection type.

        :param str conn_type: Connection type.
        :param class_: FsHook to register. Can either be the class itself
            or a string specifying the full module path for the class.
        """
        cls._conn_classes[conn_type] = class_

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def disconnect(self):
        """Closes fs connection (if applicable)."""
        pass

    # Interface methods (should be implemented by sub-classes).

    def open(self, file_path, mode='rb'):
        """Returns file_obj for given file path.

        :param str file_path: Path to the file to open.
        :param str mode: Mode to open the file in.

        :returns: An opened file object.
        """
        raise NotImplementedError()

    def exists(self, file_path):
        """Checks whether the given file path exists.

        :param str file_path: File path.

        :returns: True if the file exists, else False.
        :rtype: bool
        """
        raise NotImplementedError()

    def isdir(self, path):
        """Returns true if the given path points to a directory.

        :param str path: File or directory path.
        """
        raise NotImplementedError()

    def walk(self, dir_path):
        """Generates file names in the given directory tree."""
        raise NotImplementedError()

    def makedir(self, dir_path, mode=0o755, exist_ok=True):
        """Creates the directory, without creating intermediate directories."""
        raise NotImplementedError()

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        """Creates directory, creating intermediate directories if needed.

        :param str dir_path: Path to the directory to create.
        :param int mode: Mode to use for directory (if created).
        :param bool exist_ok: Whether the directory is already allowed to exist.
            If false, an IOError is raised if the directory exists.
        """
        raise NotImplementedError()

    def rm(self, file_path):
        """Deletes the given file path.

        :param str file_path: Path to file:
        """
        raise NotImplementedError()

    def rmtree(self, dir_path):
        """Deletes given directory tree recursively.

        :param str dir_path: Path to directory to delete.
        """
        raise NotImplementedError()

    @staticmethod
    def _raise_dir_exists(dir_path):
        raise IOError(errno.EEXIST,
                      'Directory exists: {!r}'.format(dir_path))

    # Path manipulation methods.

    # Join/split should be overriden for file systems that don't use
    # posix path conventions.

    @staticmethod
    def join(path, *paths):
        """Join one or more path components."""
        return posixpath.join(path, *paths)

    @staticmethod
    def split(path):
        """Split the pathname path into a head/tail pair."""
        return posixpath.split(path)

    @classmethod
    def dirname(cls, path):
        """Return the directory name of pathname path."""
        return cls.split(path)[0]

    @classmethod
    def basename(cls, path):
        """Return the base name of pathname path."""
        return cls.split(path)[1]

    # General utility methods built on the above interface.

    # These methods can be overridden in sub-classes if more efficient
    # implementations are available for a specific file system.

    def glob(self, pattern):
        """Returns list of file paths matching pattern (i.e., with '*'s).

        Supports recursive globbing using '**'. Built on `os.walk`.

        :param str pattern: Pattern to match.
        :returns: List of matched file paths.
        :rtype: list[str]
        """

        # Get root of pattern.
        if self.sep in pattern[:pattern.index('*')]:
            ind = pattern[:pattern.index('*')].rindex(self.sep)
            root = pattern[:ind + 1]
        else:
            root = '.'

        # Get all file paths.
        all_paths = []
        for base_dir, _, file_names in self.walk(root):
            all_paths.extend(self.join(base_dir, f) for f in file_names)

        # Filter using modified version of fnmatch.
        matches = fnmatch.filter(all_paths, pattern, sep=self.sep)

        return matches


class NotSupportedError(NotImplementedError):
    """Exception that may be raised by FsHooks if the don't support
       the given operation.
    """
    pass
