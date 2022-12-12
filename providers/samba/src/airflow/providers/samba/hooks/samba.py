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
from __future__ import annotations

import posixpath
from functools import wraps
from shutil import copyfileobj

import smbclient
import smbprotocol.connection

from airflow.hooks.base import BaseHook


class SambaHook(BaseHook):
    """Allows for interaction with a Samba server.

    The hook should be used as a context manager in order to correctly
    set up a session and disconnect open connections upon exit.

    :param samba_conn_id: The connection id reference.
    :param share:
        An optional share name. If this is unset then the "schema" field of
        the connection is used in its place.
    """

    conn_name_attr = "samba_conn_id"
    default_conn_name = "samba_default"
    conn_type = "samba"
    hook_name = "Samba"

    def __init__(self, samba_conn_id: str = default_conn_name, share: str | None = None) -> None:
        super().__init__()
        conn = self.get_connection(samba_conn_id)

        if not conn.login:
            self.log.info("Login not provided")

        if not conn.password:
            self.log.info("Password not provided")

        connection_cache: dict[str, smbprotocol.connection.Connection] = {}

        self._host = conn.host
        self._share = share or conn.schema
        self._connection_cache = connection_cache
        self._conn_kwargs = {
            "username": conn.login,
            "password": conn.password,
            "port": conn.port or 445,
            "connection_cache": connection_cache,
        }

    def __enter__(self):
        # This immediately connects to the host (which can be
        # perceived as a benefit), but also help work around an issue:
        #
        # https://github.com/jborean93/smbprotocol/issues/109.
        smbclient.register_session(self._host, **self._conn_kwargs)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for host, connection in self._connection_cache.items():
            self.log.info("Disconnecting from %s", host)
            connection.disconnect()
        self._connection_cache.clear()

    def _join_path(self, path):
        return f"//{posixpath.join(self._host, self._share, path.lstrip('/'))}"

    @wraps(smbclient.link)
    def link(self, src, dst, follow_symlinks=True):
        return smbclient.link(
            self._join_path(src),
            self._join_path(dst),
            follow_symlinks=follow_symlinks,
            **self._conn_kwargs,
        )

    @wraps(smbclient.listdir)
    def listdir(self, path):
        return smbclient.listdir(self._join_path(path), **self._conn_kwargs)

    @wraps(smbclient.lstat)
    def lstat(self, path):
        return smbclient.lstat(self._join_path(path), **self._conn_kwargs)

    @wraps(smbclient.makedirs)
    def makedirs(self, path, exist_ok=False):
        return smbclient.makedirs(self._join_path(path), exist_ok=exist_ok, **self._conn_kwargs)

    @wraps(smbclient.mkdir)
    def mkdir(self, path):
        return smbclient.mkdir(self._join_path(path), **self._conn_kwargs)

    @wraps(smbclient.open_file)
    def open_file(
        self,
        path,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        share_access=None,
        desired_access=None,
        file_attributes=None,
        file_type="file",
    ):
        return smbclient.open_file(
            self._join_path(path),
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            share_access=share_access,
            desired_access=desired_access,
            file_attributes=file_attributes,
            file_type=file_type,
            **self._conn_kwargs,
        )

    @wraps(smbclient.readlink)
    def readlink(self, path):
        return smbclient.readlink(self._join_path(path), **self._conn_kwargs)

    @wraps(smbclient.remove)
    def remove(self, path):
        return smbclient.remove(self._join_path(path), **self._conn_kwargs)

    @wraps(smbclient.removedirs)
    def removedirs(self, path):
        return smbclient.removedirs(self._join_path(path), **self._conn_kwargs)

    @wraps(smbclient.rename)
    def rename(self, src, dst):
        return smbclient.rename(self._join_path(src), self._join_path(dst), **self._conn_kwargs)

    @wraps(smbclient.replace)
    def replace(self, src, dst):
        return smbclient.replace(self._join_path(src), self._join_path(dst), **self._conn_kwargs)

    @wraps(smbclient.rmdir)
    def rmdir(self, path):
        return smbclient.rmdir(self._join_path(path), **self._conn_kwargs)

    @wraps(smbclient.scandir)
    def scandir(self, path, search_pattern="*"):
        return smbclient.scandir(
            self._join_path(path),
            search_pattern=search_pattern,
            **self._conn_kwargs,
        )

    @wraps(smbclient.stat)
    def stat(self, path, follow_symlinks=True):
        return smbclient.stat(self._join_path(path), follow_symlinks=follow_symlinks, **self._conn_kwargs)

    @wraps(smbclient.stat_volume)
    def stat_volume(self, path):
        return smbclient.stat_volume(self._join_path(path), **self._conn_kwargs)

    @wraps(smbclient.symlink)
    def symlink(self, src, dst, target_is_directory=False):
        return smbclient.symlink(
            self._join_path(src),
            self._join_path(dst),
            target_is_directory=target_is_directory,
            **self._conn_kwargs,
        )

    @wraps(smbclient.truncate)
    def truncate(self, path, length):
        return smbclient.truncate(self._join_path(path), length, **self._conn_kwargs)

    @wraps(smbclient.unlink)
    def unlink(self, path):
        return smbclient.unlink(self._join_path(path), **self._conn_kwargs)

    @wraps(smbclient.utime)
    def utime(self, path, times=None, ns=None, follow_symlinks=True):
        return smbclient.utime(
            self._join_path(path),
            times=times,
            ns=ns,
            follow_symlinks=follow_symlinks,
            **self._conn_kwargs,
        )

    @wraps(smbclient.walk)
    def walk(self, path, topdown=True, onerror=None, follow_symlinks=False):
        return smbclient.walk(
            self._join_path(path),
            topdown=topdown,
            onerror=onerror,
            follow_symlinks=follow_symlinks,
            **self._conn_kwargs,
        )

    @wraps(smbclient.getxattr)
    def getxattr(self, path, attribute, follow_symlinks=True):
        return smbclient.getxattr(
            self._join_path(path), attribute, follow_symlinks=follow_symlinks, **self._conn_kwargs
        )

    @wraps(smbclient.listxattr)
    def listxattr(self, path, follow_symlinks=True):
        return smbclient.listxattr(
            self._join_path(path), follow_symlinks=follow_symlinks, **self._conn_kwargs
        )

    @wraps(smbclient.removexattr)
    def removexattr(self, path, attribute, follow_symlinks=True):
        return smbclient.removexattr(
            self._join_path(path), attribute, follow_symlinks=follow_symlinks, **self._conn_kwargs
        )

    @wraps(smbclient.setxattr)
    def setxattr(self, path, attribute, value, flags=0, follow_symlinks=True):
        return smbclient.setxattr(
            self._join_path(path),
            attribute,
            value,
            flags=flags,
            follow_symlinks=follow_symlinks,
            **self._conn_kwargs,
        )

    def push_from_local(self, destination_filepath: str, local_filepath: str):
        """Push local file to samba server"""
        with open(local_filepath, "rb") as f, self.open_file(destination_filepath, mode="wb") as g:
            copyfileobj(f, g)
