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

from functools import wraps
from shutil import copyfileobj
from typing import Optional

from smbclient import (
    getxattr,
    link,
    listdir,
    listxattr,
    lstat,
    makedirs,
    mkdir,
    open_file,
    readlink,
    register_session,
    remove,
    removedirs,
    removexattr,
    rename,
    replace,
    rmdir,
    scandir,
    setxattr,
    stat,
    stat_volume,
    symlink,
    truncate,
    unlink,
    utime,
    walk,
)

from airflow.hooks.base import BaseHook


class SambaHook(BaseHook):
    """Allows for interaction with an samba server."""

    conn_name_attr = 'samba_conn_id'
    default_conn_name = 'samba_default'
    conn_type = 'samba'
    hook_name = 'Samba'

    def __init__(self, samba_conn_id: str = default_conn_name, share: Optional[str] = None) -> None:
        super().__init__()
        conn = self.get_connection(samba_conn_id)

        if not conn.login:
            self.log.info("Login not provided")

        if not conn.password:
            self.log.info("Password not provided")

        self._host = conn.host
        self._share = share or conn.schema
        self._connection_cache = connection_cache = {}
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
        register_session(self._host, **self._conn_kwargs)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for host, connection in self._connection_cache.items():
            self.log.info("Disconnecting from %s", host)
            connection.disconnect()
        self._connection_cache.clear()

    @property
    def _base_url(self):
        return f"//{self._host}/{self._share}"

    @wraps(link)
    def link(self, src, dst, follow_symlinks=True):
        return link(
            self._base_url + "/" + src,
            self._base_url + "/" + dst,
            follow_symlinks=follow_symlinks,
            **self._conn_kwargs,
        )

    @wraps(listdir)
    def listdir(self, path):
        return listdir(self._base_url + "/" + path, **self._conn_kwargs)

    @wraps(lstat)
    def lstat(self, path):
        return lstat(self._base_url + "/" + path, **self._conn_kwargs)

    @wraps(makedirs)
    def makedirs(self, path, exist_ok=False):
        return makedirs(self._base_url + "/" + path, exist_ok=exist_ok, **self._conn_kwargs)

    @wraps(mkdir)
    def mkdir(self, path):
        return mkdir(self._base_url + "/" + path, **self._conn_kwargs)

    @wraps(open_file)
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
        return open_file(
            self._base_url + "/" + path,
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

    @wraps(readlink)
    def readlink(self, path):
        return readlink(self._base_url + "/" + path, **self._conn_kwargs)

    @wraps(remove)
    def remove(self, path):
        return remove(self._base_url + "/" + path, **self._conn_kwargs)

    @wraps(removedirs)
    def removedirs(self, path):
        return removedirs(self._base_url + "/" + path, **self._conn_kwargs)

    @wraps(rename)
    def rename(self, src, dst):
        return rename(self._base_url + "/" + src, self._base_url + "/" + dst, **self._conn_kwargs)

    @wraps(replace)
    def replace(self, src, dst):
        return replace(self._base_url + "/" + src, self._base_url + "/" + dst, **self._conn_kwargs)

    @wraps(rmdir)
    def rmdir(self, path):
        return rmdir(self._base_url + "/" + path, **self._conn_kwargs)

    @wraps(scandir)
    def scandir(self, path, search_pattern="*"):
        return scandir(
            self._base_url + "/" + path,
            search_pattern=search_pattern,
            **self._conn_kwargs,
        )

    @wraps(stat)
    def stat(self, path, follow_symlinks=True):
        return stat(self._base_url + "/" + path, follow_symlinks=follow_symlinks, **self._conn_kwargs)

    @wraps(stat_volume)
    def stat_volume(self, path):
        return stat_volume(self._base_url + "/" + path, **self._conn_kwargs)

    @wraps(symlink)
    def symlink(self, src, dst, target_is_directory=False):
        return symlink(
            self._base_url + "/" + src,
            self._base_url + "/" + dst,
            target_is_directory=target_is_directory,
            **self._conn_kwargs,
        )

    @wraps(truncate)
    def truncate(self, path, length):
        return truncate(self._base_url + "/" + path, length, **self._conn_kwargs)

    @wraps(unlink)
    def unlink(self, path):
        return unlink(self._base_url + "/" + path, **self._conn_kwargs)

    @wraps(utime)
    def utime(self, path, times=None, ns=None, follow_symlinks=True):
        return utime(
            self._base_url + "/" + path,
            times=times,
            ns=ns,
            follow_symlinks=follow_symlinks,
            **self._conn_kwargs,
        )

    @wraps(walk)
    def walk(self, path, topdown=True, onerror=None, follow_symlinks=False):
        return walk(
            self._base_url + "/" + path,
            topdown=topdown,
            onerror=onerror,
            follow_symlinks=follow_symlinks,
            **self._conn_kwargs,
        )

    @wraps(getxattr)
    def getxattr(self, path, attribute, follow_symlinks=True):
        return getxattr(
            self._base_url + "/" + path, attribute, follow_symlinks=follow_symlinks, **self._conn_kwargs
        )

    @wraps(listxattr)
    def listxattr(self, path, follow_symlinks=True):
        return listxattr(self._base_url + "/" + path, follow_symlinks=follow_symlinks, **self._conn_kwargs)

    @wraps(removexattr)
    def removexattr(self, path, attribute, follow_symlinks=True):
        return removexattr(
            self._base_url + "/" + path, attribute, follow_symlinks=follow_symlinks, **self._conn_kwargs
        )

    @wraps(setxattr)
    def setxattr(self, path, attribute, value, flags=0, follow_symlinks=True):
        return setxattr(
            self._base_url + "/" + path,
            attribute,
            value,
            flags=flags,
            follow_symlinks=follow_symlinks,
            **self._conn_kwargs,
        )

    def push_from_local(self, destination_filepath: str, local_filepath: str):
        """Push local file to samba server"""
        with open(local_filepath, "rb") as f, self.open_file(destination_filepath, mode="w") as g:
            copyfileobj(f, g)
