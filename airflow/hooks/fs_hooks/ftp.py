from builtins import super
import ftplib

import ftputil
from ftputil import session as ftp_session

from .base import FsHook


class FtpHook(FsHook):
    """Hook for interacting with files over FTP."""

    def __init__(self, conn_id):
        super().__init__(conn_id=conn_id)
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            config = self.get_connection(self._conn_id)

            secure = config.extra_dejson.get('tls', False)
            base_class = ftplib.FTP_TLS if secure else ftplib.FTP

            session_factory = ftp_session.session_factory(
                base_class=base_class,
                port=config.port or 21,
                encrypt_data_channel=secure)

            self._conn = ftputil.FTPHost(
                config.host,
                config.login,
                config.password,
                session_factory=session_factory)

        return self._conn

    def disconnect(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def open(self, file_path, mode='rb'):
        return self.get_conn().open(file_path, mode=mode)

    def isdir(self, path):
        return self.get_conn().isdir(path)

    def exists(self, file_path):
        return self.get_conn().path.exists(file_path)

    def makedir(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            self._raise_dir_exists(dir_path)
        self.get_conn().mkdir(dir_path, mode=mode)

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            self._raise_dir_exists(dir_path)
        self.get_conn().makedirs(dir_path, mode=mode)

    def walk(self, dir_path):
        for tup in self.get_conn().walk(dir_path):
            yield tup

    def rm(self, file_path):
        self.get_conn().remove(file_path)

    def rmtree(self, dir_path):
        self.get_conn().rmtree(dir_path, ignore_errors=False)
