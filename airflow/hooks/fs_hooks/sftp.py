from builtins import super
import posixpath

import pysftp

from .base import FsHook


class SftpHook(FsHook):
    """Hook for interacting with files over SFTP."""

    def __init__(self, conn_id):
        super().__init__(conn_id=conn_id)
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            config = self.get_connection(self._conn_id)

            private_key = config.extra_dejson.get('private_key', None)

            if not private_key:
                self._conn = pysftp.Connection(
                    config.host,
                    username=config.login,
                    password=config.password)
            elif private_key and config.password:
                self._conn = pysftp.Connection(
                    config.host,
                    username=config.login,
                    private_key=private_key,
                    private_key_pass=config.password)
            else:
                self._conn = pysftp.Connection(
                    config.host,
                    username=config.login,
                    private_key=private_key)

        return self._conn

    def disconnect(self):
        if self._conn is not None:
            self._conn.close()
        self._conn = None

    def open(self, file_path, mode='rb'):
        return self.get_conn().open(file_path, mode=mode)

    def exists(self, file_path):
        return self.get_conn().exists(file_path)

    def isdir(self, path):
        return self.get_conn().isdir(path)

    def makedir(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            self._raise_dir_exists(dir_path)
        self.get_conn().mkdir(dir_path, mode=int(oct(mode)[2:]))

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            self._raise_dir_exists(dir_path)
        self.get_conn().makedirs(dir_path, mode=int(oct(mode)[2:]))

    def walk(self, dir_path):
        from stat import S_ISDIR, S_ISREG

        conn = self.get_conn()
        client = conn.sftp_client

        # Yield contents of current directory.
        dir_names, file_names = [], []
        for entry in conn.listdir(dir_path):
            full_path = posixpath.join(dir_path, entry)
            mode = client.stat(full_path).st_mode

            if S_ISDIR(mode):
                dir_names.append(entry)
            elif S_ISREG(mode):
                file_names.append(entry)

        yield dir_path, dir_names, file_names

        # Walk over sub-directories, in top-down fashion.
        for dir_name in dir_names:
            for tup in self.walk(posixpath.join(dir_path, dir_name)):
                yield tup

    def rm(self, file_path):
        self.get_conn().remove(file_path)

    def rmtree(self, dir_path):
        result = self.get_conn().execute('rm -r {!r}'.format(dir_path))

        if result:
            message = b'\n'.join(result)
            raise OSError(message.decode())
