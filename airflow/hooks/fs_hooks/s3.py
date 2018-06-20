from builtins import super

import errno
import posixpath

import s3fs

from .base import FsHook


class S3FsHook(FsHook):
    """Hook for interacting with files in S3."""

    def __init__(self, conn_id=None):
        super().__init__(conn_id=conn_id)
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            if self._conn_id is None:
                self._conn = s3fs.S3FileSystem()
            else:
                # TODO: Use same logic as existing S3/AWS hooks.
                config = self.get_connection(self._conn_id)

                extra_kwargs = {}
                if 'encryption' in config.extra_dejson:
                    extra_kwargs['ServerSideEncryption'] = \
                        config.extra_dejson['encryption']

                self._conn = s3fs.S3FileSystem(
                    key=config.login,
                    secret=config.password,
                    s3_additional_kwargs=extra_kwargs)

        return self._conn

    def disconnect(self):
        self._conn = None

    def open(self, file_path, mode='rb'):
        return self.get_conn().open(file_path, mode=mode)

    def exists(self, file_path):
        return self.get_conn().exists(file_path)

    def isdir(self, path):
        path = _remove_s3_prefix(path)

        if not self.sep in path:
            # Path is bucket name.
            return True

        parent_dir = self.dirname(path)

        for child in self.get_conn().ls(parent_dir, detail=True):
            if child['Key'] == path and \
                    child['StorageClass'] == 'DIRECTORY':
                return True

        return False

    def makedir(self, dir_path, mode=0o755, exist_ok=True):
        self.makedirs(dir_path, mode=mode, exist_ok=exist_ok)

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            self._raise_dir_exists(dir_path)

    def walk(self, dir_path):
        dir_path = _remove_s3_prefix(dir_path)
        dir_path = _remove_trailing_slash(dir_path)

        # Yield contents of current directory.
        dir_names, file_names = [], []
        for child in self.get_conn().ls(dir_path, detail=True):
            # Get relative path by removing dir_path + trailing slash.
            rel_path = child['Key'][len(dir_path) + 1:]
            if child['StorageClass'] == 'DIRECTORY':
                dir_names.append(rel_path)
            else:
                file_names.append(rel_path)

        yield dir_path, dir_names, file_names

        # Walk over sub-directories, in top-down fashion.
        for dir_name in dir_names:
            for tup in self.walk(posixpath.join(dir_path, dir_name)):
                yield tup

    def glob(self, pattern):
        pattern = _remove_s3_prefix(pattern)
        return super().glob(pattern=pattern)

    def rm(self, file_path):
        self.get_conn().rm(file_path, recursive=False)

    def rmtree(self, dir_path):
        self.get_conn().rm(dir_path, recursive=True)


def _remove_s3_prefix(path):
    if path.startswith('s3://'):
        path = path[len('s3://'):]
    return path


def _remove_trailing_slash(path):
    if path.endswith('/'):
        return path[:-1]
    return path
