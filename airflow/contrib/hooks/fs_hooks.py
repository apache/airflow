import glob
import os
from os import path
import shutil

from airflow.hooks.base_hook import BaseHook


class FsHook(BaseHook):
    """Base FsHook defining the FsHook interface and providing some basic
       functionality built on this interface.
    """

    # TODO: Allow copy_* methods to copy from non-local file systems
    #   using the hooks themselves. Requires a `walk` implementation.

    def __init__(self):
        super().__init__(source=None)

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

    def walk(self, dir_path):
        """Generates file names in the given directory tree."""
        raise NotImplementedError()

    def glob(self, pattern):
        """Returns list of paths matching pattern (i.e., with “*”s).

        :param str pattern: Pattern to match

        :returns: List of matched file paths.
        :rtype: list[str]
        """
        raise NotImplementedError()

    def makedir(self, dir_path, mode=0o755, exist_ok=True):
        """Creates the directory, without creating intermediate directories."""
        raise NotImplementedError()

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        """Creates directory, creating intermediate directories if needed.

        :param str dir_path: Path to the directory to create.
        :param int mode: Mode to use for directory (if created).
        :param bool exist_ok: Whether the directory is already allowed to exist.
            If false, a ValueError is raised if the directory exists.
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

    # General utility methods built on the above interface.
    #
    # These methodscan/should be overridden in sub-classes if more
    # efficient implementations are available for a specific file system.

    # def copy_file(self, src_path, dest_path):
    #     """Copies local file to given path.

    #     :param str src_path: Path to source file.
    #     :param str dest_path: Path to destination file.
    #     """

    #     with open(src_path, 'rb') as src_file, \
    #          self.open(dest_path, 'wb') as dest_file:
    #         shutil.copyfileobj(src_file, dest_file)

    # def copy_fileobj(self, src_obj, dest_path):
    #     """Copies fileobj to given path.

    #     :param src_obj: Source file-like object.
    #     :param str dest_path: Path to destination file.
    #     """

    #     with self.open(dest_path, 'wb') as dest_file:
    #         shutil.copyfileobj(src_obj, dest_file)

    # def copy_dir(self, src_dir, dest_dir):
    #     """Copies local directory recursively to given path.

    #     :param str src_dir: Path to source directory.
    #     :param str dest_path: Path to destination directory.
    #     """

    #     # Create root dest dir.
    #     self.makedirs(dest_dir, exist_ok=True)

    #     for root, dirs, files in os.walk(src_dir):
    #         # Copy over files.
    #         for item in files:
    #             src_path = path.join(root, item)

    #             rel_path = path.relpath(src_path, src_dir)
    #             dest_path = path.join(dest_dir, rel_path)

    #             self.copy_file(src_path, dest_path)

    #         # Create sub-directories.
    #         for item in dirs:
    #             src_path = path.join(root, item)

    #             rel_path = path.relpath(src_path, src_dir)
    #             dest_path = path.join(dest_dir, rel_path)

    #             self.makedirs(dest_path, exist_ok=True)


class NotSupportedError(NotImplementedError):
    """Exception that may be raised by FsHooks if the don't support
       the given operation.
    """
    pass


class LocalHook(FsHook):
    """Dummy fs hook for interacting with files on the local file system."""

    def get_conn(self):
        return None

    def open(self, file_path, mode='rb'):
        return open(str(file_path), mode=mode)

    def exists(self, file_path):
        return os.path.exists(str(file_path))

    def makedir(self, dir_path, mode=0o755, exist_ok=True):
        if path.exists(dir_path):
            if not exist_ok:
                # TODO: Implement FileExistsError for Python 2.
                raise FileExistsError('[Errno 17] File exists: {!r}'
                                      .format(dir_path))
        else:
            os.mkdir(dir_path, mode=mode)

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        os.makedirs(str(dir_path), mode=mode, exist_ok=exist_ok)

    def walk(self, dir_path):
        for tup in os.walk(dir_path):
            yield tup

    def glob(self, pattern):
        return glob.glob(str(pattern))

    def rm(self, file_path):
        os.unlink(str(file_path))

    def rmtree(self, dir_path):
        shutil.rmtree(str(dir_path))


class S3FsHook(FsHook):
    """Hook for interacting with files in S3."""

    def __init__(self, conn_id=None):
        super().__init__()
        self._conn_id = conn_id
        self._conn = None

    def get_conn(self):
        import s3fs

        if self._conn is None:
            if self._conn_id is None:
                self._conn = s3fs.S3FileSystem()
            else:
                config = self.get_connection(self._conn_id)

                extra_kwargs = {}
                if config.extra_dejson.get('encryption', False):
                    extra_kwargs['ServerSideEncryption'] = "AES256"

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

    def makedir(self, dir_path, mode=0o755, exist_ok=True):
        self.makedirs(dir_path, mode=mode, exist_ok=exist_ok)

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            raise ValueError('Directory already exists')

    def walk(self, dir_path):
        if dir_path.startswith('s3://'):
            dir_path = dir_path[len('s3://'):]

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
            for tup in self.walk(path.join(dir_path, dir_name)):
                yield tup

    def glob(self, pattern):
        try:
            return self.get_conn().glob(pattern)
        except FileNotFoundError:
            return []

    def rm(self, file_path):
        self.get_conn().rm(file_path, recursive=False)

    def rmtree(self, dir_path):
        self.get_conn().rm(dir_path, recursive=True)
