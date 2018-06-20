import unittest
import mock

from airflow.hooks.fs_hooks.base import FsHook


class TestBaseHook(unittest.TestCase):
    """
    Tests for the BaseHook class.

    Note most concrete behaviours are tested in the LocalFsHook or the S3FsHook
    tests, as these have (mock) file systems to test against.
    """

    def test_for_connection(self):
        hook = FsHook.for_connection()


if __name__ == '__main__':
    unittest.main()
