import ftplib
import unittest

import mock
from hdfs3.utils import MyNone

from airflow.hooks.fs_hooks.ftp import FtpHook, ftputil, ftp_session


class TestFtpHook(unittest.TestCase):
    """
    Tests for the FtpHook class.

    Note that the FTP session is mocked in most of these tests to avoid the
    requirement of having a local FTP server for testing.
    """

    def setUp(self):
        self._mock_fs = mock.Mock()

        self._mocked_hook = FtpHook(conn_id='ftp_default')
        self._mocked_hook._conn = self._mock_fs

    @mock.patch.object(ftp_session, 'session_factory')
    @mock.patch.object(ftputil, 'FTPHost')
    @mock.patch.object(FtpHook, 'get_connection')
    def test_get_conn(self, conn_mock, host_mock, session_mock):
        """Tests get_conn call with unsecured connection."""

        conn_mock.return_value = mock.Mock(
            host='example',
            login='user',
            password='password',
            port=2121,
            extra_dejson={'tls': False})

        with FtpHook(conn_id='ftp_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('ftp_default')

        host_mock.assert_called_once_with(
            'example',
            'user',
            'password',
            session_factory=mock.ANY)

        session_mock.assert_called_once_with(
            base_class=ftplib.FTP,
            port=2121,
            encrypt_data_channel=False)

    @mock.patch.object(ftp_session, 'session_factory')
    @mock.patch.object(ftputil, 'FTPHost')
    @mock.patch.object(FtpHook, 'get_connection')
    def test_get_conn_tls(self, conn_mock, host_mock, session_mock):
        """Tests get_conn call with a secured connection."""

        conn_mock.return_value = mock.Mock(
            host='example',
            login='user',
            password='password',
            port=2121,
            extra_dejson={'tls': True})

        with FtpHook(conn_id='ftp_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('ftp_default')

        host_mock.assert_called_once_with(
            'example',
            'user',
            'password',
            session_factory=mock.ANY)

        session_mock.assert_called_once_with(
            base_class=ftplib.FTP_TLS,
            port=2121,
            encrypt_data_channel=True)

    def test_open(self):
        """Tests the `open` method."""

        with self._mocked_hook as hook:
            hook.open('test.txt', mode='rb')

        self._mock_fs.open.assert_called_once_with('test.txt', mode='rb')

    def test_exists(self):
        """Tests the `exists` method."""

        with self._mocked_hook as hook:
            hook.exists('test.txt')

        self._mock_fs.path.exists.assert_called_once_with('test.txt')

    def test_isdir(self):
        """Tests the `isdir` method."""

        with self._mocked_hook as hook:
            hook.isdir('test.txt')

        self._mock_fs.isdir.assert_called_once_with('test.txt')

    def test_makedir(self):
        """Tests the `makedir` method with a non-existing dir."""

        self._mock_fs.exists.return_value = False

        with self._mocked_hook as hook:
            hook.makedir('path/to/dir', mode=0o755)

        self._mock_fs.mkdir.assert_called_once_with('path/to/dir', mode=0o755)

    def test_makedir_existing(self):
        """Tests the `makedir` method with an existing dir
           and exist_ok = False.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            with self.assertRaises(IOError):
                hook.makedir('path/to/dir', mode=0o755, exist_ok=False)

    def test_makedir_existing_ok(self):
        """Tests the `makedir` method with an existing dir
           and exist_ok = True.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            hook.makedir('path/to/dir', mode=0o755, exist_ok=True)

        self._mock_fs.chmod.assert_not_called()

    def test_makedirs(self):
        """Tests the `makedirs` method with a non-existing dir."""

        self._mock_fs.exists.return_value = False

        with self._mocked_hook as hook:
            hook.makedirs('path/to/dir', mode=0o755)

        self._mock_fs.makedirs.assert_called_once_with(
            'path/to/dir', mode=0o755)

    def test_makedirs_existing(self):
        """Tests the `makedirs` method with an existing dir
           and exist_ok = False.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            with self.assertRaises(IOError):
                hook.makedirs('path/to/dir', mode=0o755, exist_ok=False)

        self._mock_fs.makedirs.assert_not_called()

    def test_makedirs_existing_ok(self):
        """Tests the `makedir` method with an existing dir
           and exist_ok = True.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            hook.makedirs('path/to/dir', mode=0o755, exist_ok=True)

    def test_rm(self):
        """Tests the `rm` method."""

        with self._mocked_hook as hook:
            hook.rm('test_dir')

        self._mock_fs.remove.assert_called_once_with('test_dir')

    def test_rmtree(self):
        """Tests the `rmtree` method."""

        with self._mocked_hook as hook:
            hook.rmtree('test_dir')

        self._mock_fs.rmtree.assert_called_once_with(
            'test_dir', ignore_errors=False)


if __name__ == '__main__':
    unittest.main()
