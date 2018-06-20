import unittest

import mock

from airflow.hooks.fs_hooks.sftp import SftpHook, pysftp


class TestFtpHook(unittest.TestCase):
    """
    Tests for the FtpHook class.

    Note that the FTP session is mocked in most of these tests to avoid the
    requirement of having a local FTP server for testing.
    """

    def setUp(self):
        self._mock_fs = mock.Mock()

        self._mocked_hook = SftpHook(conn_id='sftp_default')
        self._mocked_hook._conn = self._mock_fs

    @mock.patch.object(pysftp, 'Connection')
    @mock.patch.object(SftpHook, 'get_connection')
    def test_get_conn_pass(self, conn_mock, pysftp_mock):
        """Tests get_conn with a password."""

        conn_mock.return_value = mock.Mock(
            host='example',
            login='user',
            password='password',
            extra_dejson={})

        with SftpHook(conn_id='sftp_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('sftp_default')

        pysftp_mock.assert_called_once_with(
            'example',
            username='user',
            password='password')

    @mock.patch.object(pysftp, 'Connection')
    @mock.patch.object(SftpHook, 'get_connection')
    def test_get_conn_key(self, conn_mock, pysftp_mock):
        """Tests get_conn with a private key."""

        conn_mock.return_value = mock.Mock(
            host='example',
            login='user',
            password=None,
            extra_dejson={'private_key': 'id_rsa'})

        with SftpHook(conn_id='sftp_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('sftp_default')

        pysftp_mock.assert_called_once_with(
            'example',
            username='user',
            private_key='id_rsa')

    @mock.patch.object(pysftp, 'Connection')
    @mock.patch.object(SftpHook, 'get_connection')
    def test_get_conn_key_pass(self, conn_mock, pysftp_mock):
        """Tests get_conn with a private key + password."""

        conn_mock.return_value = mock.Mock(
            host='example',
            login='user',
            password='key_pass',
            extra_dejson={'private_key': 'id_rsa'})

        with SftpHook(conn_id='sftp_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('sftp_default')

        pysftp_mock.assert_called_once_with(
            'example',
            username='user',
            private_key='id_rsa',
            private_key_pass='key_pass')

    def test_open(self):
        """Tests the `open` method."""

        with self._mocked_hook as hook:
            hook.open('test.txt', mode='rb')

        self._mock_fs.open.assert_called_once_with('test.txt', mode='rb')

    def test_exists(self):
        """Tests the `exists` method."""

        with self._mocked_hook as hook:
            hook.exists('test.txt')

        self._mock_fs.exists.assert_called_once_with('test.txt')

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

        self._mock_fs.mkdir.assert_called_once_with('path/to/dir', mode=755)

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

    def test_makedirs(self):
        """Tests the `makedirs` method with a non-existing dir."""

        self._mock_fs.exists.return_value = False

        with self._mocked_hook as hook:
            hook.makedirs('path/to/dir', mode=0o755)

        self._mock_fs.makedirs.assert_called_once_with(
            'path/to/dir', mode=755)

    def test_makedirs_existing(self):
        """Tests the `makedirs` method with an existing dir
           and exist_ok = False.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            with self.assertRaises(IOError):
                hook.makedirs('path/to/dir', mode=0o755, exist_ok=False)

    def test_makedirs_existing_ok(self):
        """Tests the `makedir` method with an existing dir
           and exist_ok = True.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            hook.makedirs('path/to/dir', mode=0o755, exist_ok=True)

    # def test_glob(self):
    #     """Tests the `glob` method."""

    #     with self._mocked_hook as hook:
    #         hook.glob('*.txt')

    #     self._mock_fs.glob.assert_called_once_with('*.txt')

    def test_rm(self):
        """Tests the `rm` method."""

        with self._mocked_hook as hook:
            hook.rm('test_file')

        self._mock_fs.remove.assert_called_once_with('test_file')

    def test_rmtree(self):
        """Tests the `rmtree` method."""

        self._mock_fs.execute.return_value = None

        with self._mocked_hook as hook:
            hook.rmtree('test_dir')

        self._mock_fs.execute.assert_called_once_with("rm -r 'test_dir'")


if __name__ == '__main__':
    unittest.main()
