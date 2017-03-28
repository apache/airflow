# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None
import logging
from itertools import product
from airflow.operators.s3_to_hive_operator import S3ToHiveTransfer
from collections import OrderedDict
from airflow.exceptions import AirflowException
from tempfile import NamedTemporaryFile, mkdtemp
import gzip
import bz2
import shutil
import filecmp
import errno


class S3ToHiveTransferTest(unittest.TestCase):

    def setUp(self):
        self.fn = {}
        self.task_id = 'S3ToHiveTransferTest'
        self.s3_key = 'S32hive_test_file'
        self.field_dict = OrderedDict([('Sno', 'BIGINT'), ('Some,Text', 'STRING')])
        self.hive_table = 'S32hive_test_table'
        self.delimiter = '\t'
        self.create = True
        self.recreate = True
        self.partition = {'ds': 'STRING'}
        self.headers = True
        self.check_headers = True
        self.wildcard_match = False
        self.input_compressed = False
        self.kwargs = {'task_id': self.task_id,
                       's3_key': self.s3_key,
                       'field_dict': self.field_dict,
                       'hive_table': self.hive_table,
                       'delimiter': self.delimiter,
                       'create': self.create,
                       'recreate': self.recreate,
                       'partition': self.partition,
                       'headers': self.headers,
                       'check_headers': self.check_headers,
                       'wildcard_match': self.wildcard_match,
                       'input_compressed': self.input_compressed
                       }
        try:
            header = "Sno\tSome,Text \n".encode()
            line1 = "1\tAirflow Test\n".encode()
            line2 = "2\tS32HiveTransfer\n".encode()
            self.tmp_dir = mkdtemp(prefix='test_tmps32hive_')
            # create sample txt, gz and bz2 with and without headers
            with NamedTemporaryFile(mode='wb+',
                                    dir=self.tmp_dir,
                                    delete=False) as f_txt_h:
                self._set_fn(f_txt_h.name, '.txt', True)
                f_txt_h.writelines([header, line1, line2])
            fn_gz = self._get_fn('.txt', True) + ".gz"
            with gzip.GzipFile(filename=fn_gz,
                               mode="wb") as f_gz_h:
                self._set_fn(fn_gz, '.gz', True)
                f_gz_h.writelines([header, line1, line2])
            fn_bz2 = self._get_fn('.txt', True) + '.bz2'
            with bz2.BZ2File(filename=fn_bz2,
                             mode="wb") as f_bz2_h:
                self._set_fn(fn_bz2, '.bz2', True)
                f_bz2_h.writelines([header, line1, line2])
            # create sample txt, bz and bz2 without header
            with NamedTemporaryFile(mode='wb+',
                                    dir=self.tmp_dir,
                                    delete=False) as f_txt_nh:
                self._set_fn(f_txt_nh.name, '.txt', False)
                f_txt_nh.writelines([line1, line2])
            fn_gz = self._get_fn('.txt', False) + ".gz"
            with gzip.GzipFile(filename=fn_gz,
                               mode="wb") as f_gz_nh:
                self._set_fn(fn_gz, '.gz', False)
                f_gz_nh.writelines([line1, line2])
            fn_bz2 = self._get_fn('.txt', False) + '.bz2'
            with bz2.BZ2File(filename=fn_bz2,
                             mode="wb") as f_bz2_nh:
                self._set_fn(fn_bz2, '.bz2', False)
                f_bz2_nh.writelines([line1, line2])
        # Base Exception so it catches Keyboard Interrupt
        except BaseException as e:
            logging.error(e)
            self.tearDown()

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e

    # Helper method to create a dictionary of file names and
    # file types (file extension and header)
    def _set_fn(self, fn, ext, header):
        key = self._get_key(ext, header)
        self.fn[key] = fn

    # Helper method to fetch a file of a
    # certain format (file extension and header)
    def _get_fn(self, ext, header):
        key = self._get_key(ext, header)
        return self.fn[key]

    def _get_key(self, ext, header):
        key = ext + "_" + ('h' if header else 'nh')
        return key

    def _cp_file_contents(self, fn_src, fn_dest):
        with open(fn_src, 'rb') as f_src, open(fn_dest, 'wb') as f_dest:
            shutil.copyfileobj(f_src, f_dest)

    def _check_file_equality(self, fn_1, fn_2, ext):
        # gz files contain mtime and filename in the header that
        # causes filecmp to return False even if contents are identical
        # Hence decompress to test for equality
        if(ext == '.gz'):
            with gzip.GzipFile(fn_1, 'rb') as f_1,\
                 NamedTemporaryFile(mode='wb') as f_txt_1,\
                 gzip.GzipFile(fn_2, 'rb') as f_2,\
                 NamedTemporaryFile(mode='wb') as f_txt_2:
                shutil.copyfileobj(f_1, f_txt_1)
                shutil.copyfileobj(f_2, f_txt_2)
                f_txt_1.flush()
                f_txt_2.flush()
                return filecmp.cmp(f_txt_1.name, f_txt_2.name, shallow=False)
        else:
            return filecmp.cmp(fn_1, fn_2, shallow=False)

    def test_bad_parameters(self):
        self.kwargs['check_headers'] = True
        self.kwargs['headers'] = False
        self.assertRaisesRegexp(AirflowException,
                                "To check_headers.*",
                                S3ToHiveTransfer,
                                **self.kwargs)

    def test__get_top_row_as_list(self):
        self.kwargs['delimiter'] = '\t'
        fn_txt = self._get_fn('.txt', True)
        header_list = S3ToHiveTransfer(**self.kwargs).\
            _get_top_row_as_list(fn_txt)
        self.assertEqual(header_list, ['Sno', 'Some,Text'],
                         msg="Top row from file doesnt matched expected value")

        self.kwargs['delimiter'] = ','
        header_list = S3ToHiveTransfer(**self.kwargs).\
            _get_top_row_as_list(fn_txt)
        self.assertEqual(header_list, ['Sno\tSome', 'Text'],
                         msg="Top row from file doesnt matched expected value")

    def test__match_headers(self):
        self.kwargs['field_dict'] = OrderedDict([('Sno', 'BIGINT'),
                                                ('Some,Text', 'STRING')])
        self.assertTrue(S3ToHiveTransfer(**self.kwargs).
                        _match_headers(['Sno', 'Some,Text']),
                        msg="Header row doesnt match expected value")
        # Testing with different column order
        self.assertFalse(S3ToHiveTransfer(**self.kwargs).
                         _match_headers(['Some,Text', 'Sno']),
                         msg="Header row doesnt match expected value")
        # Testing with extra column in header
        self.assertFalse(S3ToHiveTransfer(**self.kwargs).
                         _match_headers(['Sno', 'Some,Text', 'ExtraColumn']),
                         msg="Header row doesnt match expected value")

    def test__delete_top_row_and_compress(self):
        s32hive = S3ToHiveTransfer(**self.kwargs)
        # Testing gz file type
        fn_txt = self._get_fn('.txt', True)
        gz_txt_nh = s32hive._delete_top_row_and_compress(fn_txt,
                                                         '.gz',
                                                         self.tmp_dir)
        fn_gz = self._get_fn('.gz', False)
        self.assertTrue(self._check_file_equality(gz_txt_nh, fn_gz, '.gz'),
                        msg="gz Compressed file not as expected")
        # Testing bz2 file type
        bz2_txt_nh = s32hive._delete_top_row_and_compress(fn_txt,
                                                          '.bz2',
                                                          self.tmp_dir)
        fn_bz2 = self._get_fn('.bz2', False)
        self.assertTrue(self._check_file_equality(bz2_txt_nh, fn_bz2, '.bz2'),
                        msg="bz2 Compressed file not as expected")

    @unittest.skipIf(mock is None, 'mock package not present')
    @mock.patch('airflow.operators.s3_to_hive_operator.HiveCliHook')
    @mock.patch('airflow.operators.s3_to_hive_operator.S3Hook')
    def test_execute(self, mock_s3hook, mock_hiveclihook):
        # Testing txt, zip, bz2 files with and without header row
        for test in product(['.txt', '.gz', '.bz2'], [True, False]):
            ext = test[0]
            has_header = test[1]
            self.kwargs['headers'] = has_header
            self.kwargs['check_headers'] = has_header
            logging.info("Testing {0} format {1} header".
                         format(ext,
                                ('with' if has_header else 'without'))
                         )
            self.kwargs['input_compressed'] = (False if ext == '.txt' else True)
            self.kwargs['s3_key'] = self.s3_key + ext
            ip_fn = self._get_fn(ext, self.kwargs['headers'])
            op_fn = self._get_fn(ext, False)
            # Mock s3 object returned by S3Hook
            mock_s3_object = mock.Mock(key=self.kwargs['s3_key'])
            mock_s3_object.get_contents_to_file.side_effect = \
                lambda dest_file: \
                self._cp_file_contents(ip_fn, dest_file.name)
            mock_s3hook().get_key.return_value = mock_s3_object
            # file paramter to HiveCliHook.load_file is compared
            # against expected file oputput
            mock_hiveclihook().load_file.side_effect = \
                lambda *args, **kwargs: \
                self.assertTrue(
                    self._check_file_equality(args[0],
                                              op_fn,
                                              ext
                                              ),
                    msg='{0} output file not as expected'.format(ext))
            # Execute S3ToHiveTransfer
            s32hive = S3ToHiveTransfer(**self.kwargs)
            s32hive.execute(None)


if __name__ == '__main__':
    unittest.main()
