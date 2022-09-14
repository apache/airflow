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

import bz2
import errno
import filecmp
import gzip
import shutil
import tempfile

import pytest

from airflow.utils import compression


class TestCompression:
    @pytest.fixture(autouse=True)
    def setup_attrs(self):
        self.file_names = {}
        header = b"Sno\tSome,Text \n"
        line1 = b"1\tAirflow Test\n"
        line2 = b"2\tCompressionUtil\n"
        self.tmp_dir = tempfile.mkdtemp(prefix='test_utils_compression_')
        # create sample txt, gz and bz2 files
        with tempfile.NamedTemporaryFile(mode='wb+', dir=self.tmp_dir, delete=False) as f_txt:
            self._set_fn(f_txt.name, '.txt')
            f_txt.writelines([header, line1, line2])

        fn_gz = self._get_fn('.txt') + ".gz"
        with gzip.GzipFile(filename=fn_gz, mode="wb") as f_gz:
            self._set_fn(fn_gz, '.gz')
            f_gz.writelines([header, line1, line2])

        fn_bz2 = self._get_fn('.txt') + '.bz2'
        with bz2.BZ2File(filename=fn_bz2, mode="wb") as f_bz2:
            self._set_fn(fn_bz2, '.bz2')
            f_bz2.writelines([header, line1, line2])

        yield

        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e

    # Helper method to create a dictionary of file names and
    # file extension
    def _set_fn(self, fn, ext):
        self.file_names[ext] = fn

    # Helper method to fetch a file of a
    # certain extension
    def _get_fn(self, ext):
        return self.file_names[ext]

    def test_uncompress_file(self):
        # Testing txt file type
        with pytest.raises(NotImplementedError, match="^Received .txt format. Only gz and bz2.*"):
            compression.uncompress_file(
                **{'input_file_name': None, 'file_extension': '.txt', 'dest_dir': None},
            )
        # Testing gz file type
        fn_txt = self._get_fn('.txt')
        fn_gz = self._get_fn('.gz')
        txt_gz = compression.uncompress_file(fn_gz, '.gz', self.tmp_dir)
        assert filecmp.cmp(txt_gz, fn_txt, shallow=False), "Uncompressed file doest match original"
        # Testing bz2 file type
        fn_bz2 = self._get_fn('.bz2')
        txt_bz2 = compression.uncompress_file(fn_bz2, '.bz2', self.tmp_dir)
        assert filecmp.cmp(txt_bz2, fn_txt, shallow=False), "Uncompressed file doest match original"
