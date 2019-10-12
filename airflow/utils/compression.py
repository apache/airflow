# -*- coding: utf-8 -*-
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

import bz2
import gzip
import shutil
from tempfile import NamedTemporaryFile


def uncompress_file(input_file_name, file_extension, dest_dir):
    """
    Uncompress gz and bz2 files
    """
    if file_extension.lower() not in ('.gz', '.bz2'):
        raise NotImplementedError("Received {} format. Only gz and bz2 "
                                  "files can currently be uncompressed."
                                  .format(file_extension))
    if file_extension.lower() == '.gz':
        fmodule = gzip.GzipFile
    elif file_extension.lower() == '.bz2':
        fmodule = bz2.BZ2File
    with fmodule(input_file_name, mode='rb') as f_compressed,\
        NamedTemporaryFile(dir=dest_dir,
                           mode='wb',
                           delete=False) as f_uncompressed:
        shutil.copyfileobj(f_compressed, f_uncompressed)
    return f_uncompressed.name
