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
#
import logging
from csv import DictReader
from datetime import datetime
from tempfile import NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.file import TemporaryDirectory
from airflow.utils.helpers import popen_and_tail


class HDFSCliHook(BaseHook):
    '''
    HDFSCliHook is a wrap around "hadoop fs" cli tool
    '''
    def __init__(self, hdfs_cli_conn_id='hdfs_cli_default'):
        self.hdfs_cli_conn_id = hdfs_cli_conn_id
        self.conn = self.get_connection(hdfs_cli_conn_id)
        self.binary = self.conn.extra_dejson.get('binary', 'hadoop fs').split(' ')
        self.fallback_conn_id = \
            self.conn.extra_dejson.get('fallback_conn_id', 'hdfs_default')

    def run_cli(self, arguments, verbose=True):
        hdfs_cmd = self.binary + arguments
        logging.info('HDFS command "%s"', ' '.join(hdfs_cmd))
        self.sp, tail = popen_and_tail(hdfs_cmd, shell=True)

        stdout = ''
        stderr = ''
        for tag, chunk in tail():
            if verbose and tag == 2 and chunk is not None:
                logging.warn(chunk)
            elif tag == 0 and chunk is not None and chunk != 0:
                raise AirflowException(stdout, stderr, chunk)

            if tag == 1 and chunk is not None:
                stdout += chunk
                yield chunk
            elif tag == 2 and chunk is not None:
                stderr += chunk

    def cat(self, path, ignore_crc=False, verbose=True):
        arguments = ['-cat']
        if ignore_crc:
            arguments.append('-ignoreCrc')
        arguments.append(path)
        return self.run_cli(arguments, verbose)

    def checksum(self, path, verbose=True):
        arguments = ['-checksum', path]
        return self.run_cli(arguments, verbose)

    def chgrp(self, group, path, recursive=False, verbose=True):
        arguments = ['-chgrp']
        if recursive:
            arguments.append('-R')
        arguments += [group, path]
        return self.run_cli(arguments, verbose)

    def chmod(self, mode, path, recursive=False, verbose=True):
        arguments = ['-chmod']
        if recursive:
            arguments.append('-R')
        arguments += [mode, path]
        return self.run_cli(arguments, verbose)

    def chown(self, owner, path, recursive=False, verbose=True):
        arguments = ['-chown']
        if recursive:
            arguments.append('-R')
        arguments += [owner, path]
        return self.run_cli(arguments, verbose)

    def text(self, path, verbose=True):
        arguments = ['-text', path]
        return self.run_cli(arguments, verbose)

    def copy_from_local(self, local_src, dst, force=False, verbose=True):
        arguments = ['-copyFromLocal']
        if force:
            arguments.append('-f')
        arguments += [local_src, dst]
        return self.run_cli(arguments, verbose)

    def write_text(self, text, dst, force=False, verbose=True):
        with TemporaryDirectory(prefix='airflow_hdfsop_') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir) as f:
                f.write(text.encode('UTF-8'))
                f.flush()
                fname = f.name
                return list(self.copy_from_local(fname, dst, force, verbose))

    def append_to_file(self, local_src, dst, verbose=True):
        arguments = ['-appendToFile', local_src, dst]
        return self.run_cli(arguments, verbose)

    def append_text(self, text, dst, verbose=True):
        with TemporaryDirectory(prefix='airflow_hdfsop_') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir) as f:
                f.write(text.encode('UTF-8'))
                f.flush()
                fname = f.name
                return list(self.append_to_file(fname, dst, verbose))

    def ls(self, path, directory=False, recursive=False, verbose=True):
        '''
        list files for pattern <path>
        username or filename containing space might cause line parsing failed
        '''
        def parse_file_info(file_info_raw):
            logging.info('parsing %s', repr(file_info_raw))
            file_info_raw['replica'] = \
                -1 if file_info_raw['replica'] == '-' \
                else int(file_info_raw['replica'])
            file_info_raw['size'] = int(file_info_raw['size'])
            file_info_raw['date'] = \
                datetime.strptime(file_info_raw['date'], '%Y-%m-%d').date()
            file_info_raw['time'] = \
                datetime.strptime(file_info_raw['time'], '%H:%M').time()
            return file_info_raw

        arguments = ['-ls']
        if directory:
            arguments.append('-d')
        if recursive:
            arguments.append('-R')
        arguments.append(path)

        try:
            file_raw_lines = ''.join(self.run_cli(arguments, verbose)).splitlines()[1:]
        except AirflowException as e:
            if e[1].endswith('ls: `{0}\': No such file or directory\n'.format(path)):
                file_raw_lines = []
            else:
                raise e
        files_raw = DictReader([' '.join(l.split()) for l in file_raw_lines],
                               delimiter=' ',
                               skipinitialspace=True,
                               fieldnames=['permissions', 'replica', 'owner',
                               'group', 'size', 'date', 'time', 'name'])

        for file_info_raw in files_raw:
            yield parse_file_info(file_info_raw)

    def rm(self, path, recursive=False, force=False, skip_trash=False, verbose=True):
        arguments = ['-rm']
        if recursive:
            arguments.append('-r')
        if force:
            arguments.append('-f')
        if skip_trash:
            arguments.append('-skipTrash')
        arguments.append(path)
        return self.run_cli(arguments, verbose)

    def mkdir(self, path, create_intermediate=False, verbose=True):
        arguments = ['-mkdir']
        if create_intermediate:
            arguments.append('-p')
        arguments.append(path)
        return self.run_cli(arguments, verbose)

    def kill(self):
        if hasattr(self, 'sp'):
            if self.sp.poll() is None:
                logging.info("Killing the Hadoop job")
                self.sp.kill()
