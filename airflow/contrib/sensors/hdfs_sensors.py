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
from airflow.operators.sensors import HdfsSensor
import logging


class HdfsSensorRegex(HdfsSensor):
    def __init__(
            self,
            regex,
            *args, **kwargs):
        super(HdfsSensorRegex, self).__init__(*args, **kwargs)
        self.regex = regex

    def poke(self, context):
        """
        poke matching files in a directory with self.regex
        :return: Bool depending on the search criteria
        """
        sb = self.hook(self.hdfs_conn_id).get_conn()
        logging.getLogger("snakebite").setLevel(logging.WARNING)
        logging.info(
            'Poking for {self.filepath} to be a directory with files matching {self.regex.pattern}'.format(**locals()))
        result = [f for f in sb.ls([self.filepath], include_toplevel=False) if
                  f['file_type'] == 'f' and self.regex.match(f['path'].replace('%s/' % self.filepath, ''))]
        result = self.filter_for_ignored_ext(result, self.ignored_ext, self.ignore_copying)
        result = self.filter_for_filesize(result, self.file_size)
        return bool(result)


class HdfsSensorFolder(HdfsSensor):
    def __init__(
            self,
            be_empty=False,
            *args, **kwargs):
        super(HdfsSensorFolder, self).__init__(*args, **kwargs)
        self.be_empty = be_empty

    def poke(self, context):
        """
        poke for a non empty directory
        :return: Bool depending on the search criteria
        """
        sb = self.hook(self.hdfs_conn_id).get_conn()
        logging.getLogger("snakebite").setLevel(logging.WARNING)
        result = [f for f in sb.ls([self.filepath], include_toplevel=True)]
        result = self.filter_for_ignored_ext(result, self.ignored_ext, self.ignore_copying)
        result = self.filter_for_filesize(result, self.file_size)
        if self.be_empty:
            logging.info('Poking for filepath {self.filepath} to a empty directory'.format(**locals()))
            return len(result) == 1 and result[0]['path'] == self.filepath
        else:
            logging.info('Poking for filepath {self.filepath} to a non empty directory'.format(**locals()))
            result.pop(0)
            return bool(result) and result[0]['file_type'] == 'f'


