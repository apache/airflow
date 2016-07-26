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

from __future__ import division
from future import standard_library
standard_library.install_aliases()
import logging
import re
import fnmatch
import configparser
import math
import os
from urllib.parse import urlparse
import warnings

import boto
from boto.s3.connection import S3Connection
from boto.sts import STSConnection
boto.set_stream_logger('boto')
logging.getLogger("boto").setLevel(logging.INFO)

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


def _parse_s3_config(config_file_name, config_format='boto', profile=None):
    """
    Parses a config file for s3 credentials. Can currently
    parse boto, s3cmd.conf and AWS SDK config formats

    :param config_file_name: path to the config file
    :type config_file_name: str
    :param config_format: config type. One of "boto", "s3cmd" or "aws".
        Defaults to "boto"
    :type config_format: str
    :param profile: profile name in AWS type config file
    :type profile: str
    """
    Config = configparser.ConfigParser()
    if Config.read(config_file_name):  # pragma: no cover
        sections = Config.sections()
    else:
        raise AirflowException("Couldn't read {0}".format(config_file_name))
    # Setting option names depending on file format
    if config_format is None:
        config_format = 'boto'
    conf_format = config_format.lower()
    if conf_format == 'boto':  # pragma: no cover
        if profile is not None and 'profile ' + profile in sections:
            cred_section = 'profile ' + profile
        else:
            cred_section = 'Credentials'
    elif conf_format == 'aws' and profile is not None:
        cred_section = profile
    else:
        cred_section = 'default'
    # Option names
    if conf_format in ('boto', 'aws'):  # pragma: no cover
        key_id_option = 'aws_access_key_id'
        secret_key_option = 'aws_secret_access_key'
        # security_token_option = 'aws_security_token'
    else:
        key_id_option = 'access_key'
        secret_key_option = 'secret_key'
    # Actual Parsing
    if cred_section not in sections:
        raise AirflowException("This config file format is not recognized")
    else:
        try:
            access_key = Config.get(cred_section, key_id_option)
            secret_key = Config.get(cred_section, secret_key_option)
            calling_format = None
            if Config.has_option(cred_section, 'calling_format'):
                calling_format = Config.get(cred_section, 'calling_format')
        except:
            logging.warning("Option Error in parsing s3 config file")
            raise
        return (access_key, secret_key, calling_format)


class S3Hook(BaseHook):
    """
    Interact with S3. This class is a wrapper around the boto library.
    """
    def __init__(
            self,
            s3_conn_id='s3_default'):
        self.s3_conn_id = s3_conn_id
        self.s3_conn = self.get_connection(s3_conn_id)
        self.extra_params = self.s3_conn.extra_dejson
        self.profile = self.extra_params.get('profile')
        self.calling_format = None
        self._creds_in_conn = 'aws_secret_access_key' in self.extra_params
        self._creds_in_config_file = 's3_config_file' in self.extra_params
        self._default_to_boto = False
        if self._creds_in_conn:
            self._a_key = self.extra_params['aws_access_key_id']
            self._s_key = self.extra_params['aws_secret_access_key']
            if 'calling_format' in self.extra_params:
                self.calling_format = self.extra_params['calling_format']
        elif self._creds_in_config_file:
            self.s3_config_file = self.extra_params['s3_config_file']
            # The format can be None and will default to boto in the parser
            self.s3_config_format = self.extra_params.get('s3_config_format')
        else:
            self._default_to_boto = True
        # STS support for cross account resource access
        self._sts_conn_required = ('aws_account_id' in self.extra_params or
                                   'role_arn' in self.extra_params)
        if self._sts_conn_required:
            self.role_arn = (self.extra_params.get('role_arn') or
                             "arn:aws:iam::" +
                             self.extra_params['aws_account_id'] +
                             ":role/" +
                             self.extra_params['aws_iam_role'])
        self.connection = self.get_conn()

    def __getstate__(self):
        pickled_dict = dict(self.__dict__)
        del pickled_dict['connection']
        return pickled_dict

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.__dict__['connection'] = self.get_conn()

    def _parse_s3_url(self, s3url):
        warnings.warn(
            'Please note: S3Hook._parse_s3_url() is now '
            'S3Hook.parse_s3_url() (no leading underscore).',
            DeprecationWarning)
        return self.parse_s3_url(s3url)

    @staticmethod
    def parse_s3_url(s3url):
        parsed_url = urlparse(s3url)
        if not parsed_url.netloc:
            raise AirflowException('Please provide a bucket_name')
        else:
            bucket_name = parsed_url.netloc
            key = parsed_url.path.strip('/')
            return (bucket_name, key)

    def get_conn(self):
        """
        Returns the boto S3Connection object.
        """
        if self._default_to_boto:
            return S3Connection(profile_name=self.profile)
        a_key = s_key = None
        if self._creds_in_config_file:
            a_key, s_key, calling_format = _parse_s3_config(self.s3_config_file,
                                                self.s3_config_format,
                                                self.profile)
        elif self._creds_in_conn:
            a_key = self._a_key
            s_key = self._s_key
            calling_format = self.calling_format

        if calling_format is None:
            calling_format = 'boto.s3.connection.SubdomainCallingFormat'

        if self._sts_conn_required:
            sts_connection = STSConnection(aws_access_key_id=a_key,
                                           aws_secret_access_key=s_key,
                                           profile_name=self.profile)
            assumed_role_object = sts_connection.assume_role(
                role_arn=self.role_arn,
                role_session_name="Airflow_" + self.s3_conn_id
                )
            creds = assumed_role_object.credentials
            connection = S3Connection(
                aws_access_key_id=creds.access_key,
                aws_secret_access_key=creds.secret_key,
                calling_format=calling_format,
                security_token=creds.session_token
                )
        else:
            connection = S3Connection(aws_access_key_id=a_key,
                                      aws_secret_access_key=s_key,
                                      calling_format=calling_format,
                                      profile_name=self.profile)
        return connection

    def check_for_bucket(self, bucket_name):
        """
        Check if bucket_name exists.

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        """
        return self.connection.lookup(bucket_name) is not None

    def get_bucket(self, bucket_name):
        """
        Returns a boto.s3.bucket.Bucket object

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        """
        return self.connection.get_bucket(bucket_name)

    def list_keys(self, bucket_name, prefix='', delimiter=''):
        """
        Lists keys in a bucket under prefix and not containing delimiter

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        """
        b = self.get_bucket(bucket_name)
        keylist = list(b.list(prefix=prefix, delimiter=delimiter))
        return [k.name for k in keylist] if keylist != [] else None

    def list_prefixes(self, bucket_name, prefix='', delimiter=''):
        """
        Lists prefixes in a bucket under prefix

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        """
        b = self.get_bucket(bucket_name)
        plist = b.list(prefix=prefix, delimiter=delimiter)
        prefix_names = [p.name for p in plist
                        if isinstance(p, boto.s3.prefix.Prefix)]
        return prefix_names if prefix_names != [] else None

    def check_for_key(self, key, bucket_name=None):
        """
        Checks that a key exists in a bucket
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)
        bucket = self.get_bucket(bucket_name)
        return bucket.get_key(key) is not None

    def get_key(self, key, bucket_name=None):
        """
        Returns a boto.s3.key.Key object

        :param key: the path to the key
        :type key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)
        bucket = self.get_bucket(bucket_name)
        return bucket.get_key(key)

    def check_for_wildcard_key(self,
                               wildcard_key, bucket_name=None, delimiter=''):
        """
        Checks that a key matching a wildcard expression exists in a bucket
        """
        return self.get_wildcard_key(wildcard_key=wildcard_key,
                                     bucket_name=bucket_name,
                                     delimiter=delimiter) is not None

    def get_wildcard_key(self, wildcard_key, bucket_name=None, delimiter=''):
        """
        Returns a boto.s3.key.Key object matching the regular expression

        :param regex_key: the path to the key
        :type regex_key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        """
        if not bucket_name:
            (bucket_name, wildcard_key) = self.parse_s3_url(wildcard_key)
        bucket = self.get_bucket(bucket_name)
        prefix = re.split(r'[*]', wildcard_key, 1)[0]
        klist = self.list_keys(bucket_name, prefix=prefix, delimiter=delimiter)
        if not klist:
            return None
        key_matches = [k for k in klist if fnmatch.fnmatch(k, wildcard_key)]
        return bucket.get_key(key_matches[0]) if key_matches else None

    def check_for_prefix(self, bucket_name, prefix, delimiter):
        """
        Checks that a prefix exists in a bucket
        """
        prefix = prefix + delimiter if prefix[-1] != delimiter else prefix
        prefix_split = re.split(r'(\w+[{d}])$'.format(d=delimiter), prefix, 1)
        previous_level = prefix_split[0]
        plist = self.list_prefixes(bucket_name, previous_level, delimiter)
        return False if plist is None else prefix in plist

    def load_file(
            self,
            filename,
            key,
            bucket_name=None,
            replace=False,
            multipart_bytes=5 * (1024 ** 3)):
        """
        Loads a local file to S3

        :param filename: name of the file to load.
        :type filename: str
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
        :type replace: bool
        :param multipart_bytes: If provided, the file is uploaded in parts of
            this size (minimum 5242880). The default value is 5GB, since S3
            cannot accept non-multipart uploads for files larger than 5GB. If
            the file is smaller than the specified limit, the option will be
            ignored.
        :type multipart_bytes: int
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)
        bucket = self.get_bucket(bucket_name)
        key_obj = bucket.get_key(key)
        if not replace and key_obj:
            raise ValueError("The key {key} already exists.".format(
                **locals()))

        key_size = os.path.getsize(filename)
        if multipart_bytes and key_size >= multipart_bytes:
            # multipart upload
            from filechunkio import FileChunkIO
            mp = bucket.initiate_multipart_upload(key_name=key)
            total_chunks = int(math.ceil(key_size / multipart_bytes))
            sent_bytes = 0
            try:
                for chunk in range(total_chunks):
                    offset = chunk * multipart_bytes
                    bytes = min(multipart_bytes, key_size - offset)
                    with FileChunkIO(
                            filename, 'r', offset=offset, bytes=bytes) as fp:
                        logging.info('Sending chunk {c} of {tc}...'.format(
                            c=chunk + 1, tc=total_chunks))
                        mp.upload_part_from_file(fp, part_num=chunk + 1)
            except:
                mp.cancel_upload()
                raise
            mp.complete_upload()
        else:
            # regular upload
            if not key_obj:
                key_obj = bucket.new_key(key_name=key)
            key_size = key_obj.set_contents_from_filename(filename,
                                                      replace=replace)
        logging.info("The key {key} now contains"
                     " {key_size} bytes".format(**locals()))

    def load_string(self, string_data,
                    key, bucket_name=None,
                    replace=False,
                    encrypt=False):
        """
        Loads a local file to S3

        This is provided as a convenience to drop a file in S3. It uses the
        boto infrastructure to ship a file to s3. It is currently using only
        a single part download, and should not be used to move large files.

        :param string_data: string to set as content for the key.
        :type string_data: str
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists
        :type replace: bool
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)
        bucket = self.get_bucket(bucket_name)
        key_obj = bucket.get_key(key)
        if not replace and key_obj:
            raise ValueError("The key {key} already exists.".format(
                **locals()))
        if not key_obj:
            key_obj = bucket.new_key(key_name=key)
        key_size = key_obj.set_contents_from_string(string_data,
                                                    replace=replace,
                                                    encrypt_key=encrypt)
        logging.info("The key {key} now contains"
                     " {key_size} bytes".format(**locals()))
