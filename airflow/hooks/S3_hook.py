import logging
import json
import re
import fnmatch
import ConfigParser
from urlparse import urlparse

import boto
from boto.s3.connection import S3Connection
from boto.sts import STSConnection
boto.set_stream_logger('boto')
logging.getLogger("boto").setLevel(logging.INFO)

from airflow.utils import AirflowException
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
    Config = ConfigParser.ConfigParser()
    if Config.read(config_file_name):
        sections = Config.sections()
    else:
        raise AirflowException("Couldn't read {0}".format(config_file_name))
    # Setting option names depending on file format
    conf_format = config_format.lower()
    if conf_format == 'boto':
        if profile is not None and 'profile ' + profile in sections:
            cred_section = 'profile ' + profile
        else:
            cred_section = 'Credentials'
    elif conf_format == 'aws' and profile is not None:
        cred_section = profile
    else:
        cred_section = 'default'
    # Option names
    if conf_format in ('boto', 'aws'):
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
        except:
            logging.warning("Option Error in parsing s3 config file")
            raise
        return (access_key, secret_key)


class S3Hook(BaseHook):
    """
    Interact with S3. This class is a wrapper around the boto library.
    """
    def __init__(
            self,
            s3_conn_id='s3_default'):
        self.s3_conn_id = s3_conn_id
        self.s3_conn = self.get_connection(s3_conn_id)
        self.profile = None
        self._sts_conn_required = False
        self._creds_in_config_file = False
        try:
            self.extra_params = json.loads(self.s3_conn.extra)
            if 'aws_secret_access_key' in self.extra_params:
                self._a_key = self.extra_params['aws_access_key_id']
                self._s_key = self.extra_params['aws_secret_access_key']
            else:
                self._creds_in_config_file = True
                self.s3_config_format = self.extra_params['s3_config_format']
                self.s3_config_file = self.extra_params['s3_config_file']
            if 'profile' in self.extra_params:
                self.profile = self.extra_params['profile']
            self._sts_conn_required = 'aws_account_id' in self.extra_params
            if self._sts_conn_required:
                self.aws_account_id = self.extra_params['aws_account_id']
                self.aws_iam_role = self.extra_params['aws_iam_role']
                self.role_arn = "arn:aws:iam::" + self.aws_account_id
                self.role_arn += ":role/" + self.aws_iam_role
        except TypeError as e:
            raise AirflowException("S3 connection needs to set configuration"
                            " parameters in extra")
        except KeyError as e:
            raise AirflowException("S3 connection definition needs to include"
                            " {p} in extra".format(p=e.message))
        self.connection = self.get_conn()

    def __getstate__(self):
        pickled_dict = dict(self.__dict__)
        del pickled_dict['connection']
        return pickled_dict

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.__dict__['connection'] = self.get_conn()

    def _parse_s3_url(self, s3url):
        parsed_url = urlparse(s3url)
        if not parsed_url.netloc:
            raise AirflowException('Please provide a bucket_name')
        else:
            bucket_name = parsed_url.netloc
            if parsed_url.path[0] == '/':
                key = parsed_url.path[1:]
            else:
                key = parsed_url.path
            return (bucket_name, key)

    def get_conn(self):
        """
        Returns the boto S3Connection object.
        """
        if self._creds_in_config_file:
            a_key, s_key = _parse_s3_config(self.s3_config_file,
                                            self.s3_config_format,
                                            self.profile)
        else:
            a_key = self._a_key
            s_key = self._s_key
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
                security_token=creds.session_token
                )
        else:
            connection = S3Connection(aws_access_key_id=a_key,
                                      aws_secret_access_key=s_key)
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
            (bucket_name, key) = self._parse_s3_url(key)
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
            (bucket_name, key) = self._parse_s3_url(key)
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
            (bucket_name, wildcard_key) = self._parse_s3_url(wildcard_key)
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

    def load_file(self, filename,
                  key, bucket_name=None,
                  replace=False):
        """
        Loads a local file to S3

        This is provided as a convenience to drop a file in S3. It uses the
        boto infrastructure to ship a file to s3. It is currently using only
        a single part download, and should not be used to move large files.

        :param filename: name of the file to load.
        :type filename: str
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists
        :type replace: bool
        """
        if not bucket_name:
            (bucket_name, key) = self._parse_s3_url(key)
        bucket = self.get_bucket(bucket_name)
        if not self.check_for_key(key, bucket_name):
            key_obj = bucket.new_key(key_name=key)
        else:
            key_obj = bucket.get_key(key)
        key_size = key_obj.set_contents_from_filename(filename,
                                                      replace=replace)
        logging.info("The key {key} now contains"
                     " {key_size} bytes".format(**locals()))
