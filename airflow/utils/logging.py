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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from builtins import object

import logging
import re

from airflow import configuration
from airflow.exceptions import AirflowException

class LoggingMixin(object):
    """
    Convenience super-class to have a logger configured with the class name
    """

    @property
    def logger(self):
        try:
            return self._logger
        except AttributeError:
            self._logger = logging.root.getChild(self.__class__.__module__ + '.' + self.__class__.__name__)
            return self._logger


class S3Log(object):
    """
    Utility class for reading and writing logs in S3.
    Requires airflow[s3] and setting the REMOTE_BASE_LOG_FOLDER and
    REMOTE_LOG_CONN_ID configuration options in airflow.cfg.
    """
    def __init__(self):
        remote_conn_id = configuration.get('core', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.hooks import S3Hook
            self.hook = S3Hook(remote_conn_id)
        except:
            self.hook = None
            logging.error(
                'Could not create an S3Hook with connection id "{}". '
                'Please make sure that airflow[s3] is installed and '
                'the S3 connection exists.'.format(remote_conn_id))

    def read(self, remote_log_location, return_error=False):
        """
        Returns the log found at the remote_log_location. Returns '' if no
        logs are found or there is an error.

        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        """
        if self.hook:
            try:
                s3_key = self.hook.get_key(remote_log_location)
                if s3_key:
                    return s3_key.get_contents_as_string().decode()
            except:
                pass

        # raise/return error if we get here
        err = 'Could not read logs from {}'.format(remote_log_location)
        logging.error(err)
        return err if return_error else ''


    def write(self, log, remote_log_location, append=False):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the remote_log_location
        :type log: string
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool

        """
        if self.hook:

            if append:
                old_log = self.read(remote_log_location)
                log = old_log + '\n' + log
            try:
                self.hook.load_string(
                    log,
                    key=remote_log_location,
                    replace=True,
                    encrypt=configuration.getboolean('core', 'ENCRYPT_S3_LOGS'))
                return
            except:
                pass

        # raise/return error if we get here
        logging.error('Could not write logs to {}'.format(remote_log_location))


class GCSLog(object):
    """
    Utility class for reading and writing logs in GCS. Requires
    airflow[gcp_api] and setting the REMOTE_BASE_LOG_FOLDER and
    REMOTE_LOG_CONN_ID configuration options in airflow.cfg.
    """
    def __init__(self):
        """
        Attempt to create hook with airflow[gcp_api].
        """
        remote_conn_id = configuration.get('core', 'REMOTE_LOG_CONN_ID')
        self.hook = None

        try:
            from airflow.contrib.hooks import GoogleCloudStorageHook
            self.hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=remote_conn_id)
        except:
            logging.error(
                'Could not create a GoogleCloudStorageHook with connection id '
                '"{}". Please make sure that airflow[gcp_api] is installed '
                'and the GCS connection exists.'.format(remote_conn_id))

    def read(self, remote_log_location, return_error=False):
        """
        Returns the log found at the remote_log_location.

        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        """
        if self.hook:
            try:
                bkt, blob = self.parse_gcs_url(remote_log_location)
                return self.hook.download(bkt, blob).decode()
            except:
                pass

        # raise/return error if we get here
        err = 'Could not read logs from {}'.format(remote_log_location)
        logging.error(err)
        return err if return_error else ''

    def write(self, log, remote_log_location, append=False):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the remote_log_location
        :type log: string
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool

        """
        if self.hook:
            if append:
                old_log = self.read(remote_log_location)
                log = old_log + '\n' + log

            try:
                bkt, blob = self.parse_gcs_url(remote_log_location)
                from tempfile import NamedTemporaryFile
                with NamedTemporaryFile(mode='w+') as tmpfile:
                    tmpfile.write(log)
                    # Force the file to be flushed, since we're doing the
                    # upload from within the file context (it hasn't been
                    # closed).
                    tmpfile.flush()
                    self.hook.upload(bkt, blob, tmpfile.name)
            except:
                # raise/return error if we get here
                logging.error('Could not write logs to {}'.format(remote_log_location))

    def parse_gcs_url(self, gsurl):
        """
        Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
        tuple containing the corresponding bucket and blob.
        """
        # Python 3
        try:
            from urllib.parse import urlparse
        # Python 2
        except ImportError:
            from urlparse import urlparse

        parsed_url = urlparse(gsurl)
        if not parsed_url.netloc:
            raise AirflowException('Please provide a bucket name')
        else:
            bucket = parsed_url.netloc
            blob = parsed_url.path.strip('/')
            return (bucket, blob)


class HDFSLog(object):
    """
    Utility class for reading and writing logs in HDFS.
    Requires airflow[hdfs] and setting the REMOTE_BASE_LOG_FOLDER and
    REMOTE_LOG_CONN_ID configuration options in airflow.cfg.
    """
    def __init__(self):
        remote_conn_id = configuration.get('core', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.contrib.hooks.hdfs_cli_hook import HDFSCliHook
            from airflow.hooks.hdfs_hook import HDFSHook
            self.hook = HDFSCliHook(remote_conn_id)
            self.fallback_hook = HDFSHook(self.hook.fallback_conn_id).get_conn()
        except Exception:
            self.hook = None
            logging.error(
                'Could not create an HDFSCliHook with connection id "%s". '
                'Please make sure that the HDFS Cli connection '
                'exists.', remote_conn_id)

    def escape_hdfs_path(self, hdfs_path):
        from os.path import dirname, basename
        directory = dirname(hdfs_path)
        filename = basename(hdfs_path)
        if ':' in filename:
            filename = filename.replace(':', '_')
        return directory, filename

    def remove_hdfs_prefix(self, hdfs_path):
        matches = re.match(
            '^hdfs://[a-zA-Z\.0-9-]+(?::[0-9]+)?(/[\S\s]*)$', hdfs_path)
        if matches:
            return matches.group(1)
        else:
            return hdfs_path

    def read(self, remote_log_location, return_error=False):
        """
        Returns the log found at the remote_log_location. Returns '' if no
        logs are found or there is an error.

        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        """
        if self.hook or self.fallback_hook:
            directory, filename = self.escape_hdfs_path(remote_log_location)
            escaped_log_location = '{0}/{1}'.format(directory, filename)

            try:
                return ''.join(self.hook.text(escaped_log_location))
            except AirflowException as e:
                err_msg = ('HDFSCLiHook could not read logs from {}, '
                           'details: {}').format(remote_log_location, e[1])
            except OSError as e:
                err_msg = ('HDFSCLiHook could not read logs from {}, '
                           'details: {}').format(remote_log_location, e.message)
                logging.exception(e)

            # try fallback hook
            try:
                remote_log_location_no_prefix = \
                    self.remove_hdfs_prefix(escaped_log_location)
                return ''.join(self.fallback_hook.text([remote_log_location_no_prefix]))
            except BaseException as e:
                err_msg = 'HDFSHook could not read logs from {}'.format(
                    remote_log_location)
                logging.exception(e)
        else:
            err_msg = ('Could read logs from logs from {}, '
                       'no hdfs hook available').format(remote_log_location)

        logging.error(err_msg)
        return err_msg if return_error else ''

    def write(self, log, remote_log_location, append=False):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the remote_log_location
        :type log: string
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool

        """
        if self.hook:
            directory, filename = self.escape_hdfs_path(remote_log_location)
            escaped_log_location = '{0}/{1}'.format(directory, filename)
            try:
                self.hook.mkdir(directory, create_intermediate=True)
                if append:
                    return self.hook.append_text(log, escaped_log_location, True)
                else:
                    return self.hook.write_text(log, escaped_log_location, True)
            except AirflowException as e:
                err_msg = 'Could not write logs to {}, details: {}'.format(
                    remote_log_location, e.args[1])
                logging.exception(e)
            except BaseException as e:
                err_msg = 'Could not write logs to {}'.format(
                    remote_log_location)
                logging.exception(e)

        # raise/return error if we get here
        logging.error(err_msg)
