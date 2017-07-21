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

import dateutil.parser
import logging
import six

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
            from airflow.hooks.S3_hook import S3Hook
            self.hook = S3Hook(remote_conn_id)
        except:
            self.hook = None
            logging.error(
                'Could not create an S3Hook with connection id "{}". '
                'Please make sure that airflow[s3] is installed and '
                'the S3 connection exists.'.format(remote_conn_id))

    def log_exists(self, remote_log_location):
        """
        Check if remote_log_location exists in remote storage
        :param remote_log_location: log's location in remote storage
        :return: True if location exists else False
        """
        if self.hook:
            try:
                return self.hook.get_key(remote_log_location) is not None
            except Exception:
                pass
        return False

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

    def write(self, log, remote_log_location, append=True):
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
            from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
            self.hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=remote_conn_id)
        except:
            logging.error(
                'Could not create a GoogleCloudStorageHook with connection id '
                '"{}". Please make sure that airflow[gcp_api] is installed '
                'and the GCS connection exists.'.format(remote_conn_id))

    def log_exists(self, remote_log_location):
        """
        Check if remote_log_location exists in remote storage
        :param remote_log_location: log's location in remote storage
        :return: True if location exists else False
        """
        if self.hook:
            try:
                bkt, blob = self.parse_gcs_url(remote_log_location)
                return self.hook.exists(bkt, blob)
            except Exception:
                pass
        return False

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

    def write(self, log, remote_log_location, append=True):
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


# TODO: get_log_filename and get_log_directory are temporary helper
# functions to get airflow log filename. Logic of using FileHandler
# will be extract out and those two functions will be moved.
# For more details, please check issue AIRFLOW-1385.
def get_log_filename(dag_id, task_id, execution_date, try_number):
    """
    Return relative log path.
    :arg dag_id: id of the dag
    :arg task_id: id of the task
    :arg execution_date: execution date of the task instance
    :arg try_number: try_number of current task instance
    """
    relative_dir = get_log_directory(dag_id, task_id, execution_date)
    # For reporting purposes and keeping logs consistent with web UI
    # display, we report based on 1-indexed, not 0-indexed lists
    filename = "{}/{}.log".format(relative_dir, try_number+1)

    return filename


def get_log_directory(dag_id, task_id, execution_date):
    """
    Return log directory path: dag_id/task_id/execution_date
    :arg dag_id: id of the dag
    :arg task_id: id of the task
    :arg execution_date: execution date of the task instance
    """
    # execution_date could be parsed in as unicode character
    # instead of datetime object.
    if isinstance(execution_date, six.string_types):
        execution_date = dateutil.parser.parse(execution_date)
    iso = execution_date.isoformat()
    relative_dir = '{}/{}/{}'.format(dag_id, task_id, iso)

    return relative_dir
