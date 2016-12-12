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
import os

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.settings import LOGGING_LEVEL, LOG_FORMAT, SIMPLE_LOG_FORMAT

BASE_LOG_FOLDER = os.path.expanduser(
    configuration.get('core', 'BASE_LOG_FOLDER'))

_log = logging.getLogger(__name__)


class _LoggingController(object):
    """
    Utility class to provide a central management point for controlling the
    base Airflow logging settings.
    """
    def __init__(self):

        if not os.path.exists(BASE_LOG_FOLDER):
            os.makedirs(BASE_LOG_FOLDER)

        self._debug_log_file_location = '{}/debug.log'.format(BASE_LOG_FOLDER)
        self._debug_file_log_handler = logging.FileHandler(
            self._debug_log_file_location)
        self._debug_file_log_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        self._debug_file_log_handler.setLevel(logging.DEBUG)

        self._error_log_file_location = '{}/error.log'.format(BASE_LOG_FOLDER)
        self._error_file_log_handler = logging.FileHandler(
            self._error_log_file_location)
        self._error_file_log_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        self._error_file_log_handler.setLevel(logging.ERROR)

        self._console_log_handler = logging.StreamHandler()
        self._console_log_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        self._console_log_handler.setLevel(LOGGING_LEVEL)

        # Get and store a logger object to use as a base for all other logger
        # objects used within Airflow. This object needs a logging level of
        # debug to allow any handlers to have a logging level of debug.
        self._logger = logging.root.getChild('airflow')
        self._logger.setLevel(logging.DEBUG)

        # TODO: replace with config setting CLEAR_INHERITED_LOGGING_SETTINGS
        if True:
            self.clear_handlers()

        # TODO: replace with config setting LOG_TO_DEBUG_FILE
        if True:
            self.enable_debug_file_log()

        # TODO: replace with config setting LOG_TO_ERROR_FILE
        if True:
            self.enable_error_file_log()

        # TODO: replace with config setting LOG_TO_CONSOLE
        if True:
            self.enable_console_log()

    def clear_handlers(self):
        """
        Removes all handlers from the airflow logger object. This will disable
        any handlers inherited from the root logger as well as any that have
        been set up explicitly for the airflow logger.
        :return: none
        """
        self._logger.handlers = []

    def enable_debug_file_log(self):
        """
        Adds a handler to the airflow logger object to write every log message
        into a debug.log file in the BASE_LOG_FOLDER defined in airflow.cfg.
        :return: none
        """
        self._logger.addHandler(self._debug_file_log_handler)

    def disable_debug_file_log(self):
        """
        Removes the handler from the airflow logger object that is added by the
        enable_debug_file_log function.
        :return: none
        """
        self._logger.removeHandler(self._debug_file_log_handler)

    def enable_error_file_log(self):
        """
        Adds a handler to the airflow logger object to write error log messages
        into an error.log file in the BASE_LOG_FOLDER defined in airflow.cfg.
        :return: none
        """
        self._logger.addHandler(self._error_file_log_handler)

    def disable_error_file_log(self):
        """
        Removes the handler from the airflow logger object that is added by the
        enable_error_file_log function.
        :return: none
        """
        self._logger.removeHandler(self._error_file_log_handler)

    def enable_console_log(self):
        """
        Adds a handler to the airflow logger object to log messages to the
        console. The level of the log messages is defined by the LOGGING_LEVEL
        setting.
        :return: none
        """
        self._logger.addHandler(self._console_log_handler)

    def disable_console_log(self):
        """
        Removes the handler from the airflow logger object that is added by the
        enable_console_log function.
        :return: none
        """
        self._logger.removeHandler(self._console_log_handler)

logging_controller = _LoggingController()


def setup_file_logging(logger,
                       filename,
                       fmt=SIMPLE_LOG_FORMAT,
                       level=LOGGING_LEVEL):
    """
    Adds logging to the given file within the configured BASE_LOG_FOLDER to the
    given logger object.
    :param logger: The logger object to add a new FileHandler to.
    :param filename: The filename (or relative path) to log to within the
    BASE_LOG_FOLDER.
    :param fmt: The format to apply to this new handler.
    :param level: The logging level to apply to this new handler
    :return: The FileHandler object that has been added to the logger. This is
    returned such that it can later be removed from the logger if required.
    """
    # If the passed filename begins with a / it will be interpreted as an
    # absolute path by os.path.join, so cut off any leading forward slash.
    if filename[0] == "/":
        filename = filename[1:]
    file_path = os.path.join(BASE_LOG_FOLDER, filename)
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    handler = logging.FileHandler(file_path)
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)
    handler.setLevel(level)

    _log.debug(
        'Adding logging to {} into file: {}'.format(logger.name, file_path))

    logger.addHandler(handler)

    return handler


def setup_stream_logging(logger, fmt=SIMPLE_LOG_FORMAT, level=LOGGING_LEVEL):
    """
    Adds logging to the console to the given logger object.
    :param logger: The logger object to add a new StreamHandler to.
    :param fmt: The format to apply to this new handler.
    :param level: The logging level to apply to this new handler
    :return: The StreamHandler object that has been added to the logger. This
    is returned such that it can later be removed from the logger if required.
    """
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)
    handler.setLevel(level)

    logger.addHandler(handler)

    return handler


class LoggingMixin(object):
    """
    Convenience super-class to have a logger configured with the class name
    """

    @property
    def logger(self):
        try:
            return self._logger
        except AttributeError:
            self._logger = logging.getLogger(
                self.__class__.__module__ + '.' + self.__class__.__name__
            )
            return self._logger


class S3Log(LoggingMixin):
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
            self.logger.error(
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
        self.logger.error(err)
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
        self.logger.error('Could not write logs to {}'.format(
            remote_log_location))


class GCSLog(LoggingMixin):
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
            self.logger.error(
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
        self.logger.error(err)
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
                self.logger.error('Could not write logs to {}'.format(
                    remote_log_location))

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
