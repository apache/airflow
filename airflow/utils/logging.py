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

from airflow import configuration


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


class GCSLog(object):
    """
    Utility class for reading and writing logs in GCS.
    Requires either airflow[gcloud] or airflow[gcp_api] and
    setting the REMOTE_BASE_LOG_FOLDER and REMOTE_LOG_CONN_ID configuration
    options in airflow.cfg.
    """
    def __init__(self):
        """
        Attempt to create hook with airflow[gcloud] (and set
        use_gcloud = True), otherwise uses airflow[gcp_api]
        """
        remote_conn_id = configuration.get('core', 'REMOTE_LOG_CONN_ID')
        self.use_gcloud = False

        try:
            from airflow.contrib.hooks import GCSHook
            self.hook = GCSHook(remote_conn_id)
            self.use_gcloud = True
        except:
            try:
                from airflow.contrib.hooks import GoogleCloudStorageHook
                self.hook = GoogleCloudStorageHook(remote_conn_id)
            except:
                self.hook = None
                logging.error(
                    'Could not create a GCSHook with connection id "{}". '
                    'Please make sure that either airflow[gcloud] or '
                    'airflow[gcp_api] is installed and the GCS connection '
                    'exists.'.format(remote_conn_id))

    def read(self, remote_log_location, return_error=True):
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
                if self.use_gcloud:
                    gcs_blob = self.hook.get_blob(remote_log_location)
                    if gcs_blob:
                        return gcs_blob.download_as_string().decode()
                else:
                    bkt, blob = remote_log_location.lstrip('gs:/').split('/', 1)
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
                if self.use_gcloud:
                    self.hook.upload_from_string(
                        log,
                        blob=remote_log_location,
                        replace=True)
                    return
                else:
                    bkt, blob = remote_log_location.lstrip('gs:/').split('/', 1)
                    from tempfile import NamedTemporaryFile
                    with NamedTemporaryFile(mode='w+') as tmpfile:
                        tmpfile.write(log)
                        self.hook.upload(bkt, blob, tmpfile.name)
                    return
            except:
                pass

        # raise/return error if we get here
        logging.error('Could not write logs to {}'.format(remote_log_location))
