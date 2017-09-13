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

import logging
import warnings
from builtins import object


class LoggingMixin(object):
    """
    Convenience super-class to have a logger configured with the class name
    """

    # We want to deprecate the logger property in Airflow 2.0
    # The log property is the de facto standard in most programming languages
    @property
    def logger(self):
        warnings.warn(
            'Initializing logger for {} using logger(), which will '
            'be replaced by .log in Airflow 2.0'.format(
                self.__class__.__module__ + '.' + self.__class__.__name__
            ),
            DeprecationWarning
        )
        return self.log

    @property
    def log(self):
        try:
            return self._log
        except AttributeError:
            self._log = logging.root.getChild(
                self.__class__.__module__ + '.' + self.__class__.__name__
            )
            return self._log

    def set_log_contexts(self, task_instance):
        """
        Set the context for all handlers of current logger.
        """
        for handler in self.log.handlers:
            try:
                handler.set_context(task_instance)
            except AttributeError:
                pass
