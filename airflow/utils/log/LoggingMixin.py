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
from builtins import object


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

    def set_logger_contexts(self, task_instance):
        """
        Set the context for all handlers of current logger.
        """
        for handler in self.logger.handlers:
            try:
                handler.set_context(task_instance)
            except AttributeError:
                pass
