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
"""
A Action Logger module. Singleton pattern has been applied into this module so that injected action loggers can be used
all through the same python process.
"""
from __future__ import absolute_import
import logging
import airflow.settings


class DefaultActionLogger:
    """
    A default action logger that behave same as www.utils.action_logging which uses global session
    and pushes log DAO object.
    """

    @classmethod
    def submit(cls, **kwargs):
        log = kwargs.get('log')
        if log is None:
            logging.warn("log object is not found skipping")
            return

        session = airflow.settings.Session()
        session.add(log)
        session.commit()


def add(action_logger):
    """
    Adds more action logger. This is intended for any use case to inject customized action logger.
    :param action_logger: An action logger implementation
    :return: None
    """
    logging.debug("Adding {}".format(action_logger))
    __action_loggers.append(action_logger)


def submit(**kwargs):
    """
    Submits log using registered action loggers.
    :param kwargs:
    :return: None
    """
    logging.debug("Submitting report: {}".format(__action_loggers))
    for r in __action_loggers:
        r.submit(**kwargs)


__action_loggers = []
# By default, add Default action logger
add(DefaultActionLogger())
