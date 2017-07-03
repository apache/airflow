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
import jinja2
import os

from abc import ABCMeta, abstractmethod


TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')


class AirflowNotifier(object):
    __metaclass__ = ABCMeta

    template_ext = ['tpl', 'html']

    @abstractmethod
    def send_notification(self, task_instance, state, message, **kwargs):
        pass

    def render_template(self, template, task_instance, **kwargs):
        dag = task_instance.task.dag

        jinja_loader = jinja2.FileSystemLoader([dag.folder, TEMPLATE_DIR])
        jinja_env = jinja2.Environment(loader=jinja_loader,
                                       extensions=["jinja2.ext.do"],
                                       cache_size=0)

        # if template string endswith a template_ext, try to load it,
        # else interpret it as string
        if any(template.endswith(ext) for ext in self.template_ext):
            jinja_template = jinja_env.get_template(template)
        else:
            jinja_template = jinja_env.from_string(template)

        jinja_context = task_instance.get_template_context()
        for key, value in kwargs.items():
            jinja_context[key] = value

        return jinja_template.render(**jinja_context)
