from builtins import object
import imp
import inspect
import logging
import os
from itertools import chain
merge = chain.from_iterable

from airflow.configuration import conf

class AirflowPluginException(Exception):
    pass

class AirflowPlugin(object):
    name = None
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

    @classmethod
    def validate(cls):
        if not cls.name:
            raise AirflowPluginException("Your plugin needs a name.")


plugins_folder = conf.get('core', 'plugins_folder')
if not plugins_folder:
    plugins_folder = conf.get('core', 'airflow_home') + '/plugins'
plugins_folder = os.path.expanduser(plugins_folder)

plugins = []

# Crawl through the plugins folder to find AirflowPlugin derivatives
for root, dirs, files in os.walk(plugins_folder):
    for f in files:
        try:
            filepath = os.path.join(root, f)
            if not os.path.isfile(filepath):
                continue
            mod_name, file_ext = os.path.splitext(
                os.path.split(filepath)[-1])
            if file_ext != '.py':
                continue
            m = imp.load_source(mod_name, filepath)
            for obj in list(m.__dict__.values()):
                if (
                        inspect.isclass(obj) and
                        issubclass(obj, AirflowPlugin) and
                        obj is not AirflowPlugin):
                    obj.validate()
                    plugins.append(obj)

        except Exception as e:
            logging.exception(e)
            logging.error('Failed to import plugin ' + filepath)

operators = merge([p.operators for p in plugins])
hooks = merge([p.hooks for p in plugins])
executors = merge([p.executors for p in plugins])
macros = merge([p.macros for p in plugins])
admin_views = merge([p.admin_views for p in plugins])
flask_blueprints = merge([p.flask_blueprints for p in plugins])
menu_links = merge([p.menu_links for p in plugins])
