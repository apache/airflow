# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Config sub-commands"""
import io

from configupdater import ConfigUpdater, NoSectionError

from airflow.configuration import AirflowConfigException, conf, resolve_actual_config
from airflow.utils.helpers import ask_yesno


def show_config(args):
    """Show current application configuration"""
    with io.StringIO() as output:
        if args.section:
            section = args.section
            if conf.has_section(section):
                conf._write_section(  # pylint: disable=protected-access
                    fp=output,
                    section_name=section,
                    section_items=conf.getsection(section).items(),
                    delimiter=" = ",
                )
            else:
                raise AirflowConfigException(f"No section '{section}'")
        else:
            conf.write(output)
        print(output.getvalue())


def set_config_option(args):
    """Set option in config"""
    config_path = args.cfg_path or resolve_actual_config()
    section, option, value = args.section, args.option, args.value

    do_the_change = ask_yesno(f"Update section {section}, option {option} with {value}? [Y/n]", assume="Y")
    if not do_the_change:
        print("Update skipped")
        return

    updater = ConfigUpdater()
    updater.read(config_path)
    try:
        updater.set(section, option, value)
    except NoSectionError:
        # Add section if not exists
        updater.add_section(section)
        updater.set(section, option, value)
    updater.update_file()
    print(f"Config changed: {section}.{option}={value} in {config_path}")

    # Check if environment variable set
    env_value = conf._get_env_var_option(  # pylint: disable=protected-access
        section=section,
        key=option
    )
    if env_value:
        print(
            f"WARNING: Environment variable `AIRFLOW__{section.upper()}__{option.upper()}` "
            f"set to {env_value}. This will overwrite your config for `{option}`."
        )


def get_config_location(args):
    """Show location of current airflow.cfg"""
    config_path = resolve_actual_config()
    print(config_path)
