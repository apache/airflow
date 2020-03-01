#!/usr/bin/env python
#
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

import re
from collections import Counter
from unittest import TestCase

from airflow.bin.cli import CLIFactory

# Can not be `--snake_case` or contain uppercase letter
ILLEGAL_LONG_OPTION_PATTERN = re.compile("^--[a-z]+_[a-z]+|^--.*[A-Z].*")
# Only can be `-[a-z]` or `-[A-Z]`
LEGAL_SHORT_OPTION_PATTERN = re.compile("^-[a-zA-z]$")


class TestCli(TestCase):

    def test_arg_option_long_only(self):
        """
        Test if the name of CLIFactory.args long option valid
        """
        optional_long = [
            arg for _, arg in CLIFactory.args.items()
            if len(arg.flags) == 1 and arg.flags[0].startswith("-")
        ]
        for arg in optional_long:
            self.assertIsNone(ILLEGAL_LONG_OPTION_PATTERN.match(arg.flags[0]),
                              f"{arg.flags[0]} is not match")

    def test_arg_option_mix_short_long(self):
        """
        Test if the name of CLIFactory.args mix option (-s, --long) valid
        """
        optional_mix = [
            arg for _, arg in CLIFactory.args.items()
            if len(arg.flags) == 2 and arg.flags[0].startswith("-")
        ]
        for arg in optional_mix:
            self.assertIsNotNone(LEGAL_SHORT_OPTION_PATTERN.match(arg.flags[0]),
                                 f"{arg.flags[0]} is not match")
            self.assertIsNone(ILLEGAL_LONG_OPTION_PATTERN.match(arg.flags[1]),
                              f"{arg.flags[1]} is not match")

    def test_subcommand_conflict(self):
        """
        Test if each of CLIFactory.*_COMMANDS without conflict subcommand
        """
        subcommand = {
            var: CLIFactory.__dict__.get(var)
            for var in CLIFactory.__dict__
            if var.isupper() and "COMMANDS" in var
        }
        for group_name, sub in subcommand.items():
            name = [command['name'].lower() for command in sub]
            self.assertEqual(len(name), len(set(name)),
                             f"Command group {group_name} have conflict subcommand")

    def test_subcommand_arg_name_conflict(self):
        """
        Test if each of CLIFactory.*_COMMANDS.arg name without conflict
        """
        subcommand = {
            var: CLIFactory.__dict__.get(var)
            for var in CLIFactory.__dict__
            if var.isupper() and "COMMANDS" in var
        }
        for group, command in subcommand.items():
            for com in command:
                name = com['name']
                args = com['args']
                conflict_arg = [arg for arg, count in Counter(args).items() if count > 1]
                self.assertListEqual([], conflict_arg,
                                     f"Command group {group} function {name} have "
                                     f"conflict args name {conflict_arg}")

    def test_subcommand_arg_flag_conflict(self):
        """
        Test if each of CLIFactory.*_COMMANDS.arg flags without conflict
        """

        def cli_args_flags(arg):
            """
            Get CLIFactory args flags
            """
            return CLIFactory.args[arg].flags

        subcommand = {
            var: CLIFactory.__dict__.get(var)
            for var in CLIFactory.__dict__
            if var.isupper() and "COMMANDS" in var
        }
        for group, command in subcommand.items():
            for com in command:
                name = com['name']
                position = [
                    cli_args_flags(a)[0]
                    for a in com['args']
                    if (len(cli_args_flags(a)) == 1
                        and not cli_args_flags(a)[0].startswith("-"))
                ]
                conflict_position = [arg for arg, count in Counter(position).items() if count > 1]
                self.assertListEqual([], conflict_position,
                                     f"Command group {group} function {name} have conflict "
                                     f"position flags {conflict_position}")

                long_option = [cli_args_flags(a)[0]
                               for a in com['args']
                               if (len(cli_args_flags(a)) == 1
                                   and cli_args_flags(a)[0].startswith("-"))] + \
                              [cli_args_flags(a)[1]
                               for a in com['args'] if len(cli_args_flags(a)) == 2]
                conflict_long_option = [arg for arg, count in Counter(long_option).items() if count > 1]
                self.assertListEqual([], conflict_long_option,
                                     f"Command group {group} function {name} have conflict "
                                     f"long option flags {conflict_long_option}")

                short_option = [
                    cli_args_flags(a)[0]
                    for a in com['args'] if len(cli_args_flags(a)) == 2
                ]
                conflict_short_option = [arg for arg, count in Counter(short_option).items() if count > 1]
                self.assertEqual([], conflict_short_option,
                                 f"Command group {group} function {name} have conflict "
                                 f"short option flags {conflict_short_option}")
