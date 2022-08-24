#!/usr/bin/python3
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

# This tool is based on the Superset send_email script:
# https://github.com/apache/incubator-superset/blob/master/RELEASING/send_email.py
import os
import shutil
import smtplib
import ssl
import sys
from typing import List, Union

try:
    import jinja2
except ModuleNotFoundError:
    sys.exit("Jinja2 is a required dependency for this script")
try:
    import rich_click as click
except ModuleNotFoundError:
    sys.exit("Click is a required dependency for this script")


SMTP_PORT = 587
SMTP_SERVER = "mail-relay.apache.org"
MAILING_LIST = {"dev": "dev@airflow.apache.org", "users": "users@airflow.apache.org"}


def string_comma_to_list(message: str) -> List[str]:
    """
    Split string to list
    """
    return message.split(",") if message else []


def send_email(
    smtp_server: str,
    smpt_port: int,
    username: str,
    password: str,
    sender_email: str,
    receiver_email: Union[str, List],
    message: str,
):
    """
    Send a simple text email (SMTP)
    """
    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, smpt_port) as server:
        server.starttls(context=context)
        server.login(username, password)
        server.sendmail(sender_email, receiver_email, message)


def render_template(template_file: str, **kwargs) -> str:
    """
    Simple render template based on named parameters

    :param template_file: The template file location
    :kwargs: Named parameters to use when rendering the template
    :return: Rendered template
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    template = jinja2.Template(open(os.path.join(dir_path, template_file)).read())
    return template.render(kwargs)


def show_message(entity: str, message: str):
    """
    Show message on the Command Line
    """
    width, _ = shutil.get_terminal_size()
    click.secho("-" * width, fg="blue")
    click.secho(f"{entity} Message:", fg="bright_red", bold=True)
    click.secho("-" * width, fg="blue")
    click.echo(message)
    click.secho("-" * width, fg="blue")


def inter_send_email(
    username: str, password: str, sender_email: str, receiver_email: Union[str, List], message: str
):
    """
    Send email using SMTP
    """
    show_message("SMTP", message)

    click.confirm("Is the Email message ok?", abort=True)

    try:
        send_email(
            SMTP_SERVER,
            SMTP_PORT,
            username,
            password,
            sender_email,
            receiver_email,
            message,
        )
        click.secho("✅ Email sent successfully", fg="green")
    except smtplib.SMTPAuthenticationError:
        sys.exit("SMTP User authentication error, Email not sent!")
    except Exception as e:
        sys.exit(f"SMTP exception {e}")


class BaseParameters:
    """
    Base Class to send emails using Apache Creds and for Jinja templating
    """

    def __init__(self, name=None, email=None, username=None, password=None, version=None, version_rc=None):
        self.name = name
        self.email = email
        self.username = username
        self.password = password
        self.version = version
        self.version_rc = version_rc
        self.template_arguments = {}

    def __repr__(self):
        return f"Apache Credentials: {self.email}/{self.username}/{self.version}/{self.version_rc}"


@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
@click.pass_context
@click.option(
    "-e",
    "--apache_email",
    prompt="Apache Email",
    envvar="APACHE_EMAIL",
    show_envvar=True,
    help="Your Apache email will be used for SMTP From",
    required=True,
)
@click.option(
    "-u",
    "--apache_username",
    prompt="Apache Username",
    envvar="APACHE_USERNAME",
    show_envvar=True,
    help="Your LDAP Apache username",
    required=True,
)
@click.password_option(  # type: ignore
    "-p",
    "--apache_password",
    prompt="Apache Password",
    envvar="APACHE_PASSWORD",
    show_envvar=True,
    help="Your LDAP Apache password",
    required=True,
)
@click.option(
    "-v",
    "--version",
    prompt="Version",
    envvar="AIRFLOW_VERSION",
    show_envvar=True,
    help="Release Version",
    required=True,
)
@click.option(
    "-rc",
    "--version_rc",
    prompt="Version (with RC)",
    envvar="AIRFLOW_VERSION_RC",
    show_envvar=True,
    help="Release Candidate Version",
    required=True,
)
@click.option(  # type: ignore
    "-n",
    "--name",
    prompt="Your Name",
    default=lambda: os.environ.get('USER', ''),
    show_default="Current User",
    help="Name of the Release Manager",
    type=click.STRING,
    required=True,
)
def cli(
    ctx,
    apache_email: str,
    apache_username: str,
    apache_password: str,
    version: str,
    version_rc: str,
    name: str,
):
    """
    🚀 CLI to send emails for the following:

    \b
    * Voting thread for the rc
    * Result of the voting for the rc
    * Announcing that the new version has been released
    """
    base_parameters = BaseParameters(
        name, apache_email, apache_username, apache_password, version, version_rc
    )
    base_parameters.template_arguments["version"] = base_parameters.version
    base_parameters.template_arguments["version_rc"] = base_parameters.version_rc
    base_parameters.template_arguments["sender_email"] = base_parameters.email
    base_parameters.template_arguments["release_manager"] = base_parameters.name
    ctx.obj = base_parameters


@cli.command("vote")
@click.option(
    "--receiver_email",
    default=MAILING_LIST.get("dev"),
    type=click.STRING,
    prompt="The receiver email (To:)",
)
@click.pass_obj
def vote(base_parameters, receiver_email: str):
    """
    Send email calling for Votes on RC
    """
    template_file = "templates/vote_email.j2"
    base_parameters.template_arguments["receiver_email"] = receiver_email
    message = render_template(template_file, **base_parameters.template_arguments)
    inter_send_email(
        base_parameters.username,
        base_parameters.password,
        base_parameters.template_arguments["sender_email"],
        base_parameters.template_arguments["receiver_email"],
        message,
    )
    if click.confirm("Show Slack message for announcement?", default=True):
        base_parameters.template_arguments["slack_rc"] = False
        slack_msg = render_template("templates/slack.j2", **base_parameters.template_arguments)
        show_message("Slack", slack_msg)


@cli.command("result")
@click.option(
    "-re",
    "--receiver_email",
    default=MAILING_LIST.get("dev"),
    type=click.STRING,
    prompt="The receiver email (To:)",
)
@click.option(
    "--vote_bindings",
    default="",
    type=click.STRING,
    prompt="A List of people with +1 binding vote (ex: Max,Grace,Krist)",
)
@click.option(
    "--vote_nonbindings",
    default="",
    type=click.STRING,
    prompt="A List of people with +1 non binding vote (ex: Ville)",
)
@click.option(
    "--vote_negatives",
    default="",
    type=click.STRING,
    prompt="A List of people with -1 vote (ex: John)",
)
@click.pass_obj
def result(
    base_parameters,
    receiver_email: str,
    vote_bindings: str,
    vote_nonbindings: str,
    vote_negatives: str,
):
    """
    Send email with results of voting on RC
    """
    template_file = "templates/result_email.j2"
    base_parameters.template_arguments["receiver_email"] = receiver_email
    base_parameters.template_arguments["vote_bindings"] = string_comma_to_list(vote_bindings)
    base_parameters.template_arguments["vote_nonbindings"] = string_comma_to_list(vote_nonbindings)
    base_parameters.template_arguments["vote_negatives"] = string_comma_to_list(vote_negatives)
    message = render_template(template_file, **base_parameters.template_arguments)
    inter_send_email(
        base_parameters.username,
        base_parameters.password,
        base_parameters.template_arguments["sender_email"],
        base_parameters.template_arguments["receiver_email"],
        message,
    )


@cli.command("announce")
@click.option(
    "--receiver_email",
    default=",".join(list(MAILING_LIST.values())),
    prompt="The receiver email (To:)",
    help="Receiver's email address. If more than 1, separate them by comma",
)
@click.pass_obj
def announce(base_parameters, receiver_email: str):
    """
    Send email to announce release of the new version
    """
    receiver_emails: List[str] = string_comma_to_list(receiver_email)

    template_file = "templates/announce_email.j2"
    base_parameters.template_arguments["receiver_email"] = receiver_emails
    message = render_template(template_file, **base_parameters.template_arguments)

    inter_send_email(
        base_parameters.username,
        base_parameters.password,
        base_parameters.template_arguments["sender_email"],
        base_parameters.template_arguments["receiver_email"],
        message,
    )

    if click.confirm("Show Slack message for announcement?", default=True):
        base_parameters.template_arguments["slack_rc"] = False
        slack_msg = render_template("templates/slack.j2", **base_parameters.template_arguments)
        show_message("Slack", slack_msg)
    if click.confirm("Show Twitter message for announcement?", default=True):
        twitter_msg = render_template("templates/twitter.j2", **base_parameters.template_arguments)
        show_message("Twitter", twitter_msg)


if __name__ == '__main__':
    cli()
