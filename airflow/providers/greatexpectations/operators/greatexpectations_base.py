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
#

from urllib.parse import urlsplit
import logging

from airflow.models import BaseOperator
from airflow.utils.email import send_email
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class GreatExpectationsBaseOperator(BaseOperator):
    """
        This is the base operator for all Great Expectations operators.

        :param expectations_file_name: The name of the JSON file containing the expectations for the data.
        :type expectations_file_name: str
        :param send_alert_email:  Send an alert email if one or more expectations fail to be met?  Defaults to True.
        :type send_alert_email: boolean
        :param datadocs_link_in_email:  Include in the alert email a clickable link to the data doc that shows the
            validation results?  Defaults to False because the Airflow user would need to do the extra setup to configure
            a separate server to serve up data docs when someone clicked on the link in the email.  When set to False,
            only a path to the results are included in the email.
        :type datadocs_link_in_email: boolean
        :param datadocs_domain: The domain from which the data docs are set up to be served (e.g. ge-data-docs-dot-my-gcp-project.ue.r.appspot.com).
            This only needs to be set if datadocs_link_in_email is set to True.
        :type datadocs_domain: str
        :param email_to:  Email address to receive any alerts when expectations are not met.
        :type email_to: str
        :param fail_if_expectations_not_met: Fail the Airflow task if expectations are not met?  Defaults to True.
        :type fail_if_expectations_not_met: boolean
    """

    _EMAIL_CONTENT = '''
            <html>
              <head>
                <meta charset="utf-8">
              </head>
              <body style="background-color: #fafafa; font-family: Roboto, sans-serif=;">
                <div style="width: 600px; margin:0 auto;">
                    <div style="background-color: white; border-top: 4px solid #22a667; border-left: 1px solid #eee; border-right: 1px solid #eee; border-radius: 6px 6px 0 0; height: 24px;"></div>
                        <div style="background-color: white; border-left: 1px solid #eee; border-right: 1px solid #eee; padding: 0 24px; overflow: hidden;">
                          <div style="margin-left: 35px;">
                            Great Expectations Alert<br>
                            One or more data expectations were not met in the {0} file. {1}
                       </div>
              </body>
            </html>
            '''

    @apply_defaults
    def __init__(self, *, expectations_file_name, email_to, send_alert_email=True, datadocs_link_in_email=False,
                 datadocs_domain='none', fail_if_expectations_not_met=True, **kwargs):
        super().__init__(**kwargs)

        self.expectations_file_name = expectations_file_name
        self.email_to = email_to
        self.send_alert_email = send_alert_email
        self.datadocs_link_in_email = datadocs_link_in_email
        self.datadocs_domain = datadocs_domain
        self.fail_if_expectations_not_met = fail_if_expectations_not_met

    def execute(self, context):
        raise NotImplementedError('Please implement execute() in sub class!')

    def send_alert(self, data_docs_url='none'):
        results = self._format_email(data_docs_url)
        email_content = self._EMAIL_CONTENT.format(self.expectations_file_name, results)
        send_email(self.email_to, 'expectations in ' + self.expectations_file_name + ' not met', email_content,
                   files=None, cc=None, bcc=None,
                   mime_subtype='mixed', mime_charset='us_ascii')

    def _format_email(self, data_docs_url):
        # If data_docs_url is set to 'none' then only a generic warning will be included in the email.  No clickable
        # link or path to the data docs will be included.
        if data_docs_url != 'none':
            # A data docs url was passed in so a form of it will be added to the email as either a clickable link
            # or just a non-clickable directory path.
            if self.datadocs_link_in_email:
                # Get the domain name of the service serving the data docs.
                if self.datadocs_domain == 'none':
                    raise AirflowException(
                        "datadocs_link_in_email is set to true but datadocs_domain is 'none'.  datadocs_domain should be set to the domain from which datadocs are being served")
                # Replace the domain returned by ge with the domain set up to serve the data docs
                parsed = urlsplit(data_docs_url)
                new_url = parsed._replace(netloc=self.datadocs_domain)
                results = '  see the results <a href=' + new_url.geturl() + '>here</a>.'
            else:
                # From the data docs url, pull out just the directory path to the data docs and send it to the users in the email.
                parsed = urlsplit(data_docs_url)
                results = '  See the following location for results:' + parsed.path
        else:
            # No link or path to the results will be included in the email
            results = ''
        return results
