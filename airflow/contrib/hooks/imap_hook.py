# -*- coding: utf-8 -*-
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

import email
import imaplib
import os.path
import re

from airflow import LoggingMixin, AirflowException
from airflow.hooks.base_hook import BaseHook


class ImapHook(BaseHook):
    """
    This hook connects to a mail server by using the imap protocol.

    :param imap_conn_id: The connection id that contains the information used to authenticate the client.
    :type imap_conn_id: str
    """

    def __init__(self, imap_conn_id='imap_default'):
        super().__init__(imap_conn_id)
        self.conn = self.get_connection(imap_conn_id)
        self.mail_client = imaplib.IMAP4_SSL(self.conn.host)

    def __enter__(self):
        self.mail_client.login(self.conn.login, self.conn.password)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mail_client.logout()

    def has_mail_attachment(self, name, mail_folder='INBOX', check_regex=False):
        """
        Checks the mail folder for mails containing attachments with the given name.

        :param name: The name of the attachment that will be searched for.
        :type name: str
        :param mail_folder: The mail folder where to look at.
        :type mail_folder: str
        :param check_regex: Checks the name for a regular expression.
        :type check_regex: bool
        :returns: True if there is an attachment with the given name and False if not.
        :rtype: bool
        """
        mail_attachments = self._retrieve_mails_attachments_by_name(name,
                                                                    mail_folder,
                                                                    check_regex,
                                                                    latest_only=True)
        return len(mail_attachments) > 0

    def retrieve_mail_attachments(self,
                                  name,
                                  mail_folder='INBOX',
                                  check_regex=False,
                                  latest_only=False,
                                  not_found_mode='raise'):
        """
        Retrieves mail's attachments in the mail folder by its name.

        :param name: The name of the attachment that will be downloaded.
        :type name: str
        :param mail_folder: The mail folder where to look at.
        :type mail_folder: str
        :param check_regex: Checks the name for a regular expression.
        :type check_regex: bool
        :param latest_only: If set to True it will only retrieve
                            the first matched attachment.
        :type latest_only: bool
        :param not_found_mode: Specify what should happen if no attachment has been found.
                               Supported values are 'raise', 'warn' and 'ignore'.
                               If it is set to 'raise' it will raise an exception,
                               if set to 'warn' it will only print a warning and
                               if set to 'ignore' it won't notify you at all.
        :type not_found_mode: str
        :returns: a list of tuple each containing the attachment filename and its payload.
        :rtype: a list of tuple
        """
        mail_attachments = self._retrieve_mails_attachments_by_name(name,
                                                                    mail_folder,
                                                                    check_regex,
                                                                    latest_only)
        if not mail_attachments:
            self._handle_not_found_mode(not_found_mode)

        return mail_attachments

    def download_mail_attachments(self,
                                  name,
                                  local_output_directory,
                                  mail_folder='INBOX',
                                  check_regex=False,
                                  latest_only=False,
                                  not_found_mode='raise'):
        """
        Downloads mail's attachments in the mail folder by its name to the local directory.

        :param name: The name of the attachment that will be downloaded.
        :type name: str
        :param local_output_directory: The output directory on the local machine
                                       where the files will be downloaded to.
        :type local_output_directory: str
        :param mail_folder: The mail folder where to look at.
        :type mail_folder: str
        :param check_regex: Checks the name for a regular expression.
        :type check_regex: bool
        :param latest_only: If set to True it will only download
                            the first matched attachment.
        :type latest_only: bool
        :param not_found_mode: Specify what should happen if no attachment has been found.
                               Supported values are 'raise', 'warn' and 'ignore'.
                               If it is set to 'raise' it will raise an exception,
                               if set to 'warn' it will only print a warning and
                               if set to 'ignore' it won't notify you at all.
        :type not_found_mode: str
        """
        mail_attachments = self._retrieve_mails_attachments_by_name(name,
                                                                    mail_folder,
                                                                    check_regex,
                                                                    latest_only)

        if not mail_attachments:
            self._handle_not_found_mode(not_found_mode)

        self._create_files(mail_attachments, local_output_directory)

    def _handle_not_found_mode(self, not_found_mode):
        if not_found_mode == 'raise':
            raise AirflowException('No mail attachments found!')
        elif not_found_mode == 'warn':
            self.log.warning('No mail attachments found!')
        elif not_found_mode == 'ignore':
            pass  # Do not notify if the attachment has not been found.
        else:
            self.log.error('Invalid "not_found_mode" %s', not_found_mode)

    def _retrieve_mails_attachments_by_name(self, name, mail_folder, check_regex, latest_only):
        all_matching_attachments = []

        self.mail_client.select(mail_folder)

        for mail_id in self._list_mail_ids_desc():
            response_mail_body = self._fetch_mail_body(mail_id)
            matching_attachments = self._check_mail_body(response_mail_body, name, check_regex, latest_only)

            if matching_attachments:
                all_matching_attachments.extend(matching_attachments)
                if latest_only:
                    break

        self.mail_client.close()

        return all_matching_attachments

    def _list_mail_ids_desc(self):
        result, data = self.mail_client.search(None, 'All')
        mail_ids = data[0].split()
        return reversed(mail_ids)

    def _fetch_mail_body(self, mail_id):
        result, data = self.mail_client.fetch(mail_id, '(RFC822)')
        mail_body = data[0][1]  # The mail body is always in this specific location
        mail_body_str = mail_body.decode('utf-8')
        return mail_body_str

    def _check_mail_body(self, response_mail_body, name, check_regex, latest_only):
        mail = Mail(response_mail_body)
        if mail.has_attachments():
            return mail.get_attachments_by_name(name, check_regex, find_first=latest_only)

    def _create_files(self, mail_attachments, local_output_directory):
        for name, payload in mail_attachments:
            if self._is_symlink(name):
                self.log.error('Can not create file because it is a symlink!')
            elif self._is_escaping_current_directory(name):
                self.log.error('Can not create file because it is escaping the current directory!')
            else:
                self._create_file(name, payload, local_output_directory)

    def _is_symlink(self, name):
        # IMPORTANT NOTE: os.path.islink is not working for windows symlinks
        # See: https://stackoverflow.com/a/11068434
        return os.path.islink(name)

    def _is_escaping_current_directory(self, name):
        return '../' in name

    def _correct_path(self, name, local_output_directory):
        return local_output_directory + name if local_output_directory.endswith('/') \
            else local_output_directory + '/' + name

    def _create_file(self, name, payload, local_output_directory):
        file_path = self._correct_path(name, local_output_directory)

        with open(file_path, 'wb') as file:
            file.write(payload)


class Mail(LoggingMixin):
    """
    This class simplifies working with mails returned by the imaplib client.

    :param mail_body: The mail body of a mail received from imaplib client.
    :type mail_body: str
    """

    def __init__(self, mail_body):
        super().__init__()
        self.mail = email.message_from_string(mail_body)

    def has_attachments(self):
        """
        Checks the mail for a attachments.

        :returns: True if it has attachments and False if not.
        :rtype: bool
        """
        return self.mail.get_content_maintype() == 'multipart'

    def get_attachments_by_name(self, name, check_regex, find_first=False):
        """
        Gets all attachments by name for the mail.

        :param name: The name of the attachment to look for.
        :type name: str
        :param check_regex: Checks the name for a regular expression.
        :type check_regex: bool
        :param find_first: If set to True it will only find the first match and then quit.
        :type find_first: bool
        :returns: a list of tuples each containing name and payload
                  where the attachments name matches the given name.
        :rtype: list of tuple
        """
        attachments = []

        for part in self.mail.walk():
            mail_part = MailPart(part)
            if mail_part.is_attachment():
                found_attachment = mail_part.has_matching_name(name) if check_regex \
                    else mail_part.has_equal_name(name)
                if found_attachment:
                    file_name, file_payload = mail_part.get_file()
                    self.log.info('Found attachment: {}'.format(file_name))
                    attachments.append((file_name, file_payload))
                    if find_first:
                        break

        return attachments


class MailPart:
    """
    This class is a wrapper for a Mail object's part and gives it more features.

    :param part: The mail part in a Mail object.
    :type part: any
    """

    def __init__(self, part):
        self.part = part

    def is_attachment(self):
        """
        Checks if the part is a valid mail attachment.

        :returns: True if it is an attachment and False if not.
        :rtype: bool
        """
        return self.part.get_content_maintype() != 'multipart' and self.part.get('Content-Disposition')

    def has_matching_name(self, name):
        """
        Checks if the given name matches the part's name.

        :param name: The name to look for.
        :type name: str
        :returns: True if it matches the name (including regular expression).
        :rtype: tuple
        """
        return re.match(name, self.part.get_filename())

    def has_equal_name(self, name):
        """
        Checks if the given name is equal to the part's name.

        :param name: The name to look for.
        :type name: str
        :returns: True if it is equal to the given name.
        :rtype: bool
        """
        return self.part.get_filename() == name

    def get_file(self):
        """
        Gets the file including name and payload.

        :returns: the part's name and payload.
        :rtype: tuple
        """
        return self.part.get_filename(), self.part.get_payload(decode=True)
