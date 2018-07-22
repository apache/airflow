import email
import imaplib
import re

from airflow.hooks.base_hook import BaseHook


class ImapHook(BaseHook):
    """
    This hook connects to a mail server by using the imap protocol.

    :param imap_conn_id: The connection id that contains the information used to authenticate the client.
                         The default value is 'imap_default'.
    :type imap_conn_id: str
    """

    def __init__(self, imap_conn_id='imap_default'):
        super().__init__(imap_conn_id)
        self.imap_conn_id = imap_conn_id

    def __enter__(self):
        conn = self.get_connection(self.imap_conn_id)
        self.mail_client = imaplib.IMAP4_SSL(conn.host)
        self.mail_client.login(conn.login, conn.password)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mail_client.close()
        self.mail_client.logout()

    def has_mail_attachments(self, name, mail_folder='inbox', check_regex=False):
        """
        Checks the mail folder for a mail containing attachments with the given name.

        :param name: The name of the attachment that will be searched for.
        :type name: str
        :param mail_folder: The mail folder where to look at.
                            The default value is 'inbox'.
        :type mail_folder: str
        :param check_regex: Checks the name for a regular expression.
                            The default value is False.
        :type check_regex: bool
        :returns: True if there is an attachment with the given name and False if not.
        :rtype: bool
        """
        mail_attachments = self._retrieve_mail_attachments(name, mail_folder, check_regex)
        return len(mail_attachments) > 0

    def retrieve_mail_attachments(self, name, local_output_directory, mail_folder='inbox', check_regex=False):
        """
        Downloads email's attachments in the mail folder by its name to the local directory.

        :param name: The name of the attachment that will be downloaded.
        :type name: str
        :param local_output_directory: The output directory on the local machine where the files will be downloaded to.
        :type local_output_directory: str
        :param mail_folder: The mail folder where to look at.
                            The default value is 'inbox'.
        :type mail_folder: str
        :param check_regex: Checks the name for a regular expression.
                            The default value is False.
        :type check_regex: bool
        """
        mail_attachments = self._retrieve_mail_attachments(name, mail_folder, check_regex)
        self._download_mail_attachments(mail_attachments, local_output_directory)

    def _retrieve_mail_attachments(self, name, mail_folder, check_regex):
        all_mails_attachments = []

        self.mail_client.select(mail_folder)

        for mail_id in self._list_mail_ids_desc():
            response_mail_body = self._fetch_mail_body(mail_id)
            mail = Mail(response_mail_body)
            if mail.has_attachments():
                mail_attachments = mail.get_attachments_by_name(name, check_regex)
                all_mails_attachments.append(mail_attachments)

        return all_mails_attachments

    def _list_mail_ids_desc(self):
        result, data = self.mail_client.search(None, 'All')
        mail_ids = data[0].split()
        return reversed(mail_ids)

    def _fetch_mail_body(self, mail_id):
        result, data = self.mail_client.fetch(mail_id, '(RFC822)')
        email_body = data[0][1]  # The email body is always in this specific location
        email_body_str = email_body.decode('utf-8')
        return email_body_str

    def _download_mail_attachments(self, mail_attachments, local_output_directory):
        for name, payload in mail_attachments:
            with open(local_output_directory + '/' + name, 'wb') as file:
                file.write(payload)


class Mail:
    """
    This class simplifies working with emails returned by the imaplib client.

    :param email_body: The email body of a mail received from imaplib client
    :type email_body: str
    """

    def __init__(self, email_body):
        self.mail = email.message_from_string(email_body)

    def has_attachments(self):
        """
        Checks the mail for a attachments.

        :returns: True if it has attachments and False if not.
        :rtype: bool
        """
        return self.mail.get_content_maintype() == 'multipart'

    def get_attachments_by_name(self, name, check_regex):
        """
        Gets all attachments by name for the mail.

        :param name: The name of the attachment to look for.
        :type name: str
        :param check_regex: Checks the name for a regular expression.
        :type check_regex: bool
        :returns: a list of tuples each containing name and payload where the attachments name matches the given name.
        :rtype: list of tuple
        """
        attachments = []
        for part in self.mail.walk():
            mail_part = MailPart(part)
            if mail_part.is_attachment():
                found_attachment = mail_part.has_similar_name(name) if check_regex else mail_part.has_equal_name(name)
                if found_attachment:
                    attachments.append(mail_part.get_file())

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

        :return: True if it is an attachment and False if not.
        :rtype: bool
        """
        self.part.get_content_maintype() != 'multipart' and self.part.get('Content-Disposition')

    def has_similar_name(self, name):
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
        :return: True if it is equal to the given name.
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
