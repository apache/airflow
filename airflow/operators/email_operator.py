from airflow.models import BaseOperator
from airflow.utils.email import send_email
from airflow.utils.decorators import apply_defaults


class EmailOperator(BaseOperator):
    """
    Sends an email.

    :param to: list of emails to send the email to
    :type to: list or string (comma or semicolon delimited)
    :param subject: subject line for the email (templated)
    :type subject: string
    :param html_content: content of the email (templated), html markup
        is allowed
    :type html_content: string
    :param files: file names to attach in email
    :type files: list
    """

    template_fields = ('subject', 'html_content')
    template_ext = ('.html',)
    ui_color = '#e6faf9'

    @apply_defaults
    def __init__(
            self,
            to,
            subject,
            html_content,
            files=None,
            *args, **kwargs):
        super(EmailOperator, self).__init__(*args, **kwargs)
        self.to = to
        self.subject = subject
        self.html_content = html_content
        self.files = files or []

    def execute(self, context):
        send_email(self.to, self.subject, self.html_content, files=self.files)
