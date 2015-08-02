from slackclient import SlackClient
from airflow.models import BaseOperator


class SlackAPIOperator(BaseOperator):
    """
    Base Slack Operator
    The SlackAPIPostOperator is derived from this operator.
    In the future additional Slack API Operators will be derived from this class as well

    :param token: Slack api token
    :type token: String
    :param method: The Slack API Method to Call (https://api.slack.com/methods)
    :type method: String
    :param params: API Method call parameters (https://api.slack.com/methods)
    :type params: Dict
    """
    def __init__(self,
                 token='unset',
                 method='unset',
                 params=None,
                 *args, **kwargs):
        super(SlackAPIOperator, self).__init__(*args, **kwargs)
        self.token = token
        self.method = method
        self.params = params

    def execute(self, **kwargs):
        sc = SlackClient(self.token)
        sc.api_call(self.method, **self.params)


class SlackAPIPostOperator(SlackAPIOperator):
    """
    Posts messages to a slack channel

    :param channel: channel in which to post message on slack name (#general) or ID (C12318391)
    :type channel: string
    :param username: Username that airflow will be posting to Slack as
    :type username: string
    :param text: message to send to slack
    :type text: string
    :param icon_url: url to icon used for this message
    :type string
    """
    template_fields = ('username', 'text')
    ui_color = '#FFBA40'

    def __init__(self,
                 channel='#general',
                 username='Airflow',
                 text='No message has been set.\n'
                      'Here is a cat video instead\n'
                      'https://www.youtube.com/watch?v=J---aiyznGQ',
                 icon_url='https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png',
                 *args, **kwargs):
        self.method = 'chat.postMessage'
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.params = {
            'channel': self.channel,
            'username': self.username,
            'text': self.text,
            'icon_url': self.icon_url,
        }
        super(SlackAPIPostOperator, self).__init__(method=self.method,
                                                   params=self.params,
                                                   *args, **kwargs)

