import json
import telegram

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from hooks.telegram_hook import TelegramHook


class TelegramAPIOperator(BaseOperator):
    """
    Base Telegram Operator
    The TelegramAPIPostOperator is derived from this operator.
    In the future additional Telegram bot API Operators will be derived from this class as well
    :param telegram_conn_id: Telegram connection ID which its password is Telegram API token
    :type telegram_conn_id: str
    :param token: Telergram bot API token
    :type token: str
    """

    @apply_defaults
    def __init__(self, telegram_conn_id=None, token=None, *args, **kwargs):
        super(TelegramAPIOperator, self).__init__(*args, **kwargs)

        if token is None and telegram_conn_id is None:
            raise AirflowException("No valid Telegram token nor telegram_conn_id supplied.")
        if token is not None and telegram_conn_id is not None:
            raise AirflowException(
                "Cannot determine Telegram credential "
                "when both token and telegram_conn_id are supplied."
            )

        self.token = token
        self.telegram_conn_id = telegram_conn_id

    def construct_api_call_params(self):
        """
        Used by the execute function. Allows templating on the source fields
        of the api_call_params dict before construction
        Override in child classes.
        Each TelegramAPIOperator child class is responsible for
        having a construct_api_call_params function
        which sets self.api_call_params with a dict of
        API call parameters
        """

        pass

    def execute(self, **kwargs):
        """
        TelegramAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """
        if not hasattr(self, "api_params"):
            self.construct_api_call_params()
        telegram_client = TelegramHook(token=self.token, telegram_conn_id=self.telegram_conn_id)
        telegram_client.call("POST", self.api_params)


class TelegramAPIPostOperator(TelegramAPIOperator):
    """
    Posts messages to a telegram channel
    :param text: message to send to telegram. (templated)
    :type text: str
    :param chat_id: chat_id in which to post message on telegram (templated)
    :type chat_id: str
    """

    template_fields = ("text", "chat_id")
    ui_color = "#FFBA40"

    @apply_defaults
    def __init__(
        self,
        text="No message has been set.\n"
        "Here is a cat video instead\n"
        "https://www.youtube.com/watch?v=J---aiyznGQ",
        chat_id=None,
        *args,
        **kwargs
    ):
        self.text = text
        self.chat_id = chat_id
        super(TelegramAPIPostOperator, self).__init__(*args, **kwargs)

    def construct_api_call_params(self):
        self.api_params = {
            "text": self.text,
        }
