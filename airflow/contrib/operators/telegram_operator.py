import json
import telegram

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from hooks.telegram_hook import TelegramHook


class TelegramAPIOperator(BaseOperator):
    """
    Telegram Operator

    :param text: Templatable message
    :type text: str
    :param chat_id: Telegram channel ID
    :type chat_id: str
    :param telegram_conn_id: Telegram connection ID which its password is Telegram API token
    :type telegram_conn_id: str
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
        telegram_conn_id=None,
        *args,
        **kwargs
    ):
        self.text = text
        self.chat_id = chat_id

        if telegram_conn_id is None:
            raise AirflowException("No valid Telegram connection id supplied.")

        self.telegram_conn_id = telegram_conn_id

        super(TelegramAPIOperator, self).__init__(*args, **kwargs)

    def execute(self, **kwargs):
        """
        TelegramAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """

        try:
            telegram_client = TelegramHook(telegram_conn_id=self.telegram_conn_id)
            telegram_client.call("POST", {"text": self.text})
        except Exception as e:
            self.log.error("Cannot send a message to telegram, exception was %s", e)
