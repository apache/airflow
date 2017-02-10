from unittest.mock import Mock, patch
from plugins.hooks.zendesk_hook import ZendeskHook
from zdesk import RateLimitError
from pytest import raises


@patch("plugins.hooks.zendesk_hook.time")
@patch("plugins.hooks.zendesk_hook.Zendesk")
def test_sleeps_for_correct_interval(_, mocked_time):
    sleep_time = 10

    # To break out of the otherwise infinite tries
    mocked_time.sleep = Mock(side_effect=ValueError)
    conn_mock = Mock()
    mock_response = Mock()
    mock_response.headers.get.return_value = sleep_time
    conn_mock.call = Mock(
        side_effect=RateLimitError(msg="some message", code="some code",
                                   response=mock_response))

    zendesk_hook = ZendeskHook("conn_id")
    zendesk_hook.get_conn = Mock(return_value=conn_mock)

    with raises(ValueError):
        zendesk_hook.call("some_path", get_all_pages=False)
    mocked_time.sleep.assert_called_with(sleep_time)


@patch("plugins.hooks.zendesk_hook.Zendesk")
def test_returns_single_page_if_get_all_pages_false(_):
    zendesk_hook = ZendeskHook("conn_id")
    mock_connection = Mock()
    mock_connection.host = "some_host"
    zendesk_hook.get_connection = Mock(return_value=mock_connection)
    zendesk_hook.get_conn()

    mock_conn = Mock()
    mock_call = Mock(
        return_value={'next_page': 'https://some_host/something', 'path': []})
    mock_conn.call = mock_call
    print(mock_conn.call)
    zendesk_hook.get_conn = Mock(return_value=mock_conn)
    zendesk_hook.call("path", get_all_pages=False)
    mock_call.assert_called_once_with("path", None)


@patch("plugins.hooks.zendesk_hook.Zendesk")
def test_returns_multiple_pages_if_get_all_pages_true(_):
    zendesk_hook = ZendeskHook("conn_id")
    mock_connection = Mock()
    mock_connection.host = "some_host"
    zendesk_hook.get_connection = Mock(return_value=mock_connection)
    zendesk_hook.get_conn()

    mock_conn = Mock()
    mock_call = Mock(
        return_value={'next_page': 'https://some_host/something', 'path': []})
    mock_conn.call = mock_call
    print(mock_conn.call)
    zendesk_hook.get_conn = Mock(return_value=mock_conn)
    zendesk_hook.call("path", get_all_pages=True)
    assert mock_call.call_count == 2


@patch("plugins.hooks.zendesk_hook.Zendesk")
def test_zdesk_is_inited_correctly(mock_zendesk):
    conn_mock = Mock()
    conn_mock.host = "conn_host"
    conn_mock.login = "conn_login"
    conn_mock.password = "conn_pass"

    zendesk_hook = ZendeskHook("conn_id")
    zendesk_hook.get_connection = Mock(return_value=conn_mock)
    zendesk_hook.get_conn()
    mock_zendesk.assert_called_with('https://conn_host', 'conn_login',
                                    'conn_pass', True)
