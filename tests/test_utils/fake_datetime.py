from datetime import datetime


class FakeDatetime(datetime):
    """
    A fake replacement for datetime that can be mocked for testing.
    """

    def __new__(cls, *args, **kwargs):
        return date.__new__(datetime, *args, **kwargs)
