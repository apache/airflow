from io import SEEK_SET
from typing import IO


def truncate_file(fil: IO) -> None:
    fil.seek(0, SEEK_SET)
    fil.truncate(0)
