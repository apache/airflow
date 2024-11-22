from pydantic import HttpUrl, RootModel
from typing import Dict


class ExtraLinksResponse(RootModel):
    """Extra Links Response."""

    root: Dict[str, HttpUrl]
