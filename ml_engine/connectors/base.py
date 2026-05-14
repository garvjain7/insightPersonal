from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class BaseConnector(ABC):
    """
    Every connector must implement validate() and fetch().

    validate() — cheap operation: test credentials, return available sources.
    fetch()    — heavier operation: pull data, return raw DataFrame.
    """

    @abstractmethod
    def validate(self, config: dict[str, Any]) -> dict:
        pass

    @abstractmethod
    def fetch(self, config: dict[str, Any], source: str) -> pd.DataFrame:
        pass

    @classmethod
    @abstractmethod
    def get_config_schema(cls) -> dict:
        """Returns a JSON schema or field list for the frontend to render the Auth form."""
        pass

    @property
    def connector_id(self) -> str:
        return self.__class__.__name__.lower().replace("connector", "")
