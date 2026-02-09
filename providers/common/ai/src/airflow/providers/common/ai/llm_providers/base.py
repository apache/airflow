from abc import ABC, abstractmethod


class ModelProvider(ABC):
    """
    Base class for model providers.
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """
        Returns the name of the provider.
        """
        raise NotImplementedError

    @abstractmethod
    def build_model(self, model_name: str, api_key: str, **kwargs) -> object:
        """
        Builds and returns a model instance based on the provided model name and parameters.
        """
        raise NotImplementedError
