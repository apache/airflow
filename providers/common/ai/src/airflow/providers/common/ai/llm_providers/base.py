from abc import ABC


class ModelProvider(ABC):
    """
    Base class for model providers.
    """

    def provider_name(self) -> str:
        """
        Returns the name of the provider.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def build_model(self, model_name: str, api_key: str, **kwargs) -> object:
        """
        Builds and returns a model instance based on the provided model name and parameters.
        """
        raise NotImplementedError("Subclasses must implement this method.")

